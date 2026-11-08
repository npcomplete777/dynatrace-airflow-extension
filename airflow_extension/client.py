"""Dual-version Apache Airflow REST API client (v1 Basic & v2 JWT /auth/token)."""

from __future__ import annotations

import base64
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import requests

DEFAULT_PAGE_SIZE = 100
DEFAULT_MAX_PAGES = 50


class AirflowAPIError(RuntimeError):
    """Sanitized Airflow HTTP error that never embeds credentials or raw payloads."""

    def __init__(self, method: str, path: str, status_code: int, message: str = "") -> None:
        self.method = method
        self.path = path
        self.status_code = status_code
        suffix = f" ({message})" if message else ""
        super().__init__(f"Airflow API {method} {path} returned HTTP {status_code}{suffix}")


class BoundedSet:
    """FIFO/LRU-bounded string set used to deduplicate watermark boundary items."""

    def __init__(self, capacity: int = 32768) -> None:
        self._cap = max(capacity, 64)
        self._items: OrderedDict[str, None] = OrderedDict()

    def seen(self, key: str) -> bool:
        if key in self._items:
            self._items.move_to_end(key)
            return True
        return False

    def add(self, key: str) -> None:
        if key in self._items:
            self._items.move_to_end(key)
            return
        if len(self._items) >= self._cap:
            self._items.popitem(last=False)
        self._items[key] = None


def parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def format_iso(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def decode_jwt_exp(token: str) -> float | None:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode("ascii")).decode("utf-8"))
        exp = payload.get("exp")
        return float(exp) if isinstance(exp, int | float) else None
    except Exception:
        return None


@dataclass
class EndpointConfig:
    instance_name: str
    url: str
    api_version: str = "auto"
    auth_mode: str = "auto"
    username: str = ""
    password: str = ""
    verify_ssl: bool = True
    collection_interval_seconds: int = 30
    initial_lookback_minutes: int = 15
    emit_completion_logs: bool = True
    emit_audit_logs: bool = False
    emit_davis_events: bool = True
    page_size: int = DEFAULT_PAGE_SIZE
    max_pages: int = DEFAULT_MAX_PAGES
    timeout_seconds: float = 20.0

    @property
    def host_and_port(self) -> tuple[str, str]:
        parsed = urlparse(self.url)
        host = parsed.hostname or "localhost"
        if parsed.port:
            return host, str(parsed.port)
        return host, ("443" if parsed.scheme == "https" else "80")


class AirflowClient:
    """Safe, bounded client supporting Apache Airflow 2.10+/2.11 (v1) and 3.x (v2)."""

    def __init__(self, cfg: EndpointConfig, session: requests.Session | None = None) -> None:
        self.cfg = cfg
        self.base_url = cfg.url.rstrip("/")
        self.session = session or requests.Session()
        self._detected_version: str | None = cfg.api_version if cfg.api_version in ("v1", "v2") else None
        self._jwt_token: str | None = None
        self._jwt_expires_at: float = 0.0

    def close(self) -> None:
        self.session.close()

    def api_version(self) -> str:
        if self._detected_version in ("v1", "v2"):
            return self._detected_version

        # Probe Airflow 3 unauthenticated health endpoint first, then Airflow 2.
        probe_v2 = self._raw_request("GET", "/api/v2/monitor/health", authenticated=False)
        if probe_v2.status_code in (200, 401, 403):
            self._detected_version = "v2"
            return "v2"
        if probe_v2.status_code != 404:
            raise AirflowAPIError("GET", "/api/v2/monitor/health", probe_v2.status_code)

        probe_v1 = self._raw_request("GET", "/api/v1/health", authenticated=False)
        if probe_v1.status_code in (200, 401, 403):
            self._detected_version = "v1"
            return "v1"
        raise AirflowAPIError("GET", "/api/v1/health", probe_v1.status_code)

    def _effective_auth_mode(self, version: str) -> str:
        mode = (self.cfg.auth_mode or "auto").lower().strip()
        if mode in ("basic", "token", "none"):
            return mode
        if not self.cfg.username and not self.cfg.password:
            return "none"
        return "token" if version == "v2" else "basic"

    def _bearer_token(self) -> str:
        # Static bearer token when username is omitted and password holds the token
        if not self.cfg.username and self.cfg.password:
            return self.cfg.password

        now = time.time()
        if self._jwt_token and now + 60.0 < self._jwt_expires_at:
            return self._jwt_token

        resp = self._raw_request(
            "POST",
            "/auth/token",
            authenticated=False,
            json_body={"username": self.cfg.username, "password": self.cfg.password},
        )
        if resp.status_code < 200 or resp.status_code >= 300:
            raise AirflowAPIError("POST", "/auth/token", resp.status_code, "JWT token exchange failed")

        data = resp.json()
        token = data.get("access_token")
        if not token or not isinstance(token, str):
            raise AirflowAPIError("POST", "/auth/token", resp.status_code, "missing access_token")

        exp = decode_jwt_exp(token)
        self._jwt_token = token
        self._jwt_expires_at = exp if exp is not None else (now + 900.0)
        return token

    def _raw_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> requests.Response:
        headers = {"Accept": "application/json"}
        auth: tuple[str, str] | None = None

        if authenticated:
            ver = self.api_version()
            mode = self._effective_auth_mode(ver)
            if mode == "basic":
                auth = (self.cfg.username, self.cfg.password)
            elif mode == "token":
                headers["Authorization"] = f"Bearer {self._bearer_token()}"

        try:
            return self.session.request(
                method=method,
                url=f"{self.base_url}{path}",
                params=params,
                json=json_body,
                headers=headers,
                auth=auth,
                verify=self.cfg.verify_ssl,
                timeout=self.cfg.timeout_seconds,
            )
        except requests.RequestException as exc:
            # Redact password if any underlying transport exception echoed URL/headers
            msg = str(exc)
            if self.cfg.password:
                msg = msg.replace(self.cfg.password, "[REDACTED]")
            raise RuntimeError(f"Airflow request {method} {path} failed: {msg}") from None

    def _request_json(
        self,
        method: str,
        subpath: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        ver = self.api_version()
        prefix = "/api/v2" if ver == "v2" else "/api/v1"
        full_path = f"{prefix}{subpath}"

        resp = self._raw_request(method, full_path, params=params, json_body=json_body, authenticated=authenticated)
        # Refresh JWT once on 401/403 (e.g. expiry skew or jwt_secret rotation)
        if (
            authenticated
            and resp.status_code in (401, 403)
            and self._effective_auth_mode(ver) == "token"
            and self.cfg.username
        ):
            self._jwt_token = None
            self._jwt_expires_at = 0.0
            resp = self._raw_request(method, full_path, params=params, json_body=json_body, authenticated=True)

        if resp.status_code < 200 or resp.status_code >= 300:
            raise AirflowAPIError(method, full_path, resp.status_code)
        return resp.json()

    def _paginate_get(
        self, subpath: str, collection_key: str, base_params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        limit = max(1, min(self.cfg.page_size, 100))
        offset = 0
        for _ in range(self.cfg.max_pages):
            params = dict(base_params or {})
            params["limit"] = limit
            params["offset"] = offset
            payload = self._request_json("GET", subpath, params=params)
            batch = payload.get(collection_key) or []
            items.extend(batch)
            total = int(payload.get("total_entries", len(items)))
            if not batch or len(items) >= total or len(batch) < limit:
                break
            offset += len(batch)
        return items

    def get_version(self) -> str:
        data = self._request_json("GET", "/version")
        return str(data.get("version") or "")

    def get_health(self) -> dict[str, Any]:
        ver = self.api_version()
        path = "/monitor/health" if ver == "v2" else "/health"
        return self._request_json("GET", path, authenticated=False)

    def list_dags(self) -> list[dict[str, Any]]:
        ver = self.api_version()
        params = {"only_active": "false"} if ver == "v1" else {}
        return self._paginate_get("/dags", "dags", params)

    def count_import_errors(self) -> int:
        data = self._request_json("GET", "/importErrors", params={"limit": 1, "offset": 0})
        return int(data.get("total_entries", len(data.get("import_errors") or [])))

    def list_pools(self) -> list[dict[str, Any]]:
        return self._paginate_get("/pools", "pools")

    def list_alive_jobs(self) -> list[dict[str, Any]] | None:
        if self.api_version() != "v2":
            return None
        return self._paginate_get("/jobs", "jobs", {"is_alive": "true"})

    def list_dag_runs(
        self,
        *,
        states: list[str],
        end_date_gte: datetime | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"state": states}
        if end_date_gte is not None:
            params["end_date_gte"] = format_iso(end_date_gte - timedelta(seconds=1))
        return self._paginate_get("/dags/~/dagRuns", "dag_runs", params)

    def list_task_instances(
        self,
        *,
        states: list[str],
        end_date_gte: datetime | None = None,
    ) -> list[dict[str, Any]]:
        ver = self.api_version()
        if ver == "v2":
            params: dict[str, Any] = {"state": states}
            if end_date_gte is not None:
                params["end_date_gte"] = format_iso(end_date_gte - timedelta(seconds=1))
            return self._paginate_get("/dags/~/dagRuns/~/taskInstances", "task_instances", params)

        # Airflow 2 v1 requires POST /dags/~/dagRuns/taskInstances/list for cross-DAG filtering
        items: list[dict[str, Any]] = []
        limit = max(1, min(self.cfg.page_size, 100))
        offset = 0
        for _ in range(self.cfg.max_pages):
            body: dict[str, Any] = {
                "state": states,
                "page_limit": limit,
                "page_offset": offset,
            }
            if end_date_gte is not None:
                body["end_date_gte"] = format_iso(end_date_gte - timedelta(seconds=1))
            payload = self._request_json("POST", "/dags/~/dagRuns/~/taskInstances/list", json_body=body)
            batch = payload.get("task_instances") or []
            items.extend(batch)
            total = int(payload.get("total_entries", len(items)))
            if not batch or len(items) >= total or len(batch) < limit:
                break
            offset += len(batch)
        return items

    def list_event_logs(self, after: datetime) -> list[dict[str, Any]]:
        ver = self.api_version()
        param_key = "after" if ver == "v2" else "after"
        params: dict[str, Any] = {param_key: format_iso(after), "order_by": "when" if ver == "v1" else "dttm"}
        if ver == "v1":
            # Airflow 2 /api/v1/eventLogs uses `after` on 2.10+; fall back cleanly if unsupported
            params = {"after": format_iso(after)}
        return self._paginate_get("/eventLogs", "event_logs", params)
