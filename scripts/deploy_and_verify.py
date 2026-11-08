#!/usr/bin/env python3
"""End-to-end Dynatrace Extension 2.0 uploader, activator, and Grail DQL verifier.

Reads a Dynatrace Platform Token (`dt0s16.*`) from `--token-file` (supports `.rtf`
or plain text) or `DT_PLATFORM_TOKEN`, never echoing or logging secret values.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

EXTENSION_NAME = "custom:apache-airflow"
EXTENSION_VERSION = "1.0.0"


def load_platform_token(token_file: Path | None, cli_env_url: str | None = None) -> tuple[str, str]:
    env_url = (cli_env_url or os.environ.get("DT_ENV_URL", "")).strip().rstrip("/")
    tok = os.environ.get("DT_PLATFORM_TOKEN", "").strip()
    if token_file and token_file.exists():
        raw = token_file.read_text(encoding="utf-8", errors="ignore")
        m_tok = re.search(r"dt0s16\.[A-Za-z0-9]+\.[A-Za-z0-9]+", raw)
        if m_tok and not tok:
            tok = m_tok.group(0)
        m_url = re.search(r"https://[a-z0-9-]+\.apps\.dynatrace\.com", raw)
        if m_url and not env_url:
            env_url = m_url.group(0).rstrip("/")
    if not env_url:
        raise SystemExit("ERROR: Set --env-url or DT_ENV_URL=https://<ENV_ID>.apps.dynatrace.com")
    if not tok:
        raise SystemExit("ERROR: No dt0s16.* Platform Token found in --token-file or DT_PLATFORM_TOKEN")
    return env_url, tok


def api_json(
    method: str,
    url: str,
    token: str,
    payload: dict | list | None = None,
) -> tuple[int, dict]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }
    if data is not None:
        headers["Content-Type"] = "application/json; charset=utf-8"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body else {})
    except urllib.error.HTTPError as err:
        body = err.read().decode("utf-8", errors="replace")
        try:
            parsed = json.loads(body)
        except Exception:
            parsed = {"raw": body}
        return err.code, parsed


def ensure_ca_in_vault(env_url: str, token: str, ca_pem: Path) -> None:
    if not ca_pem.exists():
        return
    cert_b64 = base64.b64encode(ca_pem.read_bytes()).decode("ascii")
    url = f"{env_url}/platform/classic/environment-api/v2/credentials"
    status, resp = api_json(
        "POST",
        url,
        token,
        {
            "name": "Apache Airflow Extension Root CA",
            "description": "Root CA for custom:apache-airflow Extension 2.0 bundle validation",
            "type": "PUBLIC_CERTIFICATE",
            "scopes": ["EXTENSION"],
            "ownerAccessOnly": False,
            "certificate": cert_b64,
            "certificateFormat": "PEM",
        },
    )
    if status in (200, 201):
        print(f"[OK] Uploaded Root CA ({ca_pem}) to Dynatrace Credential Vault: {resp.get('id')}")
    elif status == 403:
        print(
            "[INFO] Token lacks `environment-api:credentials:write`; assuming Root CA "
            f"({ca_pem}) was uploaded via Dynatrace Credential Vault UI."
        )
    else:
        print(f"[INFO] Credential Vault CA upload returned HTTP {status}: {resp}")


def upload_and_activate_bundle(dt_sdk_bin: Path, zip_path: Path, env_url: str, token: str) -> None:
    cmd = [
        str(dt_sdk_bin),
        "upload",
        str(zip_path),
        "-u",
        env_url,
        "-p",
        token,
        "--active",
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    safe_out = (res.stdout + "\n" + res.stderr).replace(token, "[REDACTED]").strip()
    print(safe_out)
    if res.returncode != 0:
        raise SystemExit(
            f"\nUpload blocked (exit {res.returncode}). Ensure `{ca_pem_hint(zip_path)}` is added in "
            "Dynatrace -> Credential Vault -> Add credential -> Public Certificate (scope: Extension validation), "
            "or add `environment-api:credentials:write` scope to the Platform Token."
        )


def ca_pem_hint(zip_path: Path) -> Path:
    return zip_path.resolve().parent.parent / "certs" / "ca.pem"


def run_dql(env_url: str, token: str, query: str) -> list[dict]:
    status, resp = api_json(
        "POST",
        f"{env_url}/platform/storage/query/v1/query:execute",
        token,
        {"query": query, "requestTimeoutMilliseconds": 15000, "maxResultRecords": 25},
    )
    if status == 202 and resp.get("requestToken"):
        req_tok = resp["requestToken"]
        for _ in range(10):
            time.sleep(1.0)
            p_status, p_resp = api_json(
                "GET",
                f"{env_url}/platform/storage/query/v1/query:poll?request-token={req_tok}",
                token,
            )
            if p_status == 200 and p_resp.get("state") == "SUCCEEDED":
                return (p_resp.get("result") or {}).get("records") or []
    if status == 200 and resp.get("state") == "SUCCEEDED":
        return (resp.get("result") or {}).get("records") or []
    print(f"[WARN] DQL query ({query}) returned HTTP {status}: {resp}")
    return []


def verify_grail(env_url: str, token: str) -> None:
    queries = {
        "Airflow API Availability (Metrics)": (
            "timeseries up = avg(airflow.api.up), by: {airflow.instance.name}, from: now() - 30m"
        ),
        "Smartscape Airflow Instances": "fetch `dt.entity.airflow:instance` | fields id, entity.name",
        "Smartscape Airflow DAGs": "fetch `dt.entity.airflow:dag` | fields id, entity.name | limit 10",
        "Airflow Completion & Audit Logs": (
            'fetch logs, from: now() - 30m | filter matchesValue(event.name, "airflow.*") '
            "| fields timestamp, event.name, airflow.instance.name, airflow.dag.id, content | limit 10"
        ),
    }
    for label, dql in queries.items():
        records = run_dql(env_url, token, dql)
        print(f"=== {label}: {len(records)} record(s) ===")
        for rec in records[:5]:
            print(" ", json.dumps(rec))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-url", default=None, help="Dynatrace Platform URL, e.g. https://<ENV_ID>.apps.dynatrace.com"
    )
    parser.add_argument("--token-file", type=Path, default=None, help="Path to file containing dt0s16.* Platform Token")
    parser.add_argument(
        "--zip",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "dist" / "custom_apache-airflow-1.0.0.zip",
    )
    parser.add_argument("--verify-only", action="store_true", help="Run Grail DQL verification queries only")
    args = parser.parse_args()

    env_url, token = load_platform_token(args.token_file, args.env_url)
    root_dir = Path(__file__).resolve().parent.parent
    dt_sdk_bin = root_dir / ".venv" / "bin" / "dt-sdk"

    print(f"Target Dynatrace Environment: {env_url}")
    if not args.verify_only:
        ensure_ca_in_vault(env_url, token, root_dir / "certs" / "ca.pem")
        upload_and_activate_bundle(dt_sdk_bin, args.zip, env_url, token)

    verify_grail(env_url, token)


if __name__ == "__main__":
    sys.exit(main())
