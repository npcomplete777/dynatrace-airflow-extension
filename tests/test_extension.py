"""Unit tests for the Apache Airflow Dynatrace Extension 2.0."""

from __future__ import annotations

import base64
import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from airflow_extension.client import AirflowClient, BoundedSet, EndpointConfig, decode_jwt_exp, parse_iso


def _make_jwt(exp: int) -> str:
    hdr = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').decode().rstrip("=")
    body = base64.urlsafe_b64encode(json.dumps({"exp": exp}).encode()).decode().rstrip("=")
    return f"{hdr}.{body}.sig"


class _MockAirflowV2Handler(BaseHTTPRequestHandler):
    token_calls = 0

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return

    def _send(self, status: int, payload: dict) -> None:
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/auth/token":
            type(self).token_calls += 1
            length = int(self.headers.get("Content-Length", "0"))
            body = json.loads(self.rfile.read(length).decode())
            if body.get("username") == "monitor" and body.get("password") == "s3cr3t":
                self._send(201, {"access_token": _make_jwt(int(time.time()) + 600)})
            else:
                self._send(401, {"detail": "bad credentials"})
            return
        self._send(404, {})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/v2/monitor/health":
            self._send(
                200,
                {
                    "metadatabase": {"status": "healthy"},
                    "scheduler": {
                        "status": "healthy",
                        "latest_scheduler_heartbeat": "2026-11-08T15:00:00Z",
                    },
                },
            )
            return

        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            self._send(401, {"detail": "missing bearer"})
            return

        if parsed.path == "/api/v2/version":
            self._send(200, {"version": "3.4.0"})
        elif parsed.path == "/api/v2/dags":
            self._send(
                200,
                {
                    "dags": [
                        {
                            "dag_id": "payments_ledger",
                            "is_paused": False,
                            "is_stale": False,
                            "last_parse_duration": 0.14,
                            "file_token": "tok1",
                        }
                    ],
                    "total_entries": 1,
                },
            )
        elif parsed.path == "/api/v2/importErrors":
            self._send(200, {"import_errors": [], "total_entries": 2})
        elif parsed.path == "/api/v2/pools":
            self._send(
                200,
                {
                    "pools": [
                        {
                            "name": "default_pool",
                            "slots": 128,
                            "open_slots": 96,
                            "running_slots": 24,
                            "queued_slots": 8,
                            "scheduled_slots": 0,
                            "deferred_slots": 0,
                        }
                    ],
                    "total_entries": 1,
                },
            )
        elif parsed.path == "/api/v2/jobs":
            self._send(200, {"jobs": [{"job_type": "SchedulerJob"}], "total_entries": 1})
        elif parsed.path == "/api/v2/dags/~/dagRuns":
            self._send(200, {"dag_runs": [], "total_entries": 0})
        elif parsed.path == "/api/v2/dags/~/dagRuns/~/taskInstances":
            self._send(200, {"task_instances": [], "total_entries": 0})
        else:
            self._send(404, {})


class TestAirflowExtensionClient(unittest.TestCase):
    def test_bounded_set_evicts_oldest(self) -> None:
        bs = BoundedSet(capacity=64)
        for i in range(80):
            bs.add(f"k-{i}")
        self.assertFalse(bs.seen("k-0"))
        self.assertTrue(bs.seen("k-79"))

    def test_jwt_decode_and_iso_parse(self) -> None:
        tok = _make_jwt(1900000000)
        self.assertEqual(decode_jwt_exp(tok), 1900000000.0)
        dt = parse_iso("2026-11-08T15:00:00Z")
        self.assertIsNotNone(dt)

    def test_v2_auto_detection_and_jwt_caching(self) -> None:
        _MockAirflowV2Handler.token_calls = 0
        srv = HTTPServer(("127.0.0.1", 0), _MockAirflowV2Handler)
        port = srv.server_address[1]
        thread = threading.Thread(target=srv.serve_forever, daemon=True)
        thread.start()
        try:
            cfg = EndpointConfig(
                instance_name="unit-af3",
                url=f"http://127.0.0.1:{port}",
                username="monitor",
                password="s3cr3t",
            )
            client = AirflowClient(cfg)
            try:
                self.assertEqual(client.api_version(), "v2")
                self.assertEqual(client.get_version(), "3.4.0")
                self.assertEqual(len(client.list_dags()), 1)
                self.assertEqual(client.count_import_errors(), 2)
                self.assertEqual(len(client.list_pools()), 1)
                self.assertEqual(len(client.list_alive_jobs() or []), 1)
                # Token must be cached across all authenticated requests above
                self.assertEqual(_MockAirflowV2Handler.token_calls, 1)
            finally:
                client.close()
        finally:
            srv.shutdown()
            srv.server_close()


if __name__ == "__main__":
    unittest.main()
