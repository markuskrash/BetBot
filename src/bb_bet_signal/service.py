from __future__ import annotations

import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Lock, Thread
from typing import Any

from .engine import ProbabilityModel
from .models import Recommendation
from .providers import RealtimeFeed


class SignalService:
    def __init__(self, feed: RealtimeFeed, engine: ProbabilityModel) -> None:
        self.feed = feed
        self.engine = engine
        self._latest: list[Recommendation] = []
        self._lock = Lock()

    async def run(self) -> None:
        async for snapshot in self.feed.stream():
            recommendations = self.engine.evaluate(snapshot)
            with self._lock:
                by_event = {item.event_id: item for item in self._latest}
                for recommendation in recommendations:
                    current = by_event.get(recommendation.event_id)
                    if current is None or recommendation.expected_value > current.expected_value:
                        by_event[recommendation.event_id] = recommendation
                self._latest = sorted(by_event.values(), key=lambda item: item.expected_value, reverse=True)

    def latest(self) -> list[Recommendation]:
        with self._lock:
            return list(self._latest)


class ApiServer:
    def __init__(self, service: SignalService, host: str, port: int) -> None:
        self.service = service
        self.host = host
        self.port = port
        self.httpd = ThreadingHTTPServer((host, port), self._handler_factory())
        self.thread = Thread(target=self.httpd.serve_forever, daemon=True)

    def _handler_factory(self):
        service = self.service

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                if self.path == "/health":
                    self._write_json({"status": "ok"})
                    return
                if self.path == "/recommendations":
                    data = service.latest()
                    payload = [item.to_dict() for item in data]
                    self._write_json(payload)
                    return
                if self.path == "/expresses" and hasattr(service, "latest_expresses"):
                    data = service.latest_expresses()
                    payload = [item.to_dict() for item in data]
                    self._write_json(payload)
                    return
                if self.path == "/risk-state" and hasattr(service, "risk_state"):
                    payload = service.risk_state()
                    self._write_json(payload)
                    return
                if self.path == "/performance" and hasattr(service, "performance"):
                    payload = service.performance()
                    self._write_json(payload)
                    return
                self.send_error(HTTPStatus.NOT_FOUND, "Not found")

            def log_message(self, format: str, *args: Any) -> None:
                return

            def _write_json(self, payload: Any) -> None:
                body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.httpd.shutdown()
        self.thread.join(timeout=2)
