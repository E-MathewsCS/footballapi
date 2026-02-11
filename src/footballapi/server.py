from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from footballapi.service import LiveScoreService


class LiveScoreRequestHandler(BaseHTTPRequestHandler):
    server_version = "footballapi/0.1"
    score_service = LiveScoreService()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self._send_cors_headers()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/health", "/api/health"}:
            self._write_json(200, {"ok": True})
            return
        if parsed.path in {"/live-scores", "/api/live-scores"}:
            query = parse_qs(parsed.query)
            status = (query.get("status") or ["live"])[0]
            source = (query.get("source") or ["all"])[0]
            league = (query.get("league") or [None])[0]
            refresh_flag = (query.get("refresh") or ["0"])[0].lower()
            force_refresh = refresh_flag in {"1", "true", "yes", "on"}
            include_stale_flag = (query.get("include_stale") or ["0"])[0].lower()
            include_stale = include_stale_flag in {"1", "true", "yes", "on"}
            include_conflicts_flag = (query.get("include_conflicts") or ["0"])[0].lower()
            include_conflicts = include_conflicts_flag in {"1", "true", "yes", "on"}
            try:
                payload = self.score_service.get_scores(
                    status=status,
                    source=source,
                    league=league,
                    include_stale=include_stale,
                    include_conflicts=include_conflicts,
                    force_refresh=force_refresh,
                )
            except Exception as exc:
                self._write_json(500, {"ok": False, "error": str(exc)})
                return
            self._write_json(200, payload)
            return
        self._write_json(404, {"ok": False, "error": "Not found"})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _send_cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _write_json(self, status_code: int, payload: dict) -> None:
        encoded = json.dumps(payload, ensure_ascii=True).encode("utf-8")
        self.send_response(status_code)
        self._send_cors_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve live football scores over HTTP.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--cache-seconds", type=int, default=10)
    return parser


def main() -> None:
    args = build_arg_parser().parse_args()
    LiveScoreRequestHandler.score_service = LiveScoreService(cache_seconds=args.cache_seconds)
    server = ThreadingHTTPServer((args.host, args.port), LiveScoreRequestHandler)
    print(f"footballapi listening on http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
