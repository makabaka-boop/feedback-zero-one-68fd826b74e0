#!/usr/bin/env python3
"""HTTP front-end for the mini data warehouse (standard library only).

Endpoints:
    POST /init          one-time initialization: model + initial load
    POST /migrate       migrate to a complete new model version
    GET  /model         current version + model
    GET  /data/<table>  rows of one table, column order, pk ascending
    GET  /migrations    applied migration records (retry/idempotency log)
    GET  /health        liveness probe

All requests are serialized through one lock, so concurrent requests are
equivalent to some serial execution; a read always sees a complete version.
"""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlparse

from warehouse import Reject, Warehouse


def make_handler(wh, lock):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"
        server_version = "minidw/1.0"

        def _send(self, status, obj):
            body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _read_json(self):
            length = self.headers.get("Content-Length")
            raw = b""
            if length is not None:
                try:
                    raw = self.rfile.read(int(length))
                except ValueError:
                    raise Reject(400, "bad_request", "invalid Content-Length")
            if not raw:
                return None
            try:
                return json.loads(raw)
            except json.JSONDecodeError as exc:
                raise Reject(400, "bad_request",
                             f"request body is not valid JSON: {exc}")

        def _dispatch(self, method):
            path = urlparse(self.path).path
            try:
                if method == "GET":
                    if path == "/health":
                        return self._send(200, {"ok": True})
                    if path == "/":
                        return self._send(200, {
                            "ok": True, "service": "minidw",
                            "endpoints": ["POST /init", "POST /migrate",
                                          "GET /model", "GET /data/<table>",
                                          "GET /migrations", "GET /health"]})
                    if path == "/model":
                        return self._send(200, wh.get_model())
                    if path == "/migrations":
                        return self._send(200, wh.list_migrations())
                    if path.startswith("/data/"):
                        return self._send(200, wh.query(unquote(path[len("/data/"):])))
                    raise Reject(404, "not_found", f"unknown endpoint {path}")
                body = self._read_json()
                if path == "/init":
                    return self._send(200, wh.init(body))
                if path == "/migrate":
                    return self._send(200, wh.migrate(body))
                raise Reject(404, "not_found", f"unknown endpoint {path}")
            except Reject as r:
                self._send(r.status, {"ok": False, "error": {
                    "code": r.code, "message": r.message}})
            except Exception as exc:  # never leak a stack trace to the client
                self._send(500, {"ok": False, "error": {
                    "code": "internal", "message": str(exc)}})

        def do_GET(self):
            with lock:
                self._dispatch("GET")

        def do_POST(self):
            with lock:
                self._dispatch("POST")

        def _method_not_allowed(self):
            self._send(405, {"ok": False, "error": {
                "code": "method_not_allowed", "message": "method not allowed"}})

        do_PUT = _method_not_allowed
        do_DELETE = _method_not_allowed
        do_PATCH = _method_not_allowed

        def log_message(self, *args):
            pass

    return Handler


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="mini data warehouse (Python + SQLite, standard library only)")
    parser.add_argument("--db", default="warehouse.db",
                        help="SQLite database file (default: warehouse.db)")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args(argv)

    wh = Warehouse(args.db)
    lock = threading.RLock()  # serializes every request
    server = ThreadingHTTPServer((args.host, args.port), make_handler(wh, lock))
    print(f"minidw serving on http://{args.host}:{args.port} (db={args.db})",
          flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
