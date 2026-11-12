#!/usr/bin/env python3
"""Black-box acceptance tests for the mini data warehouse.

Spawns real server processes on temporary databases and exercises the HTTP
API: the spec's rename example, swapping table/column names, circular and
self foreign keys, migration semantics, rollback on every rejection path,
idempotent retries, and crash recovery before/after commit.

Run:  python3 tests/acceptance.py
"""

import http.client
import json
import os
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER = os.path.join(ROOT, "server.py")
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1

passed = 0
failed = 0


def check(cond, label, detail=""):
    global passed, failed
    if cond:
        passed += 1
        print(f"  ok    {label}")
    else:
        failed += 1
        print(f"  FAIL  {label}  {detail}")


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    """A server process on its own database file."""

    def __init__(self, db, env=None):
        self.db = db
        self.port = free_port()
        full_env = dict(os.environ)
        full_env.update(env or {})
        self.proc = subprocess.Popen(
            [sys.executable, SERVER, "--db", db, "--port", str(self.port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=full_env)
        self.wait_ready()

    def wait_ready(self, timeout=15):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"server exited early: {self.proc.returncode}")
            try:
                status, _ = self.request("GET", "/health")
                if status == 200:
                    return
            except OSError:
                time.sleep(0.05)
        raise RuntimeError("server did not become ready")

    def request(self, method, path, body=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json"} if data is not None else {}
        conn.request(method, path, body=data, headers=headers)
        resp = conn.getresponse()
        raw = resp.read()
        conn.close()
        return resp.status, json.loads(raw) if raw else None

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=15)


def err_code(body):
    return (body or {}).get("error", {}).get("code")


def norm(model):
    """Expand a model to the normalized form returned by GET /model."""
    return {"tables": [
        {"id": t["id"], "name": t["name"], "columns": [
            {"id": c["id"], "name": c["name"], "type": c["type"],
             "primary_key": c.get("primary_key", False), "ref": c.get("ref")}
            for c in t["columns"]]}
        for t in model["tables"]]}


# ---------------------------------------------------------------- scenarios

def scenario_spec_example():
    print("scenario: spec example -- rename table a->b and column x->y")
    with tempfile.TemporaryDirectory() as d:
        srv = Server(os.path.join(d, "w.db"))
        try:
            model = {"tables": [{"id": "t", "name": "a", "columns": [
                {"id": "k", "name": "x", "type": "integer", "primary_key": True}]}]}
            s, b = srv.request("POST", "/init", {"model": model, "data": {"t": [[1]]}})
            check(s == 200 and b["version"] == 1, "init returns version 1", f"{s} {b}")

            new_model = {"tables": [{"id": "t", "name": "b", "columns": [
                {"id": "k", "name": "y", "type": "integer", "primary_key": True}]}]}
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m1", "version": 1, "model": new_model})
            check(s == 200 and b["version"] == 2 and b["replayed"] is False,
                  "rename migration succeeds, version becomes 2", f"{s} {b}")

            s, b = srv.request("GET", "/data/b")
            check(s == 200 and b["columns"] == ["y"] and b["rows"] == [[1]],
                  "querying renamed table b yields [[1]]", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 404 and err_code(b) == "not_found",
                  "old table name a is gone", f"{s} {b}")
        finally:
            srv.stop()


def scenario_swap_names():
    print("scenario: swap table names and column names (identity follows ids)")
    with tempfile.TemporaryDirectory() as d:
        srv = Server(os.path.join(d, "w.db"))
        try:
            model = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "v", "type": "string"}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "w", "type": "string"}]}]}
            s, b = srv.request("POST", "/init",
                               {"model": model,
                                "data": {"t1": [[1, "one"]], "t2": [[2, "two"]]}})
            check(s == 200, "init with two tables", f"{s} {b}")

            # swap table names (t1<->t2) and swap column names inside each table
            swapped = {"tables": [
                {"id": "t1", "name": "b", "columns": [
                    {"id": "c1", "name": "v", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "x", "type": "string"}]},
                {"id": "t2", "name": "a", "columns": [
                    {"id": "c3", "name": "w", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "y", "type": "string"}]}]}
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "swap", "version": 1, "model": swapped})
            check(s == 200 and b["version"] == 2, "name swap migration succeeds", f"{s} {b}")

            s, b = srv.request("GET", "/data/b")
            check(s == 200 and b["columns"] == ["v", "x"] and b["rows"] == [[1, "one"]],
                  "table id t1 (now named b) kept its rows", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["columns"] == ["w", "y"] and b["rows"] == [[2, "two"]],
                  "table id t2 (now named a) kept its rows", f"{s} {b}")
            s, b = srv.request("GET", "/model")
            check(s == 200 and b["model"] == norm(swapped) and b["version"] == 2,
                  "GET /model reflects the swapped model", f"{s} {b}")
        finally:
            srv.stop()


def scenario_circular_refs():
    print("scenario: circular and self-referencing foreign keys")
    with tempfile.TemporaryDirectory() as d:
        srv = Server(os.path.join(d, "w.db"))
        try:
            model = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "f", "type": "integer", "ref": "t2"}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "g", "type": "integer", "ref": "t1"}]},
                {"id": "t3", "name": "c", "columns": [
                    {"id": "c5", "name": "i", "type": "integer", "primary_key": True},
                    {"id": "c6", "name": "p", "type": "integer", "ref": "t3"}]}]}
            data = {"t1": [[1, 10], [2, 20]],
                    "t2": [[10, 1], [20, 2]],
                    "t3": [[1, None], [2, 1], [3, 2]]}
            s, b = srv.request("POST", "/init", {"model": model, "data": data})
            check(s == 200, "init loads a reference cycle a<->b and self-ref c", f"{s} {b}")

            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[1, 10], [2, 20]], "table a rows", f"{s} {b}")
            s, b = srv.request("GET", "/data/b")
            check(s == 200 and b["rows"] == [[10, 1], [20, 2]], "table b rows", f"{s} {b}")
            s, b = srv.request("GET", "/data/c")
            check(s == 200 and b["rows"] == [[1, None], [2, 1], [3, 2]],
                  "self-referencing table c rows (null fk allowed)", f"{s} {b}")

            # migrate: add a string column to a and an fk column into the cycle
            new_model = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "f", "type": "integer", "ref": "t2"},
                    {"id": "c7", "name": "s", "type": "string"}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "g", "type": "integer", "ref": "t1"},
                    {"id": "c8", "name": "q", "type": "integer", "ref": "t3"}]},
                {"id": "t3", "name": "c", "columns": [
                    {"id": "c5", "name": "i", "type": "integer", "primary_key": True},
                    {"id": "c6", "name": "p", "type": "integer", "ref": "t3"}]}]}
            body = {"migration_id": "grow", "version": 1, "model": new_model,
                    "defaults": {"t1": {"c7": "z"}, "t2": {"c8": 2}}}
            s, b = srv.request("POST", "/migrate", body)
            check(s == 200 and b["version"] == 2,
                  "migration adding columns into the cycle succeeds", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[1, 10, "z"], [2, 20, "z"]],
                  "a gained column s with default 'z'", f"{s} {b}")
            s, b = srv.request("GET", "/data/b")
            check(s == 200 and b["rows"] == [[10, 1, 2], [20, 2, 2]],
                  "b gained fk column q defaulting to existing key 2", f"{s} {b}")

            # deleting t2 while a.f references it: the target model itself is
            # invalid (a ref to a table that no longer exists) -> rejected
            drop_t2 = {"tables": [new_model["tables"][0], new_model["tables"][2]]}
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "bad", "version": 2, "model": drop_t2})
            check(s == 400 and err_code(b) == "validation",
                  "deleting referenced table t2 is rejected", f"{s} {b}")

            # retargeting a.f from t2 to t3: the model is valid but the
            # existing values 10/20 do not exist in t3.i -> rebuild would
            # dangle, so the whole migration must be rejected
            retarget = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "f", "type": "integer", "ref": "t3"},
                    {"id": "c7", "name": "s", "type": "string"}]},
                new_model["tables"][1], new_model["tables"][2]]}
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "bad2", "version": 2, "model": retarget})
            check(s == 400 and err_code(b) == "fk_violation",
                  "rebuild leaving dangling references is rejected", f"{s} {b}")
            s, b = srv.request("GET", "/model")
            check(s == 200 and b["version"] == 2, "version unchanged after rejection",
                  f"{s} {b}")
            s, b = srv.request("GET", "/data/b")
            check(s == 200 and b["rows"] == [[10, 1, 2], [20, 2, 2]],
                  "data unchanged after rejection", f"{s} {b}")
        finally:
            srv.stop()


def scenario_migration_semantics():
    print("scenario: add/drop columns and tables, defaults, ordering, int64")
    with tempfile.TemporaryDirectory() as d:
        srv = Server(os.path.join(d, "w.db"))
        try:
            model = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "v", "type": "string"}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "w", "type": "integer", "ref": "t1"}]}]}
            data = {"t1": [[2, "two"], [1, "one"],
                           [INT64_MIN, "min"], [INT64_MAX, "max"]],
                    "t2": [[10, 1], [20, 2]]}
            s, b = srv.request("POST", "/init", {"model": model, "data": data})
            check(s == 200, "init with int64 boundaries", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[INT64_MIN, "min"], [1, "one"],
                                             [2, "two"], [INT64_MAX, "max"]],
                  "rows come back ordered by primary key ascending", f"{s} {b}")

            # add column with default, drop a column, keep the referencing table
            m1 = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c5", "name": "n", "type": "integer"}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "w", "type": "integer", "ref": "t1"}]},
                {"id": "t3", "name": "c", "columns": [
                    {"id": "c6", "name": "i", "type": "integer", "primary_key": True},
                    {"id": "c7", "name": "s", "type": "string"}]}]}
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m1", "version": 1, "model": m1,
                                "defaults": {"t1": {"c5": 42}}})
            check(s == 200 and b["version"] == 2, "add/drop column migration", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["columns"] == ["x", "n"] and
                  b["rows"] == [[INT64_MIN, 42], [1, 42], [2, 42], [INT64_MAX, 42]],
                  "dropped column v is gone, new column n filled with 42", f"{s} {b}")
            s, b = srv.request("GET", "/data/c")
            check(s == 200 and b["rows"] == [], "new table c starts empty", f"{s} {b}")
            s, b = srv.request("GET", "/data/b")
            check(s == 200 and b["rows"] == [[10, 1], [20, 2]],
                  "referencing table b untouched", f"{s} {b}")

            # delete table t2 (nothing references it), add nullable fk to empty t3
            m2 = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c5", "name": "n", "type": "integer"},
                    {"id": "c8", "name": "r", "type": "integer", "ref": "t3"}]},
                {"id": "t3", "name": "c", "columns": [
                    {"id": "c6", "name": "i", "type": "integer", "primary_key": True},
                    {"id": "c7", "name": "s", "type": "string"}]}]}
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m2", "version": 2, "model": m2,
                                "defaults": {"t1": {"c8": None}}})
            check(s == 200 and b["version"] == 3, "delete table t2, add null fk", f"{s} {b}")
            s, b = srv.request("GET", "/data/b")
            check(s == 404, "deleted table b is gone", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[INT64_MIN, 42, None], [1, 42, None],
                                             [2, 42, None], [INT64_MAX, 42, None]],
                  "null default fills the new nullable fk column", f"{s} {b}")
        finally:
            srv.stop()


def scenario_rollback():
    print("scenario: rollback -- every rejection leaves state untouched")
    with tempfile.TemporaryDirectory() as d:
        srv = Server(os.path.join(d, "w.db"))
        try:
            model = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "g", "type": "integer", "ref": "t1"}]}]}
            s, b = srv.request("POST", "/init",
                               {"model": model, "data": {"t1": [[1], [2]],
                                                         "t2": [[10, 1], [20, 2]]}})
            check(s == 200, "init", f"{s} {b}")

            t1 = model["tables"][0]
            t3 = {"id": "t3", "name": "c", "columns": [
                {"id": "c5", "name": "i", "type": "integer", "primary_key": True},
                {"id": "c6", "name": "g", "type": "integer", "ref": "t1"}]}

            # 1. deleting referenced table t1 while t2.g references it makes
            #    the target model itself invalid -> rejected
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m-del", "version": 1,
                                "model": {"tables": [model["tables"][1]]}})
            check(s == 400 and err_code(b) == "validation",
                  "deleting referenced table rejected", f"{s} {b}")
            s, b = srv.request("GET", "/model")
            check(s == 200 and b["version"] == 1, "version still 1 after rejection",
                  f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[1], [2]], "rows intact after rejection",
                  f"{s} {b}")

            # 2. a failed migration does not occupy its id: reuse m-del with a
            #    fixed body (drop t2 instead, add t3) -> now it succeeds
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m-del", "version": 1,
                                "model": {"tables": [t1, t3]}})
            check(s == 200 and b["version"] == 2,
                  "failed migration id can be reused with fixed content", f"{s} {b}")
            s, b = srv.request("GET", "/data/b")
            check(s == 404, "t2 dropped by the retried migration", f"{s} {b}")

            # 3. stale version rejected
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m2", "version": 1,
                                "model": {"tables": [t1, t3]}})
            check(s == 409 and err_code(b) == "version_conflict",
                  "stale base version rejected", f"{s} {b}")

            def mig(model_obj, defaults=None, mid="m2", ver=2):
                body = {"migration_id": mid, "version": ver, "model": model_obj}
                if defaults is not None:
                    body["defaults"] = defaults
                return srv.request("POST", "/migrate", body)

            # 4. retained column must not change type
            bad = {"tables": [t1, {"id": "t3", "name": "c", "columns": [
                {"id": "c5", "name": "i", "type": "integer", "primary_key": True},
                {"id": "c6", "name": "g", "type": "string", "ref": "t1"}]}]}
            s, b = mig(bad)
            check(s == 400 and err_code(b) == "validation",
                  "retained column type change rejected", f"{s} {b}")

            # 5. primary key column id must not change
            bad = {"tables": [{"id": "t1", "name": "a", "columns": [
                {"id": "c9", "name": "x", "type": "integer", "primary_key": True}]}, t3]}
            s, b = mig(bad)
            check(s == 400 and err_code(b) == "validation",
                  "primary key id change rejected", f"{s} {b}")

            # 6. columns must not move across tables
            bad = {"tables": [{"id": "t1", "name": "a", "columns": [
                {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                {"id": "c6", "name": "g", "type": "integer", "ref": "t1"}]},
                {"id": "t3", "name": "c", "columns": [
                    {"id": "c5", "name": "i", "type": "integer", "primary_key": True}]}]}
            s, b = mig(bad)
            check(s == 400 and err_code(b) == "validation",
                  "column moving across tables rejected", f"{s} {b}")

            # 7. new column without a declared default
            with_n = {"tables": [{"id": "t1", "name": "a", "columns": [
                {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                {"id": "c7", "name": "n", "type": "integer"}]}, t3]}
            s, b = mig(with_n)
            check(s == 400 and err_code(b) == "validation",
                  "new column without default rejected", f"{s} {b}")

            # 8. default of the wrong type / boolean default
            s, b = mig(with_n, {"t1": {"c7": "oops"}})
            check(s == 400 and err_code(b) == "validation",
                  "string default for integer column rejected", f"{s} {b}")
            s, b = mig(with_n, {"t1": {"c7": True}})
            check(s == 400 and err_code(b) == "validation",
                  "boolean default rejected", f"{s} {b}")

            # 9. default that dangles against an empty referenced table
            with_r = {"tables": [{"id": "t1", "name": "a", "columns": [
                {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                {"id": "c7", "name": "r", "type": "integer", "ref": "t3"}]}, t3]}
            s, b = mig(with_r, {"t1": {"c7": 999}})
            check(s == 400 and err_code(b) == "fk_violation",
                  "default creating a dangling reference rejected", f"{s} {b}")

            # state must be exactly as after step 2
            s, b = srv.request("GET", "/model")
            check(s == 200 and b["version"] == 2 and
                  b["model"] == norm({"tables": [t1, t3]}),
                  "model untouched by the rejected attempts", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[1], [2]], "data untouched", f"{s} {b}")

            # 10. a valid migration finally succeeds
            s, b = mig(with_n, {"t1": {"c7": 7}})
            check(s == 200 and b["version"] == 3, "valid migration succeeds", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[1, 7], [2, 7]],
                  "default 7 filled for retained rows", f"{s} {b}")

            # 11. idempotent replay of the exact same request
            replay = {"migration_id": "m2", "version": 2, "model": with_n,
                      "defaults": {"t1": {"c7": 7}}}
            s, b = srv.request("POST", "/migrate", replay)
            check(s == 200 and b["replayed"] is True and b["version"] == 3,
                  "identical retry is an idempotent replay", f"{s} {b}")
            s, b = srv.request("GET", "/model")
            check(s == 200 and b["version"] == 3, "replay did not bump the version",
                  f"{s} {b}")

            # 12. same id, different content -> conflict
            s, b = mig(with_n, {"t1": {"c7": 8}}, ver=2)
            check(s == 409 and err_code(b) == "migration_conflict",
                  "occupied id with different content rejected", f"{s} {b}")

            # 13. initialization is one-time only
            s, b = srv.request("POST", "/init", {"model": {"tables": []}})
            check(s == 409 and err_code(b) == "already_initialized",
                  "second init rejected", f"{s} {b}")

            s, b = srv.request("GET", "/migrations")
            ids = [m["migration_id"] for m in b["migrations"]]
            check(s == 200 and ids == ["m-del", "m2"],
                  "only successful migrations are recorded", f"{s} {b}")
        finally:
            srv.stop()


def scenario_validation():
    print("scenario: model and data validation battery")
    with tempfile.TemporaryDirectory() as d:
        srv = Server(os.path.join(d, "w.db"))
        try:
            s, b = srv.request("GET", "/model")
            check(s == 409 and err_code(b) == "not_initialized",
                  "model query before init rejected", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 409 and err_code(b) == "not_initialized",
                  "data query before init rejected", f"{s} {b}")
            s, b = srv.request("POST", "/migrate",
                               {"migration_id": "m", "version": 1,
                                "model": {"tables": []}})
            check(s == 409 and err_code(b) == "not_initialized",
                  "migrate before init rejected", f"{s} {b}")

            base_table = {"id": "t", "name": "a", "columns": [
                {"id": "k", "name": "x", "type": "integer", "primary_key": True}]}

            def model_with(table):
                return {"model": {"tables": [table]}}

            def init_body(table, data=None):
                body = model_with(table)
                if data is not None:
                    body["data"] = data
                return body

            cases = []

            def add(label, body, code="validation", status=400):
                cases.append((label, body, status, code))

            # names
            for bad_name in ["Abc", "sys_t", "sqlite_x", "a1", "a-b", "", "1a"]:
                t = dict(base_table, name=bad_name)
                add(f"table name {bad_name!r} rejected", model_with(t))
            t = dict(base_table, columns=[dict(base_table["columns"][0], name="A")])
            add("column name 'A' rejected", model_with(t))
            # duplicate table name / duplicate ids
            add("duplicate table name rejected",
                {"model": {"tables": [base_table,
                                      {"id": "u", "name": "a", "columns": [
                                          {"id": "k2", "name": "y", "type": "integer",
                                           "primary_key": True}]}]}})
            add("table id equal to column id rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "t", "name": "x", "type": "integer", "primary_key": True}]}]}})
            add("empty id rejected",
                {"model": {"tables": [{"id": "", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer",
                     "primary_key": True}]}]}})
            add("non-ascii id rejected",
                {"model": {"tables": [{"id": "é", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer",
                     "primary_key": True}]}]}})
            # primary key rules
            add("table without primary key rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer"}]}]}})
            add("two primary keys rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "k2", "name": "y", "type": "integer",
                     "primary_key": True}]}]}})
            add("string primary key rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "string",
                     "primary_key": True}]}]}})
            # types
            add("boolean column type rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c", "name": "v", "type": "boolean"}]}]}})
            # references
            add("reference to unknown table rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c", "name": "v", "type": "integer", "ref": "nope"}]}]}})
            add("reference on string column rejected",
                {"model": {"tables": [{"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c", "name": "v", "type": "string", "ref": "t"}]}]}})
            # data validation
            add("data for unknown table rejected",
                init_body(base_table, {"ghost": [[1]]}))
            add("row of wrong length rejected",
                init_body(base_table, {"t": [[1, 2]]}))
            add("string in integer column rejected",
                init_body(base_table, {"t": [["1"]]}))
            add("boolean in integer column rejected",
                init_body(base_table, {"t": [[True]]}))
            add("float in integer column rejected",
                init_body(base_table, {"t": [[1.5]]}))
            add("2**63 overflows int64",
                init_body(base_table, {"t": [[2 ** 63]]}))
            add("null primary key rejected",
                init_body(base_table, {"t": [[None]]}))
            str_table = {"id": "t", "name": "a", "columns": [
                {"id": "k", "name": "x", "type": "integer", "primary_key": True},
                {"id": "c", "name": "v", "type": "string"}]}
            add("integer in string column rejected",
                init_body(str_table, {"t": [[1, 2]]}))
            add("duplicate primary key rows rejected",
                init_body(base_table, {"t": [[1], [1]]}),
                code="constraint_violation")
            ref_model = {"model": {"tables": [
                {"id": "t", "name": "a", "columns": [
                    {"id": "k", "name": "x", "type": "integer", "primary_key": True}]},
                {"id": "u", "name": "b", "columns": [
                    {"id": "k2", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c", "name": "g", "type": "integer", "ref": "t"}]}]},
                "data": {"t": [[1]], "u": [[1, 999]]}}
            add("dangling foreign key in initial load rejected", ref_model,
                code="fk_violation")

            for label, body, status, code in cases:
                s, b = srv.request("POST", "/init", body)
                check(s == status and err_code(b) == code, label, f"{s} {b}")
                s, b = srv.request("GET", "/model")
                check(s == 409, f"still uninitialized after: {label}", f"{s} {b}")

            # a valid init finally succeeds, with int64 extremes
            s, b = srv.request("POST", "/init",
                               init_body(base_table,
                                         {"t": [[INT64_MIN], [INT64_MAX]]}))
            check(s == 200 and b["version"] == 1,
                  "valid init succeeds after rejected attempts", f"{s} {b}")
            s, b = srv.request("GET", "/data/a")
            check(s == 200 and b["rows"] == [[INT64_MIN], [INT64_MAX]],
                  "int64 extremes round-trip", f"{s} {b}")
        finally:
            srv.stop()


def scenario_crash_recovery():
    print("scenario: crash recovery before/after commit")
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "w.db")
        model_v1 = {"tables": [{"id": "t", "name": "a", "columns": [
            {"id": "k", "name": "x", "type": "integer", "primary_key": True},
            {"id": "c", "name": "v", "type": "string"}]}]}
        model_v2 = {"tables": [{"id": "t", "name": "b", "columns": [
            {"id": "k", "name": "y", "type": "integer", "primary_key": True},
            {"id": "c", "name": "w", "type": "string"},
            {"id": "n", "name": "z", "type": "integer"}]}]}
        mig_body = {"migration_id": "crash-m", "version": 1, "model": model_v2,
                    "defaults": {"t": {"n": 5}}}

        # crash BEFORE commit: the old version must survive
        srv = Server(db, env={"DW_CRASH_POINT": "before-commit"})
        s, b = srv.request("POST", "/init",
                           {"model": model_v1, "data": {"t": [[1, "one"], [2, "two"]]}})
        check(s == 200, "init before crash test", f"{s} {b}")
        try:
            srv.request("POST", "/migrate", mig_body)
            crashed = False
        except (OSError, http.client.HTTPException):
            crashed = True
        check(crashed and srv.proc.wait(timeout=15) is not None,
              "server process died before commit")
        srv = Server(db)
        s, b = srv.request("GET", "/model")
        check(s == 200 and b["version"] == 1 and b["model"] == norm(model_v1),
              "crash before commit keeps the old model version", f"{s} {b}")
        s, b = srv.request("GET", "/data/a")
        check(s == 200 and b["rows"] == [[1, "one"], [2, "two"]],
              "crash before commit keeps the old rows", f"{s} {b}")
        s, b = srv.request("GET", "/migrations")
        check(s == 200 and b["migrations"] == [],
              "no migration recorded for the crashed attempt", f"{s} {b}")
        srv.stop()

        # crash AFTER commit, BEFORE the response: the new version must survive
        srv = Server(db, env={"DW_CRASH_POINT": "after-commit"})
        try:
            srv.request("POST", "/migrate", mig_body)
            crashed = False
        except (OSError, http.client.HTTPException):
            crashed = True
        check(crashed and srv.proc.wait(timeout=15) is not None,
              "server process died after commit")
        srv = Server(db)
        s, b = srv.request("GET", "/model")
        check(s == 200 and b["version"] == 2 and b["model"] == norm(model_v2),
              "crash after commit keeps the new model version", f"{s} {b}")
        s, b = srv.request("GET", "/data/b")
        check(s == 200 and b["columns"] == ["y", "w", "z"] and
              b["rows"] == [[1, "one", 5], [2, "two", 5]],
              "crash after commit keeps the migrated rows", f"{s} {b}")
        # the client never saw the response; a retry must be idempotent
        s, b = srv.request("POST", "/migrate", mig_body)
        check(s == 200 and b["replayed"] is True and b["version"] == 2,
              "retry after crash is an idempotent replay", f"{s} {b}")
        s, b = srv.request("GET", "/migrations")
        check(s == 200 and [m["migration_id"] for m in b["migrations"]] == ["crash-m"],
              "the committed migration is recorded exactly once", f"{s} {b}")
        srv.stop()


def scenario_physical_storage():
    print("scenario: physical SQLite tables with real PK/FK declarations")
    with tempfile.TemporaryDirectory() as d:
        db = os.path.join(d, "w.db")
        srv = Server(db)
        try:
            model = {"tables": [
                {"id": "t1", "name": "a", "columns": [
                    {"id": "c1", "name": "x", "type": "integer", "primary_key": True},
                    {"id": "c2", "name": "f", "type": "integer", "ref": "t2"}]},
                {"id": "t2", "name": "b", "columns": [
                    {"id": "c3", "name": "y", "type": "integer", "primary_key": True},
                    {"id": "c4", "name": "g", "type": "integer", "ref": "t1"}]}]}
            s, b = srv.request("POST", "/init",
                               {"model": model,
                                "data": {"t1": [[1, 10]], "t2": [[10, 1]]}})
            check(s == 200, "init for physical inspection", f"{s} {b}")
        finally:
            srv.stop()

        conn = sqlite3.connect(db)
        ddl = {name: sql for name, sql in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type = 'table'")}
        check('"x" INTEGER PRIMARY KEY NOT NULL' in ddl["a"]
              and 'REFERENCES "b"("y")' in ddl["a"],
              "table a is a real SQLite table with PK and FK declarations",
              ddl.get("a"))
        check('REFERENCES "a"("x")' in ddl["b"],
              "table b declares its foreign key to a", ddl.get("b"))
        pk_cols = [r[1] for r in conn.execute("PRAGMA table_info(a)") if r[5]]
        check(pk_cols == ["x"], "primary key flag set on a.x", pk_cols)
        fks = [(r[2], r[3], r[4]) for r in conn.execute("PRAGMA foreign_key_list(a)")]
        check(fks == [("b", "f", "y")], "foreign key a.f -> b.y registered", fks)
        rows = conn.execute('SELECT "x", "f" FROM "a"').fetchall()
        check(rows == [(1, 10)], "user rows live in the physical table", rows)
        # foreign key enforcement is actually on for normal connections
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            conn.execute('INSERT INTO "a" ("x", "f") VALUES (2, 999)')
            conn.commit()
            enforced = False
        except sqlite3.IntegrityError:
            conn.rollback()
            enforced = True
        check(enforced, "SQLite enforces the declared foreign keys")
        conn.close()


def main():
    scenarios = [
        scenario_spec_example,
        scenario_swap_names,
        scenario_circular_refs,
        scenario_migration_semantics,
        scenario_rollback,
        scenario_validation,
        scenario_crash_recovery,
        scenario_physical_storage,
    ]
    for fn in scenarios:
        fn()
        print()
    print(f"{passed} passed, {failed} failed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
