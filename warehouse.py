"""Core engine of the mini data warehouse (see README.md).

Implemented from scratch on the Python standard library only:

* every user table is a real SQLite table carrying PRIMARY KEY and FOREIGN
  KEY declarations, with foreign-key enforcement enabled -- user rows are
  never simulated with JSON blobs;
* schema evolution (renames, add/drop of columns and tables) is planned by
  this module and executed by rebuilding tables inside a single
  transaction, following SQLite's documented schema-change procedure
  (foreign_keys=OFF -> rebuild -> foreign_key_check -> COMMIT ->
  foreign_keys=ON);
* model metadata, user tables, rows and migration retry-records are
  committed atomically, so a crash either keeps the complete old version
  or the complete new version.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone

NAME_RE = re.compile(r"[a-z][a-z_]*")
FORBIDDEN_PREFIXES = ("sys_", "sqlite_")
INT64_MIN = -(2 ** 63)
INT64_MAX = 2 ** 63 - 1
# Temporary physical names used while rebuilding; user names can never
# start with "sys_", so these can never collide with a user table.
TMP_PREFIX = "sys_new_"


class Reject(Exception):
    """A client-visible rejection: the request is refused, nothing changes."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def _bad(msg: str) -> Reject:
    return Reject(400, "bad_request", msg)


def _invalid(msg: str) -> Reject:
    return Reject(400, "validation", msg)


# --------------------------------------------------------------------- model

@dataclass
class Column:
    id: str
    name: str
    type: str          # "integer" | "string"
    is_pk: bool
    ref: str | None    # id of the referenced table, or None


@dataclass
class Table:
    id: str
    name: str
    columns: list

    @property
    def pk(self) -> Column:
        for c in self.columns:
            if c.is_pk:
                return c
        raise AssertionError("table without primary key")

    def column(self, cid: str):
        for c in self.columns:
            if c.id == cid:
                return c
        return None


def _is_id(value) -> bool:
    """Ids and migration ids: non-empty ASCII strings."""
    return isinstance(value, str) and value != "" and value.isascii()


def _is_name(value) -> bool:
    """Table/column names: [a-z][a-z_]* without the reserved prefixes."""
    return (isinstance(value, str)
            and NAME_RE.fullmatch(value) is not None
            and not value.startswith(FORBIDDEN_PREFIXES))


def parse_model(model) -> list:
    """Validate a model document, return it as a list of Table objects."""
    if not isinstance(model, dict):
        raise _bad("model must be a JSON object")
    extra = set(model) - {"tables"}
    if extra:
        raise _bad(f"model: unknown keys {sorted(extra)}")
    tables = model.get("tables")
    if not isinstance(tables, list):
        raise _bad("model.tables must be an array")

    seen_ids: set = set()          # ids are globally unique across tables+columns
    seen_table_names: set = set()
    out: list = []
    for t in tables:
        if not isinstance(t, dict):
            raise _bad("each table must be a JSON object")
        extra = set(t) - {"id", "name", "columns"}
        if extra:
            raise _bad(f"table: unknown keys {sorted(extra)}")
        tid, tname, cols = t.get("id"), t.get("name"), t.get("columns")
        if not _is_id(tid):
            raise _invalid(f"table id must be a non-empty ASCII string, got {tid!r}")
        if tid in seen_ids:
            raise _invalid(f"duplicate id {tid!r}: ids must be globally unique")
        seen_ids.add(tid)
        if not _is_name(tname):
            raise _invalid(
                f"invalid table name {tname!r}: must match [a-z][a-z_]* "
                "and must not start with sys_ or sqlite_")
        if tname in seen_table_names:
            raise _invalid(f"duplicate table name {tname!r}")
        seen_table_names.add(tname)
        if not isinstance(cols, list) or not cols:
            raise _invalid(f"table {tname!r}: columns must be a non-empty array")

        seen_col_names: set = set()
        pk_count = 0
        columns: list = []
        for c in cols:
            if not isinstance(c, dict):
                raise _bad("each column must be a JSON object")
            extra = set(c) - {"id", "name", "type", "primary_key", "ref"}
            if extra:
                raise _bad(f"column: unknown keys {sorted(extra)}")
            cid, cname = c.get("id"), c.get("name")
            ctype = c.get("type")
            is_pk = c.get("primary_key", False)
            ref = c.get("ref", None)
            if not _is_id(cid):
                raise _invalid(f"column id must be a non-empty ASCII string, got {cid!r}")
            if cid in seen_ids:
                raise _invalid(f"duplicate id {cid!r}: ids must be globally unique")
            seen_ids.add(cid)
            if not _is_name(cname):
                raise _invalid(
                    f"invalid column name {cname!r}: must match [a-z][a-z_]* "
                    "and must not start with sys_ or sqlite_")
            if cname in seen_col_names:
                raise _invalid(f"duplicate column name {cname!r} in table {tname!r}")
            seen_col_names.add(cname)
            if ctype not in ("integer", "string"):
                raise _invalid(f"column {cname!r}: type must be \"integer\" or \"string\" "
                               "(booleans are not supported)")
            if not isinstance(is_pk, bool):
                raise _invalid(f"column {cname!r}: primary_key must be a boolean")
            if ref is not None and not _is_id(ref):
                raise _invalid(f"column {cname!r}: ref must be a non-empty ASCII table id or null")
            if ref is not None and ctype != "integer":
                raise _invalid(f"column {cname!r}: only integer columns may reference a primary key")
            if is_pk:
                pk_count += 1
                if ctype != "integer":
                    raise _invalid(f"column {cname!r}: the primary key must be an integer column")
            columns.append(Column(cid, cname, ctype, is_pk, ref))
        if pk_count != 1:
            raise _invalid(f"table {tname!r}: exactly one primary key column is required, "
                           f"got {pk_count}")
        out.append(Table(tid, tname, columns))

    table_ids = {t.id for t in out}
    for t in out:
        for c in t.columns:
            if c.ref is not None and c.ref not in table_ids:
                raise _invalid(f"column {c.name!r} of table {t.name!r} references "
                               f"unknown table id {c.ref!r}")
    return out


def check_value(ctype: str, value, what: str):
    """Validate one non-null JSON value against a column type."""
    if value is None:
        return
    if ctype == "integer":
        # bool is a subclass of int in Python: reject JSON true/false explicitly
        if isinstance(value, bool) or not isinstance(value, int):
            raise _invalid(f"{what}: expected a signed 64-bit integer, got {value!r}")
        if not INT64_MIN <= value <= INT64_MAX:
            raise _invalid(f"{what}: {value} is outside the signed 64-bit range")
    else:
        if not isinstance(value, str):
            raise _invalid(f"{what}: expected a string, got {value!r}")


def model_to_json(tables) -> dict:
    return {"tables": [
        {"id": t.id, "name": t.name, "columns": [
            {"id": c.id, "name": c.name, "type": c.type,
             "primary_key": c.is_pk, "ref": c.ref}
            for c in t.columns]}
        for t in tables]}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def create_table_sql(physical_name: str, table: Table, tables_by_id: dict) -> str:
    """CREATE TABLE with real PRIMARY KEY / FOREIGN KEY declarations."""
    parts = []
    for c in table.columns:
        col = quote_ident(c.name)
        if c.is_pk:
            decl = f"{col} INTEGER PRIMARY KEY NOT NULL"
        elif c.type == "integer":
            decl = f"{col} INTEGER"
        else:
            decl = f"{col} TEXT"
        if c.ref is not None:
            target = tables_by_id[c.ref]
            decl += f" REFERENCES {quote_ident(target.name)}({quote_ident(target.pk.name)})"
        parts.append(decl)
    return f"CREATE TABLE {quote_ident(physical_name)} ({', '.join(parts)})"


def _fmt_violations(rows, limit: int = 5) -> str:
    parts = [f"table {t!r} rowid {r} references missing row in {p!r}"
             for t, r, p, _ in rows[:limit]]
    suffix = f" (and {len(rows) - limit} more)" if len(rows) > limit else ""
    return "; ".join(parts) + suffix


# ----------------------------------------------------------------- warehouse

class Warehouse:
    """Owns the single SQLite connection and all state transitions."""

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path, isolation_level=None,
                                    check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = FULL")
        self.conn.execute("PRAGMA foreign_keys = ON")
        self._bootstrap()

    # ------------------------------------------------------------ metadata
    def _bootstrap(self):
        """Internal metadata tables (sys_ prefix is reserved for these)."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS sys_meta (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO sys_meta (key, value) VALUES ('initialized', '0');
            INSERT OR IGNORE INTO sys_meta (key, value) VALUES ('version', '0');
            CREATE TABLE IF NOT EXISTS sys_tables (
                id       TEXT PRIMARY KEY,
                name     TEXT NOT NULL UNIQUE,
                position INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sys_columns (
                id           TEXT PRIMARY KEY,
                table_id     TEXT NOT NULL REFERENCES sys_tables (id) ON DELETE CASCADE,
                name         TEXT NOT NULL,
                position     INTEGER NOT NULL,
                type         TEXT NOT NULL CHECK (type IN ('integer', 'string')),
                is_pk        INTEGER NOT NULL CHECK (is_pk IN (0, 1)),
                ref_table_id TEXT REFERENCES sys_tables (id),
                UNIQUE (table_id, name)
            );
            CREATE TABLE IF NOT EXISTS sys_migrations (
                migration_id TEXT PRIMARY KEY,
                request_json TEXT NOT NULL,
                from_version INTEGER NOT NULL,
                to_version   INTEGER NOT NULL,
                applied_at   TEXT NOT NULL
            );
        """)

    def _meta(self, key: str):
        row = self.conn.execute("SELECT value FROM sys_meta WHERE key = ?",
                                (key,)).fetchone()
        return row[0] if row else None

    @property
    def initialized(self) -> bool:
        return self._meta("initialized") == "1"

    @property
    def version(self) -> int:
        return int(self._meta("version"))

    def load_model(self) -> list:
        tables = []
        for tid, tname in self.conn.execute(
                "SELECT id, name FROM sys_tables ORDER BY position"):
            cols = [Column(cid, cname, ctype, bool(is_pk), ref)
                    for cid, cname, ctype, is_pk, ref in self.conn.execute(
                        "SELECT id, name, type, is_pk, ref_table_id "
                        "FROM sys_columns WHERE table_id = ? ORDER BY position", (tid,))]
            tables.append(Table(tid, tname, cols))
        return tables

    def _write_metadata(self, tables, version: int):
        """Replace the model metadata; runs inside the caller's transaction."""
        self.conn.execute("DELETE FROM sys_columns")
        self.conn.execute("DELETE FROM sys_tables")
        for pos, t in enumerate(tables):
            self.conn.execute(
                "INSERT INTO sys_tables (id, name, position) VALUES (?, ?, ?)",
                (t.id, t.name, pos))
        for t in tables:
            for pos, c in enumerate(t.columns):
                self.conn.execute(
                    "INSERT INTO sys_columns "
                    "(id, table_id, name, position, type, is_pk, ref_table_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (c.id, t.id, c.name, pos, c.type, 1 if c.is_pk else 0, c.ref))
        self.conn.execute("UPDATE sys_meta SET value = ? WHERE key = 'version'",
                          (str(version),))

    # ---------------------------------------------------------------- init
    def init(self, body):
        if not isinstance(body, dict):
            raise _bad("request body must be a JSON object")
        extra = set(body) - {"model", "data"}
        if extra:
            raise _bad(f"unknown keys {sorted(extra)}")
        if self.initialized:
            raise Reject(409, "already_initialized",
                         "initialization is allowed only once")
        if "model" not in body:
            raise _bad("missing key 'model'")
        tables = parse_model(body["model"])
        data = body.get("data", {})
        if not isinstance(data, dict):
            raise _bad("'data' must be an object mapping table id -> array of rows")

        by_id = {t.id: t for t in tables}
        rows_by_table = {}
        for tid, rows in data.items():
            t = by_id.get(tid)
            if t is None:
                raise _invalid(f"data supplied for unknown table id {tid!r}")
            if not isinstance(rows, list):
                raise _invalid(f"data[{tid!r}] must be an array of rows")
            checked = []
            for i, row in enumerate(rows):
                checked.append(self._check_row(t, row, f"data[{tid!r}][{i}]"))
            rows_by_table[tid] = checked

        conn = self.conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            # defer FK enforcement to commit so circular references can load
            conn.execute("PRAGMA defer_foreign_keys = ON")
            for t in tables:
                conn.execute(create_table_sql(t.name, t, by_id))
            for t in tables:
                rows = rows_by_table.get(t.id)
                if not rows:
                    continue
                cols = ", ".join(quote_ident(c.name) for c in t.columns)
                marks = ", ".join("?" for _ in t.columns)
                conn.executemany(
                    f"INSERT INTO {quote_ident(t.name)} ({cols}) VALUES ({marks})",
                    rows)
            self._write_metadata(tables, 1)
            conn.execute("UPDATE sys_meta SET value = '1' WHERE key = 'initialized'")
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise Reject(400, "fk_violation",
                             "data violates foreign keys: " + _fmt_violations(violations))
            conn.execute("COMMIT")
        except sqlite3.IntegrityError as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise Reject(400, "constraint_violation",
                         f"integrity constraint violated: {exc}") from exc
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        return {"ok": True, "version": 1}

    @staticmethod
    def _check_row(t: Table, row, what: str):
        if not isinstance(row, list):
            raise _invalid(f"{what}: a row must be a JSON array")
        if len(row) != len(t.columns):
            raise _invalid(f"{what}: expected {len(t.columns)} values in column "
                           f"declaration order, got {len(row)}")
        for c, v in zip(t.columns, row):
            if v is None and c.is_pk:
                raise _invalid(f"{what}: primary key column {c.name!r} must not be null")
            check_value(c.type, v, f"{what}, column {c.name!r}")
        return row

    # ------------------------------------------------------------ migrate
    def migrate(self, body):
        if not isinstance(body, dict):
            raise _bad("request body must be a JSON object")
        extra = set(body) - {"migration_id", "version", "model", "defaults"}
        if extra:
            raise _bad(f"unknown keys {sorted(extra)}")
        if not self.initialized:
            raise Reject(409, "not_initialized", "the warehouse is not initialized yet")

        mid = body.get("migration_id")
        if not _is_id(mid):
            raise _invalid("migration_id must be a non-empty ASCII string")
        ver = body.get("version")
        if isinstance(ver, bool) or not isinstance(ver, int) or ver < 1:
            raise _invalid("version must be a positive integer")
        if "model" not in body:
            raise _bad("missing key 'model'")
        defaults = body.get("defaults", {})
        if not isinstance(defaults, dict):
            raise _bad("'defaults' must be an object mapping table id -> "
                       "{column id: value}")

        canonical = json.dumps(body, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=True)

        # Only successful migrations occupy their id.  For an occupied id the
        # JSON structure decides: identical request -> idempotent replay of the
        # stored result, different content -> conflict.
        row = self.conn.execute(
            "SELECT request_json, to_version FROM sys_migrations "
            "WHERE migration_id = ?", (mid,)).fetchone()
        if row is not None:
            if row[0] == canonical:
                return {"ok": True, "version": row[1], "migration_id": mid,
                        "replayed": True}
            raise Reject(409, "migration_conflict",
                         f"migration_id {mid!r} is already occupied by a "
                         "different request")

        current = self.version
        if ver != current:
            raise Reject(409, "version_conflict",
                         f"migration is based on version {ver} but the current "
                         f"version is {current}")

        new_tables = parse_model(body["model"])
        old_tables = self.load_model()
        added_defaults = self._plan(old_tables, new_tables, defaults)
        self._execute(old_tables, new_tables, added_defaults, mid, canonical,
                      current)
        return {"ok": True, "version": current + 1, "migration_id": mid,
                "replayed": False}

    @staticmethod
    def _plan(old_tables, new_tables, defaults):
        """Validate the delta between current and target model.

        Returns {(table_id, column_id): default} for columns added to
        retained tables.  Identity is carried by ids: a table or column that
        keeps its id keeps its identity; a same-named element with a fresh id
        is a new entity and inherits nothing.
        """
        old_by_id = {t.id: t for t in old_tables}
        new_by_id = {t.id: t for t in new_tables}
        old_column_ids = {c.id for t in old_tables for c in t.columns}
        new_column_ids = {c.id for t in new_tables for c in t.columns}

        # an id keeps its kind across versions
        for t in new_tables:
            if t.id in old_column_ids:
                raise _invalid(f"id {t.id!r} was a column id in the current model "
                               "and cannot become a table id")
        for t in old_tables:
            if t.id in new_column_ids:
                raise _invalid(f"id {t.id!r} was a table id in the current model "
                               "and cannot become a column id")

        # every supplied default must address an existing target column and
        # match its type (null is always allowed)
        for tid, per_table in defaults.items():
            t = new_by_id.get(tid)
            if t is None:
                raise _invalid(f"defaults supplied for unknown table id {tid!r}")
            if not isinstance(per_table, dict):
                raise _bad(f"defaults[{tid!r}] must be an object mapping "
                           "column id -> value")
            for cid, value in per_table.items():
                c = t.column(cid)
                if c is None:
                    raise _invalid(f"defaults[{tid!r}]: unknown column id {cid!r}")
                check_value(c.type, value,
                            f"default for column {c.name!r} of table {t.name!r}")

        added = {}
        for t in new_tables:
            old_t = old_by_id.get(t.id)
            if old_t is None:
                continue  # brand-new table: starts empty
            per_table = defaults.get(t.id) or {}
            if old_t.pk.id != t.pk.id:
                raise _invalid(
                    f"table {t.name!r}: the primary key column id must not change "
                    f"across migrations ({old_t.pk.id!r} -> {t.pk.id!r})")
            for c in t.columns:
                oc = old_t.column(c.id)
                if oc is not None:
                    if oc.type != c.type:
                        raise _invalid(
                            f"column {c.name!r} of table {t.name!r}: a retained "
                            f"column must not change type ({oc.type} -> {c.type})")
                    if c.id in per_table:
                        raise _invalid(
                            f"default supplied for retained column {c.name!r} of "
                            f"table {t.name!r}: defaults apply to new columns only")
                else:
                    owner = next((ot for ot in old_tables
                                  if ot.column(c.id) is not None), None)
                    if owner is not None:
                        raise _invalid(
                            f"column id {c.id!r} currently belongs to table "
                            f"{owner.name!r}: columns must not move across tables")
                    if c.id not in per_table:
                        raise _invalid(
                            f"new column {c.name!r} of retained table {t.name!r} "
                            "requires a declared default value (a constant of the "
                            "column type or null)")
                    added[(t.id, c.id)] = per_table[c.id]
        return added

    def _execute(self, old_tables, new_tables, added_defaults, mid, canonical,
                 current):
        """Run the migration as one atomic transaction.

        Follows SQLite's documented schema-change procedure: foreign-key
        enforcement is switched off before the transaction, the schema is
        rebuilt, referential integrity of the final state is verified with
        PRAGMA foreign_key_check, and user tables, rows, model metadata and
        the retry record are committed atomically.  Enforcement is switched
        back on immediately afterwards, so all normal operations always run
        with foreign keys checked.
        """
        conn = self.conn
        new_by_id = {t.id: t for t in new_tables}
        old_by_id = {t.id: t for t in old_tables}
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("BEGIN IMMEDIATE")
        try:
            # 1. create every table of the new model under a temporary name
            for t in new_tables:
                conn.execute(create_table_sql(TMP_PREFIX + t.name, t, new_by_id))
            # 2. copy every retained row value-by-value, filling added
            #    columns with their declared constant default
            for t in new_tables:
                old_t = old_by_id.get(t.id)
                if old_t is None:
                    continue
                insert_cols, select_exprs, params = [], [], []
                for c in t.columns:
                    insert_cols.append(quote_ident(c.name))
                    oc = old_t.column(c.id)
                    if oc is not None:
                        select_exprs.append(quote_ident(oc.name))
                    else:
                        select_exprs.append("?")
                        params.append(added_defaults[(t.id, c.id)])
                conn.execute(
                    f"INSERT INTO {quote_ident(TMP_PREFIX + t.name)} "
                    f"({', '.join(insert_cols)}) "
                    f"SELECT {', '.join(select_exprs)} "
                    f"FROM {quote_ident(old_t.name)}",
                    params)
            # 3. drop the old tables (deleted tables discard their rows here)
            for t in old_tables:
                conn.execute(f"DROP TABLE {quote_ident(t.name)}")
            # 4. rename the rebuilt tables to their final names
            for t in new_tables:
                conn.execute(f"ALTER TABLE {quote_ident(TMP_PREFIX + t.name)} "
                             f"RENAME TO {quote_ident(t.name)}")
            # 5. the rebuilt database must not contain dangling references
            violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if violations:
                raise Reject(400, "fk_violation",
                             "migration would leave dangling references: "
                             + _fmt_violations(violations))
            # 6. model metadata and the retry record commit with the data
            self._write_metadata(new_tables, current + 1)
            conn.execute(
                "INSERT INTO sys_migrations "
                "(migration_id, request_json, from_version, to_version, applied_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (mid, canonical, current, current + 1,
                 datetime.now(timezone.utc).isoformat(timespec="seconds")))
            if os.environ.get("DW_CRASH_POINT") == "before-commit":
                os._exit(1)  # test hook: power loss before COMMIT
            conn.execute("COMMIT")
            if os.environ.get("DW_CRASH_POINT") == "after-commit":
                os._exit(1)  # test hook: power loss after COMMIT, before reply
        except sqlite3.IntegrityError as exc:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise Reject(400, "constraint_violation",
                         f"integrity constraint violated: {exc}") from exc
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.execute("PRAGMA foreign_keys = ON")

    # -------------------------------------------------------------- queries
    def get_model(self):
        if not self.initialized:
            raise Reject(409, "not_initialized", "the warehouse is not initialized yet")
        return {"ok": True, "version": self.version,
                "model": model_to_json(self.load_model())}

    def query(self, table_name: str):
        """Rows of one table, in column declaration order, ordered by pk."""
        if not self.initialized:
            raise Reject(409, "not_initialized", "the warehouse is not initialized yet")
        for t in self.load_model():
            if t.name == table_name:
                cols = ", ".join(quote_ident(c.name) for c in t.columns)
                rows = [list(r) for r in self.conn.execute(
                    f"SELECT {cols} FROM {quote_ident(t.name)} "
                    f"ORDER BY {quote_ident(t.pk.name)} ASC")]
                return {"ok": True, "table": t.name,
                        "columns": [c.name for c in t.columns], "rows": rows}
        raise Reject(404, "not_found",
                     f"no table named {table_name!r} in the current model")

    def list_migrations(self):
        rows = self.conn.execute(
            "SELECT migration_id, from_version, to_version, applied_at "
            "FROM sys_migrations ORDER BY to_version, migration_id").fetchall()
        return {"ok": True, "migrations": [
            {"migration_id": m, "from_version": f, "to_version": t,
             "applied_at": a}
            for m, f, t, a in rows]}
