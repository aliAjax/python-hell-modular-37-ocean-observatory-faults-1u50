import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def transaction(self):
        """Run multiple writes in a single atomic transaction.

        Commits on success, rolls back on any exception. Methods called with
        the yielded connection join the same transaction.
        """
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS entity_versions (
                    entity_id TEXT NOT NULL,
                    version_no INTEGER NOT NULL,
                    source TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    data TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (entity_id, version_no)
                );
                CREATE INDEX IF NOT EXISTS idx_entity_versions_entity
                    ON entity_versions(entity_id, version_no);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS merge_batches (
                    batch_key TEXT PRIMARY KEY,
                    actor_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id, source=None, recorded_at=None, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            now = utcnow()
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
            self._add_version(connection, entity_id, 1, source or "online", recorded_at or now, status, payload, now)
            if own:
                connection.commit()
        finally:
            if own:
                connection.close()
        return self.get_entity(entity_id, conn=conn)

    def get_entity(self, entity_id, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        finally:
            if own:
                connection.close()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None, conn=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        own = conn is None
        connection = conn or self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        finally:
            if own:
                connection.close()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value, conn=None):
        entities = self.list_entities(kind=kind, conn=conn)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data, source=None, recorded_at=None, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            now = utcnow()
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            if own:
                connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            next_version = current_version + 1
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            self._add_version(connection, entity_id, next_version, source or "online", recorded_at or now, status, payload, now)
            if own:
                connection.commit()
        except Exception:
            if own:
                connection.rollback()
            raise
        finally:
            if own:
                connection.close()
        return self.get_entity(entity_id, conn=conn)

    def _add_version(self, connection, entity_id, version_no, source, recorded_at, status, payload, created_at):
        connection.execute(
            "INSERT OR REPLACE INTO entity_versions(entity_id, version_no, source, recorded_at, status, data, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (entity_id, version_no, source, recorded_at, status, payload, created_at),
        )

    def add_entity_version(self, entity_id, source, recorded_at, status, data, conn=None):
        """Keep an offline/alternate version without touching the current row."""
        own = conn is None
        connection = conn or self._connect()
        try:
            now = utcnow()
            payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
            row = connection.execute(
                "SELECT COALESCE(MAX(version_no), 0) AS max_v FROM entity_versions WHERE entity_id = ?",
                (entity_id,),
            ).fetchone()
            version_no = int(row["max_v"]) + 1
            self._add_version(connection, entity_id, version_no, source, recorded_at or now, status, payload, now)
            if own:
                connection.commit()
        finally:
            if own:
                connection.close()
        return version_no

    def list_entity_versions(self, entity_id, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            rows = connection.execute(
                "SELECT * FROM entity_versions WHERE entity_id = ? ORDER BY version_no",
                (entity_id,),
            ).fetchall()
        finally:
            if own:
                connection.close()
        return [
            {
                "entity_id": row["entity_id"],
                "version_no": int(row["version_no"]),
                "source": row["source"],
                "recorded_at": row["recorded_at"],
                "status": row["status"],
                "data": json.loads(row["data"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )
            if own:
                connection.commit()
        finally:
            if own:
                connection.close()

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_id"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def save_merge_batch(self, batch_key, actor_id, status, result, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            connection.execute(
                "INSERT OR REPLACE INTO merge_batches(batch_key, actor_id, status, result, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (batch_key, actor_id, status, json.dumps(result, ensure_ascii=False, sort_keys=True), utcnow()),
            )
            if own:
                connection.commit()
        finally:
            if own:
                connection.close()

    def get_merge_batch(self, batch_key, conn=None):
        own = conn is None
        connection = conn or self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM merge_batches WHERE batch_key = ?", (batch_key,)
            ).fetchone()
        finally:
            if own:
                connection.close()
        if not row:
            return None
        return {
            "batch_key": row["batch_key"],
            "actor_id": row["actor_id"],
            "status": row["status"],
            "result": json.loads(row["result"]),
            "created_at": row["created_at"],
        }

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
