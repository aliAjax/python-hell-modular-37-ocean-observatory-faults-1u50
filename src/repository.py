import json
import sqlite3
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
                CREATE TABLE IF NOT EXISTS offline_batches (
                    batch_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    submitted_by TEXT NOT NULL,
                    submitted_at TEXT NOT NULL,
                    record_count INTEGER NOT NULL,
                    errors TEXT NOT NULL,
                    summary TEXT
                );
                CREATE TABLE IF NOT EXISTS offline_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id TEXT NOT NULL,
                    record_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    incident_id TEXT,
                    asset_id TEXT,
                    payload TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    is_canonical INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(record_id, source, recorded_at, content_hash)
                );
                CREATE INDEX IF NOT EXISTS idx_offline_versions_record
                    ON offline_versions(record_id, id);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_offline_versions_canonical
                    ON offline_versions(record_id) WHERE is_canonical = 1;
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

    @staticmethod
    def _version_from_row(row):
        return {
            "id": row["id"],
            "batch_id": row["batch_id"],
            "record_id": row["record_id"],
            "source": row["source"],
            "recorded_at": row["recorded_at"],
            "incident_id": row["incident_id"],
            "asset_id": row["asset_id"],
            "payload": json.loads(row["payload"]),
            "content_hash": row["content_hash"],
            "is_canonical": bool(row["is_canonical"]),
            "created_at": row["created_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id, connection=None):
        sql = "SELECT * FROM entities WHERE id = ?"
        params = (entity_id,)
        if connection is None:
            with self._connect() as own:
                row = own.execute(sql, params).fetchone()
        else:
            row = connection.execute(sql, params).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        entities = self.list_entities(kind=kind)
        if field == "*":
            return entities
        return [
            entity
            for entity in entities
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def list_entities_in(self, connection, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = connection.execute(
            "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
        ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def begin_write(self):
        """Open a serialized write transaction for multi-step merges."""
        connection = self._connect()
        connection.execute("BEGIN IMMEDIATE")
        return connection

    def update_entity_tx(self, connection, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
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
        cursor = connection.execute(
            "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
            "WHERE id = ? AND version = ?",
            (status, payload, now, entity_id, current_version),
        )
        if cursor.rowcount != 1:
            raise ConflictError("entity changed concurrently: " + entity_id)
        return self.get_entity(entity_id, connection)

    def update_entity(self, entity_id, expected_version, status, data):
        connection = self.begin_write()
        try:
            updated = self.update_entity_tx(connection, entity_id, expected_version, status, data)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return updated

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            self.append_audit_tx(
                connection, entity_id, actor_id, actor_role, action,
                from_status, to_status, detail,
            )

    def append_audit_tx(self, connection, entity_id, actor_id, actor_role, action,
                        from_status, to_status, detail):
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
                "actor_role": row["actor_role"],
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

    # ---- offline batches -------------------------------------------------

    def get_offline_batch(self, batch_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM offline_batches WHERE batch_id = ?", (batch_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "batch_id": row["batch_id"],
            "status": row["status"],
            "submitted_by": row["submitted_by"],
            "submitted_at": row["submitted_at"],
            "record_count": row["record_count"],
            "errors": json.loads(row["errors"]),
            "summary": json.loads(row["summary"]) if row["summary"] else None,
        }

    def insert_offline_batch_failed(self, batch_id, actor_id, record_count, errors):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO offline_batches(batch_id, status, submitted_by, submitted_at, record_count, errors, summary) "
                "VALUES (?, 'failed', ?, ?, ?, ?, NULL) "
                "ON CONFLICT(batch_id) DO UPDATE SET status='failed', submitted_by=excluded.submitted_by, "
                "submitted_at=excluded.submitted_at, record_count=excluded.record_count, errors=excluded.errors, "
                "summary=NULL",
                (batch_id, actor_id, utcnow(), record_count,
                 json.dumps(errors, ensure_ascii=False, sort_keys=True)),
            )

    def mark_batch_failed_tx(self, connection, batch_id, actor_id, record_count, errors):
        connection.execute(
            "UPDATE offline_batches SET status='failed', submitted_by=?, submitted_at=?, "
            "record_count=?, errors=?, summary=NULL WHERE batch_id=?",
            (actor_id, utcnow(), record_count,
             json.dumps(errors, ensure_ascii=False, sort_keys=True), batch_id),
        )

    def claim_offline_batch(self, connection, batch_id, actor_id, record_count):
        """Insert the batch row first; losing the race means another operator
        with the same content already committed this batch."""
        try:
            connection.execute(
                "INSERT INTO offline_batches(batch_id, status, submitted_by, submitted_at, record_count, errors, summary) "
                "VALUES (?, 'merging', ?, ?, ?, '[]', NULL)",
                (batch_id, actor_id, utcnow(), record_count),
            )
            return True
        except sqlite3.IntegrityError:
            return False

    def complete_offline_batch(self, connection, batch_id, status, summary):
        connection.execute(
            "UPDATE offline_batches SET status = ?, summary = ? WHERE batch_id = ?",
            (status, json.dumps(summary, ensure_ascii=False, sort_keys=True), batch_id),
        )

    # ---- offline record versions ----------------------------------------

    def list_offline_versions(self, record_id, connection=None):
        sql = "SELECT * FROM offline_versions WHERE record_id = ? ORDER BY id"
        params = (record_id,)
        if connection is None:
            with self._connect() as own:
                rows = own.execute(sql, params).fetchall()
        else:
            rows = connection.execute(sql, params).fetchall()
        return [self._version_from_row(row) for row in rows]

    def reset_failed_batch_tx(self, connection, batch_id, actor_id, record_count):
        connection.execute(
            "UPDATE offline_batches SET status = 'merging', submitted_by = ?, submitted_at = ?, "
            "record_count = ?, errors = '[]', summary = NULL WHERE batch_id = ? AND status = 'failed'",
            (actor_id, utcnow(), record_count, batch_id),
        )
        return connection.total_changes > 0

    def insert_offline_version_tx(self, connection, entry, batch_id, is_canonical):
        now = utcnow()
        if is_canonical:
            # The partial unique index also enforces this, but checking here
            # produces a clear conflict before the INSERT.
            prior = connection.execute(
                "SELECT 1 FROM offline_versions WHERE record_id = ? AND is_canonical = 1",
                (entry["record_id"],),
            ).fetchone()
            if prior:
                raise ConflictError("canonical version changed concurrently: " + entry["record_id"])
        connection.execute(
            "INSERT INTO offline_versions(batch_id, record_id, source, recorded_at, incident_id, "
            "asset_id, payload, content_hash, is_canonical, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_id, entry["record_id"], entry["source"], entry["recorded_at"],
                entry.get("incident_id"), entry.get("asset_id"),
                json.dumps(entry["payload"], ensure_ascii=False, sort_keys=True),
                entry["content_hash"], 1 if is_canonical else 0, now,
            ),
        )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
