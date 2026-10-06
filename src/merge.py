"""Offline record merge.

Orchestration only: judgment (validation, conflict detection, reopen rules)
lives in `rules.py`; persistence lives in `repository.py`. This module ties
them together for a batch of records coming back from an offline source.

Guarantees:
- Both sides changed the same record -> two versions are kept by source and
  time; a later arrival never overwrites an earlier change.
- A telemetry revision higher than the stored one, or an offline record newer
  than an incident's resolution time, reopens resolved incidents to pending
  and lists the affected assets.
- One invalid record aborts the whole batch before anything is written; the
  error carries the record's position so the caller can locate it.
- Batches are idempotent: submitting the same batch twice applies it once.
  Two operators submitting the same batch concurrently -> the first commit
  wins; the second gets the stored result plus a diff against current state.
"""

import hashlib
import json
from datetime import datetime

from .audit import AuditTrail
from .domain import BatchValidationError, ValidationError


def _parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def batch_key_for(records):
    canonical = json.dumps(records, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class MergeEngine:
    def __init__(self, repository, rules=None):
        from .rules import RuleEngine

        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def merge(self, actor, records, batch_key=None):
        if not isinstance(records, list):
            raise ValidationError("records must be a list")
        if not records:
            raise ValidationError("records must not be empty")
        key = batch_key or batch_key_for(records)

        # First-wins: a committed batch with this key is never applied twice.
        committed = self.repository.get_merge_batch(key)
        if committed and committed["status"] == "committed":
            result = dict(committed["result"])
            result["already_merged"] = True
            result["diff"] = self._diff(records)
            return result

        # Validate the whole batch before writing anything. The first invalid
        # record aborts the batch and reports its position; nothing is stored.
        validated = []
        for index, raw in enumerate(records):
            if not isinstance(raw, dict):
                raise BatchValidationError(index, None, "each offline record must be an object")
            try:
                record = self.rules.validate_merge_record(actor, raw, self._lookup)
            except BatchValidationError:
                raise
            except Exception as exc:
                raise BatchValidationError(index, raw.get("record_id"), str(exc))
            validated.append(record)

        pre_state = {}
        applied = []
        conflicts = []
        with self.repository.transaction() as conn:
            for record in validated:
                entity, outcome = self._apply_record(actor, record, pre_state, conn)
                applied.append((record, entity, outcome))
                if outcome == "conflict":
                    conflicts.append({
                        "entity_id": entity["id"],
                        "kind": entity["kind"],
                        "source_id": record["source_id"],
                        "record_id": record["record_id"],
                        "recorded_at": record["recorded_at"],
                        "current_version": entity["version"],
                    })
            reopened = self._recompute(actor, applied, pre_state, conn)
            result = {
                "batch_key": key,
                "applied": [self._applied_view(record, entity, outcome) for record, entity, outcome in applied],
                "conflicts": conflicts,
                "reopened_incidents": reopened,
            }
            self.repository.save_merge_batch(key, actor.user_id, "committed", result, conn=conn)
        return result

    def _resolve_entity_id(self, record, conn):
        if record.get("entity_id"):
            return record["entity_id"]
        # Stable identity for new offline records so retries are idempotent.
        digest = hashlib.sha256(
            (record["source_id"] + "\0" + record["record_id"]).encode("utf-8")
        ).hexdigest()[:32]
        return "offline-" + digest

    def _apply_record(self, actor, record, pre_state, conn):
        kind = record["kind"]
        entity_id = self._resolve_entity_id(record, conn)
        entity = self.repository.get_entity(entity_id, conn=conn)

        if entity is None:
            # New entity: full create validation (required fields) then insert.
            self.rules.validate_create(actor, kind, record["data"], self._lookup)
            status = self.rules.initial_status(kind, record["data"])
            entity = self.repository.create_entity(
                entity_id, kind, status, record["data"], actor.user_id,
                source=record["source_id"], recorded_at=record["recorded_at"], conn=conn,
            )
            self.audit.record(
                entity_id, actor, "merge_create", None, status,
                {"source_id": record["source_id"], "record_id": record["record_id"]}, conn=conn,
            )
            return entity, "created"

        pre_state.setdefault(entity["id"], dict(entity))
        conflict = self.rules.detect_conflict(entity, record)
        if conflict:
            # Both sides changed: keep the offline version as an alternate,
            # never overwrite the current row.
            self.repository.add_entity_version(
                entity["id"], record["source_id"], record["recorded_at"],
                entity["status"], record["data"], conn=conn,
            )
            self.audit.record(
                entity_id, actor, "merge_conflict", entity["status"], entity["status"],
                {"source_id": record["source_id"], "record_id": record["record_id"]}, conn=conn,
            )
            return entity, "conflict"

        if kind == "telemetry":
            incoming_rev = int(record["data"].get("revision", 0) or 0)
            current_rev = int(entity["data"].get("revision", 0) or 0)
            if incoming_rev > current_rev:
                merged = dict(entity["data"])
                merged.update(record["data"])
                updated = self.repository.update_entity(
                    entity["id"], None, entity["status"], merged,
                    source=record["source_id"], recorded_at=record["recorded_at"], conn=conn,
                )
                self.audit.record(
                    entity_id, actor, "merge_update", entity["status"], updated["status"],
                    {"source_id": record["source_id"], "record_id": record["record_id"], "revision": incoming_rev}, conn=conn,
                )
                return updated, "updated"
            # Late telemetry: keep as an alternate version, do not overwrite.
            self.repository.add_entity_version(
                entity["id"], record["source_id"], record["recorded_at"],
                entity["status"], record["data"], conn=conn,
            )
            return entity, "late"

        # Other kinds: apply when the offline record is based on the current
        # online version (fast-forward) or is newer than what is stored;
        # otherwise keep it as an alternate version.
        if record.get("base_version") is not None:
            should_apply = True
        else:
            current_ts = _parse_ts(entity["updated_at"])
            incoming_ts = _parse_ts(record["recorded_at"])
            should_apply = incoming_ts is None or current_ts is None or incoming_ts >= current_ts
        if should_apply:
            merged = dict(entity["data"])
            merged.update(record["data"])
            updated = self.repository.update_entity(
                entity["id"], None, entity["status"], merged,
                source=record["source_id"], recorded_at=record["recorded_at"], conn=conn,
            )
            self.audit.record(
                entity_id, actor, "merge_update", entity["status"], updated["status"],
                {"source_id": record["source_id"], "record_id": record["record_id"]}, conn=conn,
            )
            return updated, "updated"
        self.repository.add_entity_version(
            entity["id"], record["source_id"], record["recorded_at"],
            entity["status"], record["data"], conn=conn,
        )
        return entity, "late"

    def _candidate_incidents(self, record, entity):
        incidents = self.repository.list_entities(kind="incident")
        data = record["data"]
        candidates = []
        for incident in incidents:
            idata = incident["data"]
            if entity["kind"] == "incident" and incident["id"] == entity["id"]:
                candidates.append(incident)
                continue
            if data.get("asset_id") and idata.get("asset_id") == data.get("asset_id"):
                candidates.append(incident)
            elif data.get("link_id") and idata.get("link_id") == data.get("link_id"):
                candidates.append(incident)
            elif data.get("station_id") and idata.get("station_id") == data.get("station_id"):
                candidates.append(incident)
        return candidates

    def _affected_assets(self, incident):
        """List assets tied to the incident, with their current status.

        The incident's primary asset is always affected; station/link assets
        that are currently unavailable are added as well.
        """
        affected = []
        seen = set()
        idata = incident["data"]
        station_id = idata.get("station_id")
        link_asset_ids = self._link_assets(idata.get("link_id"))
        for asset in self.repository.list_entities(kind="asset"):
            is_primary = idata.get("asset_id") and asset["id"] == idata.get("asset_id")
            is_station = station_id and asset["data"].get("station_id") == station_id
            is_link = asset["id"] in link_asset_ids
            if not (is_primary or is_station or is_link):
                continue
            if is_primary or asset["status"] in ("faulty", "offline", "rebooting"):
                if asset["id"] in seen:
                    continue
                seen.add(asset["id"])
                affected.append({"asset_id": asset["id"], "status": asset["status"]})
        return affected

    def _link_assets(self, link_id):
        link = self.repository.get_entity(link_id)
        if link and link["kind"] == "link":
            asset_id = link["data"].get("asset_id")
            return {asset_id} if asset_id else set()
        return set()

    def _recompute(self, actor, applied, pre_state, conn):
        """Invalidation recompute: reopen resolved incidents made stale by
        newer offline information, and list the affected assets."""
        reopened = []
        seen = set()
        for record, entity, outcome in applied:
            if outcome not in ("created", "updated", "conflict", "late"):
                continue
            for incident in self._candidate_incidents(record, entity):
                if incident["status"] not in ("resolved", "closed"):
                    continue
                if incident["id"] in seen:
                    continue
                reason = None
                if record["kind"] == "telemetry":
                    pre = pre_state.get(entity["id"])
                    # Only a revision that exceeds a previously stored one can
                    # invalidate a resolution; a brand-new telemetry stream has
                    # no stored revision to exceed.
                    if pre is not None:
                        pre_rev = int(pre["data"].get("revision", 0) or 0)
                        incoming_rev = int(record["data"].get("revision", 0) or 0)
                        if incoming_rev > pre_rev:
                            reason = "higher revision telemetry"
                if reason is None and record.get("recorded_at"):
                    resolved_at = incident["data"].get("resolved_at") or incident["updated_at"]
                    resolved_ts = _parse_ts(resolved_at)
                    incoming_ts = _parse_ts(record["recorded_at"])
                    if resolved_ts and incoming_ts and incoming_ts > resolved_ts:
                        reason = "offline record later than resolution"
                if reason is None:
                    continue
                seen.add(incident["id"])
                affected = self._affected_assets(incident)
                updated = self.repository.update_entity(
                    incident["id"], None, "open", incident["data"],
                    source="recompute", recorded_at=record["recorded_at"], conn=conn,
                )
                self.audit.record(
                    incident["id"], actor, "merge_reopen", incident["status"], "open",
                    {"reason": reason, "affected_assets": affected,
                     "source_id": record["source_id"], "record_id": record["record_id"]},
                    conn=conn,
                )
                reopened.append({
                    "incident_id": incident["id"],
                    "reason": reason,
                    "from_status": incident["status"],
                    "to_status": updated["status"],
                    "affected_assets": affected,
                })
        return reopened

    def _applied_view(self, record, entity, outcome):
        return {
            "source_id": record["source_id"],
            "record_id": record["record_id"],
            "kind": record["kind"],
            "entity_id": entity["id"],
            "outcome": outcome,
            "version": entity["version"],
        }

    def _diff(self, records):
        """Diff between the submitted records and current state, so a retried
        or concurrent submission can see what was already merged."""
        diff = []
        for record in records:
            entity_id = record.get("entity_id")
            if not entity_id:
                entity_id = self._resolve_entity_id(record, None)
            current = self.repository.get_entity(entity_id)
            if current is None:
                diff.append({
                    "record_id": record.get("record_id"),
                    "entity_id": entity_id,
                    "status": "missing",
                })
                continue
            incoming = record.get("data", {})
            differs = any(current["data"].get(k) != v for k, v in incoming.items())
            diff.append({
                "record_id": record.get("record_id"),
                "entity_id": entity_id,
                "status": "already_merged" if not differs else "differs",
                "current": current["data"],
                "incoming": incoming,
            })
        return diff
