"""Offline batch merge orchestration.

Rules live in rules.py (pure decisions), persistence in repository.py;
this module sequences validation, version keeping, telemetry application
and incident invalidation so the three concerns stay independently
maintainable.
"""
import hashlib
import json

from .domain import ConflictError, MergeValidationError
from .rules import content_hash, recovery_content

TELEMETRY_MERGED_FIELDS = ("value", "revision", "observed_at")


class MergeService:
    def __init__(self, repository, rules, audit):
        self.repository = repository
        self.rules = rules
        self.audit = audit

    def _batch_id(self, records):
        body = json.dumps(records, ensure_ascii=False, sort_keys=True, default=str)
        return "batch-" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:24]

    def merge_batch(self, actor, records, batch_id=None):
        """Merge one offline batch, all-or-nothing.

        Returns a result dict for both fresh merges and duplicate submissions:
        a second operator sending the same batch sees the already-merged
        outcome plus the diff between their copy and the stored versions.
        """
        if not isinstance(records, list):
            records = []
        bid = str(batch_id or "").strip() or self._batch_id(records)

        existing = self.repository.get_offline_batch(bid)
        if existing and existing["status"] != "failed":
            return self._duplicate_result(actor, bid, existing, records)

        # Phase 1: validate the entire batch against the committed state.
        entries, errors = self.rules.validate_offline_batch(
            actor, records, self.repository.find_entities
        )
        if errors:
            self.repository.insert_offline_batch_failed(bid, actor.user_id, len(records), errors)
            raise MergeValidationError(bid, errors)

        # Phase 2: one serialized transaction applies everything together.
        connection = self.repository.begin_write()
        closed = False
        try:
            claimed = self.repository.claim_offline_batch(connection, bid, actor.user_id, len(records))
            if not claimed:
                prior = self.repository.get_offline_batch(bid)
                if prior and prior["status"] == "failed":
                    # Retry: the submitter fixed records and reuses the id.
                    self.repository.reset_failed_batch_tx(connection, bid, actor.user_id, len(records))
                else:
                    # Another operator's identical batch won the race.
                    connection.commit()
                    connection.close()
                    closed = True
                    stored = self.repository.get_offline_batch(bid)
                    return self._duplicate_result(actor, bid, stored, records)

            lookup = self._tx_lookup(connection)
            # Telemetry revisions must be rechecked inside the lock.
            entries, errors = self.rules.validate_offline_batch(actor, records, lookup)
            if errors:
                # Keep the batch row as failed within the same transaction so
                # the submitter can retry with the same batch_id.
                self.repository.mark_batch_failed_tx(connection, bid, actor.user_id, len(records), errors)
                connection.commit()
                connection.close()
                closed = True
                raise MergeValidationError(bid, errors)

            recovery_entries = [e for e in entries if e["entry_type"] == "recovery"]
            telemetry_entries = [e for e in entries if e["entry_type"] == "telemetry"]

            stored_records = self._store_versions(connection, actor, bid, recovery_entries)
            telemetry_updates = self._apply_telemetry(connection, actor, bid, telemetry_entries)
            reopen_events = self._reopen_incidents(
                connection, actor, telemetry_entries, recovery_entries
            )

            summary = {
                "batch_id": bid,
                "status": "merged",
                "submitted_by": actor.user_id,
                "record_count": len(records),
                "records": stored_records + telemetry_updates,
                "incidents_reopened": reopen_events,
            }
            self.repository.complete_offline_batch(connection, bid, "merged", summary)
            connection.commit()
        except Exception:
            if not closed:
                connection.rollback()
            raise
        finally:
            if not closed:
                connection.close()
        return summary

    def _tx_lookup(self, connection):
        def lookup(kind, field, value):
            kind = self.rules.normalize_kind(kind)
            entities = self.repository.list_entities_in(connection, kind=kind)
            if field == "*":
                return entities
            return [
                entity
                for entity in entities
                if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
            ]
        return lookup

    def _store_versions(self, connection, actor, batch_id, recovery_entries):
        """Keep every non-duplicate version; later arrivals never overwrite
        the first stored (canonical) version, they are kept as conflicts."""
        results = []
        for entry in recovery_entries:
            existing = self.repository.list_offline_versions(entry["record_id"], connection)
            decision = self.rules.classify_version(existing, entry)
            if decision == "duplicate":
                results.append({
                    "record_id": entry["record_id"], "entry_type": "recovery",
                    "result": "duplicate", "source": entry["source"],
                    "recorded_at": entry["recorded_at"],
                })
                continue
            self.repository.insert_offline_version_tx(
                connection, entry, batch_id, is_canonical=(decision == "canonical")
            )
            self.repository.append_audit_tx(
                connection, entry["record_id"], actor.user_id, actor.role,
                "offline_merge_" + decision, None, "kept",
                {"batch_id": batch_id, "source": entry["source"],
                 "recorded_at": entry["recorded_at"], "incident_id": entry["incident_id"]},
            )
            results.append({
                "record_id": entry["record_id"], "entry_type": "recovery",
                "result": decision, "source": entry["source"],
                "recorded_at": entry["recorded_at"],
                "incident_id": entry["incident_id"], "asset_id": entry["asset_id"],
            })
        return results

    def _apply_telemetry(self, connection, actor, batch_id, telemetry_entries):
        """Write higher revisions onto the matching telemetry series."""
        results = []
        series_cache = {}
        all_telemetry = self.repository.list_entities_in(connection, kind="telemetry")
        for telemetry in all_telemetry:
            series_cache[(telemetry["data"].get("asset_id"), telemetry["data"].get("metric"))] = telemetry

        for entry in telemetry_entries:
            key = (entry["asset_id"], entry["metric"])
            entity = series_cache.get(key)
            if not entity:
                raise ConflictError(
                    "telemetry series not found for %s/%s" % (entry["asset_id"], entry["metric"])
                )
            current_rev = int(entity["data"].get("revision", 0) or 0)
            if entry["revision"] <= current_rev:
                # Recheck inside the transaction: a competing batch may have
                # advanced the series after phase 1.
                raise ConflictError(
                    "telemetry revision must increase: %s/%s got %s, have %s"
                    % (entry["asset_id"], entry["metric"], entry["revision"], current_rev)
                )
            from_status = entity["status"]
            data = dict(entity["data"])
            data.update({
                "value": entry["value"],
                "revision": entry["revision"],
                "observed_at": entry["observed_at"],
                "late_revision": True,
                "revised_by": actor.user_id,
                "merged_from_batch": batch_id,
                "merged_from_source": entry["source"],
            })
            updated = self.repository.update_entity_tx(
                connection, entity["id"], entity["version"], "current", data
            )
            series_cache[key] = updated
            self.repository.append_audit_tx(
                connection, entity["id"], actor.user_id, actor.role,
                "offline_merge_telemetry", from_status, "current",
                {"batch_id": batch_id, "revision": entry["revision"],
                 "record_id": entry["record_id"], "source": entry["source"]},
            )
            results.append({
                "record_id": entry["record_id"], "entry_type": "telemetry",
                "result": "applied", "entity_id": entity["id"],
                "asset_id": entry["asset_id"], "metric": entry["metric"],
                "revision": entry["revision"],
            })
        return results

    def _reopen_incidents(self, connection, actor, telemetry_entries, recovery_entries):
        lookup = self._tx_lookup(connection)
        plans = self.rules.plan_reopens(telemetry_entries, recovery_entries, lookup)
        events = []
        for incident_id, plan in sorted(plans.items()):
            entity = self.repository.get_entity(incident_id, connection)
            if not entity or entity["status"] not in ("resolved", "closed"):
                continue
            reason_text = "; ".join(plan["reasons"])
            data = dict(entity["data"])
            next_status, patch = self.rules.validate_transition(
                actor, entity, "reopen",
                {"reason": reason_text, "affected_assets": plan["affected_assets"]},
                lookup,
            )
            data.update(patch)
            data["last_reopen_reasons"] = plan["reasons"]
            updated = self.repository.update_entity_tx(
                connection, incident_id, entity["version"], next_status, data
            )
            self.repository.append_audit_tx(
                connection, incident_id, actor.user_id, actor.role,
                "reopen", entity["status"], next_status,
                {"reasons": plan["reasons"], "affected_assets": plan["affected_assets"]},
            )
            events.append({
                "incident_id": incident_id,
                "status": updated["status"],
                "reasons": plan["reasons"],
                "affected_assets": plan["affected_assets"],
            })
        return events

    # ---- duplicate / retry views ----------------------------------------

    def get_batch(self, batch_id):
        batch = self.repository.get_offline_batch(batch_id)
        if not batch:
            return None
        return batch

    def get_versions(self, record_id):
        return self.repository.list_offline_versions(record_id)

    def _duplicate_result(self, actor, batch_id, stored, records):
        """The late submitter sees the committed result plus a diff."""
        summary = dict(stored.get("summary") or {})
        summary["batch_id"] = batch_id
        summary["status"] = stored["status"]
        summary["already_submitted"] = True
        summary["submitted_by"] = stored["submitted_by"]
        summary["diff"] = self._diff(records or [])
        if stored["status"] == "failed":
            summary["errors"] = stored["errors"]
        return summary

    def _diff(self, records):
        """Compare the late submission with stored canonical versions and
        with current telemetry revisions; nothing here mutates state."""
        diffs = []
        all_telemetry = self.repository.list_entities(kind="telemetry")
        latest = {}
        for telemetry in all_telemetry:
            key = (telemetry["data"].get("asset_id"), telemetry["data"].get("metric"))
            if int(telemetry["data"].get("revision", 0) or 0) > int(
                latest.get(key, {}).get("revision", 0) or 0
            ):
                latest[key] = {"revision": int(telemetry["data"].get("revision", 0) or 0),
                               "entity_id": telemetry["id"]}

        for index, raw in enumerate(records if isinstance(records, list) else []):
            if not isinstance(raw, dict):
                continue
            kind = str(raw.get("kind", "recovery")).strip()
            record_id = str(raw.get("record_id") or "").strip()
            if not record_id:
                continue
            if kind == "telemetry":
                key = (str(raw.get("asset_id") or "").strip(), str(raw.get("metric") or "").strip())
                current = latest.get(key)
                try:
                    incoming_rev = int(raw.get("revision"))
                except (TypeError, ValueError):
                    incoming_rev = None
                if not current or incoming_rev is None:
                    change = "unverifiable"
                elif incoming_rev == current["revision"]:
                    change = "same"
                elif incoming_rev < current["revision"]:
                    change = "behind"
                else:
                    change = "newer"
                diffs.append({
                    "index": index, "record_id": record_id, "entry_type": "telemetry",
                    "change": change,
                    "incoming_revision": incoming_rev,
                    "merged_revision": current["revision"] if current else None,
                    "entity_id": current["entity_id"] if current else None,
                })
                continue

            versions = self.repository.list_offline_versions(record_id)
            canonical = next((v for v in versions if v["is_canonical"]), None)
            incoming = recovery_content(raw)
            incoming_hash = content_hash(incoming)
            if not versions:
                change = "missing"
                fields = []
            else:
                existing = canonical["payload"] if canonical else versions[0]["payload"]
                fields = []
                keys = set(existing) | set(incoming)
                for key in sorted(keys):
                    if existing.get(key) != incoming.get(key):
                        fields.append({
                            "field": key,
                            "merged": existing.get(key),
                            "incoming": incoming.get(key),
                        })
                same_recorded = any(v["recorded_at"] == str(raw.get("recorded_at") or "").strip()
                                    and v["content_hash"] == incoming_hash for v in versions)
                change = "same" if same_recorded and not fields else ("changed" if fields else "same")
            diffs.append({
                "index": index, "record_id": record_id, "entry_type": "recovery",
                "change": change, "field_differences": fields,
                "canonical_source": canonical["source"] if canonical else None,
                "canonical_recorded_at": canonical["recorded_at"] if canonical else None,
                "version_count": len(versions),
            })
        return diffs
