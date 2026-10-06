import tempfile
import threading
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, MergeValidationError, PermissionDenied
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class OfflineMergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.operator = Actor("op-1", "operator")
        self.field = Actor("eng-boat", "field")

    def tearDown(self):
        self.tmp.cleanup()

    def _station_asset(self):
        station = self.service.create(self.admin, "station", {"name": "OSN-01", "region": "East"})
        asset = self.service.create(self.admin, "asset", {
            "station_id": station["id"], "asset_type": "sensor",
            "serial_no": "S-1", "last_seen": "2026-10-01T08:00:00Z",
        })
        return station, asset

    def _resolved_incident(self, asset, station=None):
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"] if station else None,
            "asset_id": asset["id"], "kind": "link_loss",
            "severity": "high", "summary": "offline link",
        })
        for action in ("diagnose", "plan_recovery", "start_recovery", "resolve"):
            incident = self.service.transition(
                self.admin, incident["id"], action,
                {"summary": "restored"} if action == "resolve" else {},
            )
        self.assertEqual(incident["status"], "resolved")
        return incident

    def test_keeps_two_versions_when_both_sides_edited_same_record(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "link_loss", "severity": "high", "summary": "x",
        })
        shore_record = {
            "record_id": "REC-1", "kind": "recovery", "source": "shore",
            "recorded_at": "2026-10-02T09:00:00Z",
            "incident_id": incident["id"], "summary": "shore edit",
        }
        vessel_record = {
            "record_id": "REC-1", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T10:30:00Z",
            "incident_id": incident["id"], "summary": "boat edit",
        }
        first = self.service.merge_offline(self.admin, [shore_record])
        self.assertEqual(first["records"][0]["result"], "canonical")
        second = self.service.merge_offline(self.admin, [vessel_record])
        self.assertEqual(second["records"][0]["result"], "conflict")

        versions = self.service.offline_versions("REC-1")
        self.assertEqual(len(versions), 2)
        canonical = [v for v in versions if v["is_canonical"]]
        self.assertEqual(len(canonical), 1)
        self.assertEqual(canonical[0]["payload"]["summary"], "shore edit")
        # The late arrival did not overwrite anything.
        self.assertEqual(canonical[0]["source"], "shore")
        conflict = [v for v in versions if not v["is_canonical"]][0]
        self.assertEqual(conflict["payload"]["summary"], "boat edit")
        self.assertEqual(conflict["source"], "vessel")

    def test_exact_re_submission_is_idempotent_duplicate(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "loss", "severity": "low", "summary": "x",
        })
        record = {
            "record_id": "REC-2", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:00:00Z",
            "incident_id": incident["id"], "summary": "logged twice",
        }
        self.service.merge_offline(self.admin, [record], batch_id="submit-1")
        again = self.service.merge_offline(self.admin, [dict(record)], batch_id="submit-2")
        self.assertEqual(again["records"][0]["result"], "duplicate")
        self.assertEqual(len(self.service.offline_versions("REC-2")), 1)

    def test_higher_telemetry_revision_reopens_resolved_incident_with_assets(self):
        station, asset = self._station_asset()
        telemetry = self.service.create(self.admin, "telemetry", {
            "asset_id": asset["id"], "metric": "pressure", "value": 10,
            "observed_at": "2026-10-01T08:00:00Z", "revision": 1,
        })
        incident = self._resolved_incident(asset, station)
        self.assertIn(asset["id"] + "\0pressure", incident["data"]["resolved_revisions"])

        late = {
            "record_id": "TEL-1", "kind": "telemetry", "source": "vessel",
            "recorded_at": "2026-10-02T12:00:00Z",
            "asset_id": asset["id"], "metric": "pressure", "value": 2.5,
            "revision": 2, "observed_at": "2026-10-02T11:59:00Z",
        }
        result = self.service.merge_offline(self.field, [late])
        self.assertEqual(len(result["incidents_reopened"]), 1)
        event = result["incidents_reopened"][0]
        self.assertEqual(event["incident_id"], incident["id"])
        self.assertEqual(event["status"], "open")
        self.assertEqual(event["affected_assets"], [asset["id"]])
        self.assertTrue(any("telemetry_revision" in r for r in event["reasons"]))

        reloaded = self.service.get(incident["id"])
        self.assertEqual(reloaded["status"], "open")
        self.assertEqual(reloaded["data"]["reopen_count"], 1)
        updated_tel = self.service.get(telemetry["id"])
        self.assertEqual(updated_tel["data"]["revision"], 2)
        self.assertTrue(updated_tel["data"]["late_revision"])

    def test_offline_record_after_resolution_reopens_incident(self):
        station, asset = self._station_asset()
        incident = self._resolved_incident(asset, station)

        late_recovery = {
            "record_id": "REC-3", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-07T06:00:00Z",
            "incident_id": incident["id"],
            "asset_id": asset["id"],
            "summary": "boat found the link dropped again after shore closed it",
        }
        result = self.service.merge_offline(self.field, [late_recovery])
        event = result["incidents_reopened"][0]
        self.assertEqual(event["incident_id"], incident["id"])
        self.assertTrue(any("offline_record_after_resolution" in r for r in event["reasons"]))
        self.assertEqual(event["affected_assets"], [asset["id"]])
        self.assertEqual(self.service.get(incident["id"])["status"], "open")

    def test_record_before_resolution_does_not_reopen(self):
        station, asset = self._station_asset()
        incident = self._resolved_incident(asset, station)
        resolved_at = incident["data"]["resolved_at"]
        on_time = {
            "record_id": "REC-4", "kind": "recovery", "source": "vessel",
            "recorded_at": resolved_at,
            "incident_id": incident["id"], "summary": "timely log",
        }
        result = self.service.merge_offline(self.field, [on_time])
        self.assertEqual(result["incidents_reopened"], [])
        self.assertEqual(self.service.get(incident["id"])["status"], "resolved")

    def test_batch_failure_stores_nothing_and_locates_each_error(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "loss", "severity": "low", "summary": "x",
        })
        good = {
            "record_id": "OK-1", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:00:00Z",
            "incident_id": incident["id"], "summary": "fine",
        }
        bad_unknown_incident = {
            "record_id": "BAD-1", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:05:00Z",
            "incident_id": "nope", "summary": "ghost",
        }
        bad_time = {
            "record_id": "BAD-2", "kind": "recovery", "source": "vessel",
            "recorded_at": "not-a-time",
            "incident_id": incident["id"], "summary": "broken clock",
        }
        with self.assertRaises(MergeValidationError) as caught:
            self.service.merge_offline(self.field, [good, bad_unknown_incident, bad_time],
                                       batch_id="ship-batch-7")
        exc = caught.exception
        self.assertEqual(exc.batch_id, "ship-batch-7")
        locations = {(e["index"], e["field"]) for e in exc.errors}
        self.assertIn((1, "incident_id"), locations)
        self.assertIn((2, "recorded_at"), locations)

        # Nothing from the batch entered storage, including the valid record.
        self.assertEqual(self.service.offline_versions("OK-1"), [])
        stored = self.service.offline_batch("ship-batch-7")
        self.assertEqual(stored["status"], "failed")
        self.assertEqual(len(stored["errors"]), 2)

    def test_failed_batch_can_be_retried_with_same_batch_id(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "loss", "severity": "low", "summary": "x",
        })
        broken = [{
            "record_id": "RETRY-1", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:05:00Z",
            "incident_id": "missing", "summary": "typo",
        }]
        with self.assertRaises(MergeValidationError):
            self.service.merge_offline(self.field, broken, batch_id="ship-batch-8")
        fixed = [dict(broken[0], incident_id=incident["id"])]
        result = self.service.merge_offline(self.field, fixed, batch_id="ship-batch-8")
        self.assertEqual(result["status"], "merged")
        self.assertEqual(self.service.offline_batch("ship-batch-8")["status"], "merged")
        self.assertEqual(len(self.service.offline_versions("RETRY-1")), 1)

    def test_stale_telemetry_revision_is_located_and_rejected(self):
        station, asset = self._station_asset()
        self.service.create(self.admin, "telemetry", {
            "asset_id": asset["id"], "metric": "pressure", "value": 10,
            "observed_at": "2026-10-01T08:00:00Z", "revision": 5,
        })
        stale = {
            "record_id": "TEL-OLD", "kind": "telemetry", "source": "vessel",
            "recorded_at": "2026-10-02T09:00:00Z",
            "asset_id": asset["id"], "metric": "pressure", "value": 9,
            "revision": 3, "observed_at": "2026-10-02T08:59:00Z",
        }
        with self.assertRaises(MergeValidationError) as caught:
            self.service.merge_offline(self.field, [stale])
        self.assertEqual(caught.exception.errors[0]["index"], 0)
        self.assertEqual(caught.exception.errors[0]["field"], "revision")

    def test_concurrent_identical_batch_first_wins_later_sees_diff(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "loss", "severity": "low", "summary": "x",
        })
        records = [{
            "record_id": "CONC-1", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:00:00Z",
            "incident_id": incident["id"], "summary": "both operators submit this",
        }]
        results = []
        errors = []

        def submit(actor):
            try:
                results.append(self.service.merge_offline(actor, [dict(r) for r in records],
                                                          batch_id="concurrent-batch"))
            except Exception as exc:  # pragma: no cover - surfaced on failure
                errors.append(exc)

        first = threading.Thread(target=submit, args=(Actor("duty-a", "operator"),))
        second = threading.Thread(target=submit, args=(Actor("duty-b", "operator"),))
        first.start()
        second.start()
        first.join()
        second.join()
        self.assertFalse(errors)
        self.assertEqual(len(results), 2)
        fresh = [r for r in results if not r.get("already_submitted")]
        duplicate = [r for r in results if r.get("already_submitted")]
        self.assertEqual(len(fresh), 1)
        self.assertEqual(len(duplicate), 1)
        self.assertEqual(duplicate[0]["status"], "merged")
        diff_entry = duplicate[0]["diff"][0]
        self.assertEqual(diff_entry["record_id"], "CONC-1")
        self.assertEqual(diff_entry["change"], "same")
        # Exactly one canonical version exists despite two submissions.
        versions = self.service.offline_versions("CONC-1")
        self.assertEqual(len(versions), 1)
        winners = {r["submitted_by"] for r in results if not r.get("already_submitted")}
        self.assertTrue(winners.issubset({"duty-a", "duty-b"}))

    def test_concurrent_different_content_one_batch_each_no_overwrite(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "loss", "severity": "low", "summary": "x",
        })
        a_records = [{
            "record_id": "SAME-REC", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:00:00Z",
            "incident_id": incident["id"], "summary": "operator A note",
        }]
        b_records = [{
            "record_id": "SAME-REC", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:10:00Z",
            "incident_id": incident["id"], "summary": "operator B note",
        }]
        results = {}
        barrier = threading.Barrier(2)

        def submit(name, actor, payload):
            barrier.wait()
            results[name] = self.service.merge_offline(actor, payload)

        ta = threading.Thread(target=submit, args=("a", Actor("duty-a", "operator"), a_records))
        tb = threading.Thread(target=submit, args=("b", Actor("duty-b", "operator"), b_records))
        ta.start()
        tb.start()
        ta.join()
        tb.join()
        decisions = {name: result["records"][0]["result"] for name, result in results.items()}
        self.assertIn("canonical", decisions.values())
        self.assertIn("conflict", decisions.values())
        versions = self.service.offline_versions("SAME-REC")
        self.assertEqual(len(versions), 2)
        self.assertEqual(len([v for v in versions if v["is_canonical"]]), 1)

    def test_late_submitter_sees_field_level_diff(self):
        station, asset = self._station_asset()
        incident = self.service.create(self.admin, "incident", {
            "station_id": station["id"], "asset_id": asset["id"],
            "kind": "loss", "severity": "low", "summary": "x",
        })
        records = [{
            "record_id": "DIFF-1", "kind": "recovery", "source": "vessel",
            "recorded_at": "2026-10-02T09:00:00Z",
            "incident_id": incident["id"], "summary": "merged note",
        }]
        self.service.merge_offline(self.operator, [dict(r) for r in records],
                                   batch_id="diff-batch")
        late_copy = [dict(records[0], summary="different note")]
        view = self.service.merge_offline(Actor("duty-b", "operator"), late_copy,
                                          batch_id="diff-batch")
        self.assertTrue(view["already_submitted"])
        diff_entry = view["diff"][0]
        self.assertEqual(diff_entry["change"], "changed")
        field_diff = {d["field"]: d for d in diff_entry["field_differences"]}
        self.assertEqual(field_diff["summary"]["merged"], "merged note")
        self.assertEqual(field_diff["summary"]["incoming"], "different note")

    def test_viewer_cannot_merge(self):
        with self.assertRaises(PermissionDenied):
            self.service.merge_offline(Actor("eyes", "viewer"), [])

    def test_closed_incident_reopens_too_and_lists_assets(self):
        station, asset = self._station_asset()
        telemetry = self.service.create(self.admin, "telemetry", {
            "asset_id": asset["id"], "metric": "pressure", "value": 10,
            "observed_at": "2026-10-01T08:00:00Z", "revision": 1,
        })
        incident = self._resolved_incident(asset, station)
        incident = self.service.transition(self.admin, incident["id"], "close")
        self.assertEqual(incident["status"], "closed")
        late = {
            "record_id": "TEL-CLOSED", "kind": "telemetry", "source": "vessel",
            "recorded_at": "2026-10-06T00:00:00Z",
            "asset_id": asset["id"], "metric": "pressure", "value": 0,
            "revision": 9, "observed_at": "2026-10-05T23:59:00Z",
        }
        result = self.service.merge_offline(self.field, [late])
        self.assertEqual(result["incidents_reopened"][0]["status"], "open")
        self.assertEqual(result["incidents_reopened"][0]["affected_assets"], [asset["id"]])

    def test_lower_revision_in_normal_flow_still_rejected(self):
        # Regression: existing revision ordering rules remain intact.
        station, asset = self._station_asset()
        telemetry = self.service.create(self.admin, "telemetry", {
            "asset_id": asset["id"], "metric": "p", "value": 1,
            "observed_at": "2026-10-01T08:00:00Z", "revision": 2,
        })
        with self.assertRaises(ConflictError):
            self.service.transition(self.admin, telemetry["id"], "revise", {"revision": 1})


if __name__ == "__main__":
    unittest.main()
