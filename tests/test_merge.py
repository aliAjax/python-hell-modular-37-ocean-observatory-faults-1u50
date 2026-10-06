import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, BatchValidationError, ConflictError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class MergeTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(SQLiteRepository(Path(self.tmp.name) / "test.db"), RuleEngine())
        self.admin = Actor("admin", "admin")
        self.engineer = Actor("engineer", "engineer")

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, kind, data):
        return self.service.create(self.admin, kind, data)

    def act(self, entity, action, data=None):
        return self.service.transition(self.admin, entity["id"], action, data or {})

    def setup_site(self):
        station = self.create("station", {"name": "OSN-1", "region": "East"})
        asset = self.create("asset", {
            "station_id": station["id"], "asset_type": "sensor",
            "serial_no": "S-1", "last_seen": "2026-10-01T00:00:00Z",
        })
        link = self.create("link", {
            "station_id": station["id"], "asset_id": asset["id"],
            "link_type": "fiber", "capacity": 100,
        })
        telemetry = self.create("telemetry", {
            "asset_id": asset["id"], "metric": "pressure", "value": 10,
            "observed_at": "2026-10-01T00:00:00Z", "revision": 1,
        })
        return station, asset, link, telemetry

    def resolve_incident(self, incident):
        for action in ("diagnose", "plan_recovery", "start_recovery"):
            incident = self.act(incident, action)
        ra = self.create("recovery_action", {
            "incident_id": incident["id"], "action_type": "remote_restart", "dedupe_key": "rk-" + incident["id"],
        })
        ra = self.act(ra, "approve")
        ra = self.act(ra, "start")
        ra = self.act(ra, "succeed", {"outcome": "ok"})
        gap = self.create("gap", {
            "incident_id": incident["id"], "start_at": "2026-10-01T00:00:00Z", "end_at": "2026-10-01T01:00:00Z",
        })
        gap = self.act(gap, "estimate", {"estimate": "x"})
        gap = self.act(gap, "fill", {"estimate": "y"})
        return self.act(incident, "resolve", {"summary": "restored"})

    def merge(self, records, batch_key=None):
        return self.service.merge_offline(self.engineer, records, batch_key=batch_key)


class ConflictVersionTest(MergeTestBase):
    def test_both_sides_changed_keeps_two_versions(self):
        station, asset, link, telemetry = self.setup_site()
        incident = self.create("incident", {
            "station_id": station["id"], "asset_id": asset["id"], "link_id": link["id"],
            "kind": "link_loss", "severity": "medium", "summary": "degrading",
        })
        # Online side advances the incident after the offline base was taken.
        self.act(incident, "diagnose")
        # Offline side also changed the same record, based on version 1.
        records = [{
            "source_id": "ship-02", "record_id": "c1", "kind": "incident",
            "entity_id": incident["id"],
            "data": {"severity": "critical", "summary": "worsening"},
            "recorded_at": "2026-10-06T00:00:00Z", "base_version": 1,
        }]
        result = self.merge(records, batch_key="conflict-1")
        self.assertEqual(len(result["conflicts"]), 1)
        # Current row keeps the online value; the offline value is a second version.
        current = self.service.get(incident["id"])
        self.assertEqual(current["data"]["severity"], "medium")
        versions = self.service.entity_versions(incident["id"])
        sources = {v["source"] for v in versions}
        self.assertIn("online", sources)
        self.assertIn("ship-02", sources)
        offline_version = [v for v in versions if v["source"] == "ship-02"][0]
        self.assertEqual(offline_version["data"]["severity"], "critical")

    def test_late_telemetry_kept_as_alternate(self):
        station, asset, link, telemetry = self.setup_site()
        records = [{
            "source_id": "ship-01", "record_id": "r-late", "kind": "telemetry",
            "entity_id": telemetry["id"],
            "data": {"asset_id": asset["id"], "metric": "pressure", "value": 9,
                     "observed_at": "2026-09-30T00:00:00Z", "revision": 1},
            "recorded_at": "2026-09-30T00:00:00Z",
        }]
        result = self.merge(records, batch_key="late-1")
        self.assertEqual(result["applied"][0]["outcome"], "late")
        # Stored telemetry is untouched.
        self.assertEqual(self.service.get(telemetry["id"])["data"]["revision"], 1)
        versions = self.service.entity_versions(telemetry["id"])
        self.assertTrue(any(v["source"] == "ship-01" for v in versions))


class ReopenTest(MergeTestBase):
    def test_higher_revision_telemetry_reopens_resolved_incident(self):
        station, asset, link, telemetry = self.setup_site()
        incident = self.create("incident", {
            "station_id": station["id"], "asset_id": asset["id"], "link_id": link["id"],
            "kind": "link_loss", "severity": "high", "summary": "no data",
        })
        incident = self.resolve_incident(incident)
        self.assertEqual(incident["status"], "resolved")
        # Asset fails after the incident was resolved; newer telemetry then
        # invalidates the resolution.
        self.act(asset, "fail", {"reason": "dry"})

        records = [{
            "source_id": "ship-01", "record_id": "r1", "kind": "telemetry",
            "entity_id": telemetry["id"],
            "data": {"asset_id": asset["id"], "metric": "pressure", "value": 12,
                     "observed_at": "2026-10-05T00:00:00Z", "revision": 3},
            "revision": 3, "recorded_at": "2026-10-05T00:00:00Z",
        }]
        result = self.merge(records, batch_key="reopen-1")
        self.assertEqual(len(result["reopened_incidents"]), 1)
        reopened = result["reopened_incidents"][0]
        self.assertEqual(reopened["reason"], "higher revision telemetry")
        self.assertEqual(reopened["from_status"], "resolved")
        self.assertEqual(reopened["to_status"], "open")
        affected_assets = {a["asset_id"] for a in reopened["affected_assets"]}
        self.assertIn(asset["id"], affected_assets)
        self.assertEqual(self.service.get(incident["id"])["status"], "open")

    def test_offline_record_later_than_resolution_reopens(self):
        station, asset, link, telemetry = self.setup_site()
        incident = self.create("incident", {
            "station_id": station["id"], "asset_id": asset["id"], "link_id": link["id"],
            "kind": "link_loss", "severity": "high", "summary": "no data",
        })
        incident = self.resolve_incident(incident)
        self.assertEqual(incident["status"], "resolved")

        # Lower revision telemetry (so revision cannot be the trigger), but
        # recorded after the incident was resolved.
        records = [{
            "source_id": "ship-01", "record_id": "r2", "kind": "telemetry",
            "entity_id": telemetry["id"],
            "data": {"asset_id": asset["id"], "metric": "pressure", "value": 9,
                     "observed_at": "2026-10-07T00:00:00Z", "revision": 1},
            "recorded_at": "2026-10-07T00:00:00Z",
        }]
        result = self.merge(records, batch_key="reopen-2")
        self.assertEqual(len(result["reopened_incidents"]), 1)
        self.assertEqual(result["reopened_incidents"][0]["reason"], "offline record later than resolution")
        self.assertEqual(self.service.get(incident["id"])["status"], "open")


class BatchValidationTest(MergeTestBase):
    def test_one_invalid_record_aborts_whole_batch(self):
        station, asset, link, telemetry = self.setup_site()
        before = len(self.service.list("station"))
        records = [
            {"source_id": "ship-03", "record_id": "b1", "kind": "station",
             "data": {"name": "X", "region": "R"}, "recorded_at": "2026-10-06T00:00:00Z"},
            {"source_id": "ship-03", "record_id": "b2", "kind": "telemetry",
             "data": {"asset_id": "missing", "metric": "p", "value": 1,
                      "observed_at": "x", "revision": 1},
             "recorded_at": "2026-10-06T00:00:00Z"},
        ]
        with self.assertRaises(BatchValidationError) as ctx:
            self.merge(records, batch_key="bad-1")
        self.assertEqual(ctx.exception.index, 1)
        self.assertEqual(ctx.exception.record_id, "b2")
        # Nothing from the batch was persisted.
        self.assertEqual(len(self.service.list("station")), before)

    def test_retry_after_failure_succeeds(self):
        station, asset, link, telemetry = self.setup_site()
        bad = [{
            "source_id": "ship-04", "record_id": "g1", "kind": "telemetry",
            "data": {"asset_id": "missing", "metric": "p", "value": 1,
                     "observed_at": "x", "revision": 1},
            "recorded_at": "2026-10-06T00:00:00Z",
        }]
        with self.assertRaises(BatchValidationError):
            self.merge(bad, batch_key="retry-1")
        # Fix the record and retry with the same key.
        good = [{
            "source_id": "ship-04", "record_id": "g1", "kind": "telemetry",
            "data": {"asset_id": asset["id"], "metric": "p", "value": 1,
                     "observed_at": "2026-10-06T00:00:00Z", "revision": 1},
            "recorded_at": "2026-10-06T00:00:00Z",
        }]
        result = self.merge(good, batch_key="retry-1")
        self.assertEqual(len(result["applied"]), 1)
        self.assertEqual(result["applied"][0]["outcome"], "created")


class IdempotencyTest(MergeTestBase):
    def test_same_batch_applied_once(self):
        station, asset, link, telemetry = self.setup_site()
        records = [{
            "source_id": "ship-05", "record_id": "n1", "kind": "station",
            "data": {"name": "OSN-2", "region": "West"}, "recorded_at": "2026-10-06T00:00:00Z",
        }]
        first = self.merge(records, batch_key="idem-1")
        second = self.merge(records, batch_key="idem-1")
        self.assertEqual(first["batch_key"], second["batch_key"])
        self.assertTrue(second.get("already_merged"))
        # Only one station with that name exists.
        stations = [s for s in self.service.list("station") if s["data"].get("name") == "OSN-2"]
        self.assertEqual(len(stations), 1)

    def test_concurrent_submission_sees_result_and_diff(self):
        station, asset, link, telemetry = self.setup_site()
        records = [{
            "source_id": "ship-06", "record_id": "n2", "kind": "station",
            "data": {"name": "OSN-3", "region": "North"}, "recorded_at": "2026-10-06T00:00:00Z",
        }]
        first = self.merge(records, batch_key="conc-1")
        second = self.merge(records, batch_key="conc-1")
        self.assertFalse(first.get("already_merged"))
        self.assertTrue(second.get("already_merged"))
        self.assertEqual(len(second["diff"]), 1)
        self.assertEqual(second["diff"][0]["status"], "already_merged")
        # The second submission still reports the merged result.
        self.assertEqual(second["applied"][0]["entity_id"], first["applied"][0]["entity_id"])


class CreateViaMergeTest(MergeTestBase):
    def test_merge_creates_new_entity(self):
        records = [{
            "source_id": "ship-07", "record_id": "s1", "kind": "station",
            "data": {"name": "OSN-4", "region": "South"}, "recorded_at": "2026-10-06T00:00:00Z",
        }]
        result = self.merge(records, batch_key="create-1")
        self.assertEqual(result["applied"][0]["outcome"], "created")
        station = self.service.get(result["applied"][0]["entity_id"])
        self.assertEqual(station["data"]["name"], "OSN-4")
        versions = self.service.entity_versions(station["id"])
        self.assertEqual(versions[0]["source"], "ship-07")


if __name__ == "__main__":
    unittest.main()
