import hashlib
import json
from datetime import datetime, timezone

from .domain import ConflictError, InvalidTransition, PermissionDenied, ValidationError

OFFLINE_SOURCES = ("vessel", "shore")
RECOVERY_PAYLOAD_FIELDS = ("summary", "result", "notes")


def parse_ts(value, field):
    """Parse an ISO-8601 timestamp; naive values are assumed to be UTC."""
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(field + " must be an ISO-8601 timestamp")
    text = value.strip()
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00" if text.endswith("Z") else text)
    except ValueError:
        raise ValidationError(field + " must be an ISO-8601 timestamp")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def content_hash(payload):
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def recovery_content(entry):
    return {key: entry[key] for key in RECOVERY_PAYLOAD_FIELDS if entry.get(key) is not None}


def _require(data, fields):
    for field in fields:
        value = data.get(field)
        if value is None or value == "" or value == [] or value == {}:
            raise ValidationError("missing required field: " + field)


def _ensure_role(actor, allowed):
    if "*" not in allowed and actor.role not in allowed:
        raise PermissionDenied("role %s is not allowed here" % actor.role)


def _all(lookup, kind):
    return lookup(kind, "*", None) or [] if lookup else []


def _find_one(lookup, kind, field, value):
    rows = lookup(kind, field, value) or [] if lookup else []
    return rows[0] if rows else None


def _number(value, field):
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ValidationError(field + " must be numeric")
    if number < 0:
        raise ValidationError(field + " must be non-negative")
    return number


def _validate_asset(data, lookup):
    if not _find_one(lookup, "station", "id", data.get("station_id")):
        raise ValidationError("asset requires station")
    if data.get("clock_offset_seconds") not in (None, ""):
        _number(data.get("clock_offset_seconds"), "clock_offset_seconds")


def _validate_link(data, lookup):
    if not _find_one(lookup, "station", "id", data.get("station_id")):
        raise ValidationError("link requires station")
    if not _find_one(lookup, "asset", "id", data.get("asset_id")):
        raise ValidationError("link requires asset")
    _number(data.get("capacity"), "capacity")


def _validate_telemetry(data, lookup):
    asset = _find_one(lookup, "asset", "id", data.get("asset_id"))
    if not asset:
        raise ValidationError("telemetry requires asset")
    _number(data.get("value"), "value")
    try:
        revision = int(data.get("revision"))
    except (TypeError, ValueError):
        raise ValidationError("revision must be an integer")
    if revision < 1:
        raise ValidationError("revision must be positive")
    for item in _all(lookup, "telemetry"):
        if item["data"].get("asset_id") == data.get("asset_id") and item["data"].get("metric") == data.get("metric"):
            if int(item["data"].get("revision", 0)) >= revision:
                raise ConflictError("telemetry revision must increase")


def _validate_incident(data, lookup):
    if not data.get("station_id") and not data.get("asset_id") and not data.get("link_id"):
        raise ValidationError("incident requires station_id, asset_id or link_id")
    if data.get("severity") not in ("low", "medium", "high", "critical"):
        raise ValidationError("invalid incident severity")
    for item in _all(lookup, "incident"):
        if item["status"] in ("open", "diagnosing", "recovery_planned", "recovering") and item["data"].get("asset_id") == data.get("asset_id") and item["data"].get("kind") == data.get("kind"):
            raise ConflictError("active incident already exists for asset and kind")


def _validate_action(data, lookup):
    incident = _find_one(lookup, "incident", "id", data.get("incident_id"))
    if not incident or incident["status"] in ("resolved", "closed"):
        raise ValidationError("recovery action requires an active incident")
    if data.get("action_type") not in ("remote_restart", "switch_backup", "firmware_rollback", "dispatch_mission"):
        raise ValidationError("invalid action_type")
    key = data.get("dedupe_key")
    for item in _all(lookup, "recovery_action"):
        if item["data"].get("dedupe_key") == key and item["status"] not in ("succeeded", "failed", "cancelled"):
            raise ConflictError("active recovery action already exists for dedupe_key")


def _validate_mission(data, lookup):
    if not _find_one(lookup, "station", "id", data.get("station_id")):
        raise ValidationError("mission requires station")
    if not data.get("window_start") or not data.get("window_end"):
        raise ValidationError("mission window is required")


def _validate_gap(data, lookup):
    if not _find_one(lookup, "incident", "id", data.get("incident_id")):
        raise ValidationError("data gap requires incident")
    if not data.get("start_at") or not data.get("end_at"):
        raise ValidationError("gap window is required")


def _revise_telemetry(actor, entity, data, lookup):
    try:
        new_revision = int(data.get("revision"))
    except (TypeError, ValueError):
        raise ValidationError("revision must be an integer")
    if new_revision <= int(entity["data"].get("revision", 0)):
        raise ConflictError("late revision must increase revision number")
    return {"late_revision": True, "revised_by": actor.user_id}


def _resolve_incident(actor, entity, data, lookup):
    actions = [a for a in _all(lookup, "recovery_action") if a["data"].get("incident_id") == entity["id"] and a["status"] not in ("succeeded", "failed", "cancelled")]
    if actions:
        raise ConflictError("incident cannot resolve while recovery actions are active")
    gaps = [g for g in _all(lookup, "gap") if g["data"].get("incident_id") == entity["id"] and g["status"] not in ("filled", "accepted", "closed")]
    if gaps:
        raise ConflictError("incident cannot resolve while data gaps remain open")
    assets = [a for a in _all(lookup, "asset") if a["status"] in ("faulty", "offline", "rebooting")]
    if entity["data"].get("asset_id") and any(a["id"] == entity["data"].get("asset_id") for a in assets):
        raise ConflictError("affected asset is still unavailable")
    snapshot = {}
    for telemetry in _all(lookup, "telemetry"):
        rev = int(telemetry["data"].get("revision", 0) or 0)
        if rev <= 0:
            continue
        key = telemetry["data"].get("asset_id") + "\0" + str(telemetry["data"].get("metric"))
        if int(snapshot.get(key, 0)) < rev:
            snapshot[key] = rev
    return {
        "resolved_by": actor.user_id,
        "resolved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "resolved_revisions": snapshot,
    }


def _reopen_incident(actor, entity, data, lookup):
    _require(data, ("reason", "affected_assets"))
    if not isinstance(data["affected_assets"], list) or not data["affected_assets"]:
        raise ValidationError("affected_assets must be a non-empty list")
    return {
        "reopened_by": actor.user_id,
        "reopen_count": int(entity["data"].get("reopen_count", 0)) + 1,
    }


def _complete_action(actor, entity, data, lookup):
    if not data.get("outcome"):
        raise ValidationError("outcome is required")
    return {"completed_by": actor.user_id}


def _complete_mission(actor, entity, data, lookup):
    if not data.get("report"):
        raise ValidationError("report is required")
    return {"completed_by": actor.user_id}


class RuleEngine:
    ALIASES = {
        "stations": "station", "assets": "asset", "links": "link", "telemetries": "telemetry",
        "incidents": "incident", "recovery_actions": "recovery_action", "missions": "mission",
        "gaps": "gap",
    }
    INITIAL_STATUS = {
        "station": "online", "asset": "healthy", "link": "up", "telemetry": "current",
        "incident": "open", "recovery_action": "proposed", "mission": "planned", "gap": "open",
    }
    TRANSITIONS = {
        "station": {
            "degrade": (("online",), "degraded"),
            "go_offline": (("online", "degraded"), "offline"),
            "resume": (("degraded", "offline"), "online"),
        },
        "asset": {
            "degrade": (("healthy",), "degraded"),
            "fail": (("healthy", "degraded"), "faulty"),
            "start_reboot": (("faulty",), "rebooting"),
            "finish_reboot": (("rebooting",), "healthy"),
            "restore": (("faulty",), "healthy"),
        },
        "link": {
            "degrade": (("up",), "degraded"),
            "fail": (("up", "degraded"), "down"),
            "activate_backup": (("down", "degraded"), "backup_active"),
            "restore": (("down", "backup_active", "degraded"), "up"),
        },
        "telemetry": {
            "mark_stale": (("current",), "stale"),
            "quarantine": (("current", "stale"), "quarantined"),
            "revise": (("current", "stale", "quarantined"), "current"),
            "clear": (("stale",), "current"),
        },
        "incident": {
            "diagnose": (("open",), "diagnosing"),
            "plan_recovery": (("diagnosing",), "recovery_planned"),
            "start_recovery": (("recovery_planned",), "recovering"),
            "resolve": (("recovering",), "resolved"),
            "close": (("resolved",), "closed"),
            "reopen": (("resolved", "closed"), "open"),
        },
        "recovery_action": {
            "approve": (("proposed",), "approved"),
            "start": (("approved",), "running"),
            "succeed": (("running",), "succeeded"),
            "fail": (("running",), "failed"),
            "cancel": (("proposed", "approved", "running"), "cancelled"),
        },
        "mission": {
            "approve": (("planned",), "approved"),
            "depart": (("approved",), "underway"),
            "complete": (("underway",), "completed"),
            "cancel": (("planned", "approved", "underway"), "cancelled"),
        },
        "gap": {
            "estimate": (("open",), "estimated"),
            "fill": (("estimated",), "filled"),
            "accept": (("filled", "open"), "accepted"),
        },
    }
    CREATE_REQUIRED = {
        "station": ("name", "region"),
        "asset": ("station_id", "asset_type", "serial_no", "last_seen"),
        "link": ("station_id", "asset_id", "link_type", "capacity"),
        "telemetry": ("asset_id", "metric", "value", "observed_at", "revision"),
        "incident": ("kind", "severity", "summary"),
        "recovery_action": ("incident_id", "action_type", "dedupe_key"),
        "mission": ("station_id", "purpose", "window_start", "window_end"),
        "gap": ("incident_id", "start_at", "end_at"),
    }
    ACTION_REQUIRED = {
        ("station", "degrade"): ("reason",),
        ("link", "fail"): ("reason",),
        ("telemetry", "revise"): ("revision",),
        ("recovery_action", "succeed"): ("outcome",),
        ("mission", "complete"): ("report",),
        ("gap", "fill"): ("estimate",),
        ("incident", "resolve"): ("summary",),
    }
    CREATE_ROLES = {
        "station": ("admin", "engineer"),
        "asset": ("admin", "engineer"),
        "link": ("admin", "engineer"),
        "telemetry": ("admin", "operator", "engineer"),
        "incident": ("admin", "operator", "engineer"),
        "recovery_action": ("admin", "operator", "engineer"),
        "mission": ("admin", "engineer"),
        "gap": ("admin", "operator", "engineer"),
    }
    ROLE_ACTIONS = {
        "degrade": ("admin", "engineer", "operator"),
        "go_offline": ("admin", "engineer", "operator"),
        "resume": ("admin", "engineer", "operator"),
        "fail": ("admin", "engineer", "operator"),
        "start_reboot": ("admin", "engineer", "operator"),
        "finish_reboot": ("admin", "engineer", "operator"),
        "restore": ("admin", "engineer", "operator"),
        "activate_backup": ("admin", "engineer", "operator"),
        "mark_stale": ("admin", "operator", "engineer"),
        "quarantine": ("admin", "engineer", "operator"),
        "revise": ("admin", "operator", "engineer"),
        "clear": ("admin", "operator", "engineer"),
        "diagnose": ("admin", "operator", "engineer"),
        "plan_recovery": ("admin", "operator", "engineer"),
        "start_recovery": ("admin", "operator", "engineer"),
        "resolve": ("admin", "engineer"),
        "close": ("admin", "engineer"),
        "reopen": ("admin", "engineer", "operator", "field"),
        "approve": ("admin", "engineer"),
        "start": ("admin", "engineer", "operator"),
        "succeed": ("admin", "engineer", "operator"),
        "cancel": ("admin", "engineer", "operator"),
        "depart": ("admin", "engineer", "operator"),
        "estimate": ("admin", "engineer", "operator"),
        "fill": ("admin", "engineer", "operator"),
        "accept": ("admin", "engineer", "operator"),
    }
    CUSTOM_CREATE = {
        "asset": lambda a, d, l: _validate_asset(d, l),
        "link": lambda a, d, l: _validate_link(d, l),
        "telemetry": lambda a, d, l: _validate_telemetry(d, l),
        "incident": lambda a, d, l: _validate_incident(d, l),
        "recovery_action": lambda a, d, l: _validate_action(d, l),
        "mission": lambda a, d, l: _validate_mission(d, l),
        "gap": lambda a, d, l: _validate_gap(d, l),
    }
    CUSTOM_TRANSITIONS = {
        ("telemetry", "revise"): _revise_telemetry,
        ("incident", "resolve"): _resolve_incident,
        ("incident", "reopen"): _reopen_incident,
        ("recovery_action", "succeed"): _complete_action,
        ("mission", "complete"): _complete_mission,
    }

    def normalize_kind(self, kind):
        return self.ALIASES.get(kind, kind)

    def initial_status(self, kind, data=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        return self.INITIAL_STATUS[kind]

    def validate_create(self, actor, kind, data, lookup=None):
        kind = self.normalize_kind(kind)
        if kind not in self.INITIAL_STATUS:
            raise ValidationError("unknown kind: " + str(kind))
        _ensure_role(actor, self.CREATE_ROLES.get(kind, ("admin",)))
        _require(data, self.CREATE_REQUIRED.get(kind, ()))
        custom = self.CUSTOM_CREATE.get(kind)
        if custom:
            custom(actor, data, lookup)
        return dict(data)

    def validate_transition(self, actor, entity, action, data, lookup=None):
        kind = self.normalize_kind(entity["kind"])
        transition = self.TRANSITIONS.get(kind, {}).get(action)
        if not transition:
            raise InvalidTransition("unknown action %s for %s" % (action, kind))
        allowed_statuses, next_status = transition
        if entity["status"] not in allowed_statuses:
            raise InvalidTransition("cannot %s from status %s" % (action, entity["status"]))
        allowed = self.ROLE_ACTIONS.get((kind, action), self.ROLE_ACTIONS.get(action, ("admin",)))
        _ensure_role(actor, allowed)
        _require(data, self.ACTION_REQUIRED.get((kind, action), ()))
        custom = self.CUSTOM_TRANSITIONS.get((kind, action))
        extra = custom(actor, entity, data, lookup) if custom else {}
        patch = dict(data)
        if extra:
            patch.update(extra)
        return next_status, patch

    def validate_offline_batch(self, actor, records, lookup):
        """Structural and referential validation of one offline batch.

        Returns (entries, errors). Nothing here mutates state: entries are
        normalized records ready to merge, and every error carries the
        record position so the whole batch can be rejected as one.
        """
        if "*" not in self.MERGE_ROLES and actor.role not in self.MERGE_ROLES:
            raise PermissionDenied("role %s cannot merge offline records" % actor.role)
        entries = []
        errors = []
        seen_duplicates = set()
        seen_telemetry = {}
        stored_revision = {}
        for telemetry in _all(lookup, "telemetry"):
            series_key = (telemetry["data"].get("asset_id"), telemetry["data"].get("metric"))
            stored_revision[series_key] = max(
                int(telemetry["data"].get("revision", 0) or 0),
                stored_revision.get(series_key, 0),
            )
        if not isinstance(records, list):
            return entries, [{"index": None, "record_id": None, "field": "records",
                              "message": "records must be a list"}]

        def fail(index, record_id, field, message):
            errors.append({"index": index, "record_id": record_id, "field": field, "message": message})

        for index, raw in enumerate(records):
            record_id = raw.get("record_id") if isinstance(raw, dict) else None

            def local(field, message):
                fail(index, str(record_id) if record_id is not None else None, field, message)

            if not isinstance(raw, dict):
                local(None, "each record must be an object")
                continue
            rid = str(record_id or "").strip()
            if not rid:
                local("record_id", "record_id is required")
                continue
            kind = str(raw.get("kind", "recovery")).strip()
            if kind not in ("recovery", "telemetry"):
                local("kind", "kind must be recovery or telemetry")
                continue
            source = str(raw.get("source", "vessel")).strip()
            if source not in OFFLINE_SOURCES:
                local("source", "source must be vessel or shore")
                continue
            recorded = raw.get("recorded_at")
            try:
                recorded_at = parse_ts(recorded, "recorded_at")
            except ValidationError as exc:
                local("recorded_at", str(exc))
                continue

            if kind == "recovery":
                incident_id = str(raw.get("incident_id") or "").strip()
                incident = _find_one(lookup, "incident", "id", incident_id) if incident_id else None
                if not incident_id:
                    local("incident_id", "incident_id is required")
                elif not incident:
                    local("incident_id", "unknown incident: " + incident_id)
                asset_id = str(raw.get("asset_id") or "").strip() or None
                if not asset_id and incident:
                    asset_id = incident["data"].get("asset_id")
                if asset_id and not _find_one(lookup, "asset", "id", asset_id):
                    local("asset_id", "unknown asset: " + asset_id)
                payload = recovery_content(raw)
                if not payload:
                    local("summary", "at least one of summary/result/notes is required")
                if errors and any(e["index"] == index for e in errors):
                    continue
                content = content_hash(payload)
                duplicate_key = (rid, source, recorded_at.isoformat(), content)
                if duplicate_key in seen_duplicates:
                    local("record_id", "exact duplicate within batch")
                    continue
                seen_duplicates.add(duplicate_key)
                entries.append({
                    "entry_type": "recovery",
                    "record_id": rid,
                    "source": source,
                    "recorded_at": recorded_at.isoformat(),
                    "incident_id": incident_id,
                    "asset_id": asset_id,
                    "payload": payload,
                    "content_hash": content,
                })
                continue

            asset_id = str(raw.get("asset_id") or "").strip()
            if not asset_id:
                local("asset_id", "asset_id is required")
            elif not _find_one(lookup, "asset", "id", asset_id):
                local("asset_id", "unknown asset: " + asset_id)
            metric = str(raw.get("metric") or "").strip()
            if not metric:
                local("metric", "metric is required")
            try:
                value = _number(raw.get("value"), "value")
            except ValidationError as exc:
                local("value", str(exc))
                value = None
            try:
                revision = int(raw.get("revision"))
            except (TypeError, ValueError):
                local("revision", "revision must be an integer")
                revision = None
            else:
                if revision < 1:
                    local("revision", "revision must be positive")
            observed = raw.get("observed_at")
            try:
                observed_at = parse_ts(observed, "observed_at")
            except ValidationError:
                local("observed_at", "observed_at must be an ISO-8601 timestamp")
                observed_at = None
            if errors and any(e["index"] == index for e in errors):
                continue
            series_key = (asset_id, metric)
            if series_key not in stored_revision:
                local("metric", "no telemetry series exists for %s/%s" % (asset_id, metric))
                continue
            if revision <= stored_revision[series_key]:
                local("revision", "telemetry revision must be higher than stored %s"
                      % stored_revision[series_key])
                continue
            if series_key in seen_telemetry and seen_telemetry[series_key] >= revision:
                local("revision", "telemetry revisions for one series must strictly increase within the batch")
                continue
            seen_telemetry[series_key] = revision
            entries.append({
                "entry_type": "telemetry",
                "record_id": rid,
                "source": source,
                "recorded_at": recorded_at.isoformat(),
                "asset_id": asset_id,
                "metric": metric,
                "value": value,
                "revision": revision,
                "observed_at": observed_at.isoformat(),
            })
        return entries, errors

    @staticmethod
    def classify_version(existing, entry):
        """Decide how an incoming recovery entry sits beside stored versions.

        First-wins: the first version stored for a record_id stays canonical
        forever and a later arrival never clobbers it. Source and timestamp
        are kept on every row so the two sides' edits remain distinguishable.
        - identical (record_id, source, recorded_at, payload) -> duplicate
        - any other non-identical arrival                    -> conflict
        - first version for this record_id                    -> canonical
        """
        same_time = [v for v in existing if v["recorded_at"] == entry["recorded_at"]]
        if any(v["content_hash"] == entry["content_hash"] for v in same_time):
            return "duplicate"
        if existing:
            return "conflict"
        return "canonical"

    def plan_reopens(self, telemetry_entries, recovery_entries, lookup):
        """Recompute which resolved/closed incidents are invalidated.

        Trigger A: a telemetry revision above the resolution snapshot.
        Trigger B: an offline recovery record observed after resolution.
        Both return the deduplicated list of affected assets per incident.
        """
        plans = {}

        def bucket(incident_id):
            return plans.setdefault(incident_id, {"reasons": [], "affected_assets": []})

        def add_asset(bucket_ref, asset_id):
            if asset_id and asset_id not in bucket_ref["affected_assets"]:
                bucket_ref["affected_assets"].append(asset_id)

        incidents = _all(lookup, "incident")
        settled = [i for i in incidents if i["status"] in ("resolved", "closed")]
        settled_by_asset = {}
        for incident in settled:
            asset_id = incident["data"].get("asset_id")
            if asset_id:
                settled_by_asset.setdefault(asset_id, []).append(incident)

        telemetries = _all(lookup, "telemetry")
        latest = {}
        for telemetry in telemetries:
            key = (telemetry["data"].get("asset_id"), telemetry["data"].get("metric"))
            rev = int(telemetry["data"].get("revision", 0) or 0)
            if rev > int(latest.get(key, {}).get("data", {}).get("revision", 0) or 0):
                latest[key] = telemetry

        for entry in telemetry_entries:
            asset_id = entry["asset_id"]
            series = latest.get((asset_id, entry["metric"]))
            if not series:
                continue
            for incident in settled_by_asset.get(asset_id, []):
                snapshot = incident["data"].get("resolved_revisions") or {}
                seen_rev = int(snapshot.get(asset_id + "\0" + entry["metric"], 0) or 0)
                if entry["revision"] > seen_rev:
                    bucket_ref = bucket(incident["id"])
                    reason = ("telemetry_revision:%s/%s rev %s > resolved %s"
                              % (asset_id, entry["metric"], entry["revision"], seen_rev))
                    if reason not in bucket_ref["reasons"]:
                        bucket_ref["reasons"].append(reason)
                    add_asset(bucket_ref, asset_id)

        by_id = {incident["id"]: incident for incident in incidents}
        for entry in recovery_entries:
            incident = by_id.get(entry["incident_id"])
            if not incident or incident["status"] not in ("resolved", "closed"):
                continue
            resolved_at = incident["data"].get("resolved_at")
            if not resolved_at:
                continue
            if parse_ts(entry["recorded_at"], "recorded_at") > parse_ts(resolved_at, "resolved_at"):
                bucket_ref = bucket(incident["id"])
                reason = "offline_record_after_resolution:%s" % entry["recorded_at"]
                if reason not in bucket_ref["reasons"]:
                    bucket_ref["reasons"].append(reason)
                add_asset(bucket_ref, entry["asset_id"])
                add_asset(bucket_ref, incident["data"].get("asset_id"))
        for plan in plans.values():
            plan["affected_assets"].sort()
        return plans

    MERGE_ROLES = ("admin", "operator", "engineer", "field")
