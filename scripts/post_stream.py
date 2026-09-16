"""Quality-gated, idempotent post-stream reporting workflow.

This command never changes raw collection observations. It resolves one closed
stream, assesses the durable evidence, reads curated analytical views, and
creates a private owner-review packet under the ignored ``recaps/`` directory.
Power BI refresh/export and owner approval remain explicit recorded steps.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any, Iterable
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import psycopg


ANALYSIS_VERSION = "milestone-2-v1.0.0"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
PRIVATE_OUTPUT_DIR = PROJECT_ROOT / "recaps"
CORE_SOURCES = ("stream_poll", "chat", "raids", "follows")
ENHANCED_CAPABILITIES = (
    "chatter_presence", "chat_context", "stream_metadata_history",
    "raid_source_context",
)


class PostStreamError(Exception):
    """Safe operator-facing failure; messages must not contain private rows."""


@dataclass(frozen=True)
class QualityReason:
    code: str
    severity: str
    scope: str
    message: str


@dataclass(frozen=True)
class MetricAvailability:
    metric: str
    state: str
    reason: str


@dataclass(frozen=True)
class QualityAssessment:
    state: str
    reasons: tuple[QualityReason, ...]
    metrics: tuple[MetricAvailability, ...]
    source_states: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "reasons": [asdict(reason) for reason in self.reasons],
            "metrics": [asdict(metric) for metric in self.metrics],
            "sources": list(self.source_states),
        }


def _reason(code: str, severity: str, scope: str, message: str) -> QualityReason:
    return QualityReason(code=code, severity=severity, scope=scope, message=message)


def _metric(metric: str, state: str, reason: str) -> MetricAvailability:
    return MetricAvailability(metric=metric, state=state, reason=reason)


def assess_quality(evidence: dict[str, Any]) -> QualityAssessment:
    """Classify one stream from privacy-safe aggregate evidence.

    Blocking is reserved for closure, attribution, or consistency failures.
    Source-specific deficiencies produce warnings and suppress only dependent
    metrics. A quiet healthy source is available; zero captured events is not a
    failure signal.
    """
    reasons: list[QualityReason] = []
    stream = evidence["stream"]
    runs = evidence["runs"]
    sources = {row["source"]: row for row in evidence["sources"]}

    if stream["offline_observed_at"] is None:
        reasons.append(_reason(
            "stream_closure_missing", "block", "stream",
            "No successful offline observation closes this stream.",
        ))
    if not runs:
        reasons.append(_reason(
            "stream_attribution_missing", "block", "stream",
            "No collector run is durably associated with this stream.",
        ))
    if any(run["stopped_at"] is None for run in runs):
        reasons.append(_reason(
            "associated_run_open", "block", "run",
            "At least one associated collector run has no orderly close timestamp.",
        ))
    if evidence.get("provenance_conflict_count", 0):
        reasons.append(_reason(
            "run_stream_provenance_conflict", "block", "attribution",
            "Direct raw provenance conflicts with the run-to-stream bridge.",
        ))
    if stream["offline_observed_at"] is not None and (
            stream["offline_observed_at"] <= stream["started_at"]
            or stream["first_observed_at"] < stream["started_at"]):
        reasons.append(_reason(
            "stream_time_inconsistent", "block", "stream",
            "Stored stream lifecycle timestamps are internally inconsistent.",
        ))

    if evidence.get("late_start_seconds", 0) > 90:
        reasons.append(_reason(
            "late_collection_start", "warn", "stream",
            "Collection began more than 90 seconds after the reported stream start.",
        ))
    if evidence.get("early_stop_seconds", 0) > 90:
        reasons.append(_reason(
            "early_collection_stop", "warn", "stream",
            "The last successful stream observation precedes closure by more than 90 seconds.",
        ))
    if evidence.get("viewer_max_gap_seconds", 0) > 90:
        reasons.append(_reason(
            "viewer_poll_gap", "warn", "viewer",
            "At least one viewer-sampling interval exceeded 90 seconds; affected windows are unavailable.",
        ))
    if evidence.get("restart_gap_seconds", 0) > 30:
        reasons.append(_reason(
            "inter_run_coverage_gap", "warn", "run",
            "Successive collector-run observations leave an uncovered interval longer than 30 seconds.",
        ))
    if evidence.get("ambiguous_event_count", 0):
        reasons.append(_reason(
            "event_association_ambiguous", "warn", "events",
            "At least one raid or follow falls in multiple or conflicting stream intervals and is excluded.",
        ))
    if evidence.get("unresolved_event_count", 0):
        reasons.append(_reason(
            "event_association_unresolved", "warn", "events",
            "At least one raid or follow cannot be assigned to a closed stream and is excluded.",
        ))
    if evidence.get("incomplete_presence_count", 0):
        reasons.append(_reason(
            "presence_snapshot_incomplete", "warn", "presence",
            "At least one presence attempt was incomplete; only verified complete snapshots are used.",
        ))

    source_states: list[dict[str, Any]] = []
    for source in (*CORE_SOURCES, "chatter_presence"):
        row = sources.get(source, {})
        configured = bool(row.get("configured_in_any_run"))
        disabled = bool(row.get("disabled_in_any_run"))
        failed = bool(row.get("failed_in_any_run"))
        errors = int(row.get("error_observation_count") or 0)
        healthy = int(row.get("healthy_observation_count") or 0)
        configured_runs = int(row.get("configured_run_count") or 0)
        orderly_stops = int(row.get("orderly_stop_count") or 0)
        unresolved_gaps = int(row.get("unresolved_gap_count") or 0)
        historical_unknown = bool(row.get("capability_history_missing"))
        if failed:
            state = "failed"
            reason_code = "failed_to_initialize"
        elif disabled and not configured:
            state = "disabled"
            reason_code = "explicitly_disabled"
        elif (errors or unresolved_gaps
              or (configured and healthy == 0)
              or (configured_runs and orderly_stops < configured_runs)
              or (configured and evidence.get("restart_gap_seconds", 0) > 30)):
            state = "partial"
            if configured and healthy == 0:
                reason_code = "healthy_transition_missing"
            elif configured_runs and orderly_stops < configured_runs:
                reason_code = "orderly_source_stop_missing"
            elif evidence.get("restart_gap_seconds", 0) > 30:
                reason_code = "inter_run_gap"
            else:
                reason_code = "recorded_error_or_gap"
        elif configured:
            state = "available"
            reason_code = "configured_with_no_recorded_failure"
        elif historical_unknown:
            state = "historical_unknown"
            reason_code = "capability_record_not_collected"
        else:
            state = "unavailable"
            reason_code = "no_supporting_evidence"
        source_states.append({
            "source": source,
            "state": state,
            "reason": reason_code,
            "error_observations": errors,
            "unresolved_gaps": unresolved_gaps,
        })
        if state in {"failed", "partial"}:
            reasons.append(_reason(
                f"{source}_coverage_partial", "warn", source,
                f"{source} has recorded failure or gap evidence; dependent intervals are localized or suppressed.",
            ))
        elif state in {"disabled", "unavailable"}:
            reasons.append(_reason(
                f"{source}_unavailable", "warn", source,
                f"{source} was not available; unrelated supported metrics remain usable.",
            ))

    enhanced = evidence.get("enhanced_capabilities", {})
    for capability in ENHANCED_CAPABILITIES:
        status = enhanced.get(capability, "not_collected")
        if status != "configured":
            reasons.append(_reason(
                f"{capability}_{status}", "warn", capability,
                f"{capability} is {status.replace('_', ' ')} for at least part of this stream.",
            ))

    source_by_name = {item["source"]: item["state"] for item in source_states}
    viewer_state = "unavailable" if evidence.get("viewer_observation_count", 0) == 0 else (
        "partial" if any(r.code in {
            "late_collection_start", "early_collection_stop", "viewer_poll_gap",
            "inter_run_coverage_gap",
        }
                         for r in reasons) else "available"
    )
    chat_source = source_by_name.get("chat", "unavailable")
    chat_state = "available" if chat_source in {"available", "historical_unknown"} else (
        "partial" if chat_source == "partial" else "unavailable"
    )
    presence_source = source_by_name.get("chatter_presence", "unavailable")
    complete_presence = evidence.get("complete_presence_count", 0)
    presence_state = "unavailable" if not complete_presence else (
        "partial" if presence_source == "partial" or evidence.get("incomplete_presence_count", 0)
        else "available"
    )
    raid_source = source_by_name.get("raids", "unavailable")
    raid_state = "available" if raid_source in {"available", "historical_unknown"} else (
        "partial" if raid_source == "partial" else "unavailable"
    )
    if evidence.get("ambiguous_event_count", 0) or evidence.get("unresolved_event_count", 0):
        raid_state = "partial" if raid_state != "unavailable" else raid_state

    metrics = (
        _metric("viewer_timeline", viewer_state, "Uses one-minute observations with 90-second continuity limits."),
        _metric("active_chat_participation", chat_state, "Quiet healthy chat is a supported zero; uncovered intervals are NULL."),
        _metric("chatter_presence", presence_state, "Only complete, internally consistent snapshots no older than seven minutes are aligned."),
        _metric("raid_impact", raid_state, "Each horizon is independently suppressed for truncation, overlap, or missing evidence."),
        _metric("community_continuity", chat_state, "First observed and returning status use local-channel active-chat history."),
        _metric(
            "historical_comparison",
            "available" if evidence.get("prior_closed_stream_count", 0) else "unavailable",
            "Prior comparable closed-stream count is shown with every baseline.",
        ),
    )

    if any(reason.severity == "block" for reason in reasons):
        state = "blocked"
    elif reasons:
        state = "publishable_with_warnings"
    else:
        state = "publishable"
    return QualityAssessment(state, tuple(reasons), metrics, tuple(source_states))


def open_reporting_connection():
    try:
        return psycopg.connect(
            dbname="stream_pulse", host="/var/run/postgresql", autocommit=True,
            connect_timeout=5,
            options="-c search_path=public -c statement_timeout=30000 -c lock_timeout=3000",
        )
    except psycopg.Error:
        raise PostStreamError("Could not connect to local PostgreSQL.") from None


def _require_schema(connection) -> None:
    required = (
        "analytics_stream_participation", "analytics_raid_impact",
        "analytics_chatter_participation", "analytics_historical_comparison",
        "analytics_quality_source", "analytics_stream_dimension",
        "analytics_date_dimension", "analytics_community_summary",
        "analytics_stream_events",
        "post_stream_analysis_runs",
    )
    rows = connection.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = ANY(%s)",
        (list(required),),
    ).fetchall()
    present = {row[0] for row in rows}
    missing = sorted(set(required) - present)
    if missing:
        raise PostStreamError(
            "Milestone 2 schema is not installed. Apply SQL migrations 016–017 in order."
        )


def resolve_stream(connection, *, stream_id: str | None, target_date: date | None,
                   timezone_name: str | None) -> str:
    if stream_id:
        row = connection.execute(
            "SELECT stream_id FROM streams WHERE stream_id = %s", (stream_id,),
        ).fetchone()
        if row is None:
            raise PostStreamError("No stream matches the supplied stable stream identity.")
        return row[0]
    if target_date is None:
        raise PostStreamError("Supply --stream-id or --date.")
    if not timezone_name:
        raise PostStreamError("Date-based lookup requires an explicit --timezone.")
    try:
        zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError:
        raise PostStreamError("The requested IANA timezone is not installed.") from None
    start = datetime.combine(target_date, time.min, zone).astimezone(timezone.utc)
    end = datetime.combine(target_date.fromordinal(target_date.toordinal() + 1), time.min, zone).astimezone(timezone.utc)
    rows = connection.execute(
        "SELECT stream_id FROM streams WHERE started_at >= %s AND started_at < %s ORDER BY started_at",
        (start, end),
    ).fetchall()
    if not rows:
        raise PostStreamError("No stream starts on that date in the requested timezone.")
    if len(rows) > 1:
        raise PostStreamError(
            "Multiple streams start on that local date; rerun with a stable --stream-id."
        )
    return rows[0][0]


def load_evidence(connection, stream_id: str) -> dict[str, Any]:
    stream_row = connection.execute(
        "SELECT stream_id, started_at, first_observed_at, offline_observed_at "
        "FROM streams WHERE stream_id = %s", (stream_id,),
    ).fetchone()
    if stream_row is None:
        raise PostStreamError("The target stream no longer exists.")
    stream = dict(zip(
        ("stream_id", "started_at", "first_observed_at", "offline_observed_at"), stream_row,
    ))
    run_rows = connection.execute(
        "SELECT r.run_id, r.started_at, r.last_heartbeat_at, r.stopped_at, "
        "crs.first_successful_observed_at, crs.last_successful_observed_at, crs.attribution_method "
        "FROM collector_run_streams crs JOIN collector_runs r USING (run_id) "
        "WHERE crs.stream_id = %s ORDER BY r.started_at", (stream_id,),
    ).fetchall()
    run_keys = (
        "run_id", "started_at", "last_heartbeat_at", "stopped_at",
        "first_successful_observed_at", "last_successful_observed_at", "attribution_method",
    )
    runs = [dict(zip(run_keys, row)) for row in run_rows]
    source_rows = connection.execute(
        "SELECT source, configured_in_any_run, disabled_in_any_run, failed_in_any_run, "
        "capability_history_missing, configured_run_count, error_observation_count, "
        "healthy_observation_count, orderly_stop_count, "
        "unresolved_gap_count FROM analytics_quality_source WHERE stream_id = %s ORDER BY source",
        (stream_id,),
    ).fetchall()
    source_keys = (
        "source", "configured_in_any_run", "disabled_in_any_run", "failed_in_any_run",
        "capability_history_missing", "configured_run_count", "error_observation_count",
        "healthy_observation_count", "orderly_stop_count",
        "unresolved_gap_count",
    )
    sources = [dict(zip(source_keys, row)) for row in source_rows]
    capability_rows = connection.execute(
        "SELECT capability, array_agg(DISTINCT status ORDER BY status) "
        "FROM collector_run_capabilities c JOIN collector_run_streams crs USING (run_id) "
        "WHERE crs.stream_id = %s AND capability = ANY(%s) GROUP BY capability",
        (stream_id, list(ENHANCED_CAPABILITIES)),
    ).fetchall()
    enhanced: dict[str, str] = {}
    for capability, statuses in capability_rows:
        if "failed_to_initialize" in statuses:
            enhanced[capability] = "failed_to_initialize"
        elif "disabled" in statuses and "configured" not in statuses:
            enhanced[capability] = "disabled"
        elif "configured" in statuses:
            enhanced[capability] = "configured"

    viewer = connection.execute(
        "WITH x AS (SELECT observed_at, lead(observed_at) OVER (ORDER BY observed_at) AS next_at "
        "FROM viewer_snapshots WHERE stream_id = %s) "
        "SELECT count(*), coalesce(max(extract(epoch FROM (next_at - observed_at))), 0) FROM x",
        (stream_id,),
    ).fetchone()
    presence = connection.execute(
        "SELECT count(*) FILTER (WHERE status = 'complete'), "
        "count(*) FILTER (WHERE status <> 'complete') "
        "FROM chatter_presence_snapshots WHERE stream_id = %s", (stream_id,),
    ).fetchone()
    associations = connection.execute(
        "SELECT count(*) FILTER (WHERE association_status IN ('ambiguous', 'conflict')), "
        "count(*) FILTER (WHERE association_status = 'unresolved') "
        "FROM analytics_event_associations "
        "WHERE captured_stream_id = %s OR associated_stream_id = %s "
        "OR (event_at >= %s AND event_at < coalesce(%s, 'infinity'::timestamptz))",
        (stream_id, stream_id, stream["started_at"], stream["offline_observed_at"]),
    ).fetchone()
    provenance_conflict = connection.execute(
        "SELECT count(*) FROM ("
        "SELECT run_id FROM viewer_snapshots WHERE stream_id = %s AND run_id IS NOT NULL "
        "UNION ALL SELECT run_id FROM chat_messages WHERE stream_id = %s AND run_id IS NOT NULL"
        ") raw WHERE NOT EXISTS (SELECT 1 FROM collector_run_streams crs "
        "WHERE crs.run_id = raw.run_id AND crs.stream_id = %s)",
        (stream_id, stream_id, stream_id),
    ).fetchone()[0]
    prior_count = connection.execute(
        "SELECT count(*) FROM streams WHERE offline_observed_at IS NOT NULL AND started_at < %s",
        (stream["started_at"],),
    ).fetchone()[0]
    first_bridge = min((run["first_successful_observed_at"] for run in runs), default=None)
    last_bridge = max((run["last_successful_observed_at"] for run in runs), default=None)
    late_seconds = max(0, (first_bridge - stream["started_at"]).total_seconds()) if first_bridge else 0
    early_seconds = (
        max(0, (stream["offline_observed_at"] - last_bridge).total_seconds())
        if last_bridge and stream["offline_observed_at"] else 0
    )
    ordered_runs = sorted(runs, key=lambda item: item["first_successful_observed_at"])
    restart_gap_seconds = max((
        (current["first_successful_observed_at"]
         - previous["last_successful_observed_at"]).total_seconds()
        for previous, current in zip(ordered_runs, ordered_runs[1:])
    ), default=0)
    return {
        "stream": stream,
        "runs": runs,
        "sources": sources,
        "enhanced_capabilities": enhanced,
        "viewer_observation_count": viewer[0],
        "viewer_max_gap_seconds": float(viewer[1]),
        "complete_presence_count": presence[0],
        "incomplete_presence_count": presence[1],
        "ambiguous_event_count": associations[0],
        "unresolved_event_count": associations[1],
        "provenance_conflict_count": provenance_conflict,
        "prior_closed_stream_count": prior_count,
        "late_start_seconds": late_seconds,
        "early_stop_seconds": early_seconds,
        "restart_gap_seconds": max(0, restart_gap_seconds),
    }


def compute_input_fingerprint(connection, stream_id: str) -> str:
    """Hash analysis-relevant rows without writing identity-bearing values."""
    digest = hashlib.sha256()
    query_specs = (
        ("stream", "SELECT started_at, first_observed_at, offline_observed_at FROM streams WHERE stream_id=%s"),
        ("runs", "SELECT r.run_id, r.started_at, r.last_heartbeat_at, r.stopped_at, crs.* "
                 "FROM collector_run_streams crs JOIN collector_runs r USING(run_id) "
                 "WHERE crs.stream_id=%s ORDER BY r.run_id"),
        ("capabilities", "SELECT c.run_id, capability, status, observed_at, reason_code "
                         "FROM collector_run_capabilities c JOIN collector_run_streams crs USING(run_id) "
                         "WHERE crs.stream_id=%s ORDER BY c.run_id, capability"),
        ("health", "SELECT h.run_id, source, observed_at, status, reason_code "
                    "FROM collection_health h JOIN collector_run_streams crs USING(run_id) "
                    "WHERE crs.stream_id=%s ORDER BY h.run_id, health_id"),
        ("gaps", "SELECT g.run_id, detected_at, recovered_at, reason_code "
                  "FROM reconnection_gaps g JOIN collector_run_streams crs USING(run_id) "
                  "WHERE crs.stream_id=%s ORDER BY g.run_id, gap_id"),
        ("viewers", "SELECT observed_at, viewer_count, run_id FROM viewer_snapshots "
                     "WHERE stream_id=%s ORDER BY observed_at"),
        ("chat", "SELECT eventsub_message_id, md5(chatter_user_id), notification_at, received_at, "
                  "run_id, source_broadcaster_user_id IS NOT NULL, context_complete, "
                  "message_type, md5(coalesce(reply_parent_message_id,'')), "
                  "md5(coalesce(reply_parent_user_id,'')), md5(coalesce(badges::text,'')) "
                  "FROM chat_messages WHERE stream_id=%s "
                  "ORDER BY eventsub_message_id"),
        ("presence", "SELECT p.snapshot_id, p.run_id, requested_at, completed_at, status, reason_code, "
                      "reported_total, collected_distinct_count, "
                      "coalesce((SELECT md5(string_agg(md5(pm.chatter_user_id), ',' ORDER BY md5(pm.chatter_user_id))) "
                      "FROM chatter_presence_members pm WHERE pm.snapshot_id=p.snapshot_id), md5('')) "
                      "FROM chatter_presence_snapshots p WHERE p.stream_id=%s ORDER BY p.snapshot_id"),
        ("events", "SELECT a.event_type, a.event_id, a.event_at, a.received_at, "
                   "a.association_status, r.raid_viewer_count, md5(coalesce(r.from_broadcaster_user_id,'')), "
                   "f.followed_at, md5(coalesce(f.user_id,'')), rc.status, rc.reason_code, "
                   "rc.observed_at, rc.category_id, rc.category_name, rc.title, rc.language, rc.tags "
                   "FROM analytics_event_associations a "
                   "JOIN streams target ON target.stream_id=%s "
                   "LEFT JOIN incoming_raids r ON a.event_type='raid' AND r.eventsub_message_id=a.event_id "
                   "LEFT JOIN follow_events f ON a.event_type='follow' AND f.eventsub_message_id=a.event_id "
                   "LEFT JOIN raid_source_context rc ON rc.eventsub_message_id=r.eventsub_message_id "
                   "WHERE a.captured_stream_id=%s OR a.associated_stream_id=%s "
                   "OR (a.event_at >= target.started_at AND a.event_at < "
                   "coalesce(target.offline_observed_at, 'infinity'::timestamptz)) "
                   "ORDER BY a.event_type, a.event_id"),
        ("metadata", "SELECT observed_at, title, category_id, category_name, language, tags, run_id "
                     "FROM stream_metadata_history WHERE stream_id=%s ORDER BY observed_at"),
    )
    for label, query in query_specs:
        digest.update(label.encode())
        params = (stream_id, stream_id, stream_id) if label == "events" else (stream_id,)
        for row in connection.execute(query, params):
            digest.update(repr(row).encode("utf-8"))
            digest.update(b"\n")
    return digest.hexdigest()


def _query_summary(connection, stream_id: str) -> dict[str, Any]:
    participation = connection.execute(
        "SELECT count(*), count(*) FILTER (WHERE viewer_available), "
        "count(*) FILTER (WHERE chat_available), round(avg(top_five_message_share), 4), "
        "max(active_chatter_count), max(message_count) "
        "FROM analytics_stream_participation WHERE stream_id=%s", (stream_id,),
    ).fetchone()
    community = connection.execute(
        "SELECT count(*), count(*) FILTER (WHERE first_observed_chatter), "
        "count(*) FILTER (WHERE returning_chatter), count(*) FILTER (WHERE recurring_chatter) "
        "FROM analytics_chatter_participation WHERE stream_id=%s", (stream_id,),
    ).fetchone()
    raids = connection.execute(
        "SELECT count(DISTINCT raid_id), count(*) FILTER (WHERE horizon_minutes=15 "
        "AND viewer_horizon_available AND viewer_baseline_available AND NOT overlapping_raid), "
        "round(avg(viewer_change_from_baseline) FILTER (WHERE horizon_minutes=15 "
        "AND viewer_horizon_available AND viewer_baseline_available AND NOT overlapping_raid), 2) "
        "FROM analytics_raid_impact WHERE stream_id=%s", (stream_id,),
    ).fetchone()
    return {
        "participation": {
            "window_count": participation[0],
            "viewer_available_windows": participation[1],
            "chat_available_windows": participation[2],
            "mean_top_five_message_share": participation[3],
            "max_active_chatters_in_window": participation[4],
            "max_messages_in_window": participation[5],
        },
        "community": {
            "active_chatters": community[0],
            "first_observed": community[1],
            "returning": community[2],
            "recurring": community[3],
        },
        "raids": {
            "raid_count": raids[0],
            "supported_15_minute_horizons": raids[1],
            "mean_supported_15_minute_viewer_change": raids[2],
        },
    }


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def _render_briefing(evidence: dict[str, Any], assessment: QualityAssessment,
                     summary: dict[str, Any], analysis_id: str) -> str:
    stream = evidence["stream"]
    p = summary["participation"]
    c = summary["community"]
    r = summary["raids"]
    warnings = [reason for reason in assessment.reasons if reason.severity != "block"]
    limitations = "\n".join(f"- {item.message}" for item in warnings) or "- No quality warnings recorded."
    return f"""# Post-stream briefing — owner review draft

- Analysis ID: `{analysis_id}`
- Analysis version: `{ANALYSIS_VERSION}`
- Quality state: **{assessment.state.replace('_', ' ')}**
- Stream start (UTC): {stream['started_at'].isoformat()}

## Automated observations

- {p['viewer_available_windows']} of {p['window_count']} five-minute windows support the time-weighted viewer metric.
- {p['chat_available_windows']} of {p['window_count']} windows support chat-activity metrics.
- Peak observed five-minute activity was {p['max_messages_in_window'] or 0} messages from {p['max_active_chatters_in_window'] or 0} active chatters.
- The mean top-five participant message share across non-empty supported windows was {p['mean_top_five_message_share'] if p['mean_top_five_message_share'] is not None else 'unavailable'}.
- Active-chat history classified {c['first_observed']} first-observed, {c['returning']} returning, and {c['recurring']} recurring participants among {c['active_chatters']} active chatters.
- {r['raid_count']} incoming raids were resolved to this stream. {r['supported_15_minute_horizons']} have a supported, non-overlapping +15-minute viewer comparison.
- Mean supported +15-minute viewer change versus the five-minute pre-raid baseline: {r['mean_supported_15_minute_viewer_change'] if r['mean_supported_15_minute_viewer_change'] is not None else 'unavailable'}.

## Quality and limitations

{limitations}

Twitch-reported chat presence is delayed and is not video viewership. Aggregate viewer movement does not identify people. Raid-window changes are observed associations, not individual retention or causal effects. “First observed” means since local tracking began.

## Interpretation for owner review

Add or revise interpretation here after inspecting the Power BI pages. Keep interpretation separate from the automated observations above. External delivery is not approved until the workflow records owner approval.

## Manual completion

1. Refresh `stream-pulse.pbip` in Power BI Desktop.
2. Validate the three pages against the SQL values in this briefing.
3. Export the reviewed page(s), then record the artifact with `record-refresh`.
4. Record the owner decision with `review`. Do not send automatically.
"""


def _insert_execution(connection, *, analysis_id: str, stream_id: str,
                      fingerprint: str, assessment: QualityAssessment,
                      artifact_path: str | None) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    workflow_status = "blocked" if assessment.state == "blocked" else "refresh_pending"
    connection.execute(
        "INSERT INTO post_stream_analysis_runs (analysis_id, stream_id, analysis_version, "
        "input_fingerprint, quality_state, quality_reasons, workflow_status, started_at, "
        "completed_at, artifact_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (stream_id, analysis_version, input_fingerprint) DO NOTHING",
        (analysis_id, stream_id, ANALYSIS_VERSION, fingerprint, assessment.state,
         json.dumps(assessment.to_dict(), default=_json_default), workflow_status,
         now, now, artifact_path),
    )
    row = connection.execute(
        "SELECT analysis_id, workflow_status, artifact_path FROM post_stream_analysis_runs "
        "WHERE stream_id=%s AND analysis_version=%s AND input_fingerprint=%s",
        (stream_id, ANALYSIS_VERSION, fingerprint),
    ).fetchone()
    return {"analysis_id": row[0], "workflow_status": row[1], "artifact_path": row[2]}


def prepare(connection, stream_id: str, *, quality_only: bool = False) -> dict[str, Any]:
    locked = connection.execute(
        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (stream_id,),
    ).fetchone()[0]
    if not locked:
        raise PostStreamError("Another post-stream process is already handling this target.")
    try:
        evidence = load_evidence(connection, stream_id)
        assessment = assess_quality(evidence)
        result: dict[str, Any] = {"quality": assessment.to_dict()}
        if quality_only:
            return result
        fingerprint = compute_input_fingerprint(connection, stream_id)
        analysis_id = hashlib.sha256(
            f"{ANALYSIS_VERSION}:{stream_id}:{fingerprint}".encode()
        ).hexdigest()[:20]
        existing = connection.execute(
            "SELECT analysis_id, workflow_status, artifact_path "
            "FROM post_stream_analysis_runs WHERE stream_id=%s AND analysis_version=%s "
            "AND input_fingerprint=%s",
            (stream_id, ANALYSIS_VERSION, fingerprint),
        ).fetchone()
        if existing is not None:
            return result | {
                "analysis_id": existing[0],
                "workflow_status": existing[1],
                "artifact_path": existing[2],
                "reused": True,
            }
        local_date = evidence["stream"]["started_at"].astimezone(
            ZoneInfo("Europe/Amsterdam")
        ).date().isoformat()
        output_dir = PRIVATE_OUTPUT_DIR / f"{local_date}-{analysis_id}"
        if assessment.state == "blocked":
            manifest_path = output_dir / "manifest.json"
            manifest = {
                "analysis_id": analysis_id,
                "analysis_version": ANALYSIS_VERSION,
                "stream_id": stream_id,
                "input_fingerprint": fingerprint,
                "quality": assessment.to_dict(),
                "workflow_status": "blocked",
                "power_bi_refresh": "not_started",
                "owner_review": "not_started",
            }
            _atomic_write(manifest_path, json.dumps(
                manifest, indent=2, default=_json_default, sort_keys=True,
            ) + "\n")
            execution = _insert_execution(
                connection, analysis_id=analysis_id, stream_id=stream_id,
                fingerprint=fingerprint, assessment=assessment,
                artifact_path=str(output_dir.relative_to(PROJECT_ROOT)),
            )
            return result | execution

        summary = _query_summary(connection, stream_id)
        manifest = {
            "analysis_id": analysis_id,
            "analysis_version": ANALYSIS_VERSION,
            "stream_id": stream_id,
            "input_fingerprint": fingerprint,
            "quality": assessment.to_dict(),
            "summary": summary,
            "workflow_status": "refresh_pending",
            "power_bi_refresh": "pending_manual_desktop_refresh",
            "owner_review": "pending",
        }
        _atomic_write(output_dir / "manifest.json", json.dumps(
            manifest, indent=2, default=_json_default, sort_keys=True,
        ) + "\n")
        _atomic_write(
            output_dir / "briefing.md",
            _render_briefing(evidence, assessment, summary, analysis_id),
        )
        execution = _insert_execution(
            connection, analysis_id=analysis_id, stream_id=stream_id,
            fingerprint=fingerprint, assessment=assessment,
            artifact_path=str(output_dir.relative_to(PROJECT_ROOT)),
        )
        return result | execution | {"summary": summary}
    except psycopg.Error:
        raise PostStreamError("Post-stream database operation failed.") from None
    finally:
        connection.execute("SELECT pg_advisory_unlock(hashtextextended(%s, 0))", (stream_id,))


def _load_manifest_for_execution(connection, analysis_id: str) -> tuple[Path, dict[str, Any], str]:
    row = connection.execute(
        "SELECT artifact_path, workflow_status FROM post_stream_analysis_runs WHERE analysis_id=%s",
        (analysis_id,),
    ).fetchone()
    if row is None or not row[0]:
        raise PostStreamError("No prepared analysis matches that analysis ID.")
    artifact_dir = (PROJECT_ROOT / row[0]).resolve()
    if PROJECT_ROOT.resolve() not in artifact_dir.parents:
        raise PostStreamError("Stored analysis artifact path is invalid.")
    manifest_path = artifact_dir / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        raise PostStreamError("Prepared analysis manifest is missing or invalid.") from None
    return manifest_path, manifest, row[1]


def record_refresh(connection, analysis_id: str, artifact: Path) -> dict[str, Any]:
    artifact = artifact.expanduser().resolve()
    if not artifact.is_file():
        raise PostStreamError("The refresh/export artifact does not exist as a file.")
    manifest_path, manifest, status = _load_manifest_for_execution(connection, analysis_id)
    if status not in {"refresh_pending", "review_pending"}:
        raise PostStreamError("This analysis is not eligible for refresh recording.")
    now = datetime.now(timezone.utc)
    connection.execute(
        "UPDATE post_stream_analysis_runs SET workflow_status='review_pending', "
        "refresh_artifact_path=%s, refreshed_at=%s WHERE analysis_id=%s",
        (str(artifact), now, analysis_id),
    )
    manifest["workflow_status"] = "review_pending"
    manifest["power_bi_refresh"] = "completed"
    manifest["refresh_artifact_path"] = str(artifact)
    manifest["refreshed_at"] = now.isoformat()
    _atomic_write(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"analysis_id": analysis_id, "workflow_status": "review_pending"}


def record_review(connection, analysis_id: str, decision: str) -> dict[str, Any]:
    manifest_path, manifest, status = _load_manifest_for_execution(connection, analysis_id)
    if status != "review_pending":
        raise PostStreamError("Owner review requires a recorded successful refresh/export first.")
    workflow_status = "approved" if decision == "approve" else "rejected"
    now = datetime.now(timezone.utc)
    connection.execute(
        "UPDATE post_stream_analysis_runs SET workflow_status=%s, reviewed_at=%s "
        "WHERE analysis_id=%s", (workflow_status, now, analysis_id),
    )
    manifest["workflow_status"] = workflow_status
    manifest["owner_review"] = workflow_status
    manifest["reviewed_at"] = now.isoformat()
    _atomic_write(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return {"analysis_id": analysis_id, "workflow_status": workflow_status}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name, help_text in (
        ("assess", "Resolve one stream and print quality without creating an execution."),
        ("prepare", "Assess, query curated datasets, and create a review packet."),
    ):
        target = subparsers.add_parser(name, help=help_text)
        group = target.add_mutually_exclusive_group(required=True)
        group.add_argument("--stream-id")
        group.add_argument("--date", type=date.fromisoformat, dest="target_date")
        target.add_argument("--timezone")
    refresh = subparsers.add_parser("record-refresh", help="Record a current Power BI refresh/export artifact.")
    refresh.add_argument("--analysis-id", required=True)
    refresh.add_argument("--artifact", required=True, type=Path)
    review = subparsers.add_parser("review", help="Record the owner's review decision; never sends externally.")
    review.add_argument("--analysis-id", required=True)
    review.add_argument("--decision", required=True, choices=("approve", "reject"))
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        with open_reporting_connection() as connection:
            _require_schema(connection)
            if args.command in {"assess", "prepare"}:
                stream_id = resolve_stream(
                    connection, stream_id=args.stream_id,
                    target_date=args.target_date, timezone_name=args.timezone,
                )
                result = prepare(connection, stream_id, quality_only=args.command == "assess")
            elif args.command == "record-refresh":
                result = record_refresh(connection, args.analysis_id, args.artifact)
            else:
                result = record_review(connection, args.analysis_id, args.decision)
        print(json.dumps(result, indent=2, default=_json_default, sort_keys=True))
        return 2 if result.get("quality", {}).get("state") == "blocked" else 0
    except (PostStreamError, ValueError) as exc:
        print(f"post_stream_error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
