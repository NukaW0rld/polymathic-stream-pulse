"""Post-stream quality and analytical SQL tests using synthetic evidence only."""

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from scripts.post_stream import (
    PostStreamError, assess_quality, prepare, record_refresh, record_review,
    resolve_stream,
)


START = datetime(2026, 9, 17, 18, tzinfo=timezone.utc)


def clean_evidence():
    sources = []
    for source in ("stream_poll", "chat", "raids", "follows", "chatter_presence"):
        sources.append({
            "source": source,
            "configured_in_any_run": True,
            "disabled_in_any_run": False,
            "failed_in_any_run": False,
            "capability_history_missing": False,
            "configured_run_count": 1,
            "error_observation_count": 0,
            "healthy_observation_count": 1,
            "orderly_stop_count": 1,
            "unresolved_gap_count": 0,
        })
    return {
        "stream": {
            "stream_id": "synthetic-current", "started_at": START,
            "first_observed_at": START, "offline_observed_at": START + timedelta(hours=2),
        },
        "runs": [{
            "run_id": 1, "started_at": START, "last_heartbeat_at": START + timedelta(hours=2),
            "stopped_at": START + timedelta(hours=2),
            "first_successful_observed_at": START,
            "last_successful_observed_at": START + timedelta(hours=2),
            "attribution_method": "direct_observation",
        }],
        "sources": sources,
        "enhanced_capabilities": {
            "chatter_presence": "configured", "chat_context": "configured",
            "stream_metadata_history": "configured", "raid_source_context": "configured",
        },
        "viewer_observation_count": 120,
        "viewer_max_gap_seconds": 60,
        "complete_presence_count": 24,
        "incomplete_presence_count": 0,
        "ambiguous_event_count": 0,
        "unresolved_event_count": 0,
        "provenance_conflict_count": 0,
        "prior_closed_stream_count": 3,
        "late_start_seconds": 0,
        "early_stop_seconds": 0,
        "restart_gap_seconds": 0,
    }


class QualityAssessmentTests(unittest.TestCase):
    def test_clean_complete_run_is_publishable(self):
        result = assess_quality(clean_evidence())
        self.assertEqual(result.state, "publishable")
        self.assertFalse(result.reasons)
        self.assertTrue(all(metric.state == "available" for metric in result.metrics))

    def test_open_associated_run_blocks_even_when_sources_look_healthy(self):
        evidence = clean_evidence()
        evidence["runs"][0]["stopped_at"] = None
        result = assess_quality(evidence)
        self.assertEqual(result.state, "blocked")
        self.assertIn("associated_run_open", {reason.code for reason in result.reasons})

    def test_missing_stream_closure_and_provenance_conflict_block(self):
        evidence = clean_evidence()
        evidence["stream"]["offline_observed_at"] = None
        evidence["provenance_conflict_count"] = 1
        result = assess_quality(evidence)
        self.assertEqual(result.state, "blocked")
        self.assertEqual(
            {reason.code for reason in result.reasons if reason.severity == "block"},
            {"stream_closure_missing", "run_stream_provenance_conflict"},
        )

    def test_gap_and_incomplete_presence_warn_and_localize_metrics(self):
        evidence = clean_evidence()
        evidence["viewer_max_gap_seconds"] = 180
        evidence["incomplete_presence_count"] = 1
        next(row for row in evidence["sources"] if row["source"] == "chat")[
            "unresolved_gap_count"
        ] = 1
        result = assess_quality(evidence)
        states = {metric.metric: metric.state for metric in result.metrics}
        self.assertEqual(result.state, "publishable_with_warnings")
        self.assertEqual(states["viewer_timeline"], "partial")
        self.assertEqual(states["active_chat_participation"], "partial")
        self.assertEqual(states["chatter_presence"], "partial")

    def test_disabled_presence_does_not_invalidate_viewer_or_quiet_chat(self):
        evidence = clean_evidence()
        presence = next(row for row in evidence["sources"] if row["source"] == "chatter_presence")
        presence.update(configured_in_any_run=False, disabled_in_any_run=True)
        evidence["enhanced_capabilities"]["chatter_presence"] = "disabled"
        evidence["complete_presence_count"] = 0
        result = assess_quality(evidence)
        states = {metric.metric: metric.state for metric in result.metrics}
        self.assertEqual(result.state, "publishable_with_warnings")
        self.assertEqual(states["viewer_timeline"], "available")
        self.assertEqual(states["active_chat_participation"], "available")
        self.assertEqual(states["chatter_presence"], "unavailable")

    def test_historical_capability_absence_is_not_zero_or_blocking(self):
        evidence = clean_evidence()
        evidence["enhanced_capabilities"] = {}
        evidence["complete_presence_count"] = 0
        presence = next(row for row in evidence["sources"] if row["source"] == "chatter_presence")
        presence.update(configured_in_any_run=False, capability_history_missing=True)
        result = assess_quality(evidence)
        self.assertEqual(result.state, "publishable_with_warnings")
        self.assertEqual(
            next(m.state for m in result.metrics if m.metric == "chatter_presence"),
            "unavailable",
        )

    def test_restart_gap_warns_without_manufacturing_continuity(self):
        evidence = clean_evidence()
        evidence["restart_gap_seconds"] = 75
        result = assess_quality(evidence)
        self.assertEqual(result.state, "publishable_with_warnings")
        self.assertIn("inter_run_coverage_gap", {reason.code for reason in result.reasons})
        self.assertEqual(
            next(metric.state for metric in result.metrics if metric.metric == "viewer_timeline"),
            "partial",
        )

    def test_late_start_early_stop_and_ambiguous_event_warn(self):
        evidence = clean_evidence()
        evidence.update(late_start_seconds=120, early_stop_seconds=180,
                        ambiguous_event_count=1)
        result = assess_quality(evidence)
        self.assertEqual(result.state, "publishable_with_warnings")
        self.assertTrue({
            "late_collection_start", "early_collection_stop", "event_association_ambiguous",
        }.issubset({reason.code for reason in result.reasons}))

    def test_multiple_closed_runs_without_gap_are_supported(self):
        evidence = clean_evidence()
        second = dict(evidence["runs"][0])
        second["run_id"] = 2
        evidence["runs"].append(second)
        for source in evidence["sources"]:
            source.update(configured_run_count=2, orderly_stop_count=2,
                          healthy_observation_count=2)
        self.assertEqual(assess_quality(evidence).state, "publishable")

    def test_date_lookup_requires_timezone_and_rejects_multiple_candidates(self):
        connection = Mock()
        with self.assertRaisesRegex(PostStreamError, "explicit --timezone"):
            resolve_stream(
                connection, stream_id=None, target_date=date(2026, 9, 17),
                timezone_name=None,
            )
        connection.execute.return_value.fetchall.return_value = [("one",), ("two",)]
        with self.assertRaisesRegex(PostStreamError, "Multiple streams"):
            resolve_stream(
                connection, stream_id=None, target_date=date(2026, 9, 17),
                timezone_name="Europe/Amsterdam",
            )


@unittest.skipUnless(os.environ.get("STREAM_PULSE_TEST_POSTGRES") == "1",
                     "Set STREAM_PULSE_TEST_POSTGRES=1 to test local PostgreSQL")
class AnalyticalViewsTests(unittest.TestCase):
    def setUp(self):
        import psycopg

        self.connection = psycopg.connect(
            dbname="stream_pulse", host="/var/run/postgresql", autocommit=True,
            connect_timeout=5, options="-c search_path=pg_temp -c statement_timeout=30000",
        )
        self.addCleanup(self.connection.close)
        sql_dir = Path(__file__).resolve().parents[1] / "sql"
        for name in (
            "001_create_viewer_snapshots.sql", "002_create_streams.sql",
            "003_create_chat_messages.sql", "004_create_incoming_raids.sql",
            "005_create_follow_events.sql", "006_create_collector_runs.sql",
            "007_create_collection_health.sql", "009_create_reconnection_gaps.sql",
        ):
            ddl = (sql_dir / name).read_text().replace("CREATE TABLE ", "CREATE TEMP TABLE ")
            self.connection.execute(ddl)
        self.connection.execute((sql_dir / "008_add_paused_health_status.sql").read_text())
        self.connection.execute((sql_dir / "010_align_chat_messages.sql").read_text())
        for name in (
            "011_add_run_attribution.sql", "012_create_chatter_presence.sql",
            "013_add_chat_context.sql", "014_create_stream_metadata_history.sql",
            "015_create_raid_source_context.sql",
        ):
            self.connection.execute((sql_dir / name).read_text())
        milestone_sql = (sql_dir / "016_create_post_stream_analytics.sql").read_text()
        milestone_sql = milestone_sql.replace(
            "CREATE TABLE post_stream_analysis_runs", "CREATE TEMP TABLE post_stream_analysis_runs"
        ).replace("CREATE VIEW ", "CREATE TEMP VIEW ")
        self.connection.execute(milestone_sql)
        events_sql = (sql_dir / "017_create_stream_event_timeline.sql").read_text()
        self.connection.execute(events_sql.replace("CREATE VIEW ", "CREATE TEMP VIEW "))
        self._fixtures()

    def _fixtures(self):
        c = self.connection
        prior = START - timedelta(days=2)
        c.execute(
            "INSERT INTO streams VALUES "
            "('prior',%s,%s,%s),('current',%s,%s,%s)",
            (prior, prior, prior + timedelta(minutes=10),
             START, START, START + timedelta(minutes=10)),
        )
        run_id = c.execute(
            "INSERT INTO collector_runs(started_at,last_heartbeat_at,stopped_at,collector_version) "
            "VALUES (%s,%s,%s,'synthetic') RETURNING run_id",
            (START, START + timedelta(minutes=10), START + timedelta(minutes=10)),
        ).fetchone()[0]
        c.execute(
            "INSERT INTO collector_run_streams VALUES (%s,'current',%s,%s,'direct_observation')",
            (run_id, START, START + timedelta(minutes=9)),
        )
        for capability in (
            "stream_poll", "chat", "raids", "follows", "chatter_presence",
            "chat_context", "stream_metadata_history", "raid_source_context",
        ):
            c.execute(
                "INSERT INTO collector_run_capabilities VALUES (%s,%s,'configured',%s,'configured')",
                (run_id, capability, START),
            )
        for source in ("stream_poll", "chat", "raids", "follows", "chatter_presence"):
            c.execute(
                "INSERT INTO collection_health(run_id,source,observed_at,status,reason_code) "
                "VALUES (%s,%s,%s,'healthy',%s),(%s,%s,%s,'stopped','orderly_shutdown')",
                (run_id, source, START,
                 "live_poll_saved" if source == "stream_poll" else
                 "snapshot_complete" if source == "chatter_presence" else "capture_ready",
                 run_id, source, START + timedelta(minutes=10)),
            )
        for minute in range(10):
            c.execute(
                "INSERT INTO viewer_snapshots VALUES ('current',%s,%s,%s)",
                (START + timedelta(minutes=minute), 100 + minute, run_id),
            )
        c.execute("INSERT INTO viewer_snapshots VALUES ('prior',%s,90,NULL)", (prior,))
        # Six equal participants make the first-window top-five share exactly 5/6.
        for index in range(6):
            c.execute(
                "INSERT INTO chat_messages(eventsub_message_id,stream_id,chatter_user_id,message_text,"
                "notification_at,received_at,message_fragments,chat_message_id,run_id,message_type,badges,context_complete) "
                "VALUES (%s,'current',%s,'private',%s,%s,'[]',%s,%s,'text','[]',TRUE)",
                (f"chat-{index}", f"user-{index}", START + timedelta(minutes=1, seconds=index),
                 START + timedelta(minutes=1, seconds=index + 1), f"message-{index}", run_id),
            )
        c.execute(
            "INSERT INTO chat_messages(eventsub_message_id,stream_id,chatter_user_id,message_text,"
            "notification_at,received_at,message_fragments,chat_message_id) "
            "VALUES ('prior-chat','prior','user-0','private',%s,%s,'[]','prior-message')",
            (prior + timedelta(minutes=1), prior + timedelta(minutes=1, seconds=1)),
        )
        snapshot_id = c.execute(
            "INSERT INTO chatter_presence_snapshots(run_id,stream_id,requested_at,completed_at,"
            "reported_total,collected_distinct_count,status,reason_code,reported_total_changed) "
            "VALUES (%s,'current',%s,%s,6,6,'complete','snapshot_complete',FALSE) RETURNING snapshot_id",
            (run_id, START + timedelta(minutes=4), START + timedelta(minutes=4, seconds=30)),
        ).fetchone()[0]
        for index in range(6):
            c.execute(
                "INSERT INTO chatter_presence_members VALUES (%s,%s)",
                (snapshot_id, f"user-{index}"),
            )
        c.execute(
            "INSERT INTO incoming_raids(eventsub_message_id,from_broadcaster_user_id,"
            "raid_viewer_count,notification_at,received_at,run_id) "
            "VALUES ('raid-boundary','source',10,%s,%s,%s)",
            (START + timedelta(minutes=10), START + timedelta(minutes=10, seconds=1), run_id),
        )
        for index, minute in enumerate((3, 4), start=1):
            c.execute(
                "INSERT INTO incoming_raids(eventsub_message_id,from_broadcaster_user_id,"
                "raid_viewer_count,notification_at,received_at,run_id) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (f"raid-{index}", f"source-{index}", 10 * index,
                 START + timedelta(minutes=minute),
                 START + timedelta(minutes=minute, seconds=1), run_id),
            )

    def test_five_minute_window_metrics_are_hand_verifiable(self):
        row = self.connection.execute(
            "SELECT viewer_available,time_weighted_avg_viewers,chat_available,message_count,"
            "active_chatter_count,top_five_message_share,presence_available,collected_presence_count,"
            "active_chatter_in_presence_count FROM analytics_stream_participation "
            "WHERE stream_id='current' AND window_index=0"
        ).fetchone()
        self.assertEqual(row[:6], (
            True, Decimal("102.00"), True, 6, 6, Decimal("0.8333"),
        ))
        self.assertEqual(row[6:], (True, 6, 6))

    def test_distinct_counts_are_not_fanned_out_by_presence_members(self):
        row = self.connection.execute(
            "SELECT message_count,active_chatter_count,collected_presence_count "
            "FROM analytics_stream_participation WHERE stream_id='current' AND window_index=0"
        ).fetchone()
        self.assertEqual(row, (6, 6, 6))

    def test_half_open_stream_boundary_leaves_raid_unresolved(self):
        row = self.connection.execute(
            "SELECT candidate_count,associated_stream_id,association_status,timestamp_basis "
            "FROM analytics_event_associations WHERE event_id='raid-boundary'"
        ).fetchone()
        self.assertEqual(row, (0, None, "unresolved", "eventsub_envelope_time"))

    def test_returning_is_separate_from_recurring_and_identity_is_anonymous(self):
        row = self.connection.execute(
            "SELECT anonymous_chatter_id,first_observed_chatter,returning_chatter,recurring_chatter,"
            "prior_streams_180d FROM analytics_chatter_participation "
            "WHERE stream_id='current' AND anonymous_chatter_id=md5('stream-pulse:chatter:user-0')"
        ).fetchone()
        self.assertEqual(len(row[0]), 32)
        self.assertEqual(row[1:], (False, True, False, 1))

    def test_historical_baseline_reports_sample_size(self):
        row = self.connection.execute(
            "SELECT all_stream_sample_size FROM analytics_historical_comparison "
            "WHERE stream_id='current' AND window_index=0"
        ).fetchone()
        self.assertEqual(row, (1,))

    def test_chat_gap_turns_affected_window_null_not_zero(self):
        run_id = self.connection.execute(
            "SELECT run_id FROM collector_runs LIMIT 1"
        ).fetchone()[0]
        self.connection.execute(
            "INSERT INTO collection_health(run_id,source,observed_at,status,reason_code) "
            "VALUES (%s,'chat',%s,'error','network_error'),"
            "(%s,'chat',%s,'healthy','capture_ready')",
            (run_id, START + timedelta(minutes=2),
             run_id, START + timedelta(minutes=3)),
        )
        row = self.connection.execute(
            "SELECT chat_available,message_count,active_chatter_count,chat_healthy_seconds "
            "FROM analytics_stream_participation WHERE stream_id='current' AND window_index=0"
        ).fetchone()
        self.assertEqual(row, (False, None, None, 240))

    def test_raid_overlap_missing_baseline_and_end_truncation_are_explicit(self):
        five = self.connection.execute(
            "SELECT viewer_baseline_available,overlapping_raid,stream_end_truncated "
            "FROM analytics_raid_impact WHERE raid_id='raid-1' AND horizon_minutes=5"
        ).fetchone()
        fifteen = self.connection.execute(
            "SELECT stream_end_truncated,viewer_horizon_available,"
            "participation_horizon_available,follow_horizon_available "
            "FROM analytics_raid_impact WHERE raid_id='raid-1' AND horizon_minutes=15"
        ).fetchone()
        self.assertEqual(five, (False, True, False))
        self.assertEqual(fifteen, (True, False, False, False))

    def test_timeline_event_markers_distinguish_zero_from_unavailable(self):
        first = self.connection.execute(
            "SELECT raid_events_available,raid_event_count,follow_events_available,"
            "follow_event_count FROM analytics_stream_events "
            "WHERE stream_id='current' AND window_index=0"
        ).fetchone()
        self.assertEqual(first, (True, 2, True, 0))

    def test_prepare_is_idempotent_for_unchanged_inputs(self):
        with tempfile.TemporaryDirectory() as output:
            with patch("scripts.post_stream.PRIVATE_OUTPUT_DIR", Path(output)), \
                    patch("scripts.post_stream.PROJECT_ROOT", Path(output)):
                first = prepare(self.connection, "current")
                export = Path(output) / "reviewed-export.pdf"
                export.write_bytes(b"synthetic export")
                refreshed = record_refresh(self.connection, first["analysis_id"], export)
                reviewed = record_review(self.connection, first["analysis_id"], "approve")
                second = prepare(self.connection, "current")
        self.assertEqual(first["analysis_id"], second["analysis_id"])
        self.assertEqual(first["workflow_status"], "refresh_pending")
        self.assertEqual(refreshed["workflow_status"], "review_pending")
        self.assertEqual(reviewed["workflow_status"], "approved")
        self.assertEqual(second["workflow_status"], "approved")
        self.assertTrue(second["reused"])
        self.assertEqual(self.connection.execute(
            "SELECT count(*) FROM post_stream_analysis_runs"
        ).fetchone(), (1,))


if __name__ == "__main__":
    unittest.main()
