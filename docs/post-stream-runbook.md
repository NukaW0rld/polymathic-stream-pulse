# Post-stream reporting runbook

Use this only after the collector has stopped. The reporting workflow never
closes a stream or collector run and never edits raw observations.

## Reporting installation (completed September 16, 2026)

Migrations 016–017 were applied in order after the collector stopped and a
private backup was validated. Do not rerun them against the initialized
database. For a fresh database, the commands were:

```bash
psql -d stream_pulse -X -v ON_ERROR_STOP=1 \
  -f sql/016_create_post_stream_analytics.sql
psql -d stream_pulse -X -v ON_ERROR_STOP=1 \
  -f sql/017_create_stream_event_timeline.sql
```

The migrations add one execution-manifest table and curated views. They do not
materialize analytical facts or modify collection rows.

Run the full synthetic and PostgreSQL-backed suite:

```bash
STREAM_PULSE_TEST_POSTGRES=1 \
  .venv/bin/python -m unittest discover -s tests -q
```

The combined suite passed 348 tests before the official run.

## September 17, 2026 official production acceptance

1. Run the merged collector for the complete stream and stop it orderly only
   after a successful offline observation.
2. Find/assess the stream by Amsterdam local date:

   ```bash
   .venv/bin/python -m scripts.post_stream assess \
     --date 2026-09-17 --timezone Europe/Amsterdam
   ```

   If multiple streams began that date, the command refuses to guess. Obtain the
   private stable stream ID with a restricted database query and rerun with
   `--stream-id`.

3. Prepare the private packet:

   ```bash
   .venv/bin/python -m scripts.post_stream prepare \
     --date 2026-09-17 --timezone Europe/Amsterdam
   ```

   Exit `0` means publishable or publishable with warnings. Exit `2` means
   blocked and produces diagnostics only. Output lives under ignored `recaps/`.
   Save the returned analysis ID.

4. Open `stream-pulse.pbip` in Power BI Desktop. Refresh all data, select the
   target stream, and verify the Stream evolution, Raid impact, and Community
   and comparison pages against the generated briefing. Missing values must
   remain blank, not zero. Desktop may require local PostgreSQL credentials.

5. Export the reviewed report page(s) to a private PDF or image, then record the
   current artifact:

   ```bash
   .venv/bin/python -m scripts.post_stream record-refresh \
     --analysis-id <analysis-id> --artifact /absolute/private/path/recap.pdf
   ```

6. After owner review, record the decision:

   ```bash
   .venv/bin/python -m scripts.post_stream review \
     --analysis-id <analysis-id> --decision approve
   ```

   Use `reject` when revision is required. Nothing is sent automatically.

## Reruns and recovery

The input fingerprint covers the target stream's lifecycle, run attribution,
capabilities, health/gaps, viewer observations, chat analytical identity/context,
presence metadata/membership, associated events, and stream metadata. The same
inputs plus analysis version reuse the existing analysis and artifact identity.
Changed evidence creates a new traceable analysis. Advisory locking prevents
two local processes from preparing the same stream concurrently.

A failed refresh does not call `record-refresh`; the execution remains
`refresh_pending`, so an older export cannot be labeled current. A blocked run
must be fixed through new truthful collection evidence or explicitly left
blocked—never by editing raw rows or weakening the gate.

Measure closure-to-ready time manually for the first official run: stream
offline observation, packet preparation completion, Desktop refresh completion,
export completion, and owner-review completion. The comparison target is
roughly Twitch's native-summary timescale, not a guaranteed SLA.

## Privacy

`recaps/`, production exports, stream IDs, raw event IDs, usernames, and user IDs
stay private. The semantic model imports only curated aggregates and dimensions;
it does not import raw chat text or raw identity fields. Source-controlled PBIP
caches and local settings remain ignored.
