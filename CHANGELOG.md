# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.14.1] - 2026-08-16

### Fixed

- **Extraction filenames are now deterministic across `pendulum` versions, so valid files are no longer quarantined** ([#83](https://github.com/diegoscarabelli/garmin-health-data/pull/83)). The extractor stamped every filename via `pendulum.instance(midday_dt, tz="UTC").to_iso8601_string()`, whose rendering of a UTC instant changed between `pendulum` major versions: 3.x emits `...T12:00:00Z` (written to disk as `...T12-00-00Z` after the colon-to-hyphen swap), while 2.x emits `...T12:00:00+00:00` (written as `...T12-00-00+00-00`). The processor's three filename patterns accepted `Z` but not `+`, so on any environment that resolved `pendulum` 2.x every extracted JSON/FIT/TCX file failed `_parse_filename` and was routed to quarantine, leaving an empty database. The offset was always UTC (`+00:00`), never a local zone: all three filename paths force `tz="UTC"`. Filenames are now built by a single helper as a fixed, colon-free, `Z`-suffixed `YYYY-MM-DDT12-00-00Z` string that does not depend on the installed `pendulum` version, and the fragile whole-filename `.replace(":", "-")` is gone. So files already quarantined by an affected install can be recovered, the processor's three patterns also still accept the legacy `+00-00` offset form: move those files back into the ingest directory and re-run to load them. Reported by @OleMantei.

## [2.14.0] - 2026-08-14

### Changed

- **`MENSTRUAL_CYCLE_DAY` no longer fans out ~90 `dayview` calls for accounts with no menstrual data**. `MENSTRUAL_CYCLE_DAY` re-fetches a trailing 90-day window on every advancing run (the [#70](https://github.com/diegoscarabelli/garmin-health-data/issues/70) retroactive-edit refresh), one `dayview` call per day — a cost paid by every account, including those that never track menstrual cycles (e.g. most male accounts). The extractor now runs a single cheap probe of the menstrual calendar (summary) endpoint over the same window before the per-day fan-out: the `dayview` endpoint returns data only for dates inside a cycle window, and the calendar endpoint reports every cycle whose window overlaps the queried range (verified against the live API, including a cycle whose start predates the query window), so zero reported cycles means every `dayview` call in the window would be empty and the fan-out is skipped entirely. Gated on `cycleSummaries` only. The probe fails open — any error, or no calendar response at all (every chunk failing transport), falls back to running the full window — so it can never suppress a real extraction. Net effect: one extra calendar call per run for active trackers, ~90 calls removed per run for accounts with no cycles in the window. The [`--exclude-data-types`](https://github.com/diegoscarabelli/garmin-health-data/issues/79) flag remains as the manual opt-out.

## [2.13.0] - 2026-08-07

### Added

- **`--exclude-data-types` flag for `garmin extract`** ([#79](https://github.com/diegoscarabelli/garmin-health-data/issues/79)). Extracts every registered data type *except* the named ones, so a caller can skip a type (e.g. `MENSTRUAL_CYCLE_DAY` and its ~90-day retroactive re-fetch) without enumerating the ~20 types they do want. Unlike a hardcoded `--data-types` allowlist, exclusion is forward-compatible: data types added to the tool later are still extracted by default. Mutually exclusive with `--data-types`; an unknown excluded name or excluding every type aborts with a clear error before any work.

### Fixed

- **`extract` no longer calls the API when the database is already up to date** ([#77](https://github.com/diegoscarabelli/garmin-health-data/issues/77)). The auto-detected start is `get_latest_date() + 1 day`, which on a current database lands on tomorrow while the end defaults to today. Only `DAILY` types treated that inverted window as empty: `RANGE` types still fired with a backwards range (two returning HTTP 400 through the full retry ladder), `NO_DATE` types re-downloaded regardless, and `_retroactive_lookback_start` revived the window into ~90 days of `MENSTRUAL_CYCLE_DAY` calls — ~98 API calls for a no-op run. `extract()` now returns early once the resolved start is after the resolved end. Same-day windows stay inclusive and still dispatch. The CLI also tidies its `Date range` line: it is suppressed for this no-op case (previously a confusing backwards range), and a same-day window is labeled `single day, inclusive` rather than the misleading `(exclusive)`.

## [2.12.0] - 2026-07-24

### Added

- **`RUNNING_TOLERANCE` data type** ([#71](https://github.com/diegoscarabelli/garmin-health-data/issues/71)). Downloads Garmin's biomechanical running-load model from `/metrics-service/metrics/runningtolerance/stats` (`aggregation=daily`) and stores one row per calendar day in a new `running_tolerance` table: `total_impact_load` (daily biomechanical running load), `total_distance`, `tolerance` (the tolerated running-load ceiling Garmin computes for the containing week), and the `start_of_week` / `end_of_week` / `week_index` grouping. This is a distinct signal from the existing cardiovascular/metabolic load metrics (training load, ACWR, training readiness): it models running-specific musculoskeletal tolerance. Extracted as a RANGE type with a per-day splitter and upserted by `(user_id, date)`. The endpoint requires a compatible Garmin watch; accounts without one return an empty array and simply store no rows.

### Fixed

- **Multi-sport (duathlon/triathlon) activities now capture per-leg data** ([#72](https://github.com/diegoscarabelli/garmin-health-data/issues/72)). A `multi_sport` parent's per-leg sport aggregates (`running_agg_metrics` / `cycling_agg_metrics` / `swimming_agg_metrics`) were never populated: the parent dispatches on `activity_type_key == "multi_sport"` (matching no sport branch), and the individual legs are hidden under the parent (`metadataDTO.childIds`) so they never appeared in the activities list and were never fetched. (The parent's time-series and laps *were* already stored, from its single combined FIT.) Extraction now detects a parent activity via the structural `parent` flag and fetches each leg's own detail; each leg is stored as its own `activity` row linked to the parent by a new nullable `parent_activity_id` column, and its sport-specific aggregates are populated directly from the leg's `summaryDTO`. Because a leg can legitimately share a start instant with an independently-recorded standalone activity of the same event, the `activity` table's `UNIQUE (user_id, start_ts)` guard becomes a partial unique index scoped to non-leg rows (`WHERE parent_activity_id IS NULL`), preserving the duplicate guard (#66/#67) for standalone activities. Existing databases are migrated in place (add the column, rebuild the table to swap the constraint, preserving all data and child FKs) automatically on the next `extract`, or via the new `garmin migrate-multisport` command. Known limitation: Garmin's per-activity detail endpoint does not return the cycling power-curve buckets (`max_avg_power_*`), so those columns stay NULL for multi-sport bike legs. Verified end-to-end on a real triathlon: swim/bike/run legs now populate their aggregates and link to the parent.

- **`MENSTRUAL_CYCLE_DAY` now refreshes `day_in_cycle` after a retroactive period edit** ([#70](https://github.com/diegoscarabelli/garmin-health-data/issues/70)). `MENSTRUAL_CYCLE_DAY` is a per-day (`DAILY`) type: each day's `dayInCycle` (and `cycle_start_date`, `current_phase`, ...) is stored verbatim from Garmin's `dayview` response, upserted by `(user_id, date)`. The incremental extraction window is `[last_update + 1, today]` (driven by the other daily datasets; menstrual is not counted), so a routine run only re-fetches the last day or two. When a user retroactively moves or sets a period start, Garmin recomputes `dayInCycle` for every following day, but those past days fall outside the incremental window and are never re-fetched, so their rows keep stale values (`MENSTRUAL_CYCLE_SUMMARY` stays correct because it is range-extracted and fully refreshed each run). Confirmed against a real account: moving a period start two days earlier eliminated a predicted cycle and cascaded a `day_in_cycle` change back ~31 days, leaving roughly a month of stale day rows after an ordinary incremental run. The extractor now re-fetches a trailing 90-day window for `MENSTRUAL_CYCLE_DAY` on every run (`effective_start = min(requested_start, end_date − 90 days)`, only ever extending backward so explicit full-history backfills are untouched), and the existing upsert overwrites the stale rows with Garmin's authoritative values. A recompute-from-summary approach was ruled out because the calendar/summary endpoint omits predicted cycles, so their boundaries cannot be reconstructed. Days outside any cycle window return empty payloads and are skipped, so the extra calls are cheap.

- **`BODY_COMPOSITION` now captures every weigh-in on days with more than one** ([#69](https://github.com/diegoscarabelli/garmin-health-data/issues/69)). Extraction hit `/weight-service/weight/daterangesnapshot`, the endpoint behind Garmin Connect's weight *trend chart*, which returns only one representative weigh-in per calendar day. Users who weigh more than once a day (e.g. morning and evening) silently lost all but one weigh-in at the API boundary, regardless of the `(user_id, timestamp)` primary key on `body_composition` that was meant to hold multiple. This predates the per-day split in [#65](https://github.com/diegoscarabelli/garmin-health-data/pull/65): the endpoint was `daterangesnapshot` from the feature's introduction in [#49](https://github.com/diegoscarabelli/garmin-health-data/pull/49), and #65 only changed the call cadence (day-by-day → one range call + split), not the endpoint, so the "multiple weigh-ins per day are preserved" claim was never actually true end-to-end. Extraction now uses `/weight-service/weight/range/{start}/{end}` with `includeAll=true`, which returns every weigh-in grouped under `dailyWeightSummaries[].allWeightMetrics`. The per-day file splitter reads that shape and groups by Garmin's own `summaryDate` (the user-facing local day) rather than each weigh-in's UTC `timestampGMT`, so two weigh-ins on the same local day land in one per-day file even when their UTC timestamps straddle midnight (matching how the `ACTIVITIES_LIST` splitter groups by local date). Still one API call per window (the #65 optimization is preserved), and the per-day file shape at the processor boundary is unchanged (`allWeightMetrics` entries carry the same field names the processor already reads), so `_process_body_composition` is untouched. Verified end-to-end on a real account: a day with a morning and an evening weigh-in now writes two `body_composition` rows instead of one.

## [2.11.2] - 2026-05-26

### Fixed

- **Activity processor now skips duplicate `(user_id, start_ts)` rows with a warning instead of quarantining the entire day's FileSet** ([#66](https://github.com/diegoscarabelli/garmin-health-data/issues/66)). The `activity` table's `UNIQUE (user_id, start_ts)` constraint is a sensible real-world invariant ("one user, one activity at any given instant") that Garmin Connect itself does not enforce: users can create multiple activities with identical start times (manual entries created twice, two devices recording the same workout, etc.). Previously the second activity's insert raised `IntegrityError` from the secondary UNIQUE index, the per-FileSet error handler caught it, and the whole `(user, day)` FileSet quarantined — losing sleep, HR, stress, training_readiness, training_status, intensity_minutes, floors, steps, respiration, menstrual_cycle_day, etc. for that day along with the duplicate activity. The processor now checks for an existing `(user_id, start_ts)` row with a different `activity_id` before the upsert; on hit, the duplicate is logged with a yellow warning naming both `activity_id`s and a hint to delete one in Garmin Connect, then skipped. First-seen activity wins; the rest of the day's data loads normally. Re-extracting the same `activity_id` is unaffected (the existence query excludes the same id from the conflict set). Skipped `activity_id`s are tracked on the processor so the downstream FIT, TCX, and `EXERCISE_SETS` per-activity processors also skip cleanly instead of FK-failing on the missing parent row. Discovered during PR #65 e2e testing.

## [2.11.1] - 2026-05-26

### Changed

- **Extractor now makes one API call per range for `RANGE`-typed data types** ([#62](https://github.com/diegoscarabelli/garmin-health-data/issues/62), [#65](https://github.com/diegoscarabelli/garmin-health-data/pull/65)). Previously the extractor called every `RANGE`-typed method day-by-day with `startdate=enddate`, wasting API quota and (for `MENSTRUAL_CYCLE_SUMMARY`) re-firing the wipe-and-replace policy once per file. A 30-day extract of any `RANGE` type now issues one API call instead of 30. For `BODY_COMPOSITION` and `ACTIVITIES_LIST`, the per-range response is split back into one file per day before write, so the downstream `(user, day)` FileSet abstraction is unchanged and the processor parses each per-day file identically to a legacy per-day API response (range-level wrapper fields like `startDate` / `endDate` / `totalAverage` that the processor doesn't consume are dropped from the split files; the data the processor reads is the same). `MENSTRUAL_CYCLE_SUMMARY` is intentionally unsplittable and writes one file stamped with `end_date`: its wipe-and-replace policy for predicted cycles needs to see the full new set of cycles atomically, so splitting would re-introduce the redundant-write bug this PR fixes.
- **`APIMethodTimeParam.PER_ACTIVITY` taxonomy** ([#65](https://github.com/diegoscarabelli/garmin-health-data/pull/65)). `ACTIVITY` and `EXERCISE_SETS` were previously mis-classified as `RANGE` in the registry but special-cased by name in `_extract_data_by_type` because their actual API methods take an `activity_id`, not a date range. Added a fourth enum value `PER_ACTIVITY`, reclassified the two stragglers, and replaced the name-based short-circuit with an enum branch so the dispatcher is pure: every data type lands in exactly one branch based on its declared time-param shape. Registry gains a matching `per_activity_data_types` convenience property.

### Added

- **`GarminClient` delegator-coverage test** ([#63](https://github.com/diegoscarabelli/garmin-health-data/issues/63), [#65](https://github.com/diegoscarabelli/garmin-health-data/pull/65)). The extractor calls `getattr(self.garmin_client, data_type.api_method)`; a missing delegator method would raise `AttributeError` only at extract time, which the existing unit tests for the plain `api.<method>` functions couldn't catch (they call the module-level function directly). Discovered during PR #64 work: `get_menstrual_data_for_date` and `get_menstrual_calendar_data` were added to `api.py` with passing unit tests but the corresponding `GarminClient` delegators were missed; only an end-to-end `AttributeError` exposed it. New parametric test walks `GARMIN_DATA_REGISTRY` and asserts every registered `api_method` is callable on `GarminClient`.

## [2.11.0] - 2026-05-26

### Added

- **Menstrual cycle data extraction** ([#61](https://github.com/diegoscarabelli/garmin-health-data/issues/61), [#64](https://github.com/diegoscarabelli/garmin-health-data/pull/64)): two new Garmin Connect data types backed by the `periodic-health` service. `MENSTRUAL_CYCLE_DAY` (DAILY) extracts the per-day cycle state from the `dayview` endpoint: phase (MENSTRUAL / FOLLICULAR / OVULATORY / LUTEAL), day-in-cycle, period length, predicted cycle length, plus the user's symptoms / moods / discharge tags, flow, sex drive, sexual activity, freeform notes, ovulation and baby-movement flags. `MENSTRUAL_CYCLE_SUMMARY` (RANGE) extracts per-cycle summaries from the `calendar` endpoint, covering both user-logged cycles and Garmin's projections of upcoming cycles; the API wrapper paginates >92-day ranges into the endpoint's max chunk size and merges results, deduping by `startDate`. Rows persist for any day inside an observed or predicted cycle window; days the user has not logged within a predicted window get `daySummary` filled in and `dayLog` columns NULL.
- **Three new schema tables (total 35 → 38)**: `menstrual_cycle_day` (one row per day inside any observed or predicted cycle window, UPSERT by `(user_id, date)`); `menstrual_cycle_tag` (polymorphic tag table for symptoms / moods / discharge with a composite `(user_id, date)` cascade FK to `menstrual_cycle_day`; processor uses delete-then-reinsert per day so user-removed tags propagate); `menstrual_cycle_summary` (one row per cycle, observed rows UPSERT by `(user_id, start_date)`, predicted rows wipe-and-replace on every extract because Garmin recomputes projection dates as new data is logged). Cycle length is intentionally not stored; derive in SQL via `LEAD(start_date) OVER (PARTITION BY user_id ORDER BY start_date) - start_date`.
- **`MenstrualCyclePhase` IntEnum** mirroring the `SleepStage` precedent (1=MENSTRUAL, 2=FOLLICULAR, 3=OVULATORY, 4=LUTEAL): the raw integer index from `daySummary.currentPhase` is denormalized to the text label and stored in `menstrual_cycle_day.current_phase`; the integer is not persisted.

### Changed

- **Duplicate prevention is now a four-tier approach** (was three-tier). The new fourth tier documents the wipe-and-replace policy for predicted menstrual cycle summary rows: Garmin's projected start dates drift between runs, so the `(user_id, start_date)` PK no longer matches the latest projection on each extract; a pure UPSERT would accumulate stale predicted rows. The processor `DELETE`s all `predicted_cycle = TRUE` rows for the user before inserting the new set; observed cycles continue to UPSERT and survive untouched.

### Fixed

- **Test pollution: `tests/test_auth_extended.py` created an empty `~/.garminconnect/12345678/` directory on every CI run** ([#64](https://github.com/diegoscarabelli/garmin-health-data/pull/64)). The four `TestRefreshTokens` cases called `refresh_tokens()` without overriding the default `base_token_dir="~/.garminconnect"`. Because `get_user_profile` was mocked to return `{"id": "12345678"}`, the real auth code's `Path.mkdir` ran against `~/.garminconnect`. `garmin.dump()` was a MagicMock so no token file was written, but the empty directory was then discovered as a candidate account by subsequent `garmin extract` runs and surfaced as a phantom user. Tests now pass `base_token_dir=str(tmp_path)`.
- **Wrapper returning `None` for empty calendar responses made the wipe-and-replace path unreachable** ([#64](https://github.com/diegoscarabelli/garmin-health-data/pull/64)). The original `get_menstrual_calendar_data` returned `None` whenever the API responded with no cycles. Because the extractor guards file writes with `if data:`, no file landed and the processor never ran, so stale `predicted_cycle = TRUE` rows from earlier extracts could not be wiped if the user later stopped tracking cycles or queried a window with no cycles. Fix distinguishes "transport failure on every chunk" (still returns `None`) from "HTTP success with `cycleSummaries: []`" (returns the merged shape with empty arrays so the file lands and the wipe fires).

## [2.10.0] - 2026-05-16

### Added

- **TCX activity file processing** ([#56](https://github.com/diegoscarabelli/garmin-health-data/pull/56)): activities uploaded to Garmin Connect from older devices or third-party apps in TCX format are now parsed end-to-end alongside FIT. Per-trackpoint sensor data lands in `activity_ts_metric`, per-lap summaries in `activity_lap_metric`, and trackpoints with GPS materialize into `activity_path`. The extractor stops emitting "unknown format" warnings for TCX, the filename pattern accepts `.tcx`, and a new `_process_activity_file` dispatcher routes by extension so future formats (GPX, etc.) drop in cleanly. `activity_split_metric` is FIT-only (TCX has no split concept). Idempotent reprocessing matches the FIT path: re-running cleanly replaces all activity-child rows.

### Changed

- **Shared `_persist_activity_metrics` helper**: extracted the delete+insert+log persistence tail that FIT and TCX both used. Behavior is identical; the previously-duplicated ~80-line block now lives in one place.
- **TCX `position_lat`/`position_long` stored as semicircles in `activity_ts_metric`** to match FIT's contract under the same metric names. `activity_path` keeps decimal degrees for both formats.

### Fixed

- **Hardened TCX XML parsing**: parsing now uses `defusedxml` (XXE / billion-laughs defense for files from third-party sources), wraps `ET.parse` in `try/except` so a malformed TCX produces a `ValueError` naming the file rather than an opaque traceback, and reuses the existing `_parse_garmin_gmt` helper for trackpoint timestamps so single-/two-digit fractional seconds (e.g. `2024-01-01T08:00:01.5Z`) parse correctly on Python 3.10's strict `datetime.fromisoformat`.

## [2.9.1] - 2026-05-06

### Fixed

- **Defensive fix for a latent bug class in `upsert_model_instances`** ([#57](https://github.com/diegoscarabelli/garmin-health-data/pull/57)): the helper built its primary-key exclusion set from `Column.name` and indexed `excluded[...]` with `conflict_columns[0]` directly, both of which are wrong against any model declared as `Column('db_name', key='attr_name')`. Not exploitable today because every garmin model uses plain `Column(Type, ...)` so `name == key` everywhere; becomes a silent PK renumbering / `KeyError` the moment a divergent model is added. Adopts the openetl contract (`conflict_columns` are NAMES per SQLAlchemy's `index_elements`; `update_columns` and `returning_columns` are KEYS per `excluded[...]` and `getattr(model, ...)`) with a `name_to_key` translation at the boundaries. Also documents that `update_ts` is unconditionally refreshed to `CURRENT_TIMESTAMP` on every conflict update (audit semantics; cannot be backfilled or preserved through `update_columns`).

### Changed

- **`upsert_model_instances` validation hardened**: duplicate detection on `conflict_columns`, `update_columns`, `returning_columns`; column-existence checks for `conflict_columns` (against names) and `update_columns` (against keys); empty-`update_dict` fallback to the no-op `DO UPDATE` trick when the auto-derived update list resolves to empty (e.g. tables with only PK + conflict + audit columns), avoiding invalid `DO UPDATE SET` SQL.

## [2.9.0] - 2026-05-05

### Added

- **`garmin prune` command** ([#51](https://github.com/diegoscarabelli/garmin-health-data/issues/51)): deletes rows from `activity_ts_metric` for activities whose `start_ts` falls in `[--start-date, --end-date)`, with the same-day inclusion rule the `extract` command uses. Activity rows themselves, splits, laps, agg metrics, paths, and downsampled buckets are preserved. Supports `--dry-run`, `--accounts`, `--yes`, and a confirmation prompt by default. The per-second FIT-derived sensor table is ~93% of typical disk usage; pruning it solves the long-tail growth problem with no schema changes for existing users.
- **`garmin downsample` command** ([#51](https://github.com/diegoscarabelli/garmin-health-data/issues/51)): aggregates `activity_ts_metric` rows into time-bucketed records in a new `activity_ts_metric_downsampled` table. `--time-grain` accepts `^([1-9][0-9]*)(s|m)$` (e.g., `30s`, `60s`, `5m`, `15m`, `60m`); hours are intentionally not supported. Bucket alignment is activity-start-relative so buckets never span activity boundaries. Three-strategy registry covers all 28 currently observed metric names: `AGGREGATE` (default; avg + min + max for instantaneous numeric metrics), `LAST` (cumulative metrics like `distance` and `accumulated_power`, plus `accumulated_*`/`total_*` heuristic), and `SKIP` (GPS coordinates, since `activity_path` already materializes the polyline). Activity-level replace semantics: re-running for an activity with a different `--time-grain` cleanly wipes its prior buckets; activities whose source rows have been pruned are excluded from the replace set entirely so their existing buckets survive.
- **`garmin migrate-cascade` command** ([#51](https://github.com/diegoscarabelli/garmin-health-data/issues/51)): one-shot retrofit of `ON DELETE CASCADE` onto the 16 child FKs (10 activity-children + 6 sleep-children) in pre-2.9 databases. SQLite has no `ALTER TABLE` for changing FK actions, so each affected child table is rebuilt via the standard 12-step recreate dance. Idempotent (skips tables that already have cascade), pre-flight `PRAGMA foreign_key_check` refuses to migrate a database with existing FK violations, backup file written next to the database unless `--no-backup`, marked for removal in a future major version.
- **`extract` automation flags**: `--prune-older-than DURATION` and `--downsample-older-than DURATION --downsample-grain GRAIN` for cron use. `DURATION` accepts `90d`, `6m`, `1y`. Computes the effective `--end-date` as `today - DURATION`. Default `extract` behavior is unchanged when these flags are absent.
- **New `activity_ts_metric_downsampled` table** keyed on `(activity_id, bucket_ts, name)` with `bucket_seconds` recorded as metadata.

### Fixed

- **Sleep detail tables silently empty since the feature shipped** ([#52](https://github.com/diegoscarabelli/garmin-health-data/issues/52)): `_process_sleep_base` returned `None` and short-circuited the orchestrator before any of the six per-night detail extractors (`sleep_level`, `sleep_movement`, `sleep_restless_moment`, `spo2`, `hrv`, `breathing_disruption`) ran. The bulk-upsert helper was returning the input ORM instances (with `sleep_id=None`) instead of the rows actually persisted to SQLite. `_process_sleep_base` now reads the auto-generated `sleep_id` back via SQLite `RETURNING`, so the detail extractors receive a real foreign key and the six tables are populated as designed. Helper refactor (`upsert_model_instances` gains an opt-in `returning_columns` parameter, SQLite >= 3.35 required for `INSERT ... ON CONFLICT ... RETURNING`) makes the same class of bug structurally impossible. Re-running extraction over existing `storage/` SLEEP JSONs is safe (idempotent) and will backfill the detail tables. Fixed in #54.

### Changed

- **Schema: `ON DELETE CASCADE` added to all 16 activity-child and sleep-child FKs** in `tables.ddl` and `models.py`. v2.9 retention features only delete from one childless table (`activity_ts_metric`), but shipping cascade now means future expansion to full multi-table retention is code-only, not another schema migration.
- **`garmin_health_data.db.get_engine` now attaches a `connect` event listener** that runs `PRAGMA foreign_keys = ON` on every new connection. SQLite's per-connection default is OFF, so cascade clauses defined in the schema would otherwise be silently inert.

### Migration notes

Existing databases keep their pre-2.9 cascade-less FK definitions even after upgrading the package; SQLite has no in-place way to retrofit cascade. Run `garmin migrate-cascade` once to rebuild the affected child tables. The migration is safe to run on a 2.9-fresh database (it's idempotent and skips tables already correct), runs inside per-table transactions, and writes a backup file by default.

## [2.8.0] - 2026-05-02

### Added

- **`BODY_COMPOSITION` data type**: scale weigh-ins from a connected smart scale (e.g. Index S2) or manual weight entries. Captures weight, BMI, body fat %, body water %, bone mass, muscle mass, physique rating, visceral fat, metabolic age, and `source_type` (e.g. `INDEX_SCALE`, `MANUAL`). Persisted to a new `body_composition` table keyed by `(user_id, timestamp)` so multiple weigh-ins per day are preserved; weight and bone/muscle mass stored in grams to match the existing `user_profile.weight` convention. Insert-only with `ON CONFLICT DO NOTHING` (measurements are immutable). Contributed by @amanusk in #49.
- **`sample_pk` column on `body_composition`**: nullable `BIGINT` capturing Garmin's stable per-sample identifier (`samplePk` from the API), with a non-unique index. Provides a stable handle for reconciling rows against deletions made in Garmin Connect (e.g. user removes a bad weigh-in). Nullable because manual entries lack the field.

### Fixed

- **`get_body_composition` saved one useless JSON file per day for users with no scale data**. The Garmin `/weight-service/weight/daterangesnapshot` endpoint returns a populated wrapper dict on no-data days (`startDate`, `endDate`, an empty `dateWeightList`, and a `totalAverage` of nulls) rather than an empty response. The extractor's generic `if data:` truthiness check saw the wrapper as truthy and wrote a file. The API client now collapses the empty-wrapper shape to `None` so the extractor short-circuits, matching the contract of other RANGE-typed endpoints (e.g. `ACTIVITIES_LIST`).

### Changed

- **`_process_body_composition` now warns when an entry has neither `timestampGMT` nor `date`**: previously such entries were silently skipped. A yellow `⚠️ Skipping body composition entry with no timestamp` warning matches the convention in `_process_training_readiness` / `_process_floors` and surfaces silent data loss in the run log.
- **API module docstring** (`garmin_client/api.py`): bumped endpoint count from 15 to 16 and renamed the "Range activities" bucket to "Range data" to accurately describe both activity-related and wellness range endpoints.
- **README**: added `BODY_COMPOSITION` to the data types table and the Health Time-Series table-structure section (now 8 tables); bumped the total table count from 33 to 34 across the schema overview, project pitch, and comparison matrix.

## [2.7.4] - 2026-04-30

### Fixed

- **`garmin info` and `garmin verify` rejected a missing default database with an unhelpful Click validator error** (`Invalid value for '--db-path': Path 'garmin_data.db' does not exist.`). The function bodies already contained a friendlier "Database not found, run `garmin extract`" fallback, but it was unreachable because `type=click.Path(exists=True)` rejected the input first. The `exists=True` constraint has been removed so the in-function check runs; both commands now exit 1 (so scripts can detect the failure) and both print the "run `garmin extract`" hint (previously only `info` did). Fixed in #47.
- **PyPI new-version hint did not appear on bare `garmin`.** The version check is wired into the `@click.group()` callback, which Click does not invoke when no subcommand is supplied. Added `invoke_without_command=True` so the hint fires on bare invocations as well; help is rendered manually in that case to preserve the existing user-visible behavior. Fixed in #47.

## [2.7.3] - 2026-04-30

### Fixed

- **`garmin extract` crashed with `sqlite3.OperationalError: near ".": syntax error` on a fresh database.** The `INSERT INTO user ... ON CONFLICT (user_id) DO NOTHING` statement in `GarminProcessor._ensure_user_exists()` was authored as a triple-quoted string passed to `sqlalchemy.text(...)`. `docformatter` misidentified that string as a docstring and "normalized" it by appending a period after `DO NOTHING`, producing invalid SQL. The path was only reachable when the `user` table was empty, so existing installs were unaffected and CI didn't catch it. The SQL is now written as implicitly concatenated regular string literals so docformatter leaves it untouched. Reported by @nakor in #44, fixed in #45.

## [2.7.2] - 2026-04-28

### Fixed

- **Per-account partial failures dropped on account-level crash** in the multi-account `extract()` loop. `all_failures.extend(extractor.failures)` lived inside the `try` block on the success path only, so any per-date / per-data-type / per-activity failures captured BEFORE an account-level crash (e.g. an exception in `extract_fit_activities` after `extract_garmin_data` already recorded several per-day failures) were silently lost from the end-of-run summary. The merge moves to a `finally:` block so partial failures are always preserved, regardless of whether the account also crashed. Mirrors the same fix already in place in the openetl Garmin pipeline. New regression test (`test_partial_failures_preserved_when_account_crashes`) guards against the regression.

## [2.7.1] - 2026-04-28

### Fixed

- **Windows CI flake on `test_refreshes_stale_cache`**: `_read_cached()` computed `age = time.time() - st_mtime` and treated the cache as stale only when `age >= CACHE_TTL_SECONDS`. On Windows the NTFS mtime resolution is finer than `time.time()`, so a file written immediately before the check could have an mtime slightly *after* the current clock; the resulting negative age made a `TTL=0` test (and any TTL+race) treat the cache as fresh. Negative ages are now clamped to `0.0`, restoring the intended "stale at TTL=0" behavior.

## [2.7.0] - 2026-04-27

### Added

- **File lifecycle**: every extracted file is preserved on disk in a four-folder pipeline (`garmin_files/{ingest,process,storage,quarantine}/`) next to the database, mirroring the openetl pattern. State transitions are filesystem moves: extract writes to `ingest/`, the CLI bulk-moves to `process/` before parsing, then per-FileSet routes successful files to `storage/` and failed files to `quarantine/` ([#35](https://github.com/diegoscarabelli/garmin-health-data/issues/35)).
- **Crash recovery**: files left in `process/` from a crashed run are auto-moved back to `ingest/` at the start of the next run, so no extracted work is lost.
- **Concurrent-run protection**: `fcntl.flock` advisory lock on `garmin_files/.lock` prevents two simultaneous `garmin extract` runs from racing on file moves. A second invocation aborts immediately with a clear message; the lock is released automatically by the OS on process death.
- **API retries with exponential backoff**: every Garmin API call (per-day data, activity-list fetch, activity download, exercise-sets fetch) is wrapped in a 4-attempt retry loop (2s → 8s → 30s) for transient network errors (`GarminConnectionError`, `requests.exceptions.ConnectionError`, `requests.exceptions.Timeout`, `socket.gaierror`). Most DNS hiccups and brief outages absorb silently; only persistent failures reach the per-date / per-activity isolation layer ([#33](https://github.com/diegoscarabelli/garmin-health-data/issues/33)).
- **`--extract-only` flag**: download files into `ingest/` and stop, without loading them into the database. Useful for backup-only workflows or for manual inspection.
- **`--process-only` flag**: skip the API entirely and process whatever is currently in `ingest/`. Useful for retrying after a parsing fix, or for processing files that arrived from elsewhere. Does not require Garmin authentication.
- **End-of-run summary**: every per-data-type / per-date / per-activity extraction failure is listed at the end of the run, grouped for readability, so users always know what was skipped.
- **PyPI version-update hint**: every `garmin` command checks the latest version on PyPI (cached for 24h in `~/.cache/garmin-health-data/version-check.json`, opt-out with `GARMIN_NO_VERSION_CHECK=1`) and prints a one-line upgrade hint when a newer release is available. Network failures, malformed responses, and missing cache files are silently swallowed so the check never aborts a command.

### Changed

- **Per-date extraction isolation**: a transient API failure on one date is logged and recorded; extraction continues with the next date.
- **Per-data-type extraction isolation**: a structural failure for one data type is logged and recorded; extraction continues with the next data type for the same account.
- **Per-activity extraction isolation**: any exception during one activity download is logged with the activity ID; the activity-download loop continues. The activity-list (`get_activities_by_date`) call is wrapped so a list-fetch failure records an `ACTIVITIES_LIST` failure cleanly.
- **Per-FileSet processing isolation**: each FileSet runs in its own SQLAlchemy session inside try/except (mirrors openetl's `_try_process_file_set`). A bad FileSet is rolled back and moved to `quarantine/`; subsequent FileSets continue normally.
- **`extract_fit_activities` reads `ACTIVITIES_LIST` from disk**: the registry loop's saved JSON in `ingest/` is consumed directly, so the `get_activities_by_date` endpoint is hit at most once per run. Falls back to a live API call if the file is missing.
- **Renamed `_process_day_by_day` → `_extract_day_by_day`**: the function does extraction (API call + write JSON), not processing.

### Fixed

- **`UNIQUE constraint failed: activity_ts_metric` on FIT files with sub-second sampling** ([#36](https://github.com/diegoscarabelli/garmin-health-data/issues/36)): the FIT record-frame parser now reads the optional `fractional_timestamp` field paired with `timestamp` and combines them, so high-frequency devices (e.g. Fenix 7 at 2Hz smart-recording) get distinct rows per sub-second sample instead of colliding on the `(activity_id, timestamp, name)` unique key. Belt-and-suspenders: if duplicates remain (FIT files without `fractional_timestamp` that emit multiple frames within the same whole second), they are coalesced in Python before bulk insert with the last value winning, instead of aborting the activity.
- **Makefile `format` target accepts docformatter exit code 3**: `docformatter --in-place` exits 3 to signal "files modified"; the `format` target accepts both exit 1 and exit 3 as non-fatal. The pre-commit hook passes on the first run after editing any docstring.

## [2.6.1] - 2026-04-17

### Fixed

- **SQLite parameter limit safety**: `upsert_model_instances` now automatically splits large batches into chunks so the total parameter count stays within SQLite's `SQLITE_MAX_VARIABLE_NUMBER` limit (999 on pre-3.32.0 builds). Previously, a single INSERT with many rows on wide tables (e.g., Sleep at 73 columns) could exceed the limit and fail. The conservative floor of 999 guarantees safety across all supported platforms.

## [2.6.0] - 2026-04-17

### Changed

- **SQLAlchemy 2.0 ORM migration** ([#30](https://github.com/diegoscarabelli/garmin-health-data/pull/30)): Migrated all legacy SQLAlchemy 1.4 patterns to native 2.0 style, aligning runtime code with the `sqlalchemy>=2.0` dependency declared since v2.0.3.
  - Model base: `declarative_base()` replaced with `DeclarativeBase` subclass.
  - Sessions: `sessionmaker(bind=engine)` replaced with `Session(engine)` context manager.
  - Queries: all `session.query()` calls replaced with `session.execute(select(...))`.
  - Bulk deletes: `.filter_by(...).delete()` replaced with `session.execute(delete(...).where(...))`.
  - FIT metric bulk inserts: `bulk_save_objects()` replaced with core `insert()` to bypass the ORM identity map and avoid SQLite's RETURNING sentinel mismatch with `DateTime(timezone=True)` composite PKs. Column keys are precomputed once per model to avoid repeated `__table__.columns` iteration on large FIT files.
  - Strength exercise/set inserts: `bulk_save_objects()` replaced with `add_all()`.
  - Test assertions for delete statements now verify the target table and WHERE clause rather than just checking that a DELETE was executed.

### Fixed

- **Activity file format detection** ([#27](https://github.com/diegoscarabelli/garmin-health-data/pull/27)): Activity downloads containing non-FIT files (TCX, GPX, KML) no longer crash the application. Contributed by [@dillten](https://github.com/dillten).
  - Magic-byte detection identifies the actual file format from content (ANT+ FIT header, XML root elements) instead of assuming `.fit`.
  - Three-tier fallback chain: magic bytes, inner filename extension, `.bin` preservation for unrecognised formats.
  - Files are saved with the correct extension reflecting their detected format.
  - Non-FIT activity files are preserved on disk but excluded from FIT-specific processing, with clear warnings.
  - `FileSet.file_paths` now derived from matched files only, preventing `ValueError` when non-processable files sort before `.fit` files in mixed timestamp groups.
  - `GarminConnectionError` during activity download (e.g., 404 for manually-entered activities) is caught and skipped instead of aborting the entire extraction run.

## [2.5.0] - 2026-04-08

### Added

- **Vendored `garmin_client/` module** ([#25](https://github.com/diegoscarabelli/garmin-health-data/pull/25)): Replaced the `python-garminconnect` PyPI dependency with a self-contained `garmin_client/` module shipped directly in this package.
  - Five-strategy SSO fallback chain with `curl_cffi` TLS fingerprint impersonation: portal+cffi → portal+requests → mobile+cffi → mobile+requests → widget+cffi. Each strategy tries in order; the next is attempted on 429 or failure.
  - 30-45s randomized delay before the credential POST on strategies 1-4, visible at INFO log level (`"Portal login: waiting ~35s to avoid Cloudflare rate limiting..."`), so long auth runs no longer appear hung.
  - Runtime token refresh: access tokens (~18h) are auto-refreshed transparently when within 15 minutes of expiry or on a 401 retry. Refresh tokens (~30d) rotate on each use and are persisted back to disk immediately. The token chain stays alive indefinitely as long as extraction runs at least once within 30 days.
  - Atomic token writes: tokens are written to a PID-namespaced temp file and swapped in via `os.replace`, preventing truncated token stores on interrupted writes.
  - No external Garmin client library required. `curl-cffi` and `ua-generator` are now explicit runtime dependencies (previously transitive via `garminconnect`).
  - Token file format (`garmin_tokens.json`) and storage path (`~/.garminconnect/<user_id>/`) are unchanged. Existing tokens from v2.3.0+ do not require re-bootstrapping.
- **`sleep_level` table** ([#24](https://github.com/diegoscarabelli/garmin-health-data/pull/24)): New table populated from the `sleepLevels` array in the SLEEP JSON response. Each row is a contiguous interval during which a single discrete sleep stage (Deep, Light, REM, Awake) was detected, allowing reconstruction of the per-night sleep stages timeline shown in the Garmin Connect sleep view.
  - Stage codes (`stage`) and human-readable labels (`stage_label`) are sourced from the new `SleepStage` IntEnum in `constants.py`. Unknown stage codes are logged and skipped instead of failing the file.
  - Idempotent on `(sleep_id, start_ts)` via `INSERT ... ON CONFLICT DO NOTHING`.
  - Index on `stage` for cheap stage-distribution queries.
- New `SleepStage` IntEnum in `constants.py` mapping integer codes in `sleepLevels[*].activityLevel` to their human-readable names (`DEEP`, `LIGHT`, `REM`, `AWAKE`).

### Fixed

- **Python 3.10 compatibility for Garmin GMT timestamps** ([#24](https://github.com/diegoscarabelli/garmin-health-data/pull/24)): Several processors called `datetime.fromisoformat` directly on Garmin's single-digit fractional second format (e.g. `"2026-04-06T05:47:59.0"`), which Python 3.10's strict parser rejects with `ValueError`. New `_parse_garmin_iso` / `_parse_garmin_gmt` helpers on `GarminProcessor` normalize the fractional component to 6 digits and tolerate an optional trailing timezone designator (`Z` or `±HH:MM`). Applied to `sleep_level`, `sleep_movement`, `spo2`, `steps`, `floors`, `training_readiness`, and `strength_set` ingestion paths.

### Removed

- `python-garminconnect` runtime dependency ([#25](https://github.com/diegoscarabelli/garmin-health-data/pull/25)).

## [2.4.0] - 2026-04-06

### Added

- **`activity_path` table**: New table eagerly materializing GPS coordinate sequences from FIT files during processing. Each row stores an ordered `[longitude, latitude]` JSON array sorted ascending by timestamp, ready for deck.gl or any path-layer visualization. Populated automatically during FIT file processing via delete+insert for reprocessing idempotency. Activities without GPS samples (indoor workouts) have no row. Mirrors the `garmin.activity_path` table added to the openetl Garmin pipeline.
  - Three CHECK constraints enforce `path_json` integrity: valid JSON, array type, and `point_count` matching `json_array_length(path_json)`. Requires SQLite JSON1 support; JSON1 has been bundled with SQLite since 3.9, but availability in Python's built-in `sqlite3` module depends on the underlying SQLite build and may vary by environment.
  - Index on `point_count` for cheap filtering/sorting by track length.
- New constant `SEMICIRCLES_TO_DEGREES` in `constants.py` for Garmin FIT semicircle-to-decimal-degree conversion.

## [2.3.0] - 2026-04-03

### Changed

- **Upgrade to garminconnect >= 0.3.0** ([#19](https://github.com/diegoscarabelli/garmin-health-data/issues/19)): The upstream library replaced the `garth` authentication library with a native OAuth2 engine.
  - Removed `hasattr(garmin, "garth")` version guard and User-Agent override (both unnecessary with native OAuth2 and `curl_cffi` TLS fingerprint impersonation).
  - Token persistence: `garmin.garth.dump()` replaced with `garmin.client.dump()`.
  - Token file format changed from `oauth1_token.json` + `oauth2_token.json` to a single `garmin_tokens.json`. Existing tokens from garminconnect < 0.3.0 are not read by the new version; re-run `garmin auth` to bootstrap fresh tokens.
  - Token lifecycle: access tokens (~18h) are now auto-refreshed transparently using the refresh token (30 days, rotates on each use). As long as extraction runs at least once within 30 days, the token chain stays alive indefinitely.
  - `garmin auth` is now only needed for initial setup or recovery after 30+ days of inactivity (previously described as "approximately 1 year").

### Removed

- **Python 3.9 support**: garminconnect >= 0.3.0 requires Python >= 3.10. Minimum version bumped accordingly.
- `test_refresh_tokens_missing_garth_attribute` test (garminconnect 0.3.0 no longer has a `garth` attribute).

### Notes

- **Re-authentication required**: After upgrading, run `garmin auth` once per account to bootstrap tokens in the new format.

## [2.2.0] - 2026-04-01

### Added

- **Multi-account support**: Extract data from multiple Garmin Connect accounts into a single database.
  - Convention-based account discovery: scans `~/.garminconnect/` for numeric subdirectories (each is a user_id).
  - `garmin auth` auto-detects user ID and stores tokens in `~/.garminconnect/<user_id>/`.
  - `garmin extract` discovers and extracts all accounts sequentially with per-account error isolation.
  - New `--accounts` CLI option to filter which accounts to extract (comma-separated or repeated).
  - Legacy token layout (flat files at root) detected with migration warning.

### Fixed

- **SSO authentication**: Override garth's default User-Agent to avoid Cloudflare blocks during programmatic login.
- **Token file permissions**: `chmod 0o600` on token files after `garth.dump()` (garth uses default umask, leaving tokens world-readable).
- **Idempotent FIT metric reprocessing** ([#15](https://github.com/diegoscarabelli/garmin-health-data/pull/15)): Replaced the early-return guard on `activity_ts_metric`, `activity_split_metric`, and `activity_lap_metric` with a delete+insert pattern, preventing `UNIQUE` constraint violations on re-runs ([#14](https://github.com/diegoscarabelli/garmin-health-data/issues/14)). Also excludes `create_ts` from `Activity` and `Sleep` upsert update columns to preserve audit timestamps.

## [2.1.1] - 2026-04-01

### Fixed

- **Bug**: Authentication fails with `'Garmin' object has no attribute 'garth'` when using older or improperly installed `garminconnect` versions ([#13](https://github.com/diegoscarabelli/garmin-health-data/issues/13)).
  - Added a `hasattr` guard that checks for the `garth` attribute before accessing it, with a clear error message and upgrade instructions.
  - Token directory permissions tightened from `0o755` to `0o700`.
  - Auth failure messages now include the installed `garminconnect` version for easier debugging.

### Added

- Test coverage for the missing `garth` attribute scenario (`test_auth_extended.py`).

## [2.1.0] - 2026-03-27

### Added

- **Strength training exercise data** ([#11](https://github.com/diegoscarabelli/garmin-health-data/issues/11)): Per-exercise and per-set granular strength training data with two new tables and a new API data source.
  - `strength_exercise`: Per-exercise aggregates (sets, reps, volume, duration, max weight) derived from `summarizedExerciseSets` in the activities list.
  - `strength_set`: Per-set granular data (set type, duration, reps, weight, ML-classified exercise name/category) from the `/activity-service/activity/{id}/exerciseSets` API endpoint.
  - Extraction automatically fetches exercise sets for `strength_training` and `fitness_equipment` activity types alongside FIT file downloads.
  - Both tables use delete+insert for reprocessing since composite PK components can change.
  - `EXERCISE_SETS` registered as a new data type in `GarminDataRegistry`.
  - **Migration**: Seamless. New tables are created automatically on next `garmin extract` (existing data is untouched). To populate historical strength data, re-run extraction for past date ranges containing strength training activities.

## [2.0.3] - 2026-03-08

### Fixed

- **Bug**: Extractor did not function on Windows.
  - Remove incompatible char ':' from timestamp.
  - Use gettempdir() to get temp directory instead of hardcoding to /tmp.
  - Use POSIX-compatible DB URL.
  - Skip potentially problematic chmod on Windows.
  - **Impact**: Extractor now runs where it did not before.
  - **Migration**: Re-run `garmin-health-data extract`, which should now function.
- **Bug**: `garmin verify` command failed under SQLAlchemy 2.x with `sqlalchemy.exc.ArgumentError` ("Textual SQL expression ... should be explicitly declared as text(...)") due to a raw SQL string passed to `session.execute()` without a `text()` wrapper.

### Changed

- Pinned `black` to `==25.9.0` in dev dependencies to prevent formatting inconsistencies between local and CI environments.
- Bumped minimum `sqlalchemy` dependency from `>=1.4` to `>=2.0` (1.4 reached end-of-life in 2024).

### Added

- CLI test suite (`tests/test_cli.py`) with regression test for the SQLAlchemy `text()` compatibility issue.

## [2.0.2] - 2025-10-21

### Fixed

- **Bug**: Fixed extraction of sleep fields from incorrect JSON location causing NULL values in database.
  - `resting_heart_rate`, `hrv_status`, and `skin_temp_data_exists` were incorrectly being extracted from `dailySleepDTO` instead of the top-level JSON object.
  - These fields now correctly populate with data from Garmin Connect.
  - **Impact**: Existing sleep records with NULL values for these fields need to be reprocessed to populate the correct data.
  - **Migration**: Re-run `garmin-health-data process` for affected date ranges to update historical data.

## [2.0.1] - 2025-10-20

### Fixed

- **Critical**: Added missing `update_ts` column to `training_readiness` table in schema DDL.
  - Users on 2.0.0 will encounter `sqlite3.OperationalError: no such column: update_ts` when processing training readiness data.
  - Migration: Run `ALTER TABLE training_readiness ADD COLUMN update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;` or recreate database.

### Documentation

- Updated RELEASE.md instructions to match current GitHub release UI.

## [2.0.0] - 2025-10-19

### ⚠️ BREAKING CHANGES

**Database schema change: `insert_ts` renamed to `create_ts` in all tables.**

All timestamp columns previously named `insert_ts` have been renamed to `create_ts` for improved clarity and consistency with industry standards. This affects all 29 tables in the database.

**Existing databases are NOT compatible with version 2.0.0.** Users must delete their existing database and re-extract data with the new schema.

### Migration Path

**Recommended approach: Fresh database extraction**

```bash
# Backup your old database if you want to keep it
mv ~/.garmin/garmin_health_data.db ~/.garmin/garmin_health_data.db.v1_backup

# Delete the database to allow re-creation with new schema
rm ~/.garmin/garmin_health_data.db

# Re-extract all data with version 2.0.0
garmin extract --all --start-date 2020-01-01
```

All data can be re-downloaded from Garmin Connect. This is the cleanest upgrade path.

### Changed

- **BREAKING**: Renamed `insert_ts` to `create_ts` in all database tables for better semantic clarity.
- Updated SQLAlchemy models to use `create_ts`.
- Updated DDL schema file (`tables.ddl`) with `create_ts`.
- Updated all internal code references from `insert_ts` to `create_ts`.

## [1.1.0] - 2025-01-18

### Added

- DDL-based schema definition with inline SQL comments preserved in database.
- `garmin_health_data/tables.ddl` - Single source of truth for database schema.
- `CLAUDE.md` - Development guidelines and architecture documentation.
- SQLFluff configuration for SQL formatting (matching openetl standards).
- Inline SQL comments for all 29 tables and columns viewable via `sqlite_master`.
- Instructions in README.md for viewing schema documentation.

### Changed

- Personal records processing now continues with warning when activity doesn't exist (previously skipped).
- Database schema creation now executes DDL file instead of using SQLAlchemy metadata.
- SQLAlchemy models now used exclusively for ORM operations (not schema generation).
- Improved code formatting consistency across entire codebase.

### Removed

- Foreign key constraint on `personal_record.activity_id` to allow processing PRs before activities exist.

### Developer

- Added `sqlfluff>=2.0` to dev dependencies.
- Applied complete formatting standards from CLAUDE.md.
- All Python files now comply with 88 character line limit.
- Enhanced documentation in README.md Database Schema section.

### Notes

- No breaking changes for end users.
- Existing databases continue to work without modification.
- Optional: Re-initialize database to get inline comment documentation in schema.

## [1.0.1] - 2024-12-16

### Fixed

- Version consistency between package files.

## [1.0.0] - 2024-12-01

### Added

- Initial release.
- Extract Garmin Connect health data to local SQLite database.
- 29 tables for comprehensive health and fitness data.
- Automatic deduplication via SQL `ON CONFLICT` clauses.
- FIT file processing for detailed activity time-series data.
- Command-line interface with `garmin` command.
- Support for all major data types: activities, sleep, training metrics, wellness data.
- Flexible authentication with OAuth tokens.
- Comprehensive documentation and examples.

[Unreleased]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.14.0...HEAD
[2.14.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.13.0...v2.14.0
[2.13.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.12.0...v2.13.0
[2.12.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.11.2...v2.12.0
[2.11.2]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.11.1...v2.11.2
[2.11.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.11.0...v2.11.1
[2.11.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.10.0...v2.11.0
[2.10.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.9.1...v2.10.0
[2.9.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.9.0...v2.9.1
[2.9.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.8.0...v2.9.0
[2.8.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.4...v2.8.0
[2.7.4]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.3...v2.7.4
[2.7.3]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.2...v2.7.3
[2.7.2]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.1...v2.7.2
[2.7.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.7.0...v2.7.1
[2.7.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.6.1...v2.7.0
[2.6.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.6.0...v2.6.1
[2.6.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.5.0...v2.6.0
[2.5.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.4.0...v2.5.0
[2.4.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.3.0...v2.4.0
[2.3.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.1.1...v2.2.0
[2.1.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.1.0...v2.1.1
[2.1.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.3...v2.1.0
[2.0.3]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.2...v2.0.3
[2.0.2]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/diegoscarabelli/garmin-health-data/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/diegoscarabelli/garmin-health-data/releases/tag/v1.0.0
