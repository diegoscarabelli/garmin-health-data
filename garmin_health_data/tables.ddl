/*
========================================================================================
GARMIN HEALTH DATA - SQLite Database Schema
========================================================================================
Description: Database tables for storing Garmin Connect health and activity data.
             This schema is designed for SQLite and adapted from the openetl project.

Note: This file is the single source of truth for the database schema. Inline comments
      are preserved in the database and can be viewed via:
      SELECT sql FROM sqlite_master WHERE type='table';
========================================================================================
*/

----------------------------------------------------------------------------------------
-- User identity and basic demographic data from Garmin Connect.
-- Contains stable user identification and basic profile information.
CREATE TABLE IF NOT EXISTS user (
    user_id BIGINT PRIMARY KEY           -- Unique identifier for the user in Garmin Connect.
    , full_name TEXT                       -- Full name of the user.
    , birth_date DATE                      -- User birth date.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was inserted.
);

----------------------------------------------------------------------------------------
-- User fitness profile data from Garmin Connect.
-- Contains physical characteristics and fitness metrics. The latest column indicates
-- the most recent profile record for each user. Multiple records can exist per user_id,
-- but only one can have latest=True.
CREATE TABLE IF NOT EXISTS user_profile (
    user_profile_id INTEGER PRIMARY KEY  -- Auto-incrementing primary key.
    , user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , gender TEXT                          -- User gender (e.g., 'MALE', 'FEMALE').
    , weight FLOAT                         -- User weight in kilograms.
    , height FLOAT                         -- User height in centimeters.
    , vo2_max_running FLOAT                -- VO2 max for running activities.
    , vo2_max_cycling FLOAT                -- VO2 max for cycling activities.
    , lactate_threshold_speed FLOAT        -- Lactate threshold speed in m/s.
    , lactate_threshold_heart_rate INTEGER -- Lactate threshold heart rate in bpm.
    , moderate_intensity_minutes_hr_zone INTEGER -- Moderate intensity minutes heart rate zone.
    , vigorous_intensity_minutes_hr_zone INTEGER -- Vigorous intensity minutes heart rate zone.
    , latest BOOLEAN NOT NULL DEFAULT 0    -- Whether this is the most recent profile record.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS user_profile_user_id_latest_unique_idx
ON user_profile (user_id)
WHERE latest = 1;

----------------------------------------------------------------------------------------
-- Main activity table with core aggregate metrics common across activity types.
-- Additional aggregate metrics may be available in separate tables:
-- swimming_agg_metrics, cycling_agg_metrics, running_agg_metrics, supplemental_activity_metric.
CREATE TABLE IF NOT EXISTS activity (
    activity_id BIGINT PRIMARY KEY       -- Unique identifier for the activity.
    , user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , activity_name TEXT                   -- Name given to the activity.
    , activity_type_id INTEGER NOT NULL    -- Activity type identifier.
    , activity_type_key TEXT NOT NULL      -- Activity type key (e.g., 'running', 'cycling').
    , event_type_id INTEGER NOT NULL       -- Event type identifier.
    , event_type_key TEXT NOT NULL         -- Event type key.
    , start_ts DATETIME NOT NULL           -- Activity start timestamp (timezone-aware).
    , end_ts DATETIME NOT NULL             -- Activity end timestamp (timezone-aware).
    , timezone_offset_hours FLOAT NOT NULL -- Timezone offset in hours from UTC.
    , duration FLOAT                       -- Activity duration in seconds.
    , elapsed_duration FLOAT               -- Elapsed duration including pauses in seconds.
    , moving_duration FLOAT                -- Moving duration excluding pauses in seconds.
    , distance FLOAT                       -- Distance traveled in meters.
    , lap_count INTEGER                    -- Number of laps recorded.
    , average_speed FLOAT                  -- Average speed in m/s.
    , max_speed FLOAT                      -- Maximum speed in m/s.
    , start_latitude FLOAT                 -- Starting latitude coordinate.
    , start_longitude FLOAT                -- Starting longitude coordinate.
    , end_latitude FLOAT                   -- Ending latitude coordinate.
    , end_longitude FLOAT                  -- Ending longitude coordinate.
    , location_name TEXT                   -- Location name of the activity.
    , aerobic_training_effect FLOAT        -- Aerobic training effect score.
    , aerobic_training_effect_message TEXT -- Aerobic training effect message.
    , anaerobic_training_effect FLOAT      -- Anaerobic training effect score.
    , anaerobic_training_effect_message TEXT -- Anaerobic training effect message.
    , training_effect_label TEXT           -- Training effect label.
    , activity_training_load FLOAT         -- Activity training load.
    , difference_body_battery INTEGER      -- Body battery change during activity.
    , moderate_intensity_minutes INTEGER   -- Moderate intensity minutes.
    , vigorous_intensity_minutes INTEGER   -- Vigorous intensity minutes.
    , calories FLOAT                       -- Calories burned.
    , bmr_calories FLOAT                   -- BMR calories burned.
    , water_estimated FLOAT                -- Estimated water loss in milliliters.
    , hr_time_in_zone_1 FLOAT              -- Time in heart rate zone 1 in seconds.
    , hr_time_in_zone_2 FLOAT              -- Time in heart rate zone 2 in seconds.
    , hr_time_in_zone_3 FLOAT              -- Time in heart rate zone 3 in seconds.
    , hr_time_in_zone_4 FLOAT              -- Time in heart rate zone 4 in seconds.
    , hr_time_in_zone_5 FLOAT              -- Time in heart rate zone 5 in seconds.
    , average_hr FLOAT                     -- Average heart rate in bpm.
    , max_hr FLOAT                         -- Maximum heart rate in bpm.
    , device_id BIGINT                     -- Device identifier.
    , manufacturer TEXT                    -- Device manufacturer.
    , time_zone_id INTEGER                 -- Time zone identifier.
    , has_polyline BOOLEAN NOT NULL DEFAULT 0  -- Whether activity has GPS polyline data.
    , has_images BOOLEAN NOT NULL DEFAULT 0    -- Whether activity has images.
    , has_video BOOLEAN NOT NULL DEFAULT 0     -- Whether activity has video.
    , has_splits BOOLEAN DEFAULT 0             -- Whether activity has split data.
    , has_heat_map BOOLEAN NOT NULL DEFAULT 0  -- Whether activity has heat map.
    , parent BOOLEAN NOT NULL DEFAULT 0        -- Whether this is a parent activity.
    , purposeful BOOLEAN NOT NULL DEFAULT 0    -- Whether activity was purposeful.
    , favorite BOOLEAN NOT NULL DEFAULT 0      -- Whether activity is marked as favorite.
    , elevation_corrected BOOLEAN DEFAULT 0    -- Whether elevation data has been corrected.
    , atp_activity BOOLEAN DEFAULT 0           -- Whether this is an ATP activity.
    , manual_activity BOOLEAN NOT NULL DEFAULT 0 -- Whether activity was manually entered.
    , pr BOOLEAN NOT NULL DEFAULT 0            -- Whether activity contains personal records.
    , auto_calc_calories BOOLEAN NOT NULL DEFAULT 0 -- Whether calories were auto-calculated.
    , ts_data_available BOOLEAN NOT NULL DEFAULT 0  -- Whether time-series data is available.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
    , UNIQUE (user_id, start_ts)
);

CREATE INDEX IF NOT EXISTS activity_user_id_start_ts_idx
ON activity (user_id, start_ts DESC);

----------------------------------------------------------------------------------------
-- Swimming-specific aggregate metrics for pool and open water activities.
CREATE TABLE IF NOT EXISTS swimming_agg_metrics (
    activity_id BIGINT PRIMARY KEY       -- Foreign key reference to activity.activity_id.
    , pool_length FLOAT                    -- Pool length in meters.
    , active_lengths INTEGER               -- Number of active swimming lengths.
    , strokes FLOAT                        -- Total number of strokes.
    , avg_stroke_distance FLOAT            -- Average distance per stroke in meters.
    , avg_strokes FLOAT                    -- Average strokes per length.
    , avg_swim_cadence FLOAT               -- Average swim cadence in strokes/min.
    , avg_swolf FLOAT                      -- Average SWOLF score (strokes + seconds per length).
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

----------------------------------------------------------------------------------------
-- Cycling-specific aggregate metrics including power, cadence, and training.
CREATE TABLE IF NOT EXISTS cycling_agg_metrics (
    activity_id BIGINT PRIMARY KEY       -- Foreign key reference to activity.activity_id.
    , training_stress_score FLOAT          -- Training stress score.
    , intensity_factor FLOAT               -- Intensity factor.
    , vo2_max_value FLOAT                  -- VO2 max value from this activity.
    , avg_power FLOAT                      -- Average power in watts.
    , max_power FLOAT                      -- Maximum power in watts.
    , normalized_power FLOAT               -- Normalized power in watts.
    , max_20min_power FLOAT                -- Maximum 20-minute average power in watts.
    , avg_left_balance FLOAT               -- Average left/right balance percentage.
    , avg_biking_cadence FLOAT             -- Average cycling cadence in rpm.
    , max_biking_cadence FLOAT             -- Maximum cycling cadence in rpm.
    , max_avg_power_1 FLOAT                -- Max average power over 1 second.
    , max_avg_power_2 FLOAT                -- Max average power over 2 seconds.
    , max_avg_power_5 FLOAT                -- Max average power over 5 seconds.
    , max_avg_power_10 FLOAT               -- Max average power over 10 seconds.
    , max_avg_power_20 FLOAT               -- Max average power over 20 seconds.
    , max_avg_power_30 FLOAT               -- Max average power over 30 seconds.
    , max_avg_power_60 FLOAT               -- Max average power over 1 minute.
    , max_avg_power_120 FLOAT              -- Max average power over 2 minutes.
    , max_avg_power_300 FLOAT              -- Max average power over 5 minutes.
    , max_avg_power_600 FLOAT              -- Max average power over 10 minutes.
    , max_avg_power_1200 FLOAT             -- Max average power over 20 minutes.
    , max_avg_power_1800 FLOAT             -- Max average power over 30 minutes.
    , max_avg_power_3600 FLOAT             -- Max average power over 1 hour.
    , max_avg_power_7200 FLOAT             -- Max average power over 2 hours.
    , max_avg_power_18000 FLOAT            -- Max average power over 5 hours.
    , power_time_in_zone_1 FLOAT           -- Time in power zone 1 in seconds.
    , power_time_in_zone_2 FLOAT           -- Time in power zone 2 in seconds.
    , power_time_in_zone_3 FLOAT           -- Time in power zone 3 in seconds.
    , power_time_in_zone_4 FLOAT           -- Time in power zone 4 in seconds.
    , power_time_in_zone_5 FLOAT           -- Time in power zone 5 in seconds.
    , power_time_in_zone_6 FLOAT           -- Time in power zone 6 in seconds.
    , power_time_in_zone_7 FLOAT           -- Time in power zone 7 in seconds.
    , min_temperature FLOAT                -- Minimum temperature in Celsius.
    , max_temperature FLOAT                -- Maximum temperature in Celsius.
    , elevation_gain FLOAT                 -- Total elevation gain in meters.
    , elevation_loss FLOAT                 -- Total elevation loss in meters.
    , min_elevation FLOAT                  -- Minimum elevation in meters.
    , max_elevation FLOAT                  -- Maximum elevation in meters.
    , min_respiration_rate FLOAT           -- Minimum respiration rate in breaths/min.
    , max_respiration_rate FLOAT           -- Maximum respiration rate in breaths/min.
    , avg_respiration_rate FLOAT           -- Average respiration rate in breaths/min.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

----------------------------------------------------------------------------------------
-- Running-specific aggregate metrics including form, cadence, and performance.
CREATE TABLE IF NOT EXISTS running_agg_metrics (
    activity_id BIGINT PRIMARY KEY       -- Foreign key reference to activity.activity_id.
    , steps INTEGER                        -- Total number of steps.
    , vo2_max_value FLOAT                  -- VO2 max value from this activity.
    , avg_running_cadence FLOAT            -- Average running cadence in steps/min.
    , max_running_cadence FLOAT            -- Maximum running cadence in steps/min.
    , max_double_cadence FLOAT             -- Maximum double cadence in steps/min.
    , avg_vertical_oscillation FLOAT       -- Average vertical oscillation in cm.
    , avg_ground_contact_time FLOAT        -- Average ground contact time in milliseconds.
    , avg_stride_length FLOAT              -- Average stride length in meters.
    , avg_vertical_ratio FLOAT             -- Average vertical ratio percentage.
    , avg_ground_contact_balance FLOAT     -- Average ground contact balance percentage.
    , avg_power FLOAT                      -- Average power in watts.
    , max_power FLOAT                      -- Maximum power in watts.
    , normalized_power FLOAT               -- Normalized power in watts.
    , power_time_in_zone_1 FLOAT           -- Time in power zone 1 in seconds.
    , power_time_in_zone_2 FLOAT           -- Time in power zone 2 in seconds.
    , power_time_in_zone_3 FLOAT           -- Time in power zone 3 in seconds.
    , power_time_in_zone_4 FLOAT           -- Time in power zone 4 in seconds.
    , power_time_in_zone_5 FLOAT           -- Time in power zone 5 in seconds.
    , min_temperature FLOAT                -- Minimum temperature in Celsius.
    , max_temperature FLOAT                -- Maximum temperature in Celsius.
    , elevation_gain FLOAT                 -- Total elevation gain in meters.
    , elevation_loss FLOAT                 -- Total elevation loss in meters.
    , min_elevation FLOAT                  -- Minimum elevation in meters.
    , max_elevation FLOAT                  -- Maximum elevation in meters.
    , min_respiration_rate FLOAT           -- Minimum respiration rate in breaths/min.
    , max_respiration_rate FLOAT           -- Maximum respiration rate in breaths/min.
    , avg_respiration_rate FLOAT           -- Average respiration rate in breaths/min.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

----------------------------------------------------------------------------------------
-- Supplemental activity aggregate metrics.
-- This table captures any remaining metrics not covered by the main Activity table or
-- sport-specific aggregate tables using a flexible key-value structure.
CREATE TABLE IF NOT EXISTS supplemental_activity_metric (
    activity_id BIGINT NOT NULL          -- Foreign key reference to activity.activity_id.
    , metric TEXT NOT NULL                 -- Metric name.
    , value FLOAT                          -- Metric value.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , PRIMARY KEY (activity_id, metric)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

----------------------------------------------------------------------------------------
-- Sleep session data from Garmin Connect including sleep scores, duration, and quality
-- metrics. Each record represents a single sleep session with comprehensive sleep
-- analysis data.
CREATE TABLE IF NOT EXISTS sleep (
    sleep_id INTEGER PRIMARY KEY         -- Auto-generated primary key.
    , user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , start_ts DATETIME NOT NULL           -- Sleep session start timestamp (timezone-aware).
    , end_ts DATETIME NOT NULL             -- Sleep session end timestamp (timezone-aware).
    , timezone_offset_hours FLOAT NOT NULL -- Timezone offset in hours from UTC.
    , calendar_date TEXT                   -- Calendar date of the sleep session.
    , sleep_version INTEGER                -- Sleep algorithm version.
    , age_group TEXT                       -- User age group at time of sleep.
    , respiration_version INTEGER          -- Respiration algorithm version.
    , sleep_time_seconds INTEGER           -- Total sleep time in seconds.
    , nap_time_seconds INTEGER             -- Nap time in seconds.
    , unmeasurable_sleep_seconds INTEGER   -- Unmeasurable sleep time in seconds.
    , deep_sleep_seconds INTEGER           -- Deep sleep time in seconds.
    , light_sleep_seconds INTEGER          -- Light sleep time in seconds.
    , rem_sleep_seconds INTEGER            -- REM sleep time in seconds.
    , awake_sleep_seconds INTEGER          -- Awake time during sleep in seconds.
    , awake_count INTEGER                  -- Number of times awake.
    , restless_moments_count INTEGER       -- Number of restless moments.
    , rem_sleep_data BOOLEAN               -- Whether REM sleep data is available.
    , sleep_window_confirmed BOOLEAN       -- Whether sleep window was confirmed.
    , sleep_window_confirmation_type TEXT  -- Sleep window confirmation type.
    , sleep_quality_type_pk BIGINT         -- Sleep quality type primary key.
    , sleep_result_type_pk BIGINT          -- Sleep result type primary key.
    , retro BOOLEAN                        -- Whether this is retroactively added data.
    , sleep_from_device BOOLEAN            -- Whether sleep data came from device.
    , device_rem_capable BOOLEAN           -- Whether device is capable of REM detection.
    , skin_temp_data_exists BOOLEAN        -- Whether skin temperature data exists.
    , average_spo2 FLOAT                   -- Average SpO2 percentage.
    , lowest_spo2 INTEGER                  -- Lowest SpO2 percentage.
    , highest_spo2 INTEGER                 -- Highest SpO2 percentage.
    , average_spo2_hr_sleep FLOAT          -- Average SpO2 during high respiration sleep.
    , number_of_events_below_threshold INTEGER -- Number of SpO2 events below threshold.
    , duration_of_events_below_threshold INTEGER -- Duration of SpO2 events below threshold in seconds.
    , average_respiration FLOAT            -- Average respiration rate in breaths/min.
    , lowest_respiration FLOAT             -- Lowest respiration rate in breaths/min.
    , highest_respiration FLOAT            -- Highest respiration rate in breaths/min.
    , avg_sleep_stress FLOAT               -- Average sleep stress level.
    , breathing_disruption_severity TEXT   -- Breathing disruption severity level.
    , avg_overnight_hrv FLOAT              -- Average overnight HRV.
    , hrv_status TEXT                      -- HRV status.
    , body_battery_change INTEGER          -- Body battery change during sleep.
    , resting_heart_rate INTEGER           -- Resting heart rate in bpm.
    , sleep_score_feedback TEXT            -- Sleep score feedback message.
    , sleep_score_insight TEXT             -- Sleep score insight message.
    , sleep_score_personalized_insight TEXT -- Personalized sleep score insight.
    , total_duration_key TEXT              -- Total duration score key.
    , stress_key TEXT                      -- Stress score key.
    , awake_count_key TEXT                 -- Awake count score key.
    , restlessness_key TEXT                -- Restlessness score key.
    , score_overall_key TEXT               -- Overall score key.
    , score_overall_value INTEGER          -- Overall sleep score value.
    , light_pct_key TEXT                   -- Light sleep percentage score key.
    , light_pct_value INTEGER              -- Light sleep percentage value.
    , deep_pct_key TEXT                    -- Deep sleep percentage score key.
    , deep_pct_value INTEGER               -- Deep sleep percentage value.
    , rem_pct_key TEXT                     -- REM sleep percentage score key.
    , rem_pct_value INTEGER                -- REM sleep percentage value.
    , sleep_need_baseline INTEGER          -- Baseline sleep need in seconds.
    , sleep_need_actual INTEGER            -- Actual sleep need in seconds.
    , sleep_need_feedback TEXT             -- Sleep need feedback message.
    , sleep_need_training_feedback TEXT    -- Sleep need training feedback.
    , sleep_need_history_adj TEXT          -- Sleep need history adjustment.
    , sleep_need_hrv_adj TEXT              -- Sleep need HRV adjustment.
    , sleep_need_nap_adj TEXT              -- Sleep need nap adjustment.
    , next_sleep_need_baseline INTEGER     -- Next night baseline sleep need in seconds.
    , next_sleep_need_actual INTEGER       -- Next night actual sleep need in seconds.
    , next_sleep_need_feedback TEXT        -- Next night sleep need feedback.
    , next_sleep_need_training_feedback TEXT -- Next night sleep need training feedback.
    , next_sleep_need_history_adj TEXT     -- Next night sleep need history adjustment.
    , next_sleep_need_hrv_adj TEXT         -- Next night sleep need HRV adjustment.
    , next_sleep_need_nap_adj TEXT         -- Next night sleep need nap adjustment.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
    , UNIQUE (user_id, start_ts)
);

CREATE INDEX IF NOT EXISTS sleep_user_id_start_ts_idx
ON sleep (user_id, start_ts DESC);

----------------------------------------------------------------------------------------
-- Timeseries data capturing movement activity levels throughout a sleep session.
-- Time interval: 1 minute.
CREATE TABLE IF NOT EXISTS sleep_movement (
    sleep_id INTEGER NOT NULL            -- Foreign key reference to sleep.sleep_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , activity_level FLOAT                 -- Activity level during this minute.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

----------------------------------------------------------------------------------------
-- Timeseries data capturing moments of restlessness or movement during sleep.
-- Time interval: Event-based (irregular intervals when restless moments occur).
CREATE TABLE IF NOT EXISTS sleep_restless_moment (
    sleep_id INTEGER NOT NULL            -- Foreign key reference to sleep.sleep_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the restless moment (timezone-aware).
    , value INTEGER                        -- Restlessness value.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

----------------------------------------------------------------------------------------
-- Timeseries data capturing blood oxygen saturation SpO2 measurements during sleep.
-- Time interval: 1 minute.
CREATE TABLE IF NOT EXISTS spo2 (
    sleep_id INTEGER NOT NULL            -- Foreign key reference to sleep.sleep_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value INTEGER                        -- SpO2 percentage value.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

----------------------------------------------------------------------------------------
-- Timeseries data capturing heart rate variability (HRV) measurements throughout sleep
-- periods. Time interval: 5 minutes.
CREATE TABLE IF NOT EXISTS hrv (
    sleep_id INTEGER NOT NULL            -- Foreign key reference to sleep.sleep_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value FLOAT                          -- HRV value in milliseconds.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

----------------------------------------------------------------------------------------
-- Timeseries data capturing breathing disruption events and their severity during
-- sleep periods. Time interval: Event-based (irregular intervals when breathing
-- disruptions occur).
CREATE TABLE IF NOT EXISTS breathing_disruption (
    sleep_id INTEGER NOT NULL            -- Foreign key reference to sleep.sleep_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the disruption (timezone-aware).
    , value INTEGER                        -- Breathing disruption severity value.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

----------------------------------------------------------------------------------------
-- VO2 max measurements from Garmin training status data.
-- Includes both generic and cycling-specific VO2 max values with different measurement
-- dates.
CREATE TABLE IF NOT EXISTS vo2_max (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , date DATE NOT NULL                   -- Date of the VO2 max measurement.
    , vo2_max_generic FLOAT                -- Generic VO2 max value.
    , vo2_max_cycling FLOAT                -- Cycling-specific VO2 max value.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

----------------------------------------------------------------------------------------
-- Heat and altitude acclimation metrics from Garmin training status data.
-- Tracks acclimation levels and trends for environmental conditions.
CREATE TABLE IF NOT EXISTS acclimation (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , date DATE NOT NULL                   -- Date of the acclimation measurement.
    , altitude_acclimation FLOAT           -- Altitude acclimation percentage.
    , heat_acclimation_percentage FLOAT    -- Heat acclimation percentage.
    , current_altitude FLOAT               -- Current altitude in meters.
    , acclimation_percentage FLOAT         -- Overall acclimation percentage.
    , altitude_trend TEXT                  -- Altitude acclimation trend.
    , heat_trend TEXT                      -- Heat acclimation trend.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

----------------------------------------------------------------------------------------
-- Training load balance and status metrics from Garmin Connect.
-- Includes monthly load distribution, ACWR analysis, and training status indicators.
CREATE TABLE IF NOT EXISTS training_load (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , date DATE NOT NULL                   -- Date of the training load measurement.
    , monthly_load_aerobic_low FLOAT       -- Monthly low aerobic training load.
    , monthly_load_aerobic_high FLOAT      -- Monthly high aerobic training load.
    , monthly_load_anaerobic FLOAT         -- Monthly anaerobic training load.
    , monthly_load_aerobic_low_target_min FLOAT   -- Min target for low aerobic load.
    , monthly_load_aerobic_low_target_max FLOAT   -- Max target for low aerobic load.
    , monthly_load_aerobic_high_target_min FLOAT  -- Min target for high aerobic load.
    , monthly_load_aerobic_high_target_max FLOAT  -- Max target for high aerobic load.
    , monthly_load_anaerobic_target_min FLOAT     -- Min target for anaerobic load.
    , monthly_load_anaerobic_target_max FLOAT     -- Max target for anaerobic load.
    , training_balance_feedback_phrase TEXT -- Training balance feedback phrase.
    , acwr_percent FLOAT                   -- Acute chronic workload ratio percentage.
    , acwr_status TEXT                     -- ACWR status.
    , acwr_status_feedback TEXT            -- ACWR status feedback message.
    , daily_training_load_acute FLOAT      -- Daily acute training load.
    , max_training_load_chronic FLOAT      -- Maximum chronic training load.
    , min_training_load_chronic FLOAT      -- Minimum chronic training load.
    , daily_training_load_chronic FLOAT    -- Daily chronic training load.
    , daily_acute_chronic_workload_ratio FLOAT -- Daily ACWR.
    , training_status INTEGER              -- Training status code.
    , training_status_feedback_phrase TEXT -- Training status feedback phrase.
    , total_intensity_minutes INTEGER      -- Total intensity minutes.
    , moderate_minutes INTEGER             -- Moderate intensity minutes.
    , vigorous_minutes INTEGER             -- Vigorous intensity minutes.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , update_ts DATETIME DEFAULT CURRENT_TIMESTAMP           -- Record last update timestamp.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

----------------------------------------------------------------------------------------
-- Training readiness scores and factors from Garmin Connect.
-- Indicates recovery status and training capacity based on sleep, HRV, stress, and
-- training load metrics.
CREATE TABLE IF NOT EXISTS training_readiness (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the readiness measurement (timezone-aware).
    , timezone_offset_hours FLOAT NOT NULL -- Timezone offset in hours from UTC.
    , level TEXT                           -- Training readiness level (e.g., MODERATE, HIGH, LOW).
    , feedback_long TEXT                   -- Long-form feedback message.
    , feedback_short TEXT                  -- Short-form feedback message.
    , score INTEGER                        -- Overall training readiness score.
    , sleep_score INTEGER                  -- Sleep component score.
    , sleep_score_factor_percent INTEGER   -- Sleep factor percentage contribution.
    , sleep_score_factor_feedback TEXT     -- Sleep factor feedback.
    , recovery_time INTEGER                -- Recovery time in hours.
    , recovery_time_factor_percent INTEGER -- Recovery time factor percentage contribution.
    , recovery_time_factor_feedback TEXT   -- Recovery time factor feedback.
    , acwr_factor_percent INTEGER          -- ACWR factor percentage contribution.
    , acwr_factor_feedback TEXT            -- ACWR factor feedback.
    , acute_load INTEGER                   -- Acute training load.
    , stress_history_factor_percent INTEGER -- Stress history factor percentage contribution.
    , stress_history_factor_feedback TEXT  -- Stress history factor feedback.
    , hrv_factor_percent INTEGER           -- HRV factor percentage contribution.
    , hrv_factor_feedback TEXT             -- HRV factor feedback.
    , hrv_weekly_average INTEGER           -- Weekly average HRV.
    , sleep_history_factor_percent INTEGER -- Sleep history factor percentage contribution.
    , sleep_history_factor_feedback TEXT   -- Sleep history factor feedback.
    , valid_sleep BOOLEAN                  -- Whether sleep data is valid.
    , input_context TEXT                   -- Input context for readiness calculation.
    , primary_activity_tracker BOOLEAN     -- Whether this is the primary activity tracker.
    , recovery_time_change_phrase TEXT     -- Recovery time change phrase.
    , sleep_history_factor_feedback_phrase TEXT -- Sleep history factor feedback phrase.
    , hrv_factor_feedback_phrase TEXT      -- HRV factor feedback phrase.
    , stress_history_factor_feedback_phrase TEXT -- Stress history factor feedback phrase.
    , acwr_factor_feedback_phrase TEXT     -- ACWR factor feedback phrase.
    , recovery_time_factor_feedback_phrase TEXT -- Recovery time factor feedback phrase.
    , sleep_score_factor_feedback_phrase TEXT   -- Sleep score factor feedback phrase.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS training_readiness_user_id_timestamp_idx
ON training_readiness (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Stress level timeseries data capturing stress measurements throughout the day.
-- Time interval: 3 minutes.
CREATE TABLE IF NOT EXISTS stress (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value INTEGER                        -- Stress level value (0-100).
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS stress_user_id_timestamp_idx
ON stress (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Body battery level timeseries data capturing energy levels throughout the day.
-- Time interval: 3 minutes.
CREATE TABLE IF NOT EXISTS body_battery (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value INTEGER                        -- Body battery level (0-100).
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS body_battery_user_id_timestamp_idx
ON body_battery (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Timeseries heart rate data from Garmin devices.
-- Time interval: 2 minutes.
CREATE TABLE IF NOT EXISTS heart_rate (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value INTEGER                        -- Heart rate in beats per minute.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS heart_rate_user_id_timestamp_idx
ON heart_rate (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Step count timeseries data capturing movement activity throughout the day.
-- Time interval: 15 minutes.
CREATE TABLE IF NOT EXISTS steps (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value INTEGER                        -- Number of steps in this interval.
    , activity_level TEXT                  -- Activity level classification.
    , activity_level_constant BOOLEAN      -- Whether activity level was constant.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS steps_user_id_timestamp_idx
ON steps (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Timeseries respiration rate data from Garmin devices.
-- Time interval: 2 minutes.
CREATE TABLE IF NOT EXISTS respiration (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value FLOAT                          -- Respiration rate in breaths per minute.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS respiration_user_id_timestamp_idx
ON respiration (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Timeseries intensity minutes data from Garmin devices.
-- Time interval: 15 minutes.
CREATE TABLE IF NOT EXISTS intensity_minutes (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , value FLOAT                          -- Intensity minutes in this interval.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS intensity_minutes_user_id_timestamp_idx
ON intensity_minutes (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Timeseries floors data from Garmin devices.
-- Time interval: 15 minutes.
CREATE TABLE IF NOT EXISTS floors (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , ascended INTEGER                     -- Number of floors ascended.
    , descended INTEGER                    -- Number of floors descended.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS floors_user_id_timestamp_idx
ON floors (user_id, timestamp DESC);

----------------------------------------------------------------------------------------
-- Personal records achieved by users across various activity types and distances.
-- Each record represents a best performance for a specific type and user.
-- The latest column indicates the most recent personal record for each user and type.
-- Note: activity_id can be NULL for steps-based PRs (typeId 12-15) which are
-- daily/weekly/monthly aggregates not tied to specific activities.
CREATE TABLE IF NOT EXISTS personal_record (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , activity_id BIGINT                    -- Activity where PR was achieved (NULL for steps-based PRs).
    , timestamp DATETIME NOT NULL          -- Timestamp when the personal record was achieved.
    , type_id INTEGER NOT NULL             -- Personal record type identifier (1-20).
    , label TEXT                           -- Human-readable description of the PR type.
    , value FLOAT                          -- Value of the PR (time in seconds or distance in meters).
    , latest BOOLEAN NOT NULL DEFAULT 0    -- Whether this is the latest personal record for this user.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, type_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
    -- Note: No FK on activity_id to allow processing PRs before activities exist.
);

CREATE UNIQUE INDEX IF NOT EXISTS personal_record_user_id_type_id_latest_idx
ON personal_record (user_id, type_id)
WHERE latest = 1;

CREATE INDEX IF NOT EXISTS personal_record_user_id_idx
ON personal_record (user_id);

CREATE INDEX IF NOT EXISTS personal_record_activity_id_idx
ON personal_record (activity_id);

CREATE INDEX IF NOT EXISTS personal_record_type_id_idx
ON personal_record (type_id);

CREATE INDEX IF NOT EXISTS personal_record_latest_idx
ON personal_record (latest);

----------------------------------------------------------------------------------------
-- Race predictions table for storing race time predictions from Garmin Connect.
-- Includes predicted times for 5K, 10K, half marathon, and marathon distances.
CREATE TABLE IF NOT EXISTS race_predictions (
    user_id BIGINT NOT NULL              -- Foreign key reference to user.user_id.
    , date DATE NOT NULL                   -- Date of the race prediction.
    , time_5k FLOAT                        -- Predicted 5K time in seconds.
    , time_10k FLOAT                       -- Predicted 10K time in seconds.
    , time_half_marathon FLOAT             -- Predicted half marathon time in seconds.
    , time_marathon FLOAT                  -- Predicted marathon time in seconds.
    , latest BOOLEAN NOT NULL DEFAULT 0    -- Whether this is the latest prediction set.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS race_predictions_user_id_latest_unique_idx
ON race_predictions (user_id)
WHERE latest = 1;

----------------------------------------------------------------------------------------
-- Time-series metrics extracted from activity FIT files.
-- Stores granular sensor measurements recorded during activities including heart rate,
-- cadence, power, speed, distance, and other metrics.
CREATE TABLE IF NOT EXISTS activity_ts_metric (
    activity_id BIGINT NOT NULL          -- Foreign key reference to activity.activity_id.
    , timestamp DATETIME NOT NULL          -- Timestamp of the measurement (timezone-aware).
    , name TEXT NOT NULL                   -- Metric name (e.g., heart_rate, power, cadence).
    , value FLOAT                          -- Metric value.
    , units TEXT                           -- Measurement units.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (activity_id, timestamp, name)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

----------------------------------------------------------------------------------------
-- Split metrics extracted from activity FIT files.
-- Stores Garmin's algorithmic breakdown of activities into intervals such as run/walk
-- detection and active intervals. Each record represents a single metric for a
-- specific split segment.
CREATE TABLE IF NOT EXISTS activity_split_metric (
    activity_id BIGINT NOT NULL          -- Foreign key reference to activity.activity_id.
    , split_idx INTEGER NOT NULL           -- Split segment index (0-based).
    , name TEXT NOT NULL                   -- Metric name.
    , split_type TEXT                      -- Split type (e.g., active, rest).
    , value FLOAT                          -- Metric value.
    , units TEXT                           -- Measurement units.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (activity_id, split_idx, name)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

----------------------------------------------------------------------------------------
-- Lap metrics extracted from activity FIT files.
-- Stores device-triggered lap segments from manual button press or auto distance/time
-- triggers. Each record represents a single metric for a specific lap segment.
CREATE TABLE IF NOT EXISTS activity_lap_metric (
    activity_id BIGINT NOT NULL          -- Foreign key reference to activity.activity_id.
    , lap_idx INTEGER NOT NULL             -- Lap index (0-based).
    , name TEXT NOT NULL                   -- Metric name.
    , value FLOAT                          -- Metric value.
    , units TEXT                           -- Measurement units.
    , insert_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Record creation timestamp.
    , PRIMARY KEY (activity_id, lap_idx, name)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);
