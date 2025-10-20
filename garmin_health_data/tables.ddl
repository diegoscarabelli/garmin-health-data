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

-- User identity and basic demographic data from Garmin Connect. Contains stable user identification and basic profile information.
CREATE TABLE IF NOT EXISTS user (
    user_id BIGINT PRIMARY KEY           -- Unique identifier for the user in Garmin Connect.
    , full_name TEXT                       -- Full name of the user.
    , birth_date DATE                      -- User birth date.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
);

-- User fitness profile data from Garmin Connect including physical characteristics and fitness metrics. The latest column indicates the most recent profile record.
CREATE TABLE IF NOT EXISTS user_profile (
    user_profile_id INTEGER PRIMARY KEY  -- Auto-incrementing primary key for user profile records.
    , user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this profile record belongs to.
    , gender TEXT                          -- User gender (e.g., ''MALE'', ''FEMALE'').
    , weight FLOAT                         -- User weight in grams.
    , height FLOAT                         -- User height in centimeters.
    , vo2_max_running FLOAT                -- VO2 max value for running activities in ml/kg/min.
    , vo2_max_cycling FLOAT                -- VO2 max value for cycling activities in ml/kg/min.
    , lactate_threshold_speed FLOAT        -- Lactate threshold speed in meters per second.
    , lactate_threshold_heart_rate INTEGER -- Lactate threshold heart rate in beats per minute.
    , moderate_intensity_minutes_hr_zone INTEGER -- Heart rate zone for moderate intensity exercise minutes.
    , vigorous_intensity_minutes_hr_zone INTEGER -- Heart rate zone for vigorous intensity exercise minutes.
    , latest BOOLEAN NOT NULL DEFAULT 0    -- Boolean flag indicating whether this is the latest user profile record.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS user_profile_user_id_latest_unique_idx
ON user_profile (user_id)
WHERE latest = 1;

-- Garmin Connect activity records with core metrics common across all activity types.
CREATE TABLE IF NOT EXISTS activity (
    activity_id BIGINT PRIMARY KEY       -- Unique identifier for the activity in Garmin Connect.
    , user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user performed this activity.
    , activity_name TEXT                   -- User-defined name for the activity.
    , activity_type_id INTEGER NOT NULL    -- Unique identifier for the activity type (e.g., ''1'' for running, ''11'' for cardio).
    , activity_type_key TEXT NOT NULL      -- String key for the activity type (e.g., ''running'', ''lap_swimming'', ''road_biking'', ''indoor_cardio'').
    , event_type_id INTEGER NOT NULL       -- Unique identifier for the event type.
    , event_type_key TEXT NOT NULL         -- String key for the event type (e.g., ''other'').
    , start_ts DATETIME NOT NULL           -- Activity start time.
    , end_ts DATETIME NOT NULL             -- Activity end time.
    , timezone_offset_hours FLOAT NOT NULL -- Timezone offset from UTC in hours to infer local time (e.g., -7.0 for UTC-07:00, 5.5 for UTC+05:30).
    , duration FLOAT                       -- Total duration of the activity in seconds.
    , elapsed_duration FLOAT               -- Elapsed time including pauses and stops in seconds.
    , moving_duration FLOAT                -- Time spent in motion during the activity in seconds.
    , distance FLOAT                       -- Total distance covered during the activity in meters.
    , lap_count INTEGER                    -- Number of laps/segments in the activity.
    , average_speed FLOAT                  -- Average speed during the activity in meters per second.
    , max_speed FLOAT                      -- Maximum speed reached during the activity in meters per second.
    , start_latitude FLOAT                 -- Starting latitude coordinate in decimal degrees.
    , start_longitude FLOAT                -- Starting longitude coordinate in decimal degrees.
    , end_latitude FLOAT                   -- Ending latitude coordinate in decimal degrees.
    , end_longitude FLOAT                  -- Ending longitude coordinate in decimal degrees.
    , location_name TEXT                   -- Geographic location name where the activity took place.
    , aerobic_training_effect FLOAT        -- Aerobic training effect score (0.0-5.0 scale).
    , aerobic_training_effect_message TEXT -- Detailed message about aerobic training effect.
    , anaerobic_training_effect FLOAT      -- Anaerobic training effect score (0.0-5.0 scale).
    , anaerobic_training_effect_message TEXT -- Detailed message about anaerobic training effect.
    , training_effect_label TEXT           -- Text description of the training effect (e.g., ''AEROBIC_BASE'', ''UNKNOWN'').
    , activity_training_load FLOAT         -- Training load value representing the physiological impact of the activity.
    , difference_body_battery INTEGER      -- Change in body battery energy level during the activity.
    , moderate_intensity_minutes INTEGER   -- Minutes spent in moderate intensity exercise zone.
    , vigorous_intensity_minutes INTEGER   -- Minutes spent in vigorous intensity exercise zone.
    , calories FLOAT                       -- Total calories burned during the activity.
    , bmr_calories FLOAT                   -- Basal metabolic rate calories burned during the activity.
    , water_estimated FLOAT                -- Estimated water loss during the activity in milliliters.
    , hr_time_in_zone_1 FLOAT              -- Time spent in heart rate zone 1 (active recovery) in seconds.
    , hr_time_in_zone_2 FLOAT              -- Time spent in heart rate zone 2 (aerobic base) in seconds.
    , hr_time_in_zone_3 FLOAT              -- Time spent in heart rate zone 3 (aerobic) in seconds.
    , hr_time_in_zone_4 FLOAT              -- Time spent in heart rate zone 4 (lactate threshold) in seconds.
    , hr_time_in_zone_5 FLOAT              -- Time spent in heart rate zone 5 (neuromuscular power) in seconds.
    , average_hr FLOAT                     -- Average heart rate during the activity in beats per minute.
    , max_hr FLOAT                         -- Maximum heart rate reached during the activity in beats per minute.
    , device_id BIGINT                     -- Unique identifier for the Garmin device used to record the activity.
    , manufacturer TEXT                    -- Manufacturer of the device (typically ''GARMIN'').
    , time_zone_id INTEGER                 -- Garmin''s internal timezone identifier for the activity location.
    , has_polyline BOOLEAN NOT NULL DEFAULT 0  -- Whether GPS track data (polyline) is available for this activity.
    , has_images BOOLEAN NOT NULL DEFAULT 0    -- Whether images are attached to this activity.
    , has_video BOOLEAN NOT NULL DEFAULT 0     -- Whether video is attached to this activity.
    , has_splits BOOLEAN DEFAULT 0             -- Whether split/lap data is available for this activity.
    , has_heat_map BOOLEAN NOT NULL DEFAULT 0  -- Whether heat map data is available for this activity.
    , parent BOOLEAN NOT NULL DEFAULT 0        -- Whether this activity is a parent activity containing sub-activities.
    , purposeful BOOLEAN NOT NULL DEFAULT 0    -- Whether this activity was marked as purposeful training.
    , favorite BOOLEAN NOT NULL DEFAULT 0      -- Whether this activity is marked as a favorite.
    , elevation_corrected BOOLEAN DEFAULT 0    -- Whether elevation data has been corrected.
    , atp_activity BOOLEAN DEFAULT 0           -- Whether this is an Adaptive Training Plan activity.
    , manual_activity BOOLEAN NOT NULL DEFAULT 0 -- Whether this activity was manually entered rather than recorded.
    , pr BOOLEAN NOT NULL DEFAULT 0            -- Whether this activity contains a personal record.
    , auto_calc_calories BOOLEAN NOT NULL DEFAULT 0 -- Whether calorie calculation was performed automatically.
    , ts_data_available BOOLEAN NOT NULL DEFAULT 0  -- Whether time-series data from FIT file has been processed for this activity.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
    , UNIQUE (user_id, start_ts)
);

CREATE INDEX IF NOT EXISTS activity_user_id_start_ts_idx
ON activity (user_id, start_ts DESC);

-- Swimming-specific metrics including stroke data, SWOLF, and pool information. Each record corresponds to a specific swimming activity.
CREATE TABLE IF NOT EXISTS swimming_agg_metrics (
    activity_id BIGINT PRIMARY KEY       -- References activity(activity_id).
    , pool_length FLOAT                    -- Length of the swimming pool in centimeters.
    , active_lengths INTEGER               -- Number of active pool lengths swum.
    , strokes FLOAT                        -- Total number of strokes taken during the activity.
    , avg_stroke_distance FLOAT            -- Average distance covered per stroke in meters.
    , avg_strokes FLOAT                    -- Average number of strokes per pool length.
    , avg_swim_cadence FLOAT               -- Average swimming cadence in strokes per minute.
    , avg_swolf FLOAT                      -- Average SWOLF score (strokes + time in seconds to cover pool length).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

-- Cycling-specific metrics including power zones, cadence, and performance analysis. Each record corresponds to a specific cycling activity.
CREATE TABLE IF NOT EXISTS cycling_agg_metrics (
    activity_id BIGINT PRIMARY KEY       -- References activity(activity_id).
    , training_stress_score FLOAT          -- Training Stress Score quantifying workout intensity and duration.
    , intensity_factor FLOAT               -- Intensity Factor representing workout intensity relative to threshold.
    , vo2_max_value FLOAT                  -- VO2 max value measured during the activity in ml/kg/min.
    , avg_power FLOAT                      -- Average power output during the activity in watts.
    , max_power FLOAT                      -- Maximum power output reached during the activity in watts.
    , normalized_power FLOAT               -- Normalized power accounting for variable intensity in watts.
    , max_20min_power FLOAT                -- Best 20-minute average power output in watts.
    , avg_left_balance FLOAT               -- Average left/right power balance as percentage of left leg contribution.
    , avg_biking_cadence FLOAT             -- Average pedaling cadence in revolutions per minute.
    , max_biking_cadence FLOAT             -- Maximum pedaling cadence reached in revolutions per minute.
    , max_avg_power_1 FLOAT                -- Best 1-second average power in watts.
    , max_avg_power_2 FLOAT                -- Best 2-second average power in watts.
    , max_avg_power_5 FLOAT                -- Best 5-second average power in watts.
    , max_avg_power_10 FLOAT               -- Best 10-second average power in watts.
    , max_avg_power_20 FLOAT               -- Best 20-second average power in watts.
    , max_avg_power_30 FLOAT               -- Best 30-second average power in watts.
    , max_avg_power_60 FLOAT               -- Best 1-minute average power in watts.
    , max_avg_power_120 FLOAT              -- Best 2-minute average power in watts.
    , max_avg_power_300 FLOAT              -- Best 5-minute average power in watts.
    , max_avg_power_600 FLOAT              -- Best 10-minute average power in watts.
    , max_avg_power_1200 FLOAT             -- Best 20-minute average power in watts.
    , max_avg_power_1800 FLOAT             -- Best 30-minute average power in watts.
    , max_avg_power_3600 FLOAT             -- Best 60-minute average power in watts.
    , max_avg_power_7200 FLOAT             -- Best 120-minute average power in watts.
    , max_avg_power_18000 FLOAT            -- Best 300-minute average power in watts.
    , power_time_in_zone_1 FLOAT           -- Time spent in power zone 1 (active recovery) in seconds.
    , power_time_in_zone_2 FLOAT           -- Time spent in power zone 2 (endurance) in seconds.
    , power_time_in_zone_3 FLOAT           -- Time spent in power zone 3 (tempo) in seconds.
    , power_time_in_zone_4 FLOAT           -- Time spent in power zone 4 (lactate threshold) in seconds.
    , power_time_in_zone_5 FLOAT           -- Time spent in power zone 5 (VO2 max) in seconds.
    , power_time_in_zone_6 FLOAT           -- Time spent in power zone 6 (anaerobic capacity) in seconds.
    , power_time_in_zone_7 FLOAT           -- Time spent in power zone 7 (neuromuscular) in seconds.
    , min_temperature FLOAT                -- Minimum temperature recorded during the activity in Celsius.
    , max_temperature FLOAT                -- Maximum temperature recorded during the activity in Celsius.
    , elevation_gain FLOAT                 -- Total elevation gained during the activity in meters.
    , elevation_loss FLOAT                 -- Total elevation lost during the activity in meters.
    , min_elevation FLOAT                  -- Minimum elevation during the activity in meters.
    , max_elevation FLOAT                  -- Maximum elevation during the activity in meters.
    , min_respiration_rate FLOAT           -- Minimum respiration rate during the activity in breaths per minute.
    , max_respiration_rate FLOAT           -- Maximum respiration rate during the activity in breaths per minute.
    , avg_respiration_rate FLOAT           -- Average respiration rate during the activity in breaths per minute.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

-- Running-specific metrics including running form, cadence, and split times. Each record corresponds to a specific running activity.
CREATE TABLE IF NOT EXISTS running_agg_metrics (
    activity_id BIGINT PRIMARY KEY       -- References activity(activity_id).
    , steps INTEGER                        -- Total number of steps taken during the running activity.
    , vo2_max_value FLOAT                  -- VO2 max value measured during the activity in ml/kg/min.
    , avg_running_cadence FLOAT            -- Average running cadence in steps per minute.
    , max_running_cadence FLOAT            -- Maximum running cadence reached in steps per minute.
    , max_double_cadence FLOAT             -- Maximum double cadence (both feet) in steps per minute.
    , avg_vertical_oscillation FLOAT       -- Average vertical oscillation of running form in centimeters.
    , avg_ground_contact_time FLOAT        -- Average ground contact time per step in milliseconds.
    , avg_stride_length FLOAT              -- Average stride length in centimeters.
    , avg_vertical_ratio FLOAT             -- Average vertical ratio as percentage of stride length.
    , avg_ground_contact_balance FLOAT     -- Average left/right ground contact time balance as percentage.
    , avg_power FLOAT                      -- Average power output during the activity in watts.
    , max_power FLOAT                      -- Maximum power output reached during the activity in watts.
    , normalized_power FLOAT               -- Normalized power accounting for variable intensity in watts.
    , power_time_in_zone_1 FLOAT           -- Time spent in power zone 1 (active recovery) in seconds.
    , power_time_in_zone_2 FLOAT           -- Time spent in power zone 2 (endurance) in seconds.
    , power_time_in_zone_3 FLOAT           -- Time spent in power zone 3 (tempo) in seconds.
    , power_time_in_zone_4 FLOAT           -- Time spent in power zone 4 (lactate threshold) in seconds.
    , power_time_in_zone_5 FLOAT           -- Time spent in power zone 5 (VO2 max) in seconds.
    , min_temperature FLOAT                -- Minimum temperature recorded during the activity in Celsius.
    , max_temperature FLOAT                -- Maximum temperature recorded during the activity in Celsius.
    , elevation_gain FLOAT                 -- Total elevation gained during the activity in meters.
    , elevation_loss FLOAT                 -- Total elevation lost during the activity in meters.
    , min_elevation FLOAT                  -- Minimum elevation during the activity in meters.
    , max_elevation FLOAT                  -- Maximum elevation during the activity in meters.
    , min_respiration_rate FLOAT           -- Minimum respiration rate during the activity in breaths per minute.
    , max_respiration_rate FLOAT           -- Maximum respiration rate during the activity in breaths per minute.
    , avg_respiration_rate FLOAT           -- Average respiration rate during the activity in breaths per minute.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

-- Supplemental activity metrics with flexible key-value storage. Allows for additional metrics not captured in the main tables.
CREATE TABLE IF NOT EXISTS supplemental_activity_metric (
    activity_id BIGINT NOT NULL          -- References activity(activity_id).
    , metric TEXT NOT NULL                 -- Name of the metric being stored.
    , value FLOAT                          -- Numeric value of the metric.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , PRIMARY KEY (activity_id, metric)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

-- Sleep session data from Garmin Connect including sleep scores, duration, and quality metrics. Each record represents a single sleep session.
CREATE TABLE IF NOT EXISTS sleep (
    sleep_id INTEGER PRIMARY KEY         -- Auto-incrementing primary key for sleep session records.
    , user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user had this sleep session.
    , start_ts DATETIME NOT NULL           -- Sleep session start time.
    , end_ts DATETIME NOT NULL             -- Sleep session end time.
    , timezone_offset_hours FLOAT NOT NULL -- Timezone offset from UTC in hours to infer local time (e.g., -7.0 for UTC-07:00, 5.5 for UTC+05:30).
    , calendar_date TEXT                   -- Calendar date of the sleep session.
    , sleep_version INTEGER                -- Version of sleep tracking algorithm used.
    , age_group TEXT                       -- User age group category.
    , respiration_version INTEGER          -- Version of respiration tracking algorithm used.
    , sleep_time_seconds INTEGER           -- Total sleep time in seconds.
    , nap_time_seconds INTEGER             -- Total nap time in seconds.
    , unmeasurable_sleep_seconds INTEGER   -- Time spent in unmeasurable sleep in seconds.
    , deep_sleep_seconds INTEGER           -- Time spent in deep sleep in seconds.
    , light_sleep_seconds INTEGER          -- Time spent in light sleep in seconds.
    , rem_sleep_seconds INTEGER            -- Time spent in REM sleep in seconds.
    , awake_sleep_seconds INTEGER          -- Time spent awake during sleep session in seconds.
    , awake_count INTEGER                  -- Number of times user woke up during sleep.
    , restless_moments_count INTEGER       -- Total count of restless moments during sleep.
    , rem_sleep_data BOOLEAN               -- Whether REM sleep data is available for this session.
    , sleep_window_confirmed BOOLEAN       -- Whether the sleep window has been confirmed.
    , sleep_window_confirmation_type TEXT  -- Type of sleep window confirmation.
    , sleep_quality_type_pk BIGINT         -- Sleep quality type primary key identifier.
    , sleep_result_type_pk BIGINT          -- Sleep result type primary key identifier.
    , retro BOOLEAN                        -- Whether this is a retroactive sleep entry.
    , sleep_from_device BOOLEAN            -- Whether sleep data came from device or manual entry.
    , device_rem_capable BOOLEAN           -- Whether the device is capable of REM sleep detection.
    , skin_temp_data_exists BOOLEAN        -- Whether skin temperature data exists for this session.
    , average_spo2 FLOAT                   -- Average SpO2 (blood oxygen saturation) during sleep.
    , lowest_spo2 INTEGER                  -- Lowest SpO2 reading during sleep.
    , highest_spo2 INTEGER                 -- Highest SpO2 reading during sleep.
    , average_spo2_hr_sleep FLOAT          -- Average heart rate during SpO2 measurements.
    , number_of_events_below_threshold INTEGER -- Number of SpO2 events below alert threshold.
    , duration_of_events_below_threshold INTEGER -- Total duration of SpO2 events below threshold in seconds.
    , average_respiration FLOAT            -- Average respiration rate during sleep.
    , lowest_respiration FLOAT             -- Lowest respiration rate during sleep.
    , highest_respiration FLOAT            -- Highest respiration rate during sleep.
    , avg_sleep_stress FLOAT               -- Average stress level during sleep.
    , breathing_disruption_severity TEXT   -- Severity level of breathing disruptions.
    , avg_overnight_hrv FLOAT              -- Average heart rate variability during sleep.
    , hrv_status TEXT                      -- HRV status classification.
    , body_battery_change INTEGER          -- Change in body battery energy level during sleep.
    , resting_heart_rate INTEGER           -- Resting heart rate measured during sleep.
    , sleep_score_feedback TEXT            -- Sleep score feedback message.
    , sleep_score_insight TEXT             -- Sleep score insight message.
    , sleep_score_personalized_insight TEXT -- Personalized sleep score insight message.
    , total_duration_key TEXT              -- Sleep duration quality qualifier key.
    , stress_key TEXT                      -- Sleep stress level quality qualifier key.
    , awake_count_key TEXT                 -- Number of awakenings quality qualifier key.
    , restlessness_key TEXT                -- Sleep restlessness quality qualifier key.
    , score_overall_key TEXT               -- Overall sleep score quality qualifier key.
    , score_overall_value INTEGER          -- Overall sleep score numeric value (0-100 scale).
    , light_pct_key TEXT                   -- Light sleep percentage quality qualifier key.
    , light_pct_value INTEGER              -- Light sleep percentage numeric value.
    , deep_pct_key TEXT                    -- Deep sleep percentage quality qualifier key.
    , deep_pct_value INTEGER               -- Deep sleep percentage numeric value.
    , rem_pct_key TEXT                     -- REM sleep percentage quality qualifier key.
    , rem_pct_value INTEGER                -- REM sleep percentage numeric value.
    , sleep_need_baseline INTEGER          -- Baseline sleep need in minutes.
    , sleep_need_actual INTEGER            -- Actual sleep need in minutes.
    , sleep_need_feedback TEXT             -- Sleep need feedback.
    , sleep_need_training_feedback TEXT    -- Training-related sleep need feedback.
    , sleep_need_history_adj TEXT          -- Sleep history adjustment factor.
    , sleep_need_hrv_adj TEXT              -- HRV-based sleep need adjustment.
    , sleep_need_nap_adj TEXT              -- Nap-based sleep need adjustment.
    , next_sleep_need_baseline INTEGER     -- Next day baseline sleep need in minutes.
    , next_sleep_need_actual INTEGER       -- Next day actual sleep need in minutes.
    , next_sleep_need_feedback TEXT        -- Next day sleep need feedback.
    , next_sleep_need_training_feedback TEXT -- Next day training-related sleep need feedback.
    , next_sleep_need_history_adj TEXT     -- Next day sleep history adjustment factor.
    , next_sleep_need_hrv_adj TEXT         -- Next day HRV-based sleep need adjustment.
    , next_sleep_need_nap_adj TEXT         -- Next day nap-based sleep need adjustment.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
    , UNIQUE (user_id, start_ts)
);

CREATE INDEX IF NOT EXISTS sleep_user_id_start_ts_idx
ON sleep (user_id, start_ts DESC);

-- Sleep movement activity levels at regular 1-minute intervals throughout sleep sessions. Higher values indicate more movement.
CREATE TABLE IF NOT EXISTS sleep_movement (
    sleep_id INTEGER NOT NULL            -- References the sleep session identifier.
    , timestamp DATETIME NOT NULL          -- Timestamp of the movement measurement.
    , activity_level FLOAT                 -- Movement activity level (higher values indicate more movement).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

-- Sleep restless moments count capturing periods of restlessness or movement during sleep sessions.
CREATE TABLE IF NOT EXISTS sleep_restless_moment (
    sleep_id INTEGER NOT NULL            -- References the sleep session identifier.
    , timestamp DATETIME NOT NULL          -- Timestamp of the restless moment.
    , value INTEGER                        -- Restless moments count.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

-- Blood oxygen saturation (SpO2) measurements at regular 1-minute intervals during sleep sessions.
CREATE TABLE IF NOT EXISTS spo2 (
    sleep_id INTEGER NOT NULL            -- References the sleep session identifier.
    , timestamp DATETIME NOT NULL          -- Timestamp of the SpO2 measurement.
    , value INTEGER                        -- SpO2 reading as percentage (typically 85-100).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

-- Heart rate variability (HRV) measurements at regular 5-minute intervals throughout sleep periods indicating autonomic nervous system recovery.
CREATE TABLE IF NOT EXISTS hrv (
    sleep_id INTEGER NOT NULL            -- References the sleep session identifier.
    , timestamp DATETIME NOT NULL          -- Timestamp of the HRV measurement.
    , value FLOAT                          -- HRV value in milliseconds.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

-- Breathing disruption events and their severity during sleep periods indicating potential sleep apnea or breathing irregularities.
CREATE TABLE IF NOT EXISTS breathing_disruption (
    sleep_id INTEGER NOT NULL            -- References the sleep session identifier.
    , timestamp DATETIME NOT NULL          -- Timestamp of the breathing disruption event.
    , value INTEGER                        -- Breathing disruption severity or type indicator.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (sleep_id, timestamp)
    , FOREIGN KEY (sleep_id) REFERENCES sleep (sleep_id)
);

-- VO2 max measurements from Garmin training status data including both generic and cycling-specific values.
CREATE TABLE IF NOT EXISTS vo2_max (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this VO2 max measurement belongs to.
    , date DATE NOT NULL                   -- Calendar date of the VO2 max measurement.
    , vo2_max_generic FLOAT                -- Generic VO2 max value in ml/kg/min.
    , vo2_max_cycling FLOAT                -- Cycling-specific VO2 max value in ml/kg/min.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

-- Heat and altitude acclimation metrics from Garmin training status data.
CREATE TABLE IF NOT EXISTS acclimation (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this acclimation data belongs to.
    , date DATE NOT NULL                   -- Calendar date of the acclimation measurement.
    , altitude_acclimation FLOAT           -- Altitude acclimation level as a numeric value.
    , heat_acclimation_percentage FLOAT    -- Heat acclimation level as a percentage (0-100).
    , current_altitude FLOAT               -- Current altitude in meters.
    , acclimation_percentage FLOAT         -- Overall acclimation percentage.
    , altitude_trend TEXT                  -- Altitude acclimation trend (e.g., ''MAINTAINING'', ''GAINING'').
    , heat_trend TEXT                      -- Heat acclimation trend (e.g., ''DEACCLIMATIZING'', ''ACCLIMATIZING'').
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

-- Training load balance and status metrics from Garmin Connect including monthly low/high aerobic/anaerobic load distribution, ACWR Acute:Chronic Workload Ratio (ACWR) analysis, and training status indicators.
CREATE TABLE IF NOT EXISTS training_load (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this training load data belongs to.
    , date DATE NOT NULL                   -- Calendar date of the training load measurement.
    , monthly_load_aerobic_low FLOAT       -- Monthly aerobic low intensity training load.
    , monthly_load_aerobic_high FLOAT      -- Monthly aerobic high intensity training load.
    , monthly_load_anaerobic FLOAT         -- Monthly anaerobic training load.
    , monthly_load_aerobic_low_target_min FLOAT   -- Minimum target for monthly aerobic low intensity load.
    , monthly_load_aerobic_low_target_max FLOAT   -- Maximum target for monthly aerobic low intensity load.
    , monthly_load_aerobic_high_target_min FLOAT  -- Minimum target for monthly aerobic high intensity load.
    , monthly_load_aerobic_high_target_max FLOAT  -- Maximum target for monthly aerobic high intensity load.
    , monthly_load_anaerobic_target_min FLOAT     -- Minimum target for monthly anaerobic load.
    , monthly_load_anaerobic_target_max FLOAT     -- Maximum target for monthly anaerobic load.
    , training_balance_feedback_phrase TEXT -- Training balance feedback message (e.g., ''ABOVE_TARGETS'').
    , acwr_percent FLOAT                   -- Acute chronic workload ratio as a percentage.
    , acwr_status TEXT                     -- ACWR status classification (e.g., ''OPTIMAL'').
    , acwr_status_feedback TEXT            -- ACWR status feedback message.
    , daily_training_load_acute FLOAT      -- Daily acute training load value.
    , max_training_load_chronic FLOAT      -- Maximum chronic training load threshold.
    , min_training_load_chronic FLOAT      -- Minimum chronic training load threshold.
    , daily_training_load_chronic FLOAT    -- Daily chronic training load value.
    , daily_acute_chronic_workload_ratio FLOAT -- Daily acute to chronic workload ratio.
    , training_status INTEGER              -- Training status numeric code.
    , training_status_feedback_phrase TEXT -- Training status feedback message (e.g., ''STRAINED_1'').
    , total_intensity_minutes INTEGER      -- Total intensity minutes calculated as endDayMinutes - startDayMinutes.
    , moderate_minutes INTEGER             -- Daily moderate intensity minutes from intensity minutes tracking.
    , vigorous_minutes INTEGER             -- Daily vigorous intensity minutes from intensity minutes tracking.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP           -- Timestamp when the record was last modified in the database.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

-- Training readiness scores and factors from Garmin Connect indicating recovery status and training capacity based on sleep, HRV, stress, and training load metrics.
CREATE TABLE IF NOT EXISTS training_readiness (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this training readiness data belongs to.
    , timestamp DATETIME NOT NULL          -- Training readiness measurement timestamp.
    , timezone_offset_hours FLOAT NOT NULL -- Timezone offset from UTC in hours to infer local time (e.g., -7.0 for UTC-07:00, 5.5 for UTC+05:30).
    , level TEXT                           -- Training readiness level (e.g., ''HIGH'', ''MODERATE'', ''LOW'').
    , feedback_long TEXT                   -- Detailed training readiness feedback message.
    , feedback_short TEXT                  -- Short training readiness feedback message.
    , score INTEGER                        -- Overall training readiness score (0-100 scale).
    , sleep_score INTEGER                  -- Sleep quality score contributing to training readiness.
    , sleep_score_factor_percent INTEGER   -- Sleep score contribution percentage to overall readiness.
    , sleep_score_factor_feedback TEXT     -- Sleep score factor feedback (e.g., ''MODERATE'', ''GOOD'').
    , recovery_time INTEGER                -- Estimated recovery time in minutes.
    , recovery_time_factor_percent INTEGER -- Recovery time contribution percentage to overall readiness.
    , recovery_time_factor_feedback TEXT   -- Recovery time factor feedback (e.g., ''MODERATE'', ''GOOD'').
    , acwr_factor_percent INTEGER          -- Acute chronic workload ratio contribution percentage to overall readiness.
    , acwr_factor_feedback TEXT            -- ACWR factor feedback (e.g., ''GOOD'', ''VERY_GOOD'').
    , acute_load INTEGER                   -- Acute training load value.
    , stress_history_factor_percent INTEGER -- Stress history contribution percentage to overall readiness.
    , stress_history_factor_feedback TEXT  -- Stress history factor feedback (e.g., ''GOOD'').
    , hrv_factor_percent INTEGER           -- Heart rate variability contribution percentage to overall readiness.
    , hrv_factor_feedback TEXT             -- HRV factor feedback (e.g., ''GOOD'').
    , hrv_weekly_average INTEGER           -- Weekly average HRV value in milliseconds.
    , sleep_history_factor_percent INTEGER -- Sleep history contribution percentage to overall readiness.
    , sleep_history_factor_feedback TEXT   -- Sleep history factor feedback (e.g., ''MODERATE'').
    , valid_sleep BOOLEAN                  -- Whether sleep data is valid and available for calculation.
    , input_context TEXT                   -- Context of the training readiness calculation (e.g., ''UPDATE_REALTIME_VARIABLES'').
    , primary_activity_tracker BOOLEAN     -- Whether this device is the primary activity tracker.
    , recovery_time_change_phrase TEXT     -- Recovery time change feedback phrase.
    , sleep_history_factor_feedback_phrase TEXT -- Sleep history factor detailed feedback phrase.
    , hrv_factor_feedback_phrase TEXT      -- HRV factor detailed feedback phrase.
    , stress_history_factor_feedback_phrase TEXT -- Stress history factor detailed feedback phrase.
    , acwr_factor_feedback_phrase TEXT     -- ACWR factor detailed feedback phrase.
    , recovery_time_factor_feedback_phrase TEXT -- Recovery time factor detailed feedback phrase.
    , sleep_score_factor_feedback_phrase TEXT   -- Sleep score factor detailed feedback phrase.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , update_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was last modified in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS training_readiness_user_id_timestamp_idx
ON training_readiness (user_id, timestamp DESC);

-- Stress level measurements at regular 3-minute intervals throughout the day. Stress values typically range from 0-100, with negative values indicating unmeasurable periods.
CREATE TABLE IF NOT EXISTS stress (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this stress measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the stress measurement.
    , value INTEGER                        -- Stress level value (0-100 scale, negative values indicate unmeasurable periods).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS stress_user_id_timestamp_idx
ON stress (user_id, timestamp DESC);

-- Body battery energy level measurements at regular 3-minute intervals throughout the day. Body battery values typically range from 0-100.
CREATE TABLE IF NOT EXISTS body_battery (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this body battery measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the body battery measurement.
    , value INTEGER                        -- Body battery energy level (0-100 scale).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS body_battery_user_id_timestamp_idx
ON body_battery (user_id, timestamp DESC);

-- Heart rate measurements from Garmin devices at regular 2-minute intervals during periods when heart rate monitoring is active.
CREATE TABLE IF NOT EXISTS heart_rate (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this heart rate measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the heart rate measurement.
    , value INTEGER                        -- Heart rate value in beats per minute.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS heart_rate_user_id_timestamp_idx
ON heart_rate (user_id, timestamp DESC);

-- Step count measurements from Garmin devices at regular 15-minute intervals throughout the day including activity level and consistency indicators.
CREATE TABLE IF NOT EXISTS steps (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this step count measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the step count measurement.
    , value INTEGER                        -- Number of steps taken during the 15-minute interval.
    , activity_level TEXT                  -- Activity level classification (e.g., sleeping, sedentary, active, highlyActive).
    , activity_level_constant BOOLEAN      -- Whether the activity level remained constant during the interval.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS steps_user_id_timestamp_idx
ON steps (user_id, timestamp DESC);

-- Respiration rate measurements from Garmin devices at regular 2-minute intervals throughout the day during periods when respiration monitoring is active.
CREATE TABLE IF NOT EXISTS respiration (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this respiration measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the respiration rate measurement.
    , value FLOAT                          -- Respiration rate value in breaths per minute.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS respiration_user_id_timestamp_idx
ON respiration (user_id, timestamp DESC);

-- Intensity minutes measurements from Garmin devices tracking periods of moderate to vigorous physical activity throughout the day at 15-minute intervals. Records are available only when activity generating intensity is happening.
CREATE TABLE IF NOT EXISTS intensity_minutes (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this intensity minutes measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the intensity minutes measurement.
    , value FLOAT                          -- Intensity minutes value representing accumulated moderate to vigorous activity.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS intensity_minutes_user_id_timestamp_idx
ON intensity_minutes (user_id, timestamp DESC);

-- Floors climbed measurements from Garmin devices tracking floors ascended and descended throughout the day at 15-minute intervals. Records are available only when floor climbing activity is detected.
CREATE TABLE IF NOT EXISTS floors (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this floors measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp of the floors measurement (endTimeGMT from the data).
    , ascended INTEGER                     -- Number of floors ascended during this measurement period.
    , descended INTEGER                    -- Number of floors descended during this measurement period.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, timestamp)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE INDEX IF NOT EXISTS floors_user_id_timestamp_idx
ON floors (user_id, timestamp DESC);

-- Personal records achieved by users across various activity types and distances.
CREATE TABLE IF NOT EXISTS personal_record (
    user_id BIGINT NOT NULL              -- Foreign key reference to the user profile.
    , activity_id BIGINT                    -- Garmin activity ID where this personal record was achieved.
    , timestamp DATETIME NOT NULL          -- Timestamp when the personal record was achieved (prStartTimeGmt).
    , type_id INTEGER NOT NULL             -- Personal record type identifier (e.g., 1=Run 1km, 3=Run 5km, 7=Run Longest).
    , label TEXT                           -- Human-readable description of the personal record type.
    , value FLOAT                          -- Value of the personal record (time in seconds for distances, distance in meters).
    , latest BOOLEAN NOT NULL DEFAULT 0    -- Boolean flag indicating whether this is the latest personal record for this user.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
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

-- Race time predictions from Garmin Connect including 5K, 10K, half marathon, and marathon predicted times. The latest column indicates the most recent prediction.
CREATE TABLE IF NOT EXISTS race_predictions (
    user_id BIGINT NOT NULL              -- References user(user_id). Identifies which user this race prediction belongs to.
    , date DATE NOT NULL                   -- Calendar date of the race prediction.
    , time_5k FLOAT                        -- Predicted 5K race time in seconds.
    , time_10k FLOAT                       -- Predicted 10K race time in seconds.
    , time_half_marathon FLOAT             -- Predicted half marathon race time in seconds.
    , time_marathon FLOAT                  -- Predicted marathon race time in seconds.
    , latest BOOLEAN NOT NULL DEFAULT 0    -- Boolean flag indicating whether this is the latest race prediction for this user.
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (user_id, date)
    , FOREIGN KEY (user_id) REFERENCES user (user_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS race_predictions_user_id_latest_unique_idx
ON race_predictions (user_id)
WHERE latest = 1;

-- Time-series metrics extracted from activity FIT files including heart rate, cadence, power, speed, distance, GPS coordinates, and other sensor measurements recorded during activities. Each record represents a single measurement at a specific point in time.
CREATE TABLE IF NOT EXISTS activity_ts_metric (
    activity_id BIGINT NOT NULL          -- References activity(activity_id). Identifies which activity this metric measurement belongs to.
    , timestamp DATETIME NOT NULL          -- Timestamp when the metric measurement was recorded.
    , name TEXT NOT NULL                   -- Name of the metric, which varies with activity type (e.g., heart_rate, cadence, power, position_lat, position_long).
    , value FLOAT                          -- Numeric value of the metric measurement.
    , units TEXT                           -- Units of measurement for the metric value (e.g., bpm, rpm, watts).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (activity_id, timestamp, name)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

-- Split metrics extracted from activity FIT files representing Garmin''s algorithmic breakdown of activities into intervals (e.g., run/walk detection, active intervals). Each record represents a single metric for a specific split segment.
CREATE TABLE IF NOT EXISTS activity_split_metric (
    activity_id BIGINT NOT NULL          -- References activity(activity_id). Identifies which activity this split metric belongs to.
    , split_idx INTEGER NOT NULL           -- Split index number starting from 1, incrementing for each split frame in the activity.
    , name TEXT NOT NULL                   -- Name of the metric (e.g., total_elapsed_time, total_distance, avg_speed).
    , split_type TEXT                      -- Type of split segment (e.g., rwd_run, rwd_walk, rwd_stand, interval_active).
    , value FLOAT                          -- Numeric value of the split metric measurement.
    , units TEXT                           -- Units of measurement for the metric value (e.g., s, km, km/h).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (activity_id, split_idx, name)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);

-- Lap metrics extracted from activity FIT files representing device-triggered lap segments (manual button press, auto distance/time triggers). Each record represents a single metric for a specific lap segment.
CREATE TABLE IF NOT EXISTS activity_lap_metric (
    activity_id BIGINT NOT NULL          -- References activity(activity_id). Identifies which activity this lap metric belongs to.
    , lap_idx INTEGER NOT NULL             -- Lap index number starting from 1, incrementing for each lap frame in the activity.
    , name TEXT NOT NULL                   -- Name of the metric (e.g., timestamp, start_time, total_elapsed_time, distance).
    , value FLOAT                          -- Numeric value of the lap metric measurement.
    , units TEXT                           -- Units of measurement for the metric value (e.g., s, m, deg).
    , create_ts DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP  -- Timestamp when the record was created in the database.
    , PRIMARY KEY (activity_id, lap_idx, name)
    , FOREIGN KEY (activity_id) REFERENCES activity (activity_id)
);
