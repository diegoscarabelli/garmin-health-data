"""
SQLAlchemy models for Garmin Connect data compatible with SQLite.

These models reflect database tables for storing Garmin Connect wellness and activity
data. Adapted from openetl for standalone SQLite usage without schema support.
"""

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import text


# Base class for all models.
Base = declarative_base()


class InsertBase:
    """
    Base mixin for models that only track creation timestamp.

    :param create_ts: Timestamp when the record was created.
    """

    # Use CURRENT_TIMESTAMP for SQLite compatibility
    create_ts = Column(
        DateTime, server_default=text("CURRENT_TIMESTAMP"), nullable=False
    )


class UpsertBase(InsertBase):
    """
    Base mixin for models that track both creation and update timestamps.

    :param create_ts: Timestamp when the record was created.
    :param update_ts: Timestamp when the record was last updated.
    """

    # SQLite doesn't support onupdate triggers at the column level
    # This will need to be handled in application code for updates
    update_ts = Column(
        DateTime,
        server_default=text("CURRENT_TIMESTAMP"),
        nullable=False,
        onupdate=func.now(),
    )


class User(Base, InsertBase):
    """
    User identity and basic demographic data from Garmin Connect.

    Contains stable user identification and basic profile information.
    """

    __tablename__ = "user"

    # User identification.
    user_id = Column(BigInteger, primary_key=True)

    # Demographics.
    full_name = Column(String)
    birth_date = Column(Date)


class UserProfile(Base, InsertBase):
    """
    User fitness profile data from Garmin Connect.

    Contains physical characteristics and fitness metrics. The latest column
    indicates the most recent profile record for each user. Multiple records
    can exist per `user_id`, but only one can have `latest`=True.
    """

    __tablename__ = "user_profile"

    # Record identification.
    user_profile_id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("user.user_id"), nullable=False)

    # Physical characteristics.
    gender = Column(String)
    weight = Column(Float)
    height = Column(Float)

    # Fitness metrics.
    vo2_max_running = Column(Float)
    vo2_max_cycling = Column(Float)
    lactate_threshold_speed = Column(Float)
    lactate_threshold_heart_rate = Column(Integer)
    moderate_intensity_minutes_hr_zone = Column(Integer)
    vigorous_intensity_minutes_hr_zone = Column(Integer)

    # Record management.
    latest = Column(Boolean, default=False, nullable=False)


class Activity(Base, UpsertBase):
    """
    Main activity table with core aggregate metrics common across activity types.

    Additional aggregate metrics may be available in separate tables:
    - swimming_agg_metrics
    - cycling_agg_metrics
    - running_agg_metrics
    - supplemental_activity_metric
    """

    __tablename__ = "activity"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "start_ts", name="activity_user_id_start_ts_unique"
        ),
    )

    # Activity identification.
    activity_id = Column(BigInteger, primary_key=True)
    user_id = Column(BigInteger, ForeignKey("user.user_id"), nullable=False)
    activity_name = Column(String)
    activity_type_id = Column(Integer, nullable=False)
    activity_type_key = Column(String, nullable=False)
    event_type_id = Column(Integer, nullable=False)
    event_type_key = Column(String, nullable=False)

    # Time.
    start_ts = Column(DateTime(timezone=True), nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=False)
    timezone_offset_hours = Column(Float, nullable=False)
    duration = Column(Float)
    elapsed_duration = Column(Float)
    moving_duration = Column(Float)

    # Distance, speed, laps.
    distance = Column(Float)
    lap_count = Column(Integer)
    average_speed = Column(Float)
    max_speed = Column(Float)

    # Location.
    start_latitude = Column(Float)
    start_longitude = Column(Float)
    end_latitude = Column(Float)
    end_longitude = Column(Float)
    location_name = Column(String)

    # Training effect and load.
    aerobic_training_effect = Column(Float)
    aerobic_training_effect_message = Column(String)
    anaerobic_training_effect = Column(Float)
    anaerobic_training_effect_message = Column(String)
    training_effect_label = Column(String)
    activity_training_load = Column(Float)
    difference_body_battery = Column(Integer)
    moderate_intensity_minutes = Column(Integer)
    vigorous_intensity_minutes = Column(Integer)

    # Metabolism.
    calories = Column(Float)
    bmr_calories = Column(Float)
    water_estimated = Column(Float)

    # Heart rate zones.
    hr_time_in_zone_1 = Column(Float)
    hr_time_in_zone_2 = Column(Float)
    hr_time_in_zone_3 = Column(Float)
    hr_time_in_zone_4 = Column(Float)
    hr_time_in_zone_5 = Column(Float)
    average_hr = Column(Float)
    max_hr = Column(Float)

    # Device and technical info.
    device_id = Column(BigInteger)
    manufacturer = Column(String)
    time_zone_id = Column(Integer)

    # Data availability flags.
    has_polyline = Column(Boolean, nullable=False, server_default=text("0"))
    has_images = Column(Boolean, nullable=False, server_default=text("0"))
    has_video = Column(Boolean, nullable=False, server_default=text("0"))
    has_splits = Column(Boolean, server_default=text("0"))
    has_heat_map = Column(Boolean, nullable=False, server_default=text("0"))

    # Activity status flags.
    parent = Column(Boolean, nullable=False, server_default=text("0"))
    purposeful = Column(Boolean, nullable=False, server_default=text("0"))
    favorite = Column(Boolean, nullable=False, server_default=text("0"))
    elevation_corrected = Column(Boolean, server_default=text("0"))
    atp_activity = Column(Boolean, server_default=text("0"))
    manual_activity = Column(Boolean, nullable=False, server_default=text("0"))
    pr = Column(Boolean, nullable=False, server_default=text("0"))
    auto_calc_calories = Column(Boolean, nullable=False, server_default=text("0"))
    ts_data_available = Column(Boolean, nullable=False, server_default=text("0"))


class SwimmingAggMetrics(Base, UpsertBase):
    """
    Swimming-specific aggregate metrics for pool and open water activities.
    """

    __tablename__ = "swimming_agg_metrics"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    pool_length = Column(Float)
    active_lengths = Column(Integer)
    strokes = Column(Float)
    avg_stroke_distance = Column(Float)
    avg_strokes = Column(Float)
    avg_swim_cadence = Column(Float)
    avg_swolf = Column(Float)


class CyclingAggMetrics(Base, UpsertBase):
    """
    Cycling-specific aggregate metrics including power, cadence, and training.
    """

    __tablename__ = "cycling_agg_metrics"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    training_stress_score = Column(Float)
    intensity_factor = Column(Float)
    vo2_max_value = Column(Float)

    # Power metrics.
    avg_power = Column(Float)
    max_power = Column(Float)
    normalized_power = Column(Float)
    max_20min_power = Column(Float)
    avg_left_balance = Column(Float)

    # Cycling cadence.
    avg_biking_cadence = Column(Float)
    max_biking_cadence = Column(Float)

    # Power curve - maximum average power over time periods.
    max_avg_power_1 = Column(Float)
    max_avg_power_2 = Column(Float)
    max_avg_power_5 = Column(Float)
    max_avg_power_10 = Column(Float)
    max_avg_power_20 = Column(Float)
    max_avg_power_30 = Column(Float)
    max_avg_power_60 = Column(Float)
    max_avg_power_120 = Column(Float)
    max_avg_power_300 = Column(Float)
    max_avg_power_600 = Column(Float)
    max_avg_power_1200 = Column(Float)
    max_avg_power_1800 = Column(Float)
    max_avg_power_3600 = Column(Float)
    max_avg_power_7200 = Column(Float)
    max_avg_power_18000 = Column(Float)

    # Power zones - time spent in each power training zone.
    power_time_in_zone_1 = Column(Float)
    power_time_in_zone_2 = Column(Float)
    power_time_in_zone_3 = Column(Float)
    power_time_in_zone_4 = Column(Float)
    power_time_in_zone_5 = Column(Float)
    power_time_in_zone_6 = Column(Float)
    power_time_in_zone_7 = Column(Float)

    # Environmental conditions.
    min_temperature = Column(Float)
    max_temperature = Column(Float)

    # Elevation metrics.
    elevation_gain = Column(Float)
    elevation_loss = Column(Float)
    min_elevation = Column(Float)
    max_elevation = Column(Float)

    # Respiration metrics.
    min_respiration_rate = Column(Float)
    max_respiration_rate = Column(Float)
    avg_respiration_rate = Column(Float)


class RunningAggMetrics(Base, UpsertBase):
    """
    Running-specific aggregate metrics including form, cadence, and performance.
    """

    __tablename__ = "running_agg_metrics"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    steps = Column(Integer)
    vo2_max_value = Column(Float)

    # Running cadence.
    avg_running_cadence = Column(Float)
    max_running_cadence = Column(Float)
    max_double_cadence = Column(Float)

    # Running form metrics.
    avg_vertical_oscillation = Column(Float)
    avg_ground_contact_time = Column(Float)
    avg_stride_length = Column(Float)
    avg_vertical_ratio = Column(Float)
    avg_ground_contact_balance = Column(Float)

    # Power metrics.
    avg_power = Column(Float)
    max_power = Column(Float)
    normalized_power = Column(Float)

    # Power zones - time spent in each power training zone.
    power_time_in_zone_1 = Column(Float)
    power_time_in_zone_2 = Column(Float)
    power_time_in_zone_3 = Column(Float)
    power_time_in_zone_4 = Column(Float)
    power_time_in_zone_5 = Column(Float)

    # Temperature.
    min_temperature = Column(Float)
    max_temperature = Column(Float)

    # Elevation metrics.
    elevation_gain = Column(Float)
    elevation_loss = Column(Float)
    min_elevation = Column(Float)
    max_elevation = Column(Float)

    # Respiration metrics.
    min_respiration_rate = Column(Float)
    max_respiration_rate = Column(Float)
    avg_respiration_rate = Column(Float)


class SupplementalActivityMetric(Base, UpsertBase):
    """
    Supplemental activity aggregate metrics.

    This table captures any remaining metrics not covered by the main Activity table or
    sport-specific aggregate tables using a flexible key-value structure.
    """

    __tablename__ = "supplemental_activity_metric"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    metric = Column(String, primary_key=True)
    value = Column(Float)


class Sleep(Base, UpsertBase):
    """
    Sleep session data from Garmin Connect including sleep scores, duration, and quality
    metrics.

    Each record represents a single sleep session with comprehensive sleep analysis
    data.
    """

    __tablename__ = "sleep"
    __table_args__ = (
        UniqueConstraint("user_id", "start_ts", name="sleep_user_id_start_ts_unique"),
    )

    # Auto-generated primary key.
    sleep_id = Column(Integer, primary_key=True)

    # Foreign key reference.
    user_id = Column(BigInteger, ForeignKey("user.user_id"), nullable=False)

    # Non-nullable timestamps.
    start_ts = Column(DateTime(timezone=True), nullable=False)
    end_ts = Column(DateTime(timezone=True), nullable=False)
    timezone_offset_hours = Column(Float, nullable=False)

    # Sleep session metadata.
    calendar_date = Column(String)
    sleep_version = Column(Integer)
    age_group = Column(String)
    respiration_version = Column(Integer)

    # Sleep duration and stages.
    sleep_time_seconds = Column(Integer)
    nap_time_seconds = Column(Integer)
    unmeasurable_sleep_seconds = Column(Integer)
    deep_sleep_seconds = Column(Integer)
    light_sleep_seconds = Column(Integer)
    rem_sleep_seconds = Column(Integer)
    awake_sleep_seconds = Column(Integer)
    awake_count = Column(Integer)
    restless_moments_count = Column(Integer)
    rem_sleep_data = Column(Boolean)

    # Sleep window and detection.
    sleep_window_confirmed = Column(Boolean)
    sleep_window_confirmation_type = Column(String)
    sleep_quality_type_pk = Column(BigInteger)
    sleep_result_type_pk = Column(BigInteger)
    retro = Column(Boolean)
    sleep_from_device = Column(Boolean)
    device_rem_capable = Column(Boolean)
    skin_temp_data_exists = Column(Boolean)

    # Physiological metrics.
    average_spo2 = Column(Float)
    lowest_spo2 = Column(Integer)
    highest_spo2 = Column(Integer)
    average_spo2_hr_sleep = Column(Float)
    number_of_events_below_threshold = Column(Integer)
    duration_of_events_below_threshold = Column(Integer)
    average_respiration = Column(Float)
    lowest_respiration = Column(Float)
    highest_respiration = Column(Float)
    avg_sleep_stress = Column(Float)
    breathing_disruption_severity = Column(String)
    avg_overnight_hrv = Column(Float)
    hrv_status = Column(String)
    body_battery_change = Column(Integer)
    resting_heart_rate = Column(Integer)

    # Sleep insights and feedback.
    sleep_score_feedback = Column(String)
    sleep_score_insight = Column(String)
    sleep_score_personalized_insight = Column(String)

    # Sleep scores.
    total_duration_key = Column(String)
    stress_key = Column(String)
    awake_count_key = Column(String)
    restlessness_key = Column(String)
    score_overall_key = Column(String)
    score_overall_value = Column(Integer)
    light_pct_key = Column(String)
    light_pct_value = Column(Integer)
    deep_pct_key = Column(String)
    deep_pct_value = Column(Integer)
    rem_pct_key = Column(String)
    rem_pct_value = Column(Integer)

    # Sleep need.
    sleep_need_baseline = Column(Integer)
    sleep_need_actual = Column(Integer)
    sleep_need_feedback = Column(String)
    sleep_need_training_feedback = Column(String)
    sleep_need_history_adj = Column(String)
    sleep_need_hrv_adj = Column(String)
    sleep_need_nap_adj = Column(String)

    # Next sleep need.
    next_sleep_need_baseline = Column(Integer)
    next_sleep_need_actual = Column(Integer)
    next_sleep_need_feedback = Column(String)
    next_sleep_need_training_feedback = Column(String)
    next_sleep_need_history_adj = Column(String)
    next_sleep_need_hrv_adj = Column(String)
    next_sleep_need_nap_adj = Column(String)


class SleepMovement(Base, InsertBase):
    """
    Timeseries data capturing movement activity levels throughout a sleep session.

    Time interval: 1 minute.
    """

    __tablename__ = "sleep_movement"

    sleep_id = Column(Integer, ForeignKey("sleep.sleep_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    activity_level = Column(Float)


class SleepRestlessMoment(Base, InsertBase):
    """
    Timeseries data capturing moments of restlessness or movement during sleep.

    Time interval: Event-based (irregular intervals when restless moments
    occur).
    """

    __tablename__ = "sleep_restless_moment"

    sleep_id = Column(Integer, ForeignKey("sleep.sleep_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)


class SpO2(Base, InsertBase):
    """
    Timeseries data capturing blood oxygen saturation SpO2 measurements during sleep.

    Time interval: 1 minute.
    """

    __tablename__ = "spo2"

    sleep_id = Column(Integer, ForeignKey("sleep.sleep_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)


class HRV(Base, InsertBase):
    """
    Timeseries data capturing heart rate variability (HRV) measurements throughout sleep
    periods.

    Time interval: 5 minutes.
    """

    __tablename__ = "hrv"

    sleep_id = Column(Integer, ForeignKey("sleep.sleep_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float)


class BreathingDisruption(Base, InsertBase):
    """
    Timeseries data capturing breathing disruption events and their severity during
    sleep periods.

    Time interval: Event-based (irregular intervals when breathing disruptions
    occur).
    """

    __tablename__ = "breathing_disruption"

    sleep_id = Column(Integer, ForeignKey("sleep.sleep_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)


class VO2Max(Base, UpsertBase):
    """
    VO2 max measurements from Garmin training status data.

    Includes both generic and cycling-specific VO2 max values with different measurement
    dates.
    """

    __tablename__ = "vo2_max"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    date = Column(Date, primary_key=True)
    vo2_max_generic = Column(Float)
    vo2_max_cycling = Column(Float)


class Acclimation(Base, UpsertBase):
    """
    Heat and altitude acclimation metrics from Garmin training status data.

    Tracks acclimation levels and trends for environmental conditions.
    """

    __tablename__ = "acclimation"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    date = Column(Date, primary_key=True)
    altitude_acclimation = Column(Float)
    heat_acclimation_percentage = Column(Float)
    current_altitude = Column(Float)
    acclimation_percentage = Column(Float)
    altitude_trend = Column(String)
    heat_trend = Column(String)


class TrainingLoad(Base, UpsertBase):
    """
    Training load balance and status metrics from Garmin Connect.

    Includes monthly load distribution, ACWR analysis, and training status indicators.
    """

    __tablename__ = "training_load"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    date = Column(Date, primary_key=True)

    # Monthly training load balance.
    monthly_load_aerobic_low = Column(Float)
    monthly_load_aerobic_high = Column(Float)
    monthly_load_anaerobic = Column(Float)
    monthly_load_aerobic_low_target_min = Column(Float)
    monthly_load_aerobic_low_target_max = Column(Float)
    monthly_load_aerobic_high_target_min = Column(Float)
    monthly_load_aerobic_high_target_max = Column(Float)
    monthly_load_anaerobic_target_min = Column(Float)
    monthly_load_anaerobic_target_max = Column(Float)
    training_balance_feedback_phrase = Column(String)

    # Acute chronic workload ratio (ACWR) metrics.
    acwr_percent = Column(Float)
    acwr_status = Column(String)
    acwr_status_feedback = Column(String)
    daily_training_load_acute = Column(Float)
    max_training_load_chronic = Column(Float)
    min_training_load_chronic = Column(Float)
    daily_training_load_chronic = Column(Float)
    daily_acute_chronic_workload_ratio = Column(Float)

    # Training status.
    training_status = Column(Integer)
    training_status_feedback_phrase = Column(String)

    # Intensity minutes.
    total_intensity_minutes = Column(Integer)
    moderate_minutes = Column(Integer)
    vigorous_minutes = Column(Integer)


class TrainingReadiness(Base, UpsertBase):
    """
    Training readiness scores and factors from Garmin Connect.

    Indicates recovery status and training capacity based on sleep, HRV, stress, and
    training load metrics.
    """

    __tablename__ = "training_readiness"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    timezone_offset_hours = Column(Float, nullable=False)

    # Training readiness metrics.
    level = Column(String)
    feedback_long = Column(String)
    feedback_short = Column(String)
    score = Column(Integer)
    sleep_score = Column(Integer)
    sleep_score_factor_percent = Column(Integer)
    sleep_score_factor_feedback = Column(String)
    recovery_time = Column(Integer)
    recovery_time_factor_percent = Column(Integer)
    recovery_time_factor_feedback = Column(String)
    acwr_factor_percent = Column(Integer)
    acwr_factor_feedback = Column(String)
    acute_load = Column(Integer)
    stress_history_factor_percent = Column(Integer)
    stress_history_factor_feedback = Column(String)
    hrv_factor_percent = Column(Integer)
    hrv_factor_feedback = Column(String)
    hrv_weekly_average = Column(Integer)
    sleep_history_factor_percent = Column(Integer)
    sleep_history_factor_feedback = Column(String)
    valid_sleep = Column(Boolean)
    input_context = Column(String)
    primary_activity_tracker = Column(Boolean)
    recovery_time_change_phrase = Column(String)
    sleep_history_factor_feedback_phrase = Column(String)
    hrv_factor_feedback_phrase = Column(String)
    stress_history_factor_feedback_phrase = Column(String)
    acwr_factor_feedback_phrase = Column(String)
    recovery_time_factor_feedback_phrase = Column(String)
    sleep_score_factor_feedback_phrase = Column(String)


class Stress(Base, InsertBase):
    """
    Stress level timeseries data capturing stress measurements throughout the day.

    Time interval: 3 minutes.
    """

    __tablename__ = "stress"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)


class BodyBattery(Base, InsertBase):
    """
    Body battery level timeseries data capturing energy levels throughout the day.

    Time interval: 3 minutes.
    """

    __tablename__ = "body_battery"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)


class HeartRate(Base, InsertBase):
    """
    Timeseries heart rate data from Garmin devices.

    Time interval: 2 minutes.
    """

    __tablename__ = "heart_rate"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)


class Steps(Base, InsertBase):
    """
    Step count timeseries data capturing movement activity throughout the day.

    Time interval: 15 minutes.
    """

    __tablename__ = "steps"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Integer)
    activity_level = Column(String)
    activity_level_constant = Column(Boolean)


class Respiration(Base, InsertBase):
    """
    Timeseries respiration rate data from Garmin devices.

    Time interval: 2 minutes.
    """

    __tablename__ = "respiration"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float)


class IntensityMinutes(Base, InsertBase):
    """
    Timeseries intensity minutes data from Garmin devices.

    Time interval: 15 minutes.
    """

    __tablename__ = "intensity_minutes"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    value = Column(Float)


class Floors(Base, InsertBase):
    """
    Timeseries floors data from Garmin devices.

    Time interval: 15 minutes.
    """

    __tablename__ = "floors"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    ascended = Column(Integer)
    descended = Column(Integer)


class PersonalRecord(Base, InsertBase):
    """
    Personal records achieved by users across various activity types and distances.

    Each record represents a best performance for a specific type and user.
    The latest column indicates the most recent personal record for each user
    and type.

    Note: `activity_id` can be NULL for steps-based PRs (typeId 12-15) which
    are daily/weekly/monthly aggregates not tied to specific activities.
    """

    __tablename__ = "personal_record"

    user_id = Column(BigInteger, ForeignKey("user.user_id"), primary_key=True)
    activity_id = Column(BigInteger)
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    type_id = Column(Integer, primary_key=True)
    label = Column(Text)
    value = Column(Float)
    latest = Column(Boolean, nullable=False, default=False)


class RacePredictions(Base, InsertBase):
    """
    SQLAlchemy model for the race_predictions table.

    Stores race time predictions from Garmin Connect including 5K, 10K, half marathon,
    and marathon predicted times.
    """

    __tablename__ = "race_predictions"

    user_id = Column(BigInteger, primary_key=True)
    date = Column(Date, primary_key=True)
    time_5k = Column(Float)
    time_10k = Column(Float)
    time_half_marathon = Column(Float)
    time_marathon = Column(Float)
    latest = Column(Boolean, nullable=False, default=False)


class ActivityTsMetric(Base, InsertBase):
    """
    Time-series metrics extracted from activity FIT files.

    Stores granular sensor measurements recorded during activities including heart rate,
    cadence, power, speed, distance, and other metrics.
    """

    __tablename__ = "activity_ts_metric"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    timestamp = Column(DateTime(timezone=True), primary_key=True)
    name = Column(Text, primary_key=True)
    value = Column(Float)
    units = Column(Text)


class ActivitySplitMetric(Base, InsertBase):
    """
    Split metrics extracted from activity FIT files.

    Stores Garmin's algorithmic breakdown of activities into intervals such as run/walk
    detection and active intervals. Each record represents a single metric for a
    specific split segment.
    """

    __tablename__ = "activity_split_metric"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    split_idx = Column(Integer, primary_key=True)
    name = Column(Text, primary_key=True)
    split_type = Column(Text)
    value = Column(Float)
    units = Column(Text)


class ActivityLapMetric(Base, InsertBase):
    """
    Lap metrics extracted from activity FIT files.

    Stores device-triggered lap segments from manual button press or auto distance/time
    triggers. Each record represents a single metric for a specific lap segment.
    """

    __tablename__ = "activity_lap_metric"

    activity_id = Column(
        BigInteger, ForeignKey("activity.activity_id"), primary_key=True
    )
    lap_idx = Column(Integer, primary_key=True)
    name = Column(Text, primary_key=True)
    value = Column(Float)
    units = Column(Text)
