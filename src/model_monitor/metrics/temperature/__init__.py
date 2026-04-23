"""
Temperature family metrics — grouped under model_monitor.metrics.temperature.

Each module exposes:
  METRIC_FAMILY = "temperature"
  DEFAULT_WEIGHT : float
  <metric_function>(...)  → dict with at minimum {"passed": bool}

Import shortcuts:

    from model_monitor.metrics.temperature import (
        ambient_temperature_volatility,
        ambient_stability,
        ambient_range,
        bucket_reference_adherence,
        sensor_spread_within_bucket,
        bucket_temporal_stability,
        small_hive_ambient_tracking,
        large_hive_thermoregulation,
        bucket_temperature_ordering,
    )
"""

from model_monitor.metrics.temperature.ambient_temperature_volatility import (
    ambient_temperature_volatility,
    get_getway_min_temp_in_freq,
)
from model_monitor.metrics.temperature.ambient_stability import ambient_stability
from model_monitor.metrics.temperature.ambient_range import ambient_range
from model_monitor.metrics.temperature.bucket_reference_adherence import bucket_reference_adherence
from model_monitor.metrics.temperature.sensor_spread_within_bucket import sensor_spread_within_bucket
from model_monitor.metrics.temperature.bucket_temporal_stability import bucket_temporal_stability
from model_monitor.metrics.temperature.small_hive_ambient_tracking import small_hive_ambient_tracking
from model_monitor.metrics.temperature.large_hive_thermoregulation import large_hive_thermoregulation
from model_monitor.metrics.temperature.bucket_temperature_ordering import bucket_temperature_ordering

__all__ = [
    "ambient_temperature_volatility",
    "get_getway_min_temp_in_freq",
    "ambient_stability",
    "ambient_range",
    "bucket_reference_adherence",
    "sensor_spread_within_bucket",
    "bucket_temporal_stability",
    "small_hive_ambient_tracking",
    "large_hive_thermoregulation",
    "bucket_temperature_ordering",
]
