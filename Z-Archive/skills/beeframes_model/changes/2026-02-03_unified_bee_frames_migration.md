# Migration: supervised_beeframes → unified_bee_frames

**Date:** 2026-02-03
**Status:** In Progress
**Author:** Tamir

## Summary

Migrating the primary ML model output table from `supervised_beeframes` to `unified_bee_frames` in `data_lake_raw_data`.

## Complete Schema Mapping

| Prior (supervised_beeframes) | Post (unified_bee_frames) | Type | Description |
|------------------------------|---------------------------|------|-------------|
| `sensor_mac_address` | `sensor_mac_address` | string | Sensor hardware identifier |
| `model_name` | `model_name` | string | ML model name/version used |
| `model_type` | `model_type` | string | Model category |
| `feature_pipeline_version` | `feature_pipeline_version` | string | Version of feature extraction pipeline |
| `feature_file_path` | `feature_file_path` | string | S3 path to feature file |
| `is_sensor_features_exists` | `is_sensor_features_exists` | boolean | Whether features were found for sensor |
| `pred_base` | `pred_base` | double | Baseline prediction value |
| `pred_raw` | `pred_raw` | double | Raw model output (unclipped) |
| `pred_clipped` | `pred_clipped` | double | Prediction clipped to valid range |
| `pred_rounded` | `pred_rounded` | double | Final rounded prediction value |
| `switch_reset` | `switch_reset` | boolean | Whether baseline was reset |
| `switch_reset_reason` | `switch_reset_reason` | string | Reason for baseline reset |
| `log_level` | `log_level` | string | Log severity level |
| `log_timestamp` | `log_timestamp` | timestamp | When prediction was generated |
| `source_name` | `source_name` | string | Source system identifier |
| `lambda_name` | `lambda_name` | string | AWS Lambda function name |
| `lambda_remaining_time_in_sec` | `lambda_remaining_time_in_sec` | double | Lambda execution time remaining |
| `aws_request_id` | `aws_request_id` | string | AWS request tracking ID |
| `upload_time` | `upload_time` | timestamp | When record was uploaded to S3 |
| `input_date` | `input_date` | timestamp | Date of input data |
| `approved_sensor` | *NULL* | boolean | **REMOVED** - Replaced by deployment_status |
| `pred_smoothed` | *NULL* | double | **REMOVED** - Smoothing no longer stored |
| *NULL* | `group_id` | bigint | **ADDED** - Direct link to beekeeper group |
| *NULL* | `apply_clipping` | boolean | **ADDED** - Whether clipping was applied |
| *NULL* | `calibration_path` | string | **ADDED** - Path to calibration data |
| *NULL* | `deployment_status` | string | **ADDED** - Model deployment state |
| *NULL* | `deployment_confidence` | string | **ADDED** - Deployment confidence level |
| *NULL* | `deployment_reason` | string | **ADDED** - Reason for deployment decision |
| *NULL* | `is_observable` | boolean | **ADDED** - Observability flag |
| *NULL* | `observability_value` | double | **ADDED** - Observability metric |
| *NULL* | `reset_baseline_at` | string | **ADDED** - Baseline reset timestamp |

## Change Summary

| Change Type | Count | Columns |
|-------------|-------|---------|
| Retained | 20 | Core prediction & metadata columns |
| Removed | 2 | `approved_sensor`, `pred_smoothed` |
| Added | 9 | `group_id`, `apply_clipping`, `calibration_path`, `deployment_*` (3), `is_observable`, `observability_value`, `reset_baseline_at` |

## Key Improvements

1. **Direct group_id** - No longer need to join to get beekeeper group
2. **Deployment tracking** - Full visibility into model deployment decisions
3. **Observability** - New metrics for monitoring prediction quality
4. **Calibration traceability** - Path to calibration data stored

## Migration Notes

- Both tables currently active during transition
- New code should use `unified_bee_frames`
- Legacy queries may still reference `supervised_beeframes`

## Queries

### Compare row for same sensor/timestamp
```sql
SELECT
    o.sensor_mac_address,
    o.log_timestamp,
    o.pred_raw as old_pred_raw,
    n.pred_raw as new_pred_raw,
    n.group_id,
    n.deployment_status
FROM data_lake_raw_data.supervised_beeframes o
JOIN data_lake_raw_data.unified_bee_frames n
    ON o.sensor_mac_address = n.sensor_mac_address
    AND o.log_timestamp = n.log_timestamp
WHERE o.log_timestamp >= current_timestamp - interval '1' day
LIMIT 100
```
