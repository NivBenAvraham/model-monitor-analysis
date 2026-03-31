-- Name: preprocess_sample
-- Domain: beeframes_model
-- Description: Sample rows from the model monitoring preprocess table to inspect current predictions and model status
-- Created: 2026-02-03
--
-- Usage: Quick diagnostic query to check recent model predictions, deployment
--        status, and calibration data. Useful for debugging model behavior
--        or validating pipeline output.
--
-- Key columns:
--   - pred_raw, pred_clipped, pred_rounded: Model prediction values
--   - model_name: Which model version produced the prediction
--   - deployment_status, model_status: Current state
--   - calibration_average: Reference calibration value
--

SELECT *
FROM data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess
WHERE date = CURRENT_DATE
LIMIT 10
