# Beeframes Model

The ML model (lambda chain) that predicts bee frame counts from sensor data. Covers the model itself, its predictions, deployment lifecycle, calibration pipeline, and table schemas.

For monitoring, validations, and daily pipeline checks, see [beeframes_monitoring](../beeframes_monitoring/SKILL.md).

## Quick Reference

| Item | Value |
|------|-------|
| Production Table | `data_lake_raw_data.unified_bee_frames` |
| ~~Legacy Table~~ | ~~`data_lake_raw_data.supervised_beeframes`~~ **DEPRECATED -- do not use** |
| Sensor-to-Group Mapping | `data_lake_curated_data.sensor_daily_snapshot` (MAC-based, authoritative) |
| Deployments Table | `data_lake_raw_data.model_deployments` |
| Seasonal Activities Table | `data_lake_raw_data.group_to_seasonal_activities` |
| Preprocess Table | `data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess` |
| Current Season ID | `90` (hardcoded - needs V2 fix) |

## System Architecture

```
IoT Sensors (temp, humidity, ambient)
    │
    ▼
V1 Ranker (MLP)  ──►  Relative hive strength scores (inter-hive ranking)
    │
    ▼
Calibration Function  ──►  CF(v) = clip(slope * v + bias, 0, saturation)
    │                       3-parameter lossless representation of MLP
    ▼
Bee Frame Prediction  ──►  Per-hive BF count per beekeeper group
    │
    ▼
Monitoring Dashboard (Streamlit)  ──►  Review and validation
```

**Data flow:** Sensors → Ranker → Calibration → Prediction → Monitoring

## Key Prediction Fields

| Field | Meaning |
|-------|---------|
| `pred_raw` | Raw model output (unclipped) |
| `pred_clipped` | Clipped to valid range |
| `pred_rounded` | Final rounded prediction value |
| `pred_base` | Baseline prediction |

## Production Group Criteria

A group is considered "production" when ALL of these are true:

> **Note:** season_id changes each season — verify current value before querying.

- `seasonal_activities_id = 90` (current season) in `group_to_seasonal_activities`
- `status = 'PRODUCTION'` in `model_deployments`
- Latest deployment only (`ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC) = 1`)
- Calibrated at least 3 days ago (`timestamp <= CURRENT_DATE - interval '3' day`)

## Production Pipeline (Daily Cycle)

| Phase | Time (UTC) | Description |
|-------|-----------|-------------|
| Phase 1 | 06:00 | Core model execution - ranker runs on all active sensors |
| Phase 2 | 06:10 | Data ingestion - results stored in data lake |
| Phase 3 | 06:20 | Metadata enrichment - dbt preprocess tables updated |
| Phase 4 | 06:30 | Validation & release - quality gates and publish |

## Calibration Function

The calibration function maps V1 ranker output to absolute bee frame counts:

```
CF(v) = clip(slope * v + bias, 0, saturation)
```

- **3 parameters:** slope, bias, saturation
- **Lossless:** validated to recover original MLP predictions exactly
- **Saturation:** fixed from hive configuration data, not learned
- **Today:** calibrated manually by 1-2 domain experts per beekeeper group
- **Process:** histogram matching + distribution shape + ambient temp + professional intuition

### (slope, y_bar) Reparameterization

Alternative prediction target where `y_bar = slope * V_bar + bias` (V_bar = mean ranker output):
- Reduces slope-bias correlation from -0.96 to ~0
- Makes parameters nearly independent (easier to learn)
- **Status:** theoretically validated, needs production confirmation
- Used in current best model (P2/SY NN)

## V2: Automated Calibration Pipeline

### The Problem
- Manual calibration by 1-2 experts is a significant time burden
- Cannot scale with customer growth (50-100 groups, 10-20K sensors per season)
- Experts blocked from higher-value work

### Current State (March 2026)
- Best auto-calibration: **69.3% combined pass rate** (moderate tier)
- Target: **>95%**
- **Feature set is the identified bottleneck** (not model architecture)
  - NN and RF converge to same performance with same features
  - Embeddings provide only marginal lift
- Training data: ~500 group_id × date tuples, ~250K sensors
- Evaluation: temporal split (train on old, test on new)

### Evaluation Tiers

| Tier | Thresholds (sensor/group) | Current Pass Rate |
|------|--------------------------|-------------------|
| Strict | ±2 / ±0.5 BF | 37.7% |
| Moderate | ±3 / ±1.0 BF | 69.3% |
| Loose | ±4 / ±1.5 BF | 85.5% |

**Note:** accuracy compared to manual CF prediction, not inspections.

### KPI Conflict (Unresolved)
Three conflicting definitions exist across documents:
- Accuracy KPIs doc: ±1 BF, 100% daily compliance
- Monitoring V2 HLD: ±2 BF
- Function Fitting KPIs: three tiers (strict/moderate/loose), no single tier selected

**Must be resolved at kickoff.**

### Four Improvement Approaches

| Approach | Description | Status |
|----------|-------------|--------|
| A: Better features | Encode expert signals: temp patterns, ambient, historical calibrations | **Primary - highest impact** |
| B: Smarter prediction target | (slope, y_bar) reparameterization | In use, needs validation |
| C: Richer CFs | More parameters for hard-to-fit groups | Diminishing returns so far |
| D: Scorer approach | Score candidate calibrations instead of predicting params | Poor initial results, revisit later |

### Feature Engineering Categories

1. **What experts see:** temperature distribution shape, ambient conditions, population health
2. **Historical patterns:** past calibrations as priors, group trajectories, cross-group similarity
3. **Physical context:** sensor age/health, seasonality, inventory dynamics

### V2 Pipeline Components

1. **Feasibility Assessment** - binary gate: can we auto-calibrate this group?
   - Per-sensor validity classification
   - Ambient temperature influence model
   - Coverage threshold check
   - **Thresholds:** data team will define (not blocked on Product)
   - **KPI:** Recall >90% @ Precision >90%
2. **Calibration Generation** - compute CF parameters automatically
3. **Drift Detection** - when to recalibrate
4. **Apply/Defer** - automated or flag for manual

### Sensor Categories

| Category | Description |
|----------|-------------|
| Dead | No signal, failed hardware, removed from hive |
| Bio | Active sensor, in-hive, producing temperature data |
| Bio New | Recently installed, calibration not yet established |
| New | Just deployed, no data history, needs initial calibration |

Inventory changes (splits, additions, removals) can invalidate calibration.

### V2 Timeline & Milestones

**Hard deadline: June 2026 - Calibration + Feasibility running SBS with current flow**

| Milestone | Duration | Description | KPIs |
|-----------|----------|-------------|------|
| MS1: Auto-Cal USA | 2 weeks | Model dev + validation tool | >90% success rate |
| MS2: Sensor Context Module | 3 weeks | Anomaly detector + labeling + classifier | Recall 90% @ Prec 90%, 1000+ labeled sensors |
| MS3: Auto-Cal AUS | 1 week | Generalize to AUS configuration | Cross-region validation |
| MS4: Working Sensor Context | 3 weeks | End-to-end sensor context pipeline | Integrated with auto-cal |
| MS5: Optimizations | Ongoing | Performance tuning, edge cases | |

**Two parallel tracks:** Auto-Calibration (MS1→MS3→MS5) and Sensor Context (MS2→MS4→MS5)

### Cross-Region: Australia 2026

- All evaluation so far: California data only
- AUS has known differences (climate, species, practices) with mitigation plan
- AUS25 historical data evaluation planned for March/April
- Target: <5% performance degradation vs CA results
- Ground truth requires certified inspector data

### Team & Resources
- 2-3 people, 100% dedicated to V2
- No external benchmarks - entirely novel problem

## Table Schemas

### unified_bee_frames (Production Table)

Core prediction output. Key columns:
- `sensor_mac_address`, `group_id` -- identifiers
- `pred_raw`, `pred_clipped`, `pred_rounded`, `pred_base` -- predictions
- `model_name`, `model_type`, `feature_pipeline_version` -- model metadata
- `deployment_status`, `deployment_confidence`, `deployment_reason` -- deployment info
- `calibration_path`, `is_observable`, `observability_value` -- observability
- `log_timestamp`, `upload_time`, `input_date` -- timestamps

### model_deployments

| Column | Description |
|--------|-------------|
| `group_id` | Beekeeper group |
| `status` | e.g., `'PRODUCTION'`, `'DISABLED'` |
| `timestamp` | Calibration date |

**Pattern:** Use `ROW_NUMBER() OVER (PARTITION BY group_id ORDER BY timestamp DESC)` to get the latest deployment per group.

### group_to_seasonal_activities

| Column | Description |
|--------|-------------|
| `group_id` | Beekeeper group |
| `seasonal_activities_id` | Current season = `90` |

### Preprocess Table (7-step join)

> For full details on the preprocess join logic and debugging, see [dbt_gold_layer](../dbt_gold_layer/SKILL.md).

`beekeeper_beeframe_model_monitoring_preprocess` joins 6 sources:
1. `hive_updates_metadata` -- BF model predictions per sensor
2. `unified_bee_frames` -- raw + clipped + rounded predictions (INNER JOIN -- drops sensors without predictions)
3. `sensor_daily_snapshot` -- sensor location & group assignment (MAC-based, authoritative)
4. `group_to_seasonal_activities` -- active season per group
5. `model_deployments` -- calibration date & deploy status
6. `daily_beekeeper_metrics` -- user-visible averages

**Computed filter indicators:**
- `group_in_season` -- active seasonal activity
- `groups_in_season_with_hive_updates` -- in-season + model predictions available
- `groups_in_season_ready_for_review` -- above + 3+ days since last calibration + PRODUCTION status

**Known issues:** Season ID (90) and 3-day calibration threshold are hardcoded.

### Correct Join Patterns

> Learned from group 2794 investigation (March 2026). See [data_quality](../data_quality/SKILL.md) for full details.

**For sensor-to-group lookups, ALWAYS use `sensor_daily_snapshot` (MAC-based):**
```sql
-- CORRECT: MAC-based join stays current
JOIN data_lake_curated_data.sensor_daily_snapshot sds
    ON sds.mac = hum.sensor_mac_address
    AND sds.date = DATE(hum.created)
WHERE sds.group_id = {{group_id}}
```

**Do NOT use `sensors.hive_id`** -- hive-to-sensor mappings go stale when sensors are moved between hives. The `sensors.hive_id` join showed data only through March 9 for group 2794 when data actually existed through March 11.

## Monitoring Dashboard (Streamlit)

- **Status:** In production, used by data team only (not leadership)
- **Branch:** DAS-120 (feature branch, not merged to main)
- **Repo:** beehero-algorythms
- **Features:** gateway temp visualization, temp per hive size, BF distribution histogram, observability distribution, two-tier review (Analytics → Product)
- **Data:** 2-day sensor window, max 1000 sensors/group (random sampling)
- **Caching:** 3-level (HTML visualization, observability pickle, DB query MD5)

## Temperature Data Export Package

Self-contained Python package for offline sensor data analysis:
- Sensor samples: hive internal temp at 30-min resolution
- Gateway samples: ambient temperature from gateways
- Hive updates: one record per sensor per day with BF prediction
- **Key:** negative temp correction at -40°C (hardware artifact: add 175.71)
- **Outlier detection:** geometric mean of forward/backward temp differences, z-score > 3

## Knowledge Index

### Schema & Tables
- [2026-02-03: unified_bee_frames migration](changes/2026-02-03_unified_bee_frames_migration.md)

### Queries
- [beeframes_table_migration.sql](queries/beeframes_table_migration.sql) - Compare rows during migration (uses deprecated supervised_beeframes for historical comparison only)
- [preprocess_sample.sql](queries/preprocess_sample.sql) - Sample model monitoring preprocess data

### Changes
- [2026-03-09: V2 Kickoff Prep](changes/2026-03-09_v2_kickoff_prep.md)

### Kickoff Prep (March 2026)
- [Raw Confluence Documents](../../kickoff-prep-output/raw-documents.md) - 17 documents from Confluence
- [Document Summaries](../../kickoff-prep-output/document-summaries.md) - Structured summaries
- [Analysis](../../kickoff-prep-output/analysis.md) - Themes, questions, risks, assumptions
- [Kickoff Plan](../../kickoff-prep-output/kickoff-plan.md) - Full plan with goals/objectives
- [Executive Summary](../../kickoff-prep-output/kickoff-summary.md) - One-page summary

## Related Skills

- [sbs_pipeline](../sbs_pipeline/SKILL.md) -- Daily SBS prediction pipeline (Lambda chain)
- [dbt_gold_layer](../dbt_gold_layer/SKILL.md) -- Preprocess model join logic and gate flags
- [beeframes_monitoring](../beeframes_monitoring/SKILL.md) -- Downstream validation pipeline
- [data_quality](../data_quality/SKILL.md) -- Correct join patterns and data quality investigation

---
*Last updated: 2026-03-17*
