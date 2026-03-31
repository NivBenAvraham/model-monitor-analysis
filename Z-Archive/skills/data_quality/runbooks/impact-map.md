# BeeHero Data Pipeline Impact Map

Last updated: 2026-03-26
Source: Salvaged from `src/data_team_agent/bridge/impact.py`

## Overview

This document maps the dependencies between tables, pipelines, and teams in the
BeeHero data ecosystem. Use this when assessing the impact of schema changes,
pipeline modifications, or data source updates.

## Table-to-Pipeline Mapping

Which pipeline owns or produces each table:

| Table (fully qualified) | Pipeline |
|-------------------------|----------|
| `data_lake_raw_data.unified_bee_frames` | beeframes |
| `data_lake_raw_data.model_deployments` | beeframes |
| `data_lake_raw_data.group_to_seasonal_activities` | beeframes |
| `data_lake_curated_data.sensor_daily_snapshot` | beeframes |
| `data_lake_curated_data.beekeeper_beeframe_model_monitoring_preprocess` | beeframes_monitoring |
| `data_lake_curated_data.beekeeper_beeframe_model_monitoring_validations` | beeframes_monitoring |
| `data_lake_curated_data.model_metric_test` | beeframes_monitoring |
| `data_lake_raw_data.hive_updates` | hive_updates |
| `data_lake_curated_data.hive_updates` | hive_updates |
| `data_lake_raw_data.sensors` | sensors |

## Pipeline Dependency Graph

Which pipelines consume output from other pipelines:

```
sensors
  └─> beeframes
       └─> beeframes_monitoring
       │    └─> validations_review
       └─> hive_updates
       │    └─> predictions
       │    └─> beeframes_monitoring
       └─> predictions
```

| Pipeline | Downstream Pipelines |
|----------|---------------------|
| sensors | beeframes, hive_updates |
| beeframes | beeframes_monitoring, hive_updates, predictions |
| beeframes_monitoring | validations_review |
| hive_updates | predictions, beeframes_monitoring |
| predictions | (none) |
| validations_review | (none) |

## Table Dependency Graph

Direct data dependencies between tables:

| Table | Downstream Tables |
|-------|-------------------|
| `data_lake_raw_data.unified_bee_frames` | `beekeeper_beeframe_model_monitoring_preprocess`, `curated hive_updates` |
| `data_lake_raw_data.model_deployments` | `beekeeper_beeframe_model_monitoring_preprocess` |
| `data_lake_raw_data.sensors` | `sensor_daily_snapshot`, `curated hive_updates` |
| `beekeeper_beeframe_model_monitoring_preprocess` | `beekeeper_beeframe_model_monitoring_validations`, `model_metric_test` |
| `beekeeper_beeframe_model_monitoring_validations` | `model_metric_test` |
| `data_lake_raw_data.hive_updates` | `data_lake_curated_data.hive_updates` |

## Pipeline-to-Team Mapping

Which teams own or are affected by each pipeline:

| Pipeline | Teams |
|----------|-------|
| beeframes | data-team, ml-team |
| beeframes_monitoring | data-team |
| hive_updates | data-team, backend-team |
| sensors | iot-team, data-team |
| predictions | data-team, product-team |
| validations_review | data-team, domain-experts |

## Freshness SLAs

How stale data can be before it is a problem:

| Pipeline | SLA (hours) | Notes |
|----------|------------|-------|
| sensors | 6h | Most time-sensitive -- feeds into beeframes and hive_updates |
| hive_updates | 12h | Daily records per sensor with BF prediction |
| beeframes | 24h | Core model pipeline |
| beeframes_monitoring | 24h | Monitoring runs after beeframes |
| predictions | 24h | End-of-chain predictions |

## Severity Classification Rules

| Condition | Severity |
|-----------|----------|
| Direct change to production pipeline (beeframes, predictions, hive_updates) AND >3 downstream tables affected | CRITICAL |
| Direct change to production pipeline | HIGH |
| >2 downstream tables OR >3 teams affected | MEDIUM |
| Any pipeline affected but not production-critical | LOW |

## Impact Assessment Checklist

When assessing a change:

1. Identify which tables are directly modified
2. Trace pipeline ownership from the table-to-pipeline map
3. Trace downstream pipelines from the dependency graph
4. Trace downstream tables from the table dependency graph
5. Identify affected teams from the pipeline-to-team map
6. Check freshness SLAs for timing constraints
7. Classify severity using the rules above
8. Generate recommendations:
   - CRITICAL/HIGH: Schedule off-peak (after daily pipeline, ~08:00 UTC), prepare rollback
   - Any downstream tables: Verify after deployment
   - Freshness risk >50% of SLA: Notify dependent teams
   - Multiple direct pipelines: Consider phased rollout
