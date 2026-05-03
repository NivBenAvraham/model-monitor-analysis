# Calibration Review Triage Specs

## Purpose

This spec defines only the decision logic that separates stale calibration groups into:

- `must_review`: human review required because yesterday's tier2 decision was invalid.
- `needs_review`: human review required because one or more risk signals fired, or because auto-validation is blocked.
- `auto_valid`: safe to auto-approve because no risk signal fired and the group passed the auto-valid blockers.

The target reader is an engineer or agent rebuilding this logic in another implementation. After reading this file, they should be able to reconstruct the decision engine using their own database, scheduler, and artifact infrastructure.

The spec is intentionally infrastructure-neutral. It names the logical data needed and the decision rules. It does not require the source repository layout, local script paths, cron setup, Apple Notes, Telegram, or any local cache path.

## Output Contract

The decision engine writes one row per reviewed group.

Required columns:

- `group_id`
- `group_name`
- `review_bucket`
- `status`
- `reason`

Decision and status mapping:

| Decision | review_bucket | status | Meaning |
| --- | ---: | --- | --- |
| `must_review` | 1 | `needs_review` | Yesterday's tier2 review was invalid. |
| `needs_review` | 2 | `needs_review` | A review signal fired, or auto-valid is blocked. |
| `auto_valid` | 3 | `valid` | No review signal fired and blockers passed. |

Important naming detail: the CSV status for auto-valid groups is `valid`. Summaries may call this count `auto_valid`, but the row value is `valid`.

Compatibility note: existing CSV artifacts may write the numeric bucket column as `tier`. A new implementation should prefer `review_bucket` or another non-conflicting name because `tier1` and `tier2` already mean something in BeeHero review history.

## Input Parameters

Default parameters from the current implementation:

```text
STALE_DAYS_THRESHOLD = 3
CLIPPING_DIFF_THRESHOLD = 1.0
DIPPING_YARD_PCT_THRESHOLD = 15.0
AUTO_REVIEW_THRESHOLD = 2.4
HIST_VALID_WINDOW_DAYS = 14
INSPECTION_LOOKBACK_DAYS = 14
THERMOREG_LOOKBACK_DAYS = 14
AUTO_REVIEW_LOOKBACK_DAYS = 21
AUTO_REVIEW_RECENT_DAYS = 7
```

Use a `reference_date` in `YYYY-MM-DD` format. The daily run uses today's date. The `must_review` invalid check uses `yesterday = reference_date - 1 day`.

## Data Required

The logic needs these data sets or equivalent views.

### Candidate Discovery

- Active pollination beekeeper groups:
  - group to seasonal activity mapping
  - seasonal activity metadata
  - group metadata
- Model deployments:
  - `group_id`
  - deployment `status`
  - deployment `timestamp`

Active group filter uses the `reference_date`, not the wall-clock run date. This keeps backfills and manual re-runs deterministic.

```sql
DATE(:reference_date) >= DATE(season.general_start_date)
AND DATE(:reference_date) <= DATE(season.general_end_date)
AND season.is_test = false
AND season.activity_type = 'POLLINATION'
AND group.group_type = 'BEEKEEPER'
```

### Validation History

Used for `must_review`, prior-invalid blocking, and HU anchor calculation.

- `group_id`
- review timestamp or review date
- `tier2_status`

### Unified Bee Frames

Used for clipping signal, auto-review scoring, historical clipping exemption, and same-day data presence.

- `group_id`
- `sensor_mac_address`
- `input_date`
- `log_timestamp`
- `pred_raw`
- `pred_clipped`

### Inspection Signal

Used by the inspection discrepancy signal.

- yard inspections:
  - inspection ID
  - yard ID
  - inspection end time
  - `bee_frames_distribution`
- yards:
  - yard ID
  - group ID
- hive update metadata:
  - sensor MAC
  - created timestamp
  - model name
  - hive update ID
  - numerical model result
- sensors:
  - sensor MAC
  - group ID

### Thermoregulation Signal

Used by the thermoregulation dipping signal.

- hive update metadata:
  - sensor MACs with metadata on `reference_date`
- current sensor to gateway to yard mapping:
  - sensor MAC
  - group ID
  - yard ID
  - yard name
- sensor daily model input:
  - sensor MAC
  - date
  - average temperature
  - temperature std
  - temperature range

### HU Stats

Used to order `must_review` and to block auto-valid when a prior invalid has not been superseded.

- validation history
- daily latest hive update:
  - group ID
  - activity date
  - hive ID
  - number of bee frames

### Historical Labels

Optional. Used only to exempt clipping-only groups.

- `group_id`
- label date
- `phase2_label`

Expected labels:

- valid label: `valid`
- bad labels: `invalid`, `needs_recalibration`

## Decision Pseudo-Code

This is the full decision flow.

```python
def classify_groups(reference_date, manual_group_ids=None):
    yesterday = reference_date - days(1)

    candidates = resolve_candidates(reference_date, manual_group_ids)
    if not candidates:
        return empty_results()

    must_review_ids = find_must_review_groups(candidates, yesterday)
    remaining = candidates - must_review_ids

    needs_review_reasons = {}
    needs_review_clip_score = {}
    signal_health = {}

    if remaining:
        apply_clipping_signal(remaining, reference_date, needs_review_reasons, needs_review_clip_score, signal_health)
        apply_inspection_signal(remaining, reference_date, needs_review_reasons, needs_review_clip_score, signal_health)
        apply_thermoreg_signal(remaining, reference_date, needs_review_reasons, needs_review_clip_score, signal_health)
        apply_auto_review_signal(remaining, reference_date, needs_review_reasons, needs_review_clip_score, signal_health)

    historical_safe = remove_historically_safe_clipping_only_groups(
        needs_review_reasons,
        needs_review_clip_score,
        reference_date,
    )

    needs_review_ids = set(needs_review_reasons)
    auto_valid_candidates = candidates - must_review_ids - needs_review_ids

    no_data = groups_without_ubf_on_reference_date(auto_valid_candidates, reference_date)
    for group_id in no_data:
        needs_review_reasons[group_id] = ["no_data"]
        needs_review_clip_score[group_id] = 0
    needs_review_ids |= no_data
    auto_valid_candidates -= no_data

    blocked = groups_with_unsuperseded_prior_invalid(auto_valid_candidates)
    for group_id, reason in blocked.items():
        needs_review_reasons[group_id] = [reason]
        needs_review_clip_score[group_id] = 0
    needs_review_ids |= set(blocked)
    auto_valid_ids = auto_valid_candidates - set(blocked)

    return build_rows(
        must_review=must_review_ids,
        needs_review=needs_review_ids,
        auto_valid=auto_valid_ids,
        historical_safe=historical_safe,
        needs_review_reasons=needs_review_reasons,
    )
```

## Candidate Resolution

Daily mode reviews only stale production groups.

```python
def resolve_candidates(reference_date, manual_group_ids=None):
    if manual_group_ids:
        # Current manual mode resolves deployment info for the supplied IDs and
        # does not reapply the stale-production filter.
        return resolve_deployment_info(manual_group_ids)

    active_group_ids = get_active_pollination_beekeeper_groups()
    latest_deployment = latest_model_deployment_per_group(active_group_ids)

    return {
        group
        for group in latest_deployment
        if group.status == "PRODUCTION"
        and date(group.timestamp) <= reference_date - days(STALE_DAYS_THRESHOLD)
    }
```

Carry `group_name` and `days_since_deployment` with each candidate for output reasons.

## Must Review

A group is `must_review` when yesterday's tier2 review was invalid.

Data:

- validation history
- stale candidate group IDs

Pseudo-code:

```python
def find_must_review_groups(candidates, yesterday):
    return {
        row.group_id
        for row in validation_history
        if date(row.review_date) == yesterday
        and row.tier2_status == "invalid"
        and row.group_id in candidates
    }
```

Output rows:

```python
{
    "review_bucket": 1,
    "status": "needs_review",
    "reason": f"tier2_invalid; latest_hu={latest_hu}; stale={days_since_deployment}d",
}
```

Ordering:

- Sort `must_review` rows by latest valid HU date ascending.
- Latest valid HU date comes from the HU stats algorithm below.

## Needs Review Signal A: Clipping Diff

This signal catches same-day raw-vs-clipped prediction shifts.

Data:

- unified bee frames on `reference_date`
- remaining stale candidates, excluding `must_review`

Pseudo-code:

```python
def apply_clipping_signal(groups, reference_date):
    rows = unified_bee_frames.filter(
        group_id in groups,
        date(input_date) == reference_date,
        pred_raw is not null,
        pred_clipped is not null,
    )

    latest = keep_latest_per_group_and_sensor(rows, order_by=input_date_desc)

    by_group = latest.group_by(group_id).agg(
        avg_abs_clip_diff=avg(abs(pred_raw - pred_clipped))
    )

    for row in by_group:
        if row.avg_abs_clip_diff > CLIPPING_DIFF_THRESHOLD:
            flag_needs_review(
                row.group_id,
                reason=f"clipping_diff={round(row.avg_abs_clip_diff, 2)}",
                clip_score=round(row.avg_abs_clip_diff, 2),
            )
```

Reason format:

```text
clipping_diff=<diff>
```

## Needs Review Signal B: Inspection Discrepancy

This signal catches groups where recent manual inspections disagree with same-day production model outputs.

Data:

- yard inspections in the last 14 days
- yard to group mapping
- same-day hive update metadata production results
- sensor to group mapping

Inspection average:

```python
def parse_bee_frames_distribution(distribution):
    # Example: {"0": 5, "1": 3, "2": 8}
    total_frames = sum(float(frames) * int(count) for frames, count in distribution.items())
    total_hives = sum(int(count) for count in distribution.values())
    return total_frames / total_hives if total_hives else None
```

Pseudo-code:

```python
def apply_inspection_signal(groups, reference_date):
    inspections = yard_inspections.filter(
        group_id in active_groups,
        date(utc_end_time) >= reference_date - days(INSPECTION_LOOKBACK_DAYS),
        date(utc_end_time) <= reference_date,
    )

    inspection_avg_by_group = inspections.parse_distribution().group_by(group_id).agg(
        inspection_avg=avg(parsed_distribution_avg),
        inspection_count=count(inspection_id),
    )

    model_rows = hive_updates_metadata.join(sensors).filter(
        group_id in inspection_avg_by_group.group_ids,
        hive_update_id is not null,
        model == "BEE_FRAMES",
        date(created) == reference_date,
    )
    latest_model_rows = keep_latest_per_sensor(model_rows, order_by=created_desc)
    model_avg_by_group = latest_model_rows.group_by(group_id).agg(
        model_avg=avg(numerical_model_result),
        sensor_count=count_distinct(sensor_mac_address),
    )

    comparison = inspection_avg_by_group.left_join(model_avg_by_group)
    comparison.discrepancy = abs(comparison.inspection_avg - comparison.model_avg)

    for row in comparison:
        # The standalone inspection monitor flags above 1.0, but the review
        # triage parser currently accepts only gaps above 1.5.
        if row.group_id in groups and row.discrepancy > 1.5:
            flag_needs_review(row.group_id, reason=f"inspection_gap={row.discrepancy}")
```

Reason format:

```text
inspection_gap=<gap>
```

Implementation note: the current implementation parses this from monitor stdout. A new implementation should return structured rows with `group_id` and `gap`.

## Needs Review Signal C: Thermoregulation Dipping

This signal catches groups where too many yards show increasing temperature dispersion.

Data:

- same-day hive update metadata sensor MACs
- current sensor to gateway to yard mapping
- sensor daily model input over the last 14 days

Per-yard trend:

```python
def classify_yard(yard_daily):
    if len(yard_daily) < 4:
        return "insufficient_data"

    stds = yard_daily.temperature_std.sorted_by_date()
    slope = linear_slope(range(len(stds)), stds)
    peak_idx = argmax(stds)
    trough_idx = argmin(stds)
    std_of_stds = std(stds)

    if slope < -0.03 and peak_idx < len(stds) * 0.6:
        return "recovering"
    if slope > 0.03 and trough_idx < len(stds) * 0.6:
        return "dipping"
    if abs(slope) > 0.02 and std_of_stds > 0.15:
        return "volatile"
    return "stable"
```

Pseudo-code:

```python
def apply_thermoreg_signal(groups, reference_date):
    hum_macs = hive_updates_metadata.filter(date(created) == reference_date).sensor_macs

    yard_map = current_sensor_gateway_yard_mapping(groups)
    yard_map = yard_map.filter(mac in hum_macs)

    sensor_data = sensor_daily_model_input.filter(
        group_id in groups,
        date >= reference_date - days(THERMOREG_LOOKBACK_DAYS),
        date <= reference_date,
        mac in hum_macs,
    )

    yard_daily = sensor_data.join(yard_map).group_by(group_id, yard_id, yard_name, date).agg(
        temp_mean=avg(avg_temperature),
        temp_std=avg(temperature_std),
        temp_range=avg(temperature_range),
        sensors=count_distinct(mac),
    )

    for group_id, group_yards in yard_daily.group_by(group_id):
        trends = [classify_yard(yard) for yard in group_yards.by_yard()]
        dip_pct = 100 * count(trend == "dipping") / count(trends)

        if dip_pct > DIPPING_YARD_PCT_THRESHOLD:
            flag_needs_review(group_id, reason=f"thermoreg_dipping={dip_pct:.1f}%")
```

Reason format:

```text
thermoreg_dipping=<percent>%
```

## Needs Review Signal D: Auto Review Score

This signal catches unstable or anomalous prediction behavior using the auto-review classifier.

Data:

- unified bee frames for the last 21 days through `reference_date`

Preprocessing:

```python
rows = unified_bee_frames.filter(
    group_id in groups,
    input_date >= reference_date - days(AUTO_REVIEW_LOOKBACK_DAYS),
    input_date <= reference_date,
)

# Keep earliest log per group, sensor, and input date.
rows = rows.sort_by(group_id, sensor_mac_address, input_date, log_timestamp)
rows = rows.group_by(group_id, sensor_mac_address, input_date).first()
```

Feature window:

- Use rows where `input_date > reference_date - 7 days` and `input_date <= reference_date`.
- Require at least 50 rows in the recent window.
- Require at least 3 daily aggregates.
- Each usable day needs at least 10 raw prediction values.

Features:

- `detrended_vol`: range of residuals after fitting a line to daily means.
- `median_tail`: median of daily `median(pred_raw) - p5(pred_raw)`.
- `cv_floor`: minimum daily coefficient of variation.
- `cv_trend`: slope of daily CV over time.
- `cv_range`: max daily CV minus min daily CV.
- `sensor_temporal_cv`: median per-sensor CV over the window.
- `cv_volatility`: standard deviation of daily CVs.

Scoring:

```python
score = (
    min(max(cv_floor - 0.20, 0) / 0.09, 2.5)
    + min(max(detrended_vol - 0.5, 0) / 2.5, 1.0)
    + min(max(median_tail - 4.5, 0) / 3.0, 1.0)
    + min(max(cv_trend - -0.003, 0) / 0.008, 1.0) * 0.8
    + min(max(cv_range - 0.03, 0) / 0.09, 1.0) * 0.8
    + min(max(sensor_temporal_cv - 0.09, 0) / 0.03, 1.0) * 0.30
    + min(max(cv_volatility - 0.025, 0) / 0.02, 0.5)
)

auto_review_tier = "invalid" if score >= AUTO_REVIEW_THRESHOLD else "valid"
```

Pseudo-code:

```python
def apply_auto_review_signal(groups, reference_date):
    for group_id in groups:
        features = compute_auto_review_features(group_id, reference_date)
        if features is None:
            continue

        score = score_auto_review_features(features)
        if score >= AUTO_REVIEW_THRESHOLD:
            flag_needs_review(group_id, reason=f"auto_review_invalid(score={score:.1f})")
```

Reason format:

```text
auto_review_invalid(score=<score>)
```

Important: `insufficient_data` from this classifier does not itself create a `needs_review` row. Same-day UBF absence is handled separately by the auto-valid blocker.

## Historical Clipping Exemption

This step can remove a group from `needs_review`, but only when clipping is the only reason it was flagged.

Data:

- historical labels
- unified bee frames on historical valid dates

Pseudo-code:

```python
def remove_historically_safe_clipping_only_groups(needs_review_reasons, needs_review_clip_score, reference_date):
    clipping_only = {
        group_id
        for group_id, reasons in needs_review_reasons.items()
        if all(reason.startswith("clipping_diff=") for reason in reasons)
    }

    historical_safe = set()
    cutoff = reference_date - days(HIST_VALID_WINDOW_DAYS)

    for group_id in clipping_only:
        valid_dates = {
            label.date
            for label in labels
            if label.group_id == group_id
            and label.phase2_label == "valid"
            and label.date >= cutoff
        }
        if not valid_dates:
            continue

        most_recent_valid = max(valid_dates)
        bad_dates = [
            label.date
            for label in labels
            if label.group_id == group_id
            and label.phase2_label in {"invalid", "needs_recalibration"}
        ]
        most_recent_bad = max(bad_dates) if bad_dates else None

        if most_recent_bad and most_recent_bad > most_recent_valid:
            continue

        historical_clip = avg_abs_clip_diff_by_group_and_date(group_id, valid_dates)
        if not historical_clip:
            continue
        if max(historical_clip.values()) >= needs_review_clip_score[group_id] * 0.8:
            historical_safe.add(group_id)

    for group_id in historical_safe:
        del needs_review_reasons[group_id]
        del needs_review_clip_score[group_id]

    return historical_safe
```

If the labels source is missing, skip this exemption and keep clipping-only groups in `needs_review`.

If there are no bad historical labels, do not apply the deterioration block. If there are no historical UBF clipping rows for the recent valid dates, keep the group in `needs_review`.

Auto-valid reason for exempted groups:

```text
auto_valid; clipping-only but historically valid at similar levels
```

## Auto-Valid Blocker: Same-Day UBF Data Required

A group cannot be `auto_valid` unless it has at least one unified bee frames row on the reference date.

Data:

- unified bee frames
- auto-valid candidates

Pseudo-code:

```python
def groups_without_ubf_on_reference_date(auto_valid_candidates, reference_date):
    groups_with_data = unified_bee_frames.filter(
        group_id in auto_valid_candidates,
        date(input_date) == reference_date,
    ).distinct_group_ids()

    return auto_valid_candidates - groups_with_data
```

Blocked output:

```python
{
    "review_bucket": 2,
    "status": "needs_review",
    "reason": "no_data",
}
```

Current implementation note: the script sets these groups into `needs_review` state after initially appending needs-review rows. A reconstruction should emit them explicitly as `needs_review`; they must not become `auto_valid`.

## Auto-Valid Blocker: Prior Invalid Must Be Superseded

A group cannot be `auto_valid` if it has a previous tier2 invalid decision and no later valid hive-update date.

Data:

- validation history
- HU stats from the invalid-streak-aware algorithm

Pseudo-code:

```python
def groups_with_unsuperseded_prior_invalid(auto_valid_candidates):
    latest_invalid = validation_history.filter(
        group_id in auto_valid_candidates,
        tier2_status == "invalid",
    ).group_by(group_id).agg(latest_invalid_date=max(date(review_date)))

    hu_stats = get_latest_valid_hu_stats(latest_invalid.group_ids)

    blocked = {}
    for group_id, invalid_date in latest_invalid.items():
        latest_hu_date = hu_stats[group_id].latest_hu_date

        if latest_hu_date is None:
            blocked[group_id] = f"prior_tier2_invalid({invalid_date}); no_hu_data"
        elif latest_hu_date <= invalid_date:
            blocked[group_id] = f"prior_tier2_invalid({invalid_date}); latest_hu={latest_hu_date}"

    return blocked
```

Blocked output:

```python
{
    "review_bucket": 2,
    "status": "needs_review",
    "reason": "prior_tier2_invalid(<date>); latest_hu=<date>",
}
```

or:

```python
{
    "review_bucket": 2,
    "status": "needs_review",
    "reason": "prior_tier2_invalid(<date>); no_hu_data",
}
```

## HU Stats Algorithm

This algorithm finds the latest valid hive-update date for a group. It is used by `must_review` ordering and the prior-invalid blocker.

Data:

- validation history
- daily latest hive updates

Pseudo-code:

```python
def compute_anchor_from_reviews(review_rows):
    rows = latest_review_per_group_per_day(review_rows)
    rows = rows.sort_by(review_date_desc)

    if rows[0].tier2_status != "invalid":
        return rows[0].review_date

    streak_start = rows[0].review_date
    for row in rows[1:]:
        if row.tier2_status == "invalid":
            streak_start = row.review_date
        else:
            return streak_start - days(1)

    return None  # all known reviews are invalid
```

```python
def get_latest_valid_hu_stats(group_ids):
    anchors = {}

    for group_id in group_ids:
        reviews = validation_history.for_group(group_id)
        if reviews:
            anchors[group_id] = compute_anchor_from_reviews(reviews)
        else:
            group_hu_dates = [
                row.activity_date
                for row in daily_latest_hive_update
                if row.group_id == group_id
            ]
            anchors[group_id] = max(group_hu_dates) if group_hu_dates else None

    latest_hu = {
        group_id: {"latest_hu_date": None, "sensors": None, "avg_bf": None}
        for group_id, anchor in anchors.items()
        if anchor is None
    }
    anchors = {gid: anchor for gid, anchor in anchors.items() if anchor is not None}

    for group_id, anchor in anchors.items():
        candidate_dates = [
            row.activity_date
            for row in daily_latest_hive_update
            if row.group_id == group_id
            and row.activity_date <= anchor
        ]
        if not candidate_dates:
            latest_hu[group_id] = {"latest_hu_date": None, "sensors": None, "avg_bf": None}
            continue

        hu_date = max(candidate_dates)

        rows = daily_latest_hive_update.filter(group_id == group_id, activity_date == hu_date)
        latest_hu[group_id] = {
            "latest_hu_date": hu_date,
            "sensors": count_distinct(rows.hive_id),
            "avg_bf": round(avg(rows.number_of_bee_frames), 1),
        }

    return latest_hu
```

Groups with no daily latest hive update rows, or no hive update rows at or before their anchor date, must remain present in the returned mapping with `latest_hu_date = None`. The prior-invalid blocker depends on that `None` value.

## Final Row Construction

Build rows in this order.

```python
rows = []

for group_id in sort_by_latest_valid_hu_date_ascending(must_review):
    rows.append({
        "group_id": group_id,
        "group_name": group_name[group_id],
        "review_bucket": 1,
        "status": "needs_review",
        "reason": f"tier2_invalid; latest_hu={latest_hu[group_id]}; stale={days_since_deployment[group_id]}d",
    })

for group_id in sort_by_clip_score_desc(needs_review):
    rows.append({
        "group_id": group_id,
        "group_name": group_name[group_id],
        "review_bucket": 2,
        "status": "needs_review",
        "reason": "; ".join(needs_review_reasons[group_id]),
    })

for group_id in auto_valid:
    reason = (
        "auto_valid; clipping-only but historically valid at similar levels"
        if group_id in historical_safe
        else "auto_valid; no flags"
    )
    rows.append({
        "group_id": group_id,
        "group_name": group_name[group_id],
        "review_bucket": 3,
        "status": "valid",
        "reason": reason,
    })
```

## Signal Health

Track whether each signal ran cleanly:

- `clipping`
- `inspection`
- `thermoreg`
- `auto_review`

Normative policy: `auto_valid` rows are actionable only when `SIGNAL_HEALTH` is `ALL_OK`. If any expected signal is `ERROR`, `TIMEOUT`, or `SKIPPED`, the implementation may still emit the computed rows for diagnosis, but `auto_valid` rows are advisory and must not be applied as automatic approvals.

Expected format:

```text
SIGNAL_HEALTH: ALL_OK | clipping=OK(<count>) | inspection=OK(<count>) | thermoreg=OK(<count>) | auto_review=OK(<count>)
```

or:

```text
SIGNAL_HEALTH: DEGRADED | clipping=OK(<count>) | inspection=TIMEOUT(timeout 300s) | thermoreg=OK(<count>) | auto_review=OK(<count>)
```

## Reconstruction Checklist

A target implementation is equivalent when:

- Candidate discovery produces stale production groups in daily mode.
- `must_review` is exactly "invalid yesterday".
- `needs_review` includes clipping, inspection, thermoregulation, auto-review invalid, no-data blockers, and unsuperseded-prior-invalid blockers.
- Historical clipping exemption applies only to clipping-only groups.
- `auto_valid` contains only groups with same-day UBF data, no active reasons, and no unsuperseded prior invalid.
- Output rows preserve `review_bucket`, `status`, and `reason` values as specified above.
- Signal health is emitted with enough detail to know whether auto-valid was based on complete evidence.
