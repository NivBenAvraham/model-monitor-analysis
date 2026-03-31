# V2 Kickoff Preparation

**Date:** 2026-03-09
**Status:** Complete
**Author:** Tamir

## Summary

Comprehensive preparation for Bee Frame Models V2 kickoff meeting. Analyzed 17 Confluence documents (14 text + 3 whiteboards), conducted structured interview to clarify scope and constraints, and produced presentation materials.

## Key Findings

### Performance Baseline
- Best auto-calibration: 69.3% combined pass rate (moderate tier, P2/SY NN)
- Strict: 37.7%, Moderate: 69.3%, Loose: 85.5%
- Feature set (not model architecture) is the bottleneck

### Technical Architecture
- CF(v) = clip(slope * v + bias, 0, saturation) — 3-parameter lossless
- (slope, y_bar) reparameterization reduces coupling from ρ=-0.96 to ~0
- V1 ranker stays unchanged; V2 automates only the calibration step

### Decisions Made
- P3 (direct BF prediction): **out of scope** for V2
- Feasibility thresholds: **data team owns** (not waiting on Product)
- Team dedication: 2-3 people, 100% on V2
- June 2026: hard deadline for calibration + feasibility SBS

### Unresolved (for kickoff)
- KPI target: ±1 BF vs ±2 BF
- Constraint tier: strict / moderate / loose
- SBS infrastructure readiness

## Documents Analyzed

1. Automated Bee Frame Estimation and Calibration Pipeline
2. Automated Calibration - Function Fitting
3. Automatic Calibration - Feasibility Assessment
4. KPIs - Evaluation and Learning Calibration Functions
5. Function Fitting Model Investigation - Summary
6. Bee Frames Foundation
7. Monitoring V2 - High Level Design
8. Production Data Pipeline Overview
9. Bee Frame Accuracy KPIs
10. Beekeeper Frame Forecasting: Inventory Dynamics
11. Model deployment, calibration, etc.
12. Pre Process (Model Monitoring Preprocess Tables)
13. Temperature Data Export Package
14. Monitoring Streamlit manual
15-17. Three whiteboards (visual only)

## Artifacts Produced

- `kickoff-prep-output/raw-documents.md` — Full text of all documents
- `kickoff-prep-output/document-summaries.md` — Structured summaries
- `kickoff-prep-output/analysis.md` — 7 themes, 25 questions, 11 risks, 10 assumptions
- `kickoff-prep-output/kickoff-plan.md` — 10-section plan with goals/objectives
- `kickoff-prep-output/kickoff-summary.md` — Executive summary
- `kickoff-prep-output/BeeFrame_V2_Kickoff.pptx` — 28-slide technical deck
- `kickoff-prep-output/BeeFrame_V2_Approach.pptx` — 4-slide approach section
- `kickoff-prep-output/BeeFrame_V2_Timeline.pptx` — 2-slide timeline
- Final Google Slides: "Beeframe Remodeling Kickoff - March 2026" (20 slides)
