# LTF-020: Empty model output fails during flag assignment

Status: Draft — deferred from latest-only recovery maintenance patch.
Date: 2026-11-18

A live latest-only recovery attempt reported `SM_GBT_LR: ValueError: cannot set a frame with no defined index and a scalar`. Logs show static-metadata exclusions followed by no prediction data for all GBT variants. No traceback was captured.

Read-only source tracing points to `apply_success_flag()` in `apps/long_term_forecasting/lt_recovery.py`: assigning a scalar through `.loc` when adding the flag column to an empty forecast frame reproduces the same pandas exception. This existing helper is unchanged by the recovery patch. Attribution remains a strong inference, not stack-trace proof.

## Minimal follow-up

1. Capture a traceback or reproduce the actual empty-model-output call through `run_single_model` with synthetic data.
2. If confirmed, define empty output as no usable forecast without writing rows or reporting successful recovery; preserve nonempty flag behavior.
3. Add empty/nonempty regressions and run the complete LT scope with `run_tests.sh`, followed by independent review and required pre-commit validation.

Do not fill missing discharge, invent static metadata, relax feature thresholds, change service contracts, or expand automatic historical recovery. Current latest-only recovery already reports this model failure and continues independent work and postprocessing, so it does not block the scoped recovery patch.
