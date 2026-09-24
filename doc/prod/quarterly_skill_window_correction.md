# Quarterly skill window correction

Quarterly skill rows currently identify Q1–Q4 and a lead in months. They do
not identify all twelve possible rolling three-month start months. This
correction therefore evaluates **calendar quarters only**: Jan–Mar,
Apr–Jun, Jul–Sep and Oct–Dec.

For example, Q2 / lead 1 evaluates forecasts issued in March for Apr–Jun.
A May-issued Jun–Aug forecast is not a Q2 forecast and cannot contribute to
that score. Direct rolling forecasts remain available in the forecast API
and dashboard, but do not receive calendar-quarter skill scores. Supporting
skills for those rolling windows requires a separate storage/UI contract.

## Corrected processing

- Reject non-calendar-quarter target windows before issuance selection,
  deduplication, ensemble creation and scoring. Preserve actual issue dates.
- Aggregate monthly forecasts only within one station, model and issuance.
  Require all three distinct full months of a calendar quarter. Derive the
  quarterly lead from issue month to the first target month. For a March
  issuance, April/May/June monthly leads 1/2/3 become quarterly lead 1.
- Weight monthly discharge means by calendar days, for both forecasts and
  observations. Missing months and duplicate copies do not satisfy coverage.
- Do not infer quarterly quantiles from monthly marginal quantiles. Derived
  forecasts carry deterministic discharge; quarterly CRPS and uncertainty
  metrics are unavailable for these rows without a temporal dependence model.
- Use only direct quarterly forecasts for LR_Base and LR_SM, even when a
  direct prediction is missing. Never derive monthly fallback copies for them.
  For other supported models, prefer direct over derived forecasts for the same
  station, target quarter, model and lead. Keep leads separate in skill and
  ensemble calculations and writes, including when the issuance-selection
  feature flag is disabled.
- With `SAPPHIRE_SKILL_LEAD_AWARE=true`, use configured operational issue
  days and monthly leads before aggregation. With the flag disabled, retain
  the configured single quarterly lead and choose its latest complete
  issuance. This legacy mode does not enforce the configured issue day.

Quarterly raw models include LR_Base, LR_SM, GBT, LR_SM_DT, LR_SM_ROF,
MC_ALD, SM_GBT, SM_GBT_LR and SM_GBT_Norm. Each additional model requires
three valid monthly predictions from the same issuance to derive a quarter.
Historical and latest readers retain these products, and their deterministic
skill and forecasts use the existing API model identifiers. The Naive Mean
includes available raw models; Skilled Mean applies its existing skill gates.
Quarterly EM follows monthly EM: the equal-weight mean of all raw models
passing the standard configured skill thresholds for that station, quarter
and lead, requiring at least two qualifying models. It uses the quarterly
minimum-pair setting (default 5), in both recalculation and operational forecasts.
LR models contribute at most once, through their direct quarterly products.
Seasonal eligibility and its fixed LR_Base/LR_SM EM are unchanged.

With monthly leads 0–3 available, only quarterly leads 0 or 1 can potentially
be constructed. Lead 0 also requires the three monthly predictions to actually
share an issue date; differing configured issue days can make it unavailable.
The current Q1–Q4 skill contract still excludes non-calendar rolling windows.

## Repairing existing deployments

No database writes or deployment are performed by this local correction.
After deploying the corrected producer and dashboard, rerun quarterly skill
calculation using the intended operational configuration. Existing skill
invalidation can clear obsolete skill keys when the refreshed input is
complete. Check affected station/model/quarter/lead pairs and counts against
raw same-window data before relying on the refreshed scores.

Previously written synthetic quarterly forecast rows are not deleted by
this change. The operational exact-day selector rejects old first-of-month
synthetic issue dates when the configured day is 25. Any historical forecast
cleanup must be reviewed separately; do not delete direct forecast archives.

A recalculation using day-weighted observations can differ slightly from
older diagnostic results that used an unweighted average of monthly means.
