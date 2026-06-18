 ROLE: You are a senior software architect producing an implementation PLAN (no code) for the
  SAPPHIRE_forecast_tools repository. Follow the repo's CLAUDE.md orchestration plan format exactly: phases with
  Goal / Files / Depends-on / Agents / Acceptance-criteria, ending with a JSON dependency graph. Do read-only
  exploration as needed to fill the gaps flagged below, but DO NOT write or modify any code.

  OBJECTIVE
  Build a read-only, DB-backed analysis that evaluates forecast skill as the irrigation-management binary decision
  it actually drives: "issue the limit plan" vs "run the normal plan." For every forecast horizon, every model,
  every station, over the entire DB archive, quantify how often each model's forecast would have led to the WRONG
  irrigation plan (false alarms and misses) and how often it was right.

  THE DECISION RULE (locked)
  - A "limit plan" is issued when forecast runoff < THRESHOLD x norm. THRESHOLD is an EDITABLE parameter, DEFAULT
  0.80 (i.e. 80% of norm). Keep it a single configurable scalar; design so an optional upper threshold could be
  added later, but default behavior is the single lower cut.
  - Positive class = "limit plan" event (value < THRESHOLD x norm).
  - Ground truth = OBSERVED runoff < THRESHOLD x norm (was the limit plan actually warranted).
  - Classify BOTH the forecast value and the observed value against the SAME boundary (THRESHOLD x norm) and build a
  2x2 contingency table:
      TP = forecast below & observed below   (correctly restricted)
      FP = forecast below & observed not-below (FALSE ALARM: needless restriction)
      FN = forecast not-below & observed below (MISS: over-allocation, runs short)  <-- operationally the costliest
  error
      TN = forecast not-below & observed not-below (correctly normal)
  - Round 1 is DETERMINISTIC: use the point forecast (forecasted_discharge for short-term; the median/q50/q column
  for long-term). A probabilistic version may follow later — design so it is not precluded, but do not build it now.

  DATA SOURCES (grounded against the actual schema — all reads via the API gateway at http://localhost:8000, NO CSV)
  - NORM (with PROVENANCE -- see dedicated section below): preprocessing-db table `hydrographs`, column `norm`, keyed
  by (horizon_type, code, period-index). The table physically contains norms for all horizons, but their PROVENANCE
  differs and is NOT directly trustworthy as "official" across the board (see NORM RESOLUTION & PROVENANCE). Also
  read `count` (n historical obs behind the stored stat) for a sufficiency check. horizon_type enum: day, pentad,
  decade, month, quarter, season. Read via SapphirePreprocessingClient.read_hydrograph(horizon=...).
  - OBSERVED RUNOFF (truth): preprocessing-db table `runoffs`, column `discharge`, pre-aggregated per (horizon_type,
  code, date) for ALL horizons incl. day. Read via SapphirePreprocessingClient.read_runoff(horizon=...).
  - FORECASTS:
      * Short-term (day/pentad/decade): postprocessing-db table `forecasts`. `date` = ISSUE date (last day of prior
  period), `target` = TARGET period start (= date + 1 day). Point value = `forecasted_discharge`. Unique key
  (horizon_type, code, model_type, date, target) => effectively ONE issue per target (fixed ~1-day lead) => NO
  lead-time variation for short-term.
      * Long-term (month/quarter/season): postprocessing-db table `long_forecasts`. `date` = ISSUE date,
  `valid_from`/`valid_to` = TARGET period, `horizon_value` = lead time in MONTHS. Same target period can carry
  multiple leads (horizon_value = 1,2,3,...). Point value = median/q50 (`q50` or `q` — verify which is populated).
  Read via SapphirePostprocessingClient (verify exact method names for forecasts and long_forecasts).
  - MODEL ENUM (model_type), exact values present in schema: TSMixer, TiDE, TFT, EM, NE, RRAM, GBT, LR_Base, LR_SM,
  LR_SM_DT, LR_SM_ROF, MC_ALD, SM_GBT, SM_GBT_LR, SM_GBT_Norm, LR, "Skilled Mean", "Naive Mean". DO NOT assume which
  models exist at which horizon — ENUMERATE the distinct (model_type, horizon_type) pairs actually present in the
  data and drive the analysis off that. (Note: config references "GBT_Base" which has no enum value — a known gap;
  just report whatever model_type values actually appear.)

  NORM RESOLUTION & PROVENANCE (critical)
  Hydromet (iEH HF) only PROVIDES decadal and monthly norms. Quarterly/seasonal norms are AGGREGATED from monthly.
  Day/pentad and any gaps are CALCULATED. Resolve the norm for each (horizon_type, code, period) in this priority
  order and TAG every resulting pair with `norm_provenance`:

    1. FIRST verify (read-only) whether `hydrographs` has an explicit provenance/source column.
       If it does, use it. If it does NOT, apply this configurable per-horizon convention:
          decade, month   -> provenance = "official"
          quarter, season -> provenance = "aggregated_from_monthly"
          pentad, day     -> provenance = "calculated"
       Make this mapping an explicit, documented, overridable parameter -- do not bury it.
    2. Use the stored `hydrographs.norm` when present, non-null, and > 0. For provenance=="calculated" stored norms,
       optionally require `hydrographs.count` >= (10 years' worth of that period); if below, treat as if missing (go
       to step 3) and note it.
    3. If no usable stored norm: CALCULATE from observed `runoffs` history for that (horizon_type, code, period),
       requiring >= 10 DISTINCT years. Tag provenance = "calculated". To avoid leakage, compute this fallback norm
       LEAVE-ONE-OUT: exclude the year being scored from the mean used to threshold that year. (Leave-one-out applies
       ONLY to this calculated fallback; official/aggregated norms are external and used as-is.)
    4. If < 10 distinct years available and no usable stored norm: EXCLUDE the pair, count it in the ledger under
       "norm_unavailable".

  Provenance is a first-class output dimension: all contingency tables and skill metrics must be reportable BOTH
  pooled AND broken down by norm_provenance, so conclusions resting on calculated (non-official) norms can be
  isolated.

  JOIN LOGIC (per horizon_type, code, model_type, target period [, lead for long-term])
  1. forecast value <- forecasts/long_forecasts point value for that key.
  2. observed value <- runoffs.discharge for the SAME (horizon_type, code, target period).
  3. norm + norm_provenance <- resolve per NORM RESOLUTION & PROVENANCE for the SAME (horizon_type, code,
  period-index).
  4. Classify forecast and observed vs THRESHOLD x norm -> contingency cell.
  RESOLVE EMPIRICALLY (flag in plan): the correct norm JOIN KEY. `hydrographs` is keyed by (horizon_type, code,
  date) but the norm is climatological (same for, e.g., pentad-1 every year). Determine whether to join on the
  within-year PERIOD INDEX (`horizon_in_year` / `horizon_value` / `day_of_year`) rather than calendar `date`, and
  whether there is one norm row per period-index or one per (period, year). The plan MUST specify the verified join
  key.

  METRICS (per (horizon_type, model_type, code), plus pooled across stations per (horizon_type, model_type))
  - Raw counts TP/FP/FN/TN and n_pairs.
  - Base rate of the limit-plan event (per station — so the right-skew effect of "80% of a mean norm" is visible).
  - POD (=TP/(TP+FN), the headline), FAR (=FP/(FP+TN) or FP/(TP+FP) — specify which and why), CSI, frequency bias.
  - Heidke Skill Score (HSS) and Peirce Skill Score (PSS) computed AGAINST the climatology baseline.
  - Wilson 95% confidence intervals on POD/FAR (cells will be thin for quarter/season).
  - Emphasize FN (miss) reporting since it is the costliest operational error.
  - Every per-(horizon,model,code) and pooled result row carries `norm_provenance`. Produce the headline metrics
  overall AND stratified by norm_provenance {official, aggregated_from_monthly, calculated}. The exclusion ledger
  must separately count "norm_unavailable (<10yr)".
  BASELINES:
  - Climatology = "always run the normal plan" (never trigger limit) — the do-nothing default every model must beat.
  - LR (short-term) and LR_Base (long-term) as a PROXY for the hydromet's current operational base (note in output
  that LR_Base is a proxy, not their actual method).
  LEAD-TIME STRATIFICATION: for long-term only, stratify metrics by `horizon_value` (lead months). First verify the
  data actually contains multiple leads per target; if not, report single-lead and say so.

  EDGE CASES the plan must explicitly handle (each -> exclude-and-COUNT, never silently drop):
  - norm IS NULL  -> exclude, count.
  - norm == 0     -> boundary degenerates to 0, classification meaningless -> exclude, count.
  - norm provenance == "calculated" with a short or leakage-prone record -> use leave-one-out, require >= 10 distinct
  years, else exclude+count.
  - stored "calculated" norm with low `count` -> treat as missing, recompute or exclude; note it.
  - observed IS NULL / missing period -> exclude, count.
  - forecast missing -> exclude, count.
  - tie: value exactly == THRESHOLD x norm -> define convention (strictly `<` => below; `==` => normal) and document
  it.
  - non-finite floats -> exclude, count.
  - "decade" (API enum) vs "decad" (internal) naming mismatch -> normalize.
  - DAY horizon: norms are calculated and forecast archive is only ~1-2 years => label DAY results as
  exploratory/thin and isolate via `norm_provenance`.
  - Report a full exclusion ledger so coverage is never overstated.

  DELIVERABLE FORM (you propose the concrete structure, within these constraints):
  - A NEW analysis module under apps/ (e.g. apps/forecast_skill_eval/ or inside apps/validate_pipeline/). It must
  NOT modify anything under sapphire/services/ (read schema/clients only).
  - Reads exclusively via the sapphire API clients (with the SAPPHIRE_API_AVAILABLE dependency-gate pattern already
  used in the repo).
  - Outputs tidy result tables (per-station and pooled) as DataFrames persisted to a parquet/CSV artifact directory,
  plus a concise text/markdown summary. Plots optional/phase-gated.
  - THRESHOLD and horizon/model/station/date-range are parameters captured ONCE at the entry point (honor the repo
  "forecast date is a parameter, not date.today()" rule).

  REPO CONVENTIONS YOU MUST ENCODE IN THE PLAN:
  - Python: line length 100 (79 docstrings), Google-style docstrings, type hints, ruff clean (E/F/I/UP/B/SIM).
  - TDD: every unit of logic gets tests. Required test coverage: threshold-boundary classification (incl. ==,
  norm=0, NULLs), contingency-count correctness, POD/FAR/CSI/HSS/PSS against hand-computed fixtures, Wilson CI, the
  forecast<->observed<->norm join against a FAKE in-memory client, lead-time stratification, and the exclusion
  ledger. Use fakes over mocks; reserve mocks for the API client boundary.
  - Tests run via `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`; ZERO failures, ZERO skips except the
  allowed `sapphire-api-client not installed` gate.
  - SENSITIVE DATA: never put real station codes or discharge values in the plan or tests — use placeholder code
  "19999" and synthetic discharge.
  - Implementation will be done by Sonnet agents; plan changes as additive; do NOT change existing function
  signatures or data flow in shared modules.

  STRUCTURE THE PLAN INTO PHASES with explicit dependencies. Suggested shape (refine as you see fit):
    P1 Data-access layer: thin read wrappers (forecasts, long_forecasts, hydrograph norms/provenance, runoff
  observed) + the verified norm join key + a fake client for tests.
    P2 Classifier + contingency engine: THRESHOLD x norm classification, 2x2 counts, exclusion ledger, edge-case
  handling.
    P3 Skill metrics: POD/FAR/CSI/bias/HSS/PSS + Wilson CIs + climatology & LR/LR_Base baselines + provenance
  stratification.
    P4 Orchestration over (horizon x model x station), pooled + per-station aggregation, lead-time stratification
  for long-term, artifact + summary output.
    P5 End-to-end validation on the live DB for ONE horizon (pentad) before fanning out to all six, plus full test
  pass.
  End with the JSON dependency graph (phases, depends_on, parallel_agents).

  OPEN ITEMS YOU MUST RESOLVE during planning (read-only exploration) and state answers for in the plan:
  1. The exact norm join key (period-index vs date; one row per period-index or per period-year).
  2. The exact SapphirePostprocessingClient method names + signatures for reading `forecasts` and `long_forecasts`,
  and which column carries the long-term deterministic point value (q50 vs q).
  3. Whether `long_forecasts` actually contains multiple leads (horizon_value>1) per target in the data, and which
  models cover quarter/season (enumerate present (model_type,horizon_type) pairs).
  4. The artifact output location/format consistent with repo conventions.
  5. Does `hydrographs` carry an explicit provenance/source column? If yes, use it instead of the per-horizon
  convention. If no, confirm the convention mapping (decade/month=official, quarter/season=aggregated,
  pentad/day=calculated) and surface it as a documented parameter.
  6. Confirm the aggregation method for quarter/season norms (mean of monthly norms, days-in-month weighted, or
  volume-summed) so calculated/aggregated fallbacks match how the stored ones were built.

  OUTPUT: the full phased plan in the CLAUDE.md format, ready for me to review.
