# Note: how should quarter/season `horizon_value` be assigned for long forecasts?

**For**: postprocessing-service owner + long-term modeller
**From**: forecast-tools side (surfaced during the MIG-007 from-file long-forecast backfill)
**Tracking**: issue MIG-008
**TL;DR**: The from-file backfill writes `horizon_value=0` for all quarter/season rows. We confirmed
this is a **convention question**, not a code bug -- the service stores hv but defines no meaning,
and all three writers stamp `operational_month_lead_time`. We need you to decide how quarter/season
forecasts should be bucketed into hv before we backfill anything beyond `hv0`.

## What we found

1. **The service is hv-agnostic.** `LongForecast.horizon_value` is a plain `Integer` in the natural
   key; `schemas.py` has no validator/range for it; `data_migrator.py` stamps the value it is
   handed. The API accepts any int and imposes no quarter/season semantic.
2. **All three writers are identical** -- each sets `horizon_value = config["operational_month_lead_time"]`,
   a hand-set constant per config; none derives hv from a date:
   - service migrator `sapphire/services/postprocessing/app/data_migrator.py:669,769`
   - operational pipeline `apps/long_term_forecasting/run_forecast.py:269,409` (`config_forecast.py:231`)
   - from-file importer `bin/utils/migration_py/long_forecast.py:251,272`
3. **The convention is "one config per hv bucket."** Months do this cleanly (`month_1/2/3` ->
   `MONTH hv0/1/2`). Quarter and season ship a **single** config each (`quarter.json`,
   `seasonal_april.json`, both lead 0), so they can only ever write `hv0`.

## Why this is awkward for quarter/season

The single hindcast CSVs contain many issue points that the single config collapses to one hv:

- **quarter** = a 3-month forecast **issued monthly Mar-Sep** (7 windows/yr; `valid_from->valid_to`
  = 03->05, 04->06, ... 09->11). Not 4 calendar quarters.
- **seasonal_april** = the Apr-Sep (6-month) forecast **issued in April only**. (No
  Jan/Feb/Mar seasonal configs or CSVs exist in this repo, so those issues can't be backfilled here.)

And the **existing operational `QUARTER hv1..4` is internally inconsistent** (hv1 mixes issue
months Jan + Mar-Sep; hv2 mostly April; hv3 mostly July; hv4 October), which suggests it was
migrated from a config set that has since changed.

## The questions we need answered

1. **Quarter** -- what should `horizon_value` encode, given the source is a rolling monthly 3-month
   product (7 windows), not 4 calendar quarters?
   - (a) target calendar quarter (Mar=1, Apr/May/Jun=2, Jul/Aug/Sep=3),
   - (b) the issue-month sequence (7 distinct values), or
   - (c) something else you intend operationally.
2. **Season** -- confirm the issue-date convention. The DB currently looks like
   `hv = April(4) - issue_month` (April=0, March=1, Feb=2, Jan=3). Is that the intended scheme?
3. **Provenance** -- where did the existing operational `QUARTER hv1..4` / `SEASON hv0..3` rows come
   from (which config set / migration), and should they be **re-derived/cleaned** to whatever
   convention you choose, or left as-is?

## How we'd implement it (your decision drives which)

- **Preferred / consistent: config-per-bucket.** You/modeller produce per-bucket configs + hindcast
  CSVs (e.g. `seasonal_jan/feb/mar/apr` with lead 3/2/1/0; quarter configs per bucket), each with
  the right `operational_month_lead_time`. **No migrator code changes** -- the existing loop-and-stamp
  logic just works. This keeps the importer, the operational pipeline, and your service migrator
  identical.
- **Alternative: date-derived hv.** We add per-row derivation in the importer -- but that **diverges**
  from your service migrator and the operational pipeline unless all three change (including service
  code). We do **not** recommend this and would not touch service code without you.

## What we did locally meanwhile

- Applied the `seasonal_april` backfill into `SEASON hv0` (correct for the April issue): additive,
  62 -> 79 stations, no overwrite of `hv1..3`.
- **Held** the quarter write -- it would land in an orphan `QUARTER hv0` disjoint from operational
  `hv1..4`.

Once you confirm the convention (esp. question 1), we'll proceed accordingly. No code changes are
pending on our side until then.
