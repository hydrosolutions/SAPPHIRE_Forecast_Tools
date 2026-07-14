# Bulletin `forecast_year` derived from issue date, not target period (quarter never rolls; flag-OFF path rolls wrong)

**Priority:** mid — no crash; on the reachable path (flag-OFF, Dec-31 lead-0) the bulletin
is written with the wrong year in the persisted record, which is a real data-quality bug on
every deployment (flag defaults OFF). The quarter defect is currently latent (see below),
which is reflected in this priority rather than "high".
**Module:** `apps/forecast_dashboard` (fair game).
**Found:** 2026-07-14, adversarial review of `develop_ltf_monthly_horizon_value`. Both bugs
are **pre-existing on trunk** — neither was introduced by that branch, and the branch
deliberately did not fix either because it is held to a hard "flag-OFF behaviour must be
byte-identical to trunk" contract (`SAPPHIRE_SKILL_LEAD_AWARE`, default OFF).
**Depends on:** none to file. A future fix must decide, and state explicitly, whether it
changes flag-OFF behaviour (see Acceptance criteria).

## The principle these two bugs both violate

`get_bulletin_metadata` returns `(last_date, forecast_horizon, forecast_year)`, and
`forecast_year` is a **published bulletin field** (consumed by `bulletin_publish.py` to
compute the bulletin's target period, and persisted to the bulletin record). Its value must
come from the **target period** the forecast is *for* — derived from `valid_from` / the
resolved lead — never from the issue date, and never from `issue date + 1 day`.

The branch's own monthly flag-ON path already gets this right and is the reference
implementation (`apps/forecast_dashboard/dashboard/data_manager.py:368-371`):

```python
if skill_lead_aware_enabled() and horizon == "month":
    forecast_year = (
        max_date.year + 1 if forecast_horizon < max_date.month else max_date.year
    )
```

This rolls the year forward exactly when the target month (`forecast_horizon`, i.e.
`month_in_year`) numerically precedes the issue month (`max_date.month`) — a real
lead/target-period test. Both bugs below use a different, non-target-derived year instead.

## Bug 1 — quarter bulletins never roll the year

`get_bulletin_metadata`'s quarter branch (`data_manager.py:346-352`) derives
`forecast_horizon` (the quarter number, 1-4) from `month_in_year`, correctly reading the
**target** quarter's start month:

```python
elif horizon == "quarter":
    start_month = int(self.forecasts_all["month_in_year"].tail(1).values[0])
    forecast_horizon = ((start_month - 1) // 3) + 1
```

But the year-rollover block right below it (`data_manager.py:367-371`) is gated on
`horizon == "month"` only — `forecast_year` for a quarter forecast falls through to
`last_date.year` (the issue year) unconditionally, with no lead/target check at all.

A Kyrgyz-style quarter forecast issued `2026-12-25` with `month_in_year=1` (i.e.
targeting Q1 = Jan-Mar **2027**) would get `forecast_horizon=1`, `forecast_year=2026`
(unrolled). Downstream, `bulletin_publish.py::_period_start_date` (`:142-144`) maps
`(horizon="quarter", forecast_horizon=1, forecast_year=2026)` to `date(2026, 1, 1)` —
January **2026**, a quarter that already happened, not the Q1 2027 the forecast is
actually for.

### Is this reachable today? — **Latent, not live**

I checked every call site of `get_bulletin_metadata` and of the dashboard's horizon
selector and found `"quarter"` is never passed through the interactive UI:

- `apps/forecast_dashboard/dashboard/widgets.py:97-103` (`create_horizon_selector`) only
  ever offers `pentad`, `decade`, `month`, `season` as options — `"quarter"` is not a
  selectable value, gated or otherwise.
- Every live call site passes either `wm.horizon_selector.value` (which per the above can
  never be `"quarter"`) or a hardcoded `"month"`:
  `widget_manager.py:66`, `widget_manager.py:246`, `bulletin_manager.py:335`
  (`_on_horizon_change`), `bulletin_manager.py:403` (`_month_hydration_params`, hardcoded
  `"month"`), `bulletin_manager.py:686` (`_on_write`).
- `bulletin_publish.py`'s own docstring says as much:
  `assemble_bulletin_snapshot`'s `horizon` param is documented as "One of \"pentad\",
  \"decade\", \"month\", \"season\" (or \"quarter\", supported for completeness though not
  exposed by the horizon multiselect)" (`bulletin_publish.py:314-315`).

So the quarter branch in `get_bulletin_metadata` and its consumer in `_period_start_date`
are dead code on every current deployment path — reachable only from direct unit-test
calls, not from anything a user can trigger. This sets the priority: real defect, zero
current user-facing impact. If a quarter option is ever added to the horizon selector (see
FD-011, which already flags the selector as fragile), this bug becomes live immediately —
whoever exposes quarter in the UI must fix this first or inherit a wrong-year bulletin on
day one.

## Bug 2 — the flag-OFF (legacy) path derives the year from `issue date + 1 day`, not the target period

On the flag-OFF (default) path, `get_bulletin_metadata` returns `forecast_year =
last_date.year`, where `last_date = max_date + timedelta(days=1)` (`data_manager.py:340,
367`). `last_date` is a save-time bookkeeping artifact (the day after the issue date), not
a target-period value.

This is invisible for most of the year, because `+1 day` only crosses a year boundary when
the issue date is December 31. On that one day, it fires regardless of what the forecast
actually targets. Concretely, for a **lead-0** December forecast (target month = December,
the same month as the issue date — no rollover should happen):

- `max_date = 2026-12-31`, `month_in_year = 12` (target = December, same year).
- `last_date = 2027-01-01` → `last_date.year = 2027`.
- Flag-OFF returns `forecast_year = 2027` — wrong; the target period is December **2026**.
- Flag-ON (using `max_date.year`, not `last_date.year`) correctly returns `2026`, because
  `forecast_horizon (12) < max_date.month (12)` is false, so no roll.

This path **is** live — flag-OFF is the default (`SAPPHIRE_SKILL_LEAD_AWARE` defaults off)
on every deployment today, and any Dec-31-issued lead-0 monthly forecast hits it.

This exact quirk is already pinned by a characterisation test on
`develop_ltf_monthly_horizon_value`, added specifically so a future fix cannot silently
change flag-OFF behaviour without the test author noticing:
`test_dec31_lead0_flag_off_rolls_year_anyway_known_quirk` in
`apps/forecast_dashboard/tests/test_monthly_lead_golden.py`
(class `TestLastDateYearVsMaxDateYearAsymmetry`). Its sibling
`test_dec31_lead0_flag_on_does_not_roll_year` in the same class documents the correct
(flag-ON) answer for the identical fixture, so the two tests together are the executable
spec of "these two paths currently disagree, and here is how."

## Scope across horizons

| Horizon | Year rollover implemented? | Status |
|---|---|---|
| month, flag ON | Yes — `data_manager.py:368-371`, uses `max_date.year` vs. target month | **Correct** (reference implementation) |
| month, flag OFF | No — uses `last_date.year` | **Broken** (Bug 2), live by default |
| quarter (either flag) | No — falls through to `last_date.year` unconditionally | **Broken** (Bug 1), currently latent (see above) |
| season (either flag) | No — `forecast_horizon` is always `1` (`data_manager.py:342-345`), year is `last_date.year` | **Not exercised today, but not principled either** — see note below |

**Season note:** I checked whether season forecasts can be issued late enough in a year to
need a rollover. Per `apps/long_term_forecasting/readme.md:194-196` and the seasonal mode
names in the repo (`seasonal_january`, `seasonal_february`, `seasonal_march`,
`seasonal_april`), a season forecast is only ever issued January-April and always targets
April-September of that **same** calendar year — so `last_date.year` happens to be correct
under every currently-configured seasonal mode. This is a domain invariant of the current
config, not something `get_bulletin_metadata` enforces or derives from the target period.
If a seasonal mode issued outside Jan-Apr (e.g. a hypothetical `seasonal_december`
targeting next year's April-September) is ever added, `get_bulletin_metadata`'s season
branch would silently reproduce Bug 1's failure mode with no test to catch it. Flag this to
whoever picks up the fix; it does not need a code change today, but the season branch
should not be assumed safe by construction.

## Any real fix must

- Derive `forecast_year` for **every** horizon from the target period (the resolved
  `month_in_year`/quarter-start-month/lead vs. `max_date`), the same style of check the
  month/flag-ON branch already uses — not from `last_date` and not from the issue date.
- For quarter: gate the same target-vs-issue-month rollover test on the quarter's
  `start_month` the same way month does on `month_in_year`.
- For the flag-OFF/legacy path: this is the harder call. Fixing it necessarily changes
  flag-OFF output for the Dec-31 lead-0 case (`2027` → `2026`), which is a deliberate
  behaviour change, not a byte-identical-to-trunk patch. **Explicitly decide and record**
  whether this fix ships as a flag-OFF behaviour change (update
  `test_dec31_lead0_flag_off_rolls_year_anyway_known_quirk`'s assertions and rename/re-doc
  it as no longer a quirk) or stays deferred until `SAPPHIRE_SKILL_LEAD_AWARE` is the only
  code path (flag removed). Do not silently delete or weaken the test to make it pass —
  either it keeps pinning the old (wrong) value with an explicit "still not fixed" note, or
  it is updated in the same commit that changes the behaviour it pins.
- For season: either add an explicit rollover check mirroring month's (defensive, matches
  the "derive from target period" principle even though no current config needs it), or
  explicitly document in code why `last_date.year` is safe given the current
  Jan-Apr-issue-only invariant, with a pointer back to this issue so a future new seasonal
  mode doesn't reintroduce Bug 1 silently.

## Acceptance criteria

- Quarter: a quarter forecast issued in Q4 with a target quarter in the following year
  (e.g. issued `2026-12-25`, `month_in_year=1`) returns `forecast_year` for the target year
  (`2027`), and `bulletin_publish._period_start_date("quarter", ...)` resolves to the
  correct target month/year — verified via a direct unit test on `get_bulletin_metadata`
  (the UI path remains unreachable per Bug 1's finding above, so this cannot be an
  end-to-end/UI test).
- Flag-OFF Dec-31 lead-0 case: whatever the decision above, `test_monthly_lead_golden.py`'s
  `test_dec31_lead0_flag_off_rolls_year_anyway_known_quirk` and
  `test_dec31_lead0_flag_on_does_not_roll_year` are both still present and both still pass
  — either unchanged (deferred decision) or updated in the same commit with a clear
  before/after note in the test docstring (fixed decision). A PR that makes one of these
  tests silently start failing, or deletes it instead of updating it, has not met this
  criterion.
- No change to flag-ON month behaviour (already correct; treat
  `test_dec31_lead0_flag_on_does_not_roll_year` and
  `test_forecast_year_is_issue_year_for_dec_issued_lead1` as regression guards).
- No real station codes/discharge in code, tests, or fixtures (`19999` placeholder if
  needed).
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh forecast_dashboard` passes, zero unexpected
  skips.

## Notes

Both bugs live in the same function and share the same root principle violation (year from
issue-time arithmetic instead of target period), which is why they are filed together
rather than as two issues — a fix should address the principle once, per horizon, rather
than patching each call site independently. Sibling to FD-016 (norm N/A labeling, also a
month/quarter/season bulletin presentation defect) and FD-018 (m0 per-site target month) —
all three surfaced from the same `develop_ltf_monthly_horizon_value` review pass but are
independent mechanisms; do not conflate.
