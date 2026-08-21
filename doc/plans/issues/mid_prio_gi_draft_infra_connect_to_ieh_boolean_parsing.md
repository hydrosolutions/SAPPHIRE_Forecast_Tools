# INFRA-038: `ieasyhydroforecast_connect_to_iEH` is parsed three different ways, and a non-canonical value passes validation then silently misreads

**Status**: Draft (2026-08-21)
**Module**: `apps/iEasyHydroForecast` (`setup_library.py`, `forecast_library.py`) +
`apps/preprocessing_runoff/preprocessing_runoff.py` + `apps/linear_regression/linear_regression.py`
**Priority**: Medium — **latent, not live**, on the two kghm env files checked (both use the
canonical `False`). It is live for any deployment whose env file uses a different casing, and one
such file exists in the shared config directory today. The failure is silent and takes a wrong
data-source branch rather than erroring.
**Labels**: `infra`, `env-contract`, `silent-failure`, `cross-module`
**Found**: 2026-08-21, comparing per-user kghm env files while investigating a colleague's
"machine_learning produces no forecasts" report.

---

## Defect

One environment variable, **three incompatible parsing styles across 13 call sites**:

| Style | Sites | Behaviour |
|---|---|---|
| **Case-insensitive** | `setup_library.py:364`, `:372`, `:380` | `.lower() == "true"` / `"false"` |
| **Case-sensitive positive** | `preprocessing_runoff.py:122`, `:294`, `:414`, `:497`, `:546`; `linear_regression.py:663` | `== "True"` |
| **Case-sensitive negative** | `forecast_library.py:4753`, `:5127`, `:5658`; `setup_library.py:981` | `== "False"` |

`ieasyhydroforecast_ssh_to_iEH` has the same split: `setup_library.py:372`/`:380` are
case-insensitive, `linear_regression.py:651` is `== "True"`.

### Why the third style makes this worse than a casing nit

The `== "False"` sites are **negative** tests, so a non-canonical value satisfies **neither**
branch. With `ieasyhydroforecast_connect_to_iEH=TRUE`:

- `setup_library.py:364` — `.lower() == "true"` → **True**. Validation passes; the run is
  considered a legitimate connect-to-iEH configuration.
- `preprocessing_runoff.py:122` — `== "True"` → **False**. Takes the *not connected* branch and
  builds an `IEasyHydroHFSDK` instead of an `IEasyHydroSDK`.
- `forecast_library.py:4753` — `== "False"` → **False**. The *not connected* guard also does not
  fire.

So the value lands in an **unhandled middle state**: neither "connected" nor "explicitly not
connected", with different modules disagreeing about which. Nothing raises. `setup_library`'s
consistency check — the one function whose whole job is catching an inconsistent configuration —
is precisely the one that waves it through, because it is the only case-insensitive reader.

## Evidence

Latency confirmed by measurement rather than by reading the files:

```
# loaded via setup_library.load_environment(), .env_bea_kghm
ieasyhydroforecast_connect_to_iEH = 'False'
   == 'True'  -> False      == 'False' -> True      .lower()=='false' -> True
```

Both kghm env files checked (`.env_bea_kghm`, `.env_kghm`) use canonical `False`, and all three
styles agree on it — hence latent. `python-dotenv` strips surrounding whitespace at load, so
trailing spaces in the file are **not** an additional trigger (checked, because the raw file
suggested otherwise).

**A third per-user file in the same shared config directory uses `TRUE`**, so this is not
hypothetical drift — it is present drift that happens not to be on the two machines examined.

## Operational impact

The `connect_to_iEH` branches select the **data source** (`IEasyHydroSDK` vs `IEasyHydroHFSDK`)
and, in `forecast_library`, whether an already-constructed HF SDK handle is used at all. A
deployment with non-canonical casing therefore reads runoff from a different place than its
configuration says, without any error. Given `preprocessing_runoff` is the pipeline's first
module, a wrong branch there propagates to everything downstream.

## Proposed direction (needs owner sign-off — do not implement from this draft)

1. Add one shared boolean-env helper in `iEasyHydroForecast` (e.g. `env_flag(name, default)`)
   that trims, lower-cases, accepts an explicit token set, and **raises on an unrecognised
   value** rather than silently choosing a branch. Route all 13 sites through it.
2. Decide the accepted token set explicitly — at minimum `true/false`; consider `1/0`, `yes/no`,
   `on/off`. Anything outside it must fail loudly at startup, not at the point of use.
3. Keep `setup_library.check_connect_to_iEH_and_ssh` as the single validation gate, but have it
   validate **parseability** as well as consistency, so an unparseable value is rejected before
   any module branches on it.

**Sequencing note.** Step 1 changes behaviour for any deployment currently relying on the
accident that its casing happens to match one style — a run that silently took a branch will
begin to fail loudly. That is the intended outcome, but it should land with a release note and a
survey of deployed env files, not silently.

## Scope boundaries

- **Do not** change which branch is correct for a given value — this is about parsing one value
  consistently, not about redefining the iEH connection semantics.
- **Do not** fold in the wider "env var contract" cleanup. Other booleans in these env files
  (`SAPPHIRE_API_ENABLED`, `SAPPHIRE_SKILL_LEAD_AWARE`) may have the same shape, but each needs
  its own consumer survey before being swept in.
- `sapphire/services/` is out of scope.

## Acceptance criteria

1. All 13 `connect_to_iEH` / `ssh_to_iEH` comparison sites go through one helper.
2. An unrecognised value fails loudly at startup with a message naming the variable and the
   offending value; a test pins that.
3. Tests cover `True`/`true`/`TRUE` and `False`/`false`/`FALSE` producing identical behaviour at a
   representative site in each of the three former styles.
4. A test pins that `setup_library`'s validation gate rejects an unparseable value rather than
   passing it to consumers.
5. `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.

## Related

| ID | Relation |
|---|---|
| PREPQ-014 | Also concerns iEH-HF SDK behaviour per site; unrelated cause, same subsystem |
| INFRA-029 | Another case of a cross-cutting default that individual modules re-interpret locally |
