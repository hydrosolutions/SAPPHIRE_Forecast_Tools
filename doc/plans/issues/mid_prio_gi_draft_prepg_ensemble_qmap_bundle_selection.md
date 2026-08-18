## Ensemble quantile mapping implicitly uses the **last** control-HRU parameter bundle — multiple control bundles have no deterministic station-to-bundle association (PREPG-012)

**Status**: Draft (2026-08-18)
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Priority**: **Medium** — *downgraded from High on 2026-08-18 after out-of-loop review.*

> **The original headline was wrong and is withdrawn.** It claimed "every ensemble HRU is
> downscaled with another HRU's parameters". In fact `HRU{x}_P_params.csv` is a **bundle**
> covering multiple catchments, and `do_quantile_mapping()` selects coefficient rows **by the
> ensemble station's own code** (`dg_utils.py:126`, `:130`). With a single configured control
> bundle the current `c_m_hru` reference is **ugly but correct**. The real defect is narrower:
> with **multiple** control bundles, "last bundle wins" gives no deterministic
> station-to-bundle association.

Three further corrections from the same review:
- **The missing-file consequence is effectively impossible** in the normal flow — the same
  control-bundle files were already read successfully earlier in the process
  (`Quantile_Mapping_OP.py:739`) before being re-read at `:884`. A wrong bundle more plausibly
  yields zero matching coefficient rows and fails in arithmetic.
- **No per-ensemble-HRU parameter files exist** anywhere in the repository, fixtures, or git
  history (`HRU*_P_params.csv` / `HRU*_T_params.csv` searched across all history). The
  integration fixture disables quantile mapping entirely.
- **Git history refutes "a previously-correct variable survived a later loop insertion"** —
  both loops and the stale reference arrived in the file's initial commit (2024-07-15).
**Labels**: `preprocessing_gateway`, `data-integrity`, `quantile-mapping`, `ensemble`
**Found**: 2026-08-18, out-of-loop `codex exec` review of PREPG-010; **verified in code** in the
same session.
**Related**: PREPG-011 (same code path, also silent data corruption). PREPG-010 (same path,
transport handling).

---

## The defect

Inside the **ensemble** HRU loop (`Quantile_Mapping_OP.py:~799` onward, loop variable
`code_ens`), the quantile-mapping parameters are loaded using **`c_m_hru`** — the variable left
over from the preceding **control-member** loop:

```python
        # load the parameters
        if perform_qmapping:
            P_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f"HRU{c_m_hru}_P_params.csv"))
            T_params_hru = pd.read_csv(os.path.join(Q_MAP_PARAM_PATH, f"HRU{c_m_hru}_T_params.csv"))
```

`c_m_hru` is not reassigned in the ensemble loop, so it holds whatever value the control-member
loop left behind — in practice the **last** control-member HRU processed.

## Actual consequences (revised — the original list is withdrawn)

**With one configured control bundle — the common case — output is correct.** The bundle is
selected by an implicit leftover binding rather than an explicit lookup, but
`do_quantile_mapping()` filters rows by the ensemble station's own code, so the right
coefficients are applied.

**With multiple control bundles**, the association is non-deterministic: whichever bundle the
control loop processed last is the one every ensemble station is looked up in. Then either

- the station's code **is** present in that bundle → correct by luck, or
- it is **absent** → zero matching coefficient rows, failing in arithmetic rather than with a
  clear message, or
- it is present in that bundle **with different coefficients** from its intended bundle → silent
  wrong downscaling. This is the only genuine corruption path, and it requires duplicate station
  codes across bundles.

**Triage note:** severity depends entirely on how many control bundles a deployment configures.
Check `ieasyhydroforecast_HRU_CONTROL_MEMBER` per deployment — a single value means this is
a code-clarity issue, not a data issue, there.

## What to inspect

1. How many control bundles each deployment configures — this determines whether the issue is
   cosmetic or real for that deployment.
2. Whether any station code appears in more than one bundle with differing coefficients. That is
   the precondition for silent wrong downscaling, and it is checkable offline.
3. The control-member loop's own parameter load, to confirm the intended lookup shape before
   copying it.

## What the fix should be

*Refusing the one-word `c_m_hru` → `code_ens` change was correct — but blocking on an owner
decision was over-cautious.* The documented model already settles it: bundles are per control
HRU, rows are per hydropost (`apps/config/models_and_scalers/README.md`,
`doc/configuration.md` under `ieasyhydroforecast_HRU_ENSEMBLE`). Neither `code_ens` nor
`code_ens_data_gateway` is a valid filename key.

**Implement explicit bundle resolution** instead of an implicit leftover binding:

1. Load every configured control bundle.
2. Index rows by mapped station code.
3. Require **exactly one** P row and one T row for the station being processed.
4. Fail naming both the station code **and** the candidate bundles if coverage is missing or
   duplicated.

That removes the ambiguity without needing to know which bundle "should" win.

## Acceptance criteria

- Bundle selection is **explicit**: the station's coefficients are resolved by searching the
  configured bundles for its code, not by an implicit leftover variable.
- A fixture with **two** control bundles, where the target station appears in the first, proves
  the correct bundle is used regardless of processing order.
- A station present in **no** bundle fails with a message naming the station and the candidate
  bundles — not an arithmetic error.
- A station present in **two** bundles fails as ambiguous rather than silently taking one.
- Single-bundle deployments are byte-identical to today.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- `perform_qmapping` may be false; the raw ERA5/IFS passthrough path must be unaffected.
- Do not change the control-member loop's behaviour — it is not implicated here.
