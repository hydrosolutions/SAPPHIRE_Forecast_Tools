## Ensemble quantile mapping resolves its parameter bundle from a leftover loop variable — "last configured bundle wins" rather than an explicit per-station lookup (PREPG-012)

**Status**: Draft (2026-08-18)
**Module**: `apps/preprocessing_gateway` (`Quantile_Mapping_OP.py`)
**Priority**: **Low — dormant.** *Resolved 2026-08-18 by inventory:* **every deployment
configures exactly one control bundle** (kyg 1, taj 1, uzb unset), so "last bundle wins" has a
single candidate everywhere and selection is unambiguous today. This is a code-clarity and
future-proofing fix, not a data-correctness one. (Two earlier severity claims — High, then
Medium, then "TBD" — are all superseded by the measurement.)
**Depends on**: **PREPG-011.** Bundle selection cannot be made correct until station identity is
normalised — today the lookup key is a file-level constant, not the station.

> **The original headline was wrong and is withdrawn.** It claimed "every ensemble HRU is
> downscaled with another HRU's parameters". In fact `HRU{x}_P_params.csv` is a **bundle**
> covering multiple catchments and `do_quantile_mapping()` filters coefficient rows by `code`.
>
> **A second correction, 2026-08-18:** the withdrawal above then claimed that filter uses
> "the ensemble station's own code". **Also wrong.** It uses the *transform-assigned constant*
> `code` (the file-level HRU code — see PREPG-011), **not** the per-station identity in `name`.
> So the "ugly but correct" verdict for single-bundle deployments does not hold: at most the
> *bundle* is right, while the *row selection within it* can still be wrong.

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
loop left behind.

**This is deterministic, not random.** `ieasyhydroforecast_HRU_CONTROL_MEMBER` is a
comma-separated list processed in order (`Quantile_Mapping_OP.py:670`), and the leftover value
used at `:884` is therefore always the **last configured** bundle. *An earlier revision called
this "non-deterministic"; that is withdrawn.* It is implicit and potentially wrong — a reader
cannot tell which bundle applies without tracing the config order — but it is reproducible.

## Actual consequences (revised twice — earlier lists withdrawn)

**With one configured control bundle**, *bundle selection* is unambiguous — there is only one
candidate. That is the most that can be claimed. Row selection inside it is still keyed on the
file-level constant `code` (**PREPG-011**), so the coefficients applied to a given station may
still be another station's.

**With multiple control bundles**, every ensemble station is looked up in the **last configured**
bundle. Then either

- the station's code **is** present in that bundle → correct by luck, or
- it is **absent** → zero matching coefficient rows, failing in arithmetic rather than with a
  clear message, or
- it is present in that bundle **with different coefficients** from its intended bundle → silent
  wrong downscaling. This is the only genuine corruption path, and it requires duplicate station
  codes across bundles.

## Phase-0 inventory — RESOLVED 2026-08-18

`ieasyhydroforecast_HRU_CONTROL_MEMBER`, read from the three deployment env files
(`~/Documents/GitHub/<org>_data_forecast_tools/config/`):

| Org | Control bundles configured |
|---|---|
| kyg | **1** |
| taj | **1** |
| uzb | unset (does not use this path) |

**No deployment configures more than one bundle**, so the risk table below is entirely
hypothetical today:

| Configuration | Severity | Present on any deployment? |
|---|---|---|
| One bundle | Code clarity only | **Yes — all of them** |
| Several, target absent from the last | Loud arithmetic failure | No |
| Several, same station code, differing coefficients | High — silent wrong downscaling | No |

**Caveat that keeps this open rather than closed:** with one bundle the *bundle* is right, but row
selection inside it is still keyed on the file-level constant `code` (**PREPG-011**). That defect
is separately dormant because ensemble files are single-feature — so the two dormancies are
independent, and arming either one re-arms this.

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

1. **Land PREPG-011 first**, so a per-station mapped identity exists to look up. Apply the DG
   `name` → station mapping **before** bundle lookup.
2. Load and index every configured control bundle **once, outside** the ensemble-member/HRU loop
   — not re-read per iteration as today.
3. Index rows by mapped station code.
4. Require **exactly one** P row and one T row for the station, **and require both to resolve to
   the same bundle** — "one P row somewhere and one T row somewhere" would permit mismatched
   provenance.
5. Detect duplicate coverage **within** a single bundle as well as across bundles.
6. On failure, name the **raw DG identity, the mapped station identity, and the originating
   bundle file(s)** — all three, or the diagnostic will not be actionable.

That removes the ambiguity without needing to know which bundle "should" win.

## Acceptance criteria

- Bundle selection is **explicit**: the station's coefficients are resolved by searching the
  configured bundles for its code, not by an implicit leftover variable.
- A fixture with **two** control bundles, where the target station appears in the first, proves
  the correct bundle is used regardless of processing order.
- A station present in **no** bundle fails with a message naming the station and the candidate
  bundles — not an arithmetic error.
- A station present in **two** bundles — or twice in **one** bundle — fails as ambiguous rather
  than silently taking one.
- P and T coefficients for a station provably come from the **same** bundle.
- Bundles are loaded and indexed once, not per loop iteration.
- Single-bundle deployments are **semantically equal** where the station identity was already
  correct. *"Byte-identical" is withdrawn* — it cannot hold across a simultaneous PREPG-011
  identity correction, which legitimately changes which coefficients some rows receive.
- `SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_gateway` green.

## Contract not to break

- `perform_qmapping` may be false; the raw ERA5/IFS passthrough path must be unaffected.
- Do not change the control-member loop's behaviour — it is not implicated here.
