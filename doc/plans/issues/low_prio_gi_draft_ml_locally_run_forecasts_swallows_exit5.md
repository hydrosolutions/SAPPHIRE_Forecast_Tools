# ML-025: `locally_run_ml_forecasts.sh` swallows `make_forecast.py`'s exit 5 (and every other model script's exit code)

**Status**: Draft (2026-09-09)
**Module**: `apps/machine_learning` (`locally_run_ml_forecasts.sh`)
**Priority**: Low — real defect, but developer-only tooling with no
repository-wired production invocation found anywhere in this repo (see
Reachability below; external or manual invocation cannot be excluded from
repository evidence alone).
**Labels**: `ml`, `run-script`, `exit-code`, `dev-tooling`
**Found**: 2026-09-08/09, out-of-loop review of ML-021 (PR #503). Listed as
"deliberately not addressed" defect 2 in
[`review_gi_draft_ml_forecast_api_write_silent_success.md`](review_gi_draft_ml_forecast_api_write_silent_success.md)
(`## Deliberately not addressed`); this file expands it to a standalone issue.
**Pre-existing**: yes — this script's pipe-and-continue shape predates ML-021;
ML-021 only added the exit code (5) that now gets lost here.

---

## Defect

`apps/machine_learning/locally_run_ml_forecasts.sh` pipes every model script
through `tee` with no `set -o pipefail` and no `${PIPESTATUS[@]}` handling
anywhere in the file. **Every occurrence, enumerated** (`grep -n 'tee\|pipefail\|PIPESTATUS'`
over the file returns exactly these four lines, all `tee`, no `pipefail` or
`PIPESTATUS` anywhere):

| Line | Script |
|---|---|
| `:72` | `recalculate_nan_forecasts.py` |
| `:78` | `make_forecast.py` — the script ML-021 gave exit code 5 |
| `:85` | `fill_ml_gaps.py` |
| `:92` | `add_new_station.py` |

Under bash's default pipeline semantics, `cmd | tee -a file`'s exit status is
`tee`'s, not `cmd`'s — `tee` almost always exits 0. So **none** of these four
scripts' exit codes are visible to the wrapper, not just `make_forecast.py`'s
new exit 5. The loop (`for model in TFT TIDE TSMIXER; do ... for horizon in
$SAPPHIRE_PREDICTION_MODE; do ...`) always continues, and the script always
prints `"All runs completed. Check logs/summary.log for the last 10 lines of
each run."` (`:127`) regardless of what any of the four scripts did.

**Correction to an earlier draft of this description**: `:127` is not itself a
second `tee` occurrence — it is a plain `echo` after the loop. The four `tee`
sites above are the complete list.

## Reachability — no repository-wired production invocation found

Four independent facts, checked directly against this worktree:

1. **The script says so itself.** Line 4: `# Note that this script is not used
   in operational mode.`
2. **Its only reference in the repository sits inside a wrapper function whose
   own invocation is commented out — there is no active repository caller at
   all.** `bin/locally_run_forecast_tools.sh:170` calls
   `bash locally_run_ml_forecasts.sh` from inside `run_machine_learning_models()`
   (defined at `:154`), but that function's own call site,
   `#run_machine_learning_models` at `:264` (in the script's executable section,
   `:252-267`), is commented out — so even the deprecated wrapper never reaches
   this line when run as-is. Reaching `locally_run_ml_forecasts.sh` today
   requires an operator to manually uncomment that line, or to call the function
   directly.
3. **The wrapper is separately marked deprecated.** Its own header (`:3-16`)
   reads: `"DEPRECATED: Use apps/run_locally.sh instead... This script is
   outdated — it uses hardcoded paths and conda environments that no longer
   exist."` (a claim about the wrapper's own state, per the wrapper's own
   comment — not independently re-verified here). It also prints a deprecation
   `WARNING` at every invocation (`:18-19`). It is not installed in any crontab
   this repository documents — `grep -rn locally_run_forecast_tools doc/`
   matches only issue-tracking documents discussing it as an example (including
   this one), not a deployment or scheduling doc.
4. **`grep -rn locally_run_ml_forecasts doc/` matches only issue-tracking
   documents** (this issue and the source review it was split from) — no
   runbook, deployment doc, or crontab reference invokes it. This grep result
   will also match this issue's own file and its `module_issues.md` tracker
   row going forward; the claim is about the absence of non-issue-tracking
   (runbook/deployment/crontab) matches, not a literal match count.

**Does INFRA-023 cover this?** No. INFRA-023 (Complete, PR #494) is scoped to
`run_periodic_maintenance.sh` and `yearly_runoff_hydrograph_aggregation.sh` — a
different pair of scripts entirely, verified by reading its file — and its own
"installed-crontab survey" note explicitly says which wrappers were checked;
`locally_run_ml_forecasts.sh` is not among them. This is the **same defect
shape** (a `tee` pipe with no exit-code propagation) but a **different script**,
filed separately so nobody closes one against the other's fix.

## Desired outcome

If this script continues to be maintained as a developer convenience, it should
capture and report each script's real exit code (`${PIPESTATUS[0]}` after each
`tee`, or `set -o pipefail`) and stop claiming "All runs completed" when a run
failed — at minimum, print a non-generic summary line per model/horizon showing
which scripts failed. Given the confirmed non-operational reachability (above),
a fix here is a nice-to-have for whoever still runs it by hand, not an
operational gap.

## Out of scope

- `bin/locally_run_forecast_tools.sh` itself — already marked deprecated with
  its own replacement path (`apps/run_locally.sh`); not touched here.
- Any change to `make_forecast.py`'s exit codes or ML-021's truth table.
- Deciding whether `locally_run_ml_forecasts.sh` should be deleted instead of
  fixed — an owner call once reachability is understood, not made here.

## Acceptance criteria

- [ ] Each of the four `tee` sites (`:72`, `:78`, `:85`, `:92`) has its
      underlying command's exit code captured, not `tee`'s.
- [ ] A failure in any one script (exit 5 from `make_forecast.py` in
      particular) is visible in the script's own output — not just inferable
      from reading the per-model log file by hand.
- [ ] A test or manual verification note records that this script has no
      pytest coverage today (it is a bash script, not imported by any test
      module) — if a fix adds coverage, say how; if not, say why manual
      verification is sufficient given the confirmed reachability above.

---

## Related

| ID | Relation |
|---|---|
| ML-021 | Source issue; this is "deliberately not addressed" defect 2 there |
| INFRA-023 | Same defect shape (unguarded `tee`), different scripts — does NOT cover this file |
