# INFRA-046: a gateway failure stops forecasts that never read the data it failed on

**Status**: Draft (2026-09-04)
**Module**: `apps/pipeline/pipeline_docker.py`
**Priority**: **Low — parked pending PREPG-023** (owner decision, 2026-09-04: implement the
ensemble gate first, then re-assess whether this is worth doing at all). Was Medium.

**Why parked**: once ensembles are downloaded only when something declares it needs them, the
gateway stops failing on this path for every deployment that does not consume ensemble forcing —
which today is all of them. This issue then only bites where ensembles genuinely *are* required and
genuinely fail. **Re-assess after PREPG-023 lands**: if no deployment turns the new flag on, close
this rather than build it.

**Original assessment** — after PREPG-023's gate lands, deployments that do not need ensemble data
stop failing here at all, so this only bites when ensembles genuinely are required and genuinely
fail. It is not Low because when it does bite, it silently costs a whole day of machine-learning and
long-term forecasts.
**Labels**: `infra`, `pipeline`, `dependency-scope`
**Found**: 2026-09-04. **Owner decision, same day**: "failed ensemble should not stop models that
don't require ensembles from running."
**Related**: **PREPG-023** (gates the ensemble stage on whether anything consumes it — reduces how
often this is reachable, does not fix it), **PREPG-024** (the in-container `&&` chain suppressing
snow — adjacent, *not* a duplicate: that is the shell chain inside the image, this is the Luigi
task graph; cross-reference, do not merge), **PREPG-016**, **P-007** (the exit-code propagation fix
that made this visible).

---

## The coupling

`RunMLModel`, `RunAllMLModels` and `RunLongTermForecast` all require the gateway task
(`pipeline_docker.py:743`, `:786`, and `:2231` for the **operational/undated** long-term task, via
`get_gateway_dependency()` at `:125-159`; the *dated recovery* form returns no gateway dependency at
all, so it is unaffected). The
gateway task writes its Luigi output marker **only** when the container exits 0
(`:384-400`) and raises after exhausting retries (`:421-429`).

So any gateway failure — including one caused solely by the **ensemble** stage — leaves the marker
unwritten and blocks all three. None of them reads
`{code}_{P,T}_ensemble_forecast.csv`; they consume the **control member** meteo, which the gateway
writes to the API well before the ensemble stage runs (`Quantile_Mapping_OP.py:875-890`, ensemble
loop begins `:896`).

Measured on kghm, 2026-09-04: the Data Gateway publishes ensemble precipitation without temperature
on roughly one day in four (see PREPG-023's Phase 0 findings). On those days the gateway exits 1
after the control-member stage has already run.

> **"The control member succeeded" needs defining before it can unblock anything.** The stage writes
> its CSV (`Quantile_Mapping_OP.py:871`) and then *attempts* the API write (`:875`) — and an
> API-disabled, not-ready or failed write is logged and swallowed (`:348`), so the stage can
> "succeed" having persisted nothing to the database. Both consumers can require the API path
> (`machine_learning/make_forecast.py:404`, `long_term_forecasting/run_forecast.py:465`). So a
> marker that unblocks them must be conditioned on **verified persistence for the configured
> backend**, not merely on the stage having run without raising. Getting this wrong would trade a
> false block for a false unblock, which is worse.

Before P-007 (PR #478) the container's exit code was discarded, so this never surfaced. It does now.

## The contract

**C1 — the dependency must express what is actually needed.** `RunMLModel`, `RunAllMLModels` and
`RunLongTermForecast` need the gateway's **control-member** output. Their dependency should be
satisfied when that has succeeded, independently of the ensemble stage's outcome.

**C2 — the conceptual model's dependency is unchanged.** `ConceptualModel` (`:679`, requires at
`:685`) *is* the ensemble consumer. It must keep failing when the ensemble stage fails; do not
weaken it.

**C3 — an ensemble failure must stay visible.** This issue removes a *blocking* relationship, not a
signal. The gateway must still report the ensemble failure and still exit non-zero; what changes is
who is prevented from running. A design where the ensemble failure becomes invisible is a
regression, not a fix.

**How to satisfy C1 is an open design question — and the answer changes this issue's scope.**
Resolve it before implementing, and record the choice here.

The constraint that rules options in or out: the gateway runs as **one container**, and the pipeline
only learns a terminal status after waiting for it (`pipeline_docker.py:329`), raising once retries
are exhausted (`:421-429`). There is no stage-level interface today.

- **(a) Split the gateway into two Luigi tasks** (control member, ensemble). Cleanest graph, but it
  needs the gateway to expose a stage selector — so it **is** a gateway change and C5 below must be
  relaxed to allow it.
- **(b) One task, two markers** — the gateway publishes a durable control-member result as soon as
  that stage has verifiably persisted, and the existing marker on full success. Non-consumers depend
  on the first. Smaller graph change, but it **also** requires a gateway change: something inside the
  container must emit that intermediate result. Not free either.
- **(c) Make the ensemble stage non-fatal to the gateway's exit code.** **Rejected** — it
  contradicts C3, and it pushes discovery onto the conceptual model, which would find out only by
  reading absent files.

**So there is no option that lives purely in `pipeline_docker.py`**, and an earlier revision of this
issue implied there was. Whichever of (a) or (b) is chosen, this issue's scope must widen to include
the gateway side of it. Recommend **(b)**: it is the smaller change and keeps one container run.

## Files that may be modified

- `apps/pipeline/pipeline_docker.py`
- `apps/pipeline/tests/` — the Luigi dependency tests

`apps/preprocessing_gateway/Quantile_Mapping_OP.py` — **only** for whichever of (a)/(b) is chosen,
and only for emitting the stage result. Its ensemble-gating behaviour is PREPG-023's territory and
must not be touched here.

**Do not** change `ConceptualModel`'s dependency (C2).

## Tests

1. Gateway succeeds on the control member and fails on the ensemble → `RunMLModel`,
   `RunAllMLModels` and `RunLongTermForecast` all become runnable; `ConceptualModel` does not.
2. Gateway fails on the control member → all four remain blocked, including the three
   non-consumers. This is the test that stops (C1) from being over-applied.
3. Gateway fully succeeds → unchanged behaviour for all four.
4. The ensemble failure is still reported and the gateway's own status is still non-zero (C3).

## Acceptance criteria

- [ ] A control-member-only success unblocks the three non-consumers and only them.
- [ ] A control-member failure still blocks everything.
- [ ] The ensemble failure remains visible in the gateway's output and exit status.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh` — zero failures, zero unexpected skips.
- [ ] The chosen design (a/b/c) is recorded in this file before implementation.

## Out of scope

- Gating the ensemble stage on consumption — **PREPG-023**.
- Why the Data Gateway omits ensemble temperature — upstream, tracked in PREPG-023's Phase 0.
- Counting ensemble members — PREPG-016.
