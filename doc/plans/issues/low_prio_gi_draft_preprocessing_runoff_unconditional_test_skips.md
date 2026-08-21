# PREPQ-017: Two unconditional `@pytest.mark.skip` placeholders violate the Zero Skips Policy

**Status**: Draft (2026-08-21)
**Module**: `apps/preprocessing_runoff/test/test_src.py`
(`TestGetRunoffDataForSitesOrganizationDispatch`, starting `:1889`)
**Priority**: Low — the skipped paths are documented as indirectly covered by other tests in the
same class; no evidence of an undetected defect, only a coverage gap.
**Labels**: `preprocessing_runoff`, `test-hygiene`, `zero-skips-policy`
**Found**: 2026-08-21, filed as a follow-up while implementing INFRA-037.
**Related**: None specific to this module; CLAUDE.md § Zero Skips Policy is the governing
convention.

---

## Defect

`apps/preprocessing_runoff/test/test_src.py` contains two `@pytest.mark.skip` placeholders inside
`TestGetRunoffDataForSitesOrganizationDispatch`, both **unconditional** (no `condition=` argument,
no environment/dependency gate — they skip on every run, every environment):

`test_demo_reprocess_path` (`:2049-2058`):

```python
@pytest.mark.skip(
    reason=(
        "No multi-river/kghm-format fixture helper exists in test_src.py; "
        "demo dispatch path covered indirectly via test_unknown_org_still_raises. "
        "TODO: add _build_multiple_rivers_xlsx helper and implement this test."
    )
)
def test_demo_reprocess_path(self, tmp_path, monkeypatch):
    """Placeholder: demo dispatch path in get_runoff_data_for_sites."""
    pass  # noqa: unnecessary-pass
```

`test_kghm_reprocess_path_unchanged` (`:2093-2103`):

```python
@pytest.mark.skip(
    reason=(
        "No kghm/multi-river fixture helper exists in test_src.py; "
        "kghm dispatch path is indirectly covered by existing "
        "test_read_all_runoff_data_from_excel and test_unknown_org_still_raises. "
        "TODO: add _build_multiple_rivers_xlsx helper and implement this test."
    )
)
def test_kghm_reprocess_path_unchanged(self, tmp_path, monkeypatch):
    """Placeholder: kghm dispatch path regression in get_runoff_data_for_sites."""
    pass  # noqa: unnecessary-pass
```

Both bodies are a bare `pass` — these are not tests with weakened assertions, they are empty
placeholders that never execute.

**Pre-existing on trunk** — verified against `origin/maxat_sapphire_2`: both skip decorators and
both placeholder bodies are present unchanged there; not introduced by INFRA-037 or by this branch.

## Why this violates the Zero Skips Policy

CLAUDE.md's Zero Skips Policy states: "No tests may be skipped without justification... **One
exception**: dependency-gated skips are acceptable when `sapphire-api-client` is not installed.
These tests guard on `SAPPHIRE_API_AVAILABLE`... This is the only valid skip pattern — all other
skips indicate hidden bugs." Both `test_demo_reprocess_path` and `test_kghm_reprocess_path_unchanged`
are unconditional `pytest.mark.skip` calls with a `TODO` in the reason — not a
`SAPPHIRE_API_AVAILABLE` dependency gate. They do not match the one documented exception.

## What the tests were meant to cover

Both are inside `TestGetRunoffDataForSitesOrganizationDispatch`, which exercises
`src.get_runoff_data_for_sites`'s per-organization dispatch branches (see the sibling,
non-skipped tests in the same class: `test_uzhm_unreadable_cache_path` at `:2006`,
`test_unknown_org_still_raises` at `:2065`). The two skipped tests were meant to cover the
**demo** and **kghm** organization dispatch branches specifically — the same class already covers
`uzhm` (via `test_uzhm_unreadable_cache_path`) and validates that an unrecognized org raises with
all four org names in the message (`test_unknown_org_still_raises`), but exercises neither `demo`
nor `kghm`'s own reprocess-dispatch branch directly.

## What is missing

Both skip reasons name the same missing piece: **no multi-river/kghm-format fixture helper exists
in `test_src.py`**. The reason text explicitly proposes the fix: "add `_build_multiple_rivers_xlsx`
helper and implement this test." That helper does not currently exist anywhere in `test_src.py`
(not verified against other test files in the module — only `test_src.py` was checked). Without
it, there is no way to build a fixture in the multi-river/kghm xlsx format the `demo` and `kghm`
dispatch branches expect, which is why both tests were left as placeholders rather than written
against some other, less faithful fixture shape.

## Out of scope

- Writing `_build_multiple_rivers_xlsx` or the two tests themselves — this is a draft documenting
  the gap, not an implementation.
- Any other skip in `apps/preprocessing_runoff/test/` or elsewhere — not surveyed by this draft;
  scoped to these two specific unconditional skips.
- Whether the "indirect coverage" claim in each skip reason (via `test_unknown_org_still_raises`
  and `test_read_all_runoff_data_from_excel`) is actually sufficient — not evaluated here; the
  skip reasons assert it, this draft does not independently verify it.

## Acceptance criteria

- [ ] `_build_multiple_rivers_xlsx` (or an equivalent fixture helper) is added to
      `test_src.py`, matching the multi-river/kghm xlsx format `get_runoff_data_for_sites`'s
      `demo` and `kghm` dispatch branches expect.
- [ ] `test_demo_reprocess_path` and `test_kghm_reprocess_path_unchanged` are implemented against
      that helper (or an equivalent) and the `@pytest.mark.skip` decorators are removed — or, if a
      decision is made not to implement them, the `TODO` is resolved by an explicit owner decision
      recorded in this file, not left as a standing unconditional skip.
- [ ] `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh preprocessing_runoff` — zero failures,
      zero unexpected skips (these two no longer counted as skips).
