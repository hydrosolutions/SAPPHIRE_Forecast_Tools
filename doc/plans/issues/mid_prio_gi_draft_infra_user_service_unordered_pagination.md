# INFRA-057: the user service paginates without ORDER BY

**Status**: Draft (2026-09-22)
**Module**: `sapphire/services/user` — **colleague-owned, propose rather than edit**
**Priority**: **Medium** — same defect class as #516, far smaller blast radius
**Labels**: `bug`, `api`, `data-integrity`
**Found**: 2026-09-22, while reviewing the #516 fix (PR #517)
**Related**: #516 / PR #517 (the same defect in `postprocessing`, fixed)

---

## Problem

`sapphire/services/user/app/crud.py` has two paginated readers with no `ORDER BY`:

- `:37` — `return query.offset(skip).limit(limit).all()`
- `:76` — `return db.query(Role).offset(skip).limit(limit).all()`

`grep -n order_by sapphire/services/user/app/crud.py` returns nothing: the file has none.

PostgreSQL guarantees no row order without `ORDER BY`, so successive pages of the same query can
overlap and omit rows. This is exactly the defect reproduced in #516 for postprocessing — 10,773
rows returned, 7,277 distinct ids — and fixed in PR #517 by adding a total ordering on the primary
key.

## Why it is Medium, not High

The affected tables are users and roles. They are small enough that a filtered result rarely spans
a page, and no in-repo consumer was found paginating them. So the defect is latent rather than
active. It is worth fixing because it is the same class, the fix is trivial, and "small table"
stops being true silently.

**Checked and NOT affected**: `sapphire/services/preprocessing/app/crud.py` already orders all its
paginated queries.

## Proposed fix

Mirror PR #517: add a deterministic **total** ordering before `offset`/`limit`, ending in the
primary key so ties cannot span a page boundary. Confirm each model's `id` is indexed first.

## Ownership

`sapphire/services/` is colleague-managed (CLAUDE.md). Open as a proposal for the service owner,
as was done for #516 — do not edit directly.

## Acceptance criteria

- Both readers order by a unique column before `offset`/`limit`.
- A test that fails if the ordering is removed. **Note the trap from PR #517**: the service tests
  run on SQLite, which returns rowid order on a plain table scan even without `ORDER BY`, so a
  purely behavioural test passes with the bug present. Assert at the SQL level that the emitted
  SELECT's `ORDER BY` clause targets the id column, as `TestPaginationOrdering` does in
  `sapphire/services/postprocessing/tests/test_crud.py`.
- `bash run_tests.sh service:user` green.
