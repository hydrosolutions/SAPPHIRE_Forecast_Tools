# Design: Basin numbering + merged basin name column for Tajik bulletins

## Goal

For the Tajikistan deployment, change the four forecast bulletins so each
station table gains two leading columns:

1. **Column 1 (`№`)** — a per-basin number (1, 2, 3 …).
2. **Column 2 (`Бассейн`)** — the basin name.

When a basin contains several stations, the `№` and `Бассейн` cells are
**merged vertically** across that basin's rows, so the number and name appear
once. The current full-width basin banner row (`{{HEADER.BASIN_RU}}` /
`{{HEADER.BASIN_NAME}}`) is removed.

Bulletins for any other organization must be **completely unaffected**.

## Scope of templates

Affected horizons / templates (Tajik copies only):

- `pentadal_forecast_bulletin_template_tj.xlsx`
- `decadal_forecast_bulletin_template_tj.xlsx`
- `monthly_forecast_bulletin_template_tj.xlsx`
- `seasonal_forecast_bulletin_template_tj.xlsx`

Physically edited only under
`taj_data_forecast_tools/templates/`. The Kyrgyz (`kyg_data_forecast_tools/`)
copies of these `_tj` files are **left untouched** because the
`.env_develop_kghm` config (org = `kghm`) points at them and must keep the old
layout.

## Detection (no filename checks)

The behaviour is gated on organization, read once in `write_to_excel`:

```python
is_tj = os.getenv("ieasyhydroforecast_organization") == "tjhm"
```

We never branch on `_tj` in a filename — on the production server the template
file is not named with a `_tj` suffix, but the organization is still `tjhm`.

## Decision summary (from brainstorming)

- **Layout:** replace the banner row; `№` numbers **basins** (not stations);
  both `№` and `Бассейн` merge vertically per basin.
- **Reservoir blocks (monthly/seasonal):** the second
  "ПРЕДВАРИТЕЛЬНЫЙ ПРОГНОЗ ПРИТОКА ВОДЫ В ВОДОХРАНИЛИЩА" block **also** gets
  `№` + `Бассейн`, grouped/merged by basin.
- **Pentad/decad two sub-tables:** the row shows actual discharge (left,
  cols A–G) and forecast (right, cols I–S) for the same stations. `№` + `Бассейн`
  are added **once at the far left only**; the right forecast sub-table keeps
  just `РЕКА`/`ПУНКТ`.

## Mechanism

The basin number is *positional* (depends on grouping order), so it is not a
static per-station attribute. Two shared helpers in `bulletins.py`:

### `_assign_basin_numbers(ordered_sites)`
Sites are already ordered so all stations of a basin are contiguous
(`oder_sites_list_according_to_bulletin_order`, sorted by `basin_ru` then
`bulletin_order`). Walk the ordered list and stamp each object with
`obj._bulletin_basin_no` — an integer that increments on each new distinct
basin (by first appearance). Returns the same list.

### `_merge_basin_columns(ws, no_col, basin_col, start_row, end_row)`
After the table rows are filled, scan `basin_col` from `start_row` to
`end_row`. For each run of adjacent rows with the same basin name, merge the
`no_col` cells and the `basin_col` cells across the run, and set
center/middle alignment. Single-station basins are simply centered (no merge).
This runs only when the `№`/`Бассейн` columns exist (i.e. `is_tj`).

### New tags (data tags, only built when `is_tj`)
- `BASIN_NO` → `get_value_fn = lambda obj: getattr(obj, "_bulletin_basin_no", "")`
- `BASIN_NAME` (pentad/decad: `BASIN_RU`) → `obj.basin_ru`, as a **data** tag.

## Per-horizon wiring in `write_to_excel`

### Monthly & seasonal (`MultiSectionReportGenerator`, already used)
- When `is_tj`:
  - In each section's tag list (sec0, plus reservoir sec1/sec2), add
    `BASIN_NO` + `BASIN_NAME` **data** tags. For sec0, the basin is a data tag
    (not `header=True`).
  - Call `_assign_basin_numbers` on each section's object list before render.
  - After `generate_report_multi`, call `_merge_basin_columns` on each rendered
    section's row span (the generator already tracks section bounds).
- When not `is_tj`: keep today's behaviour exactly (sec0 `BASIN_NAME`
  `header=True`, no reservoir basin grouping).

### Pentad & decad (currently `DefaultReportGenerator`, per-basin + all_basins files)
- When `is_tj`:
  - The template has **no** `{{HEADER.*}}` tag and one data row carrying the
    far-left `{{DATA.BASIN_NO}}` / `{{DATA.BASIN_RU}}` plus the existing
    left/right data tags.
  - Render through `MultiSectionReportGenerator` with a single header-less
    section (its existing flat-rendering branch replicates the data row per
    site). Keep the existing per-basin file loop + `copy_worksheet` multi-sheet
    assembly. Assign basin numbers per file's site list; merge after render.
  - In a per-basin file there is one basin → number `1`, name merged across all
    its rows. The `all_basins` file gets the full numbering.
- When not `is_tj`: the existing `DefaultReportGenerator` path runs untouched.

## Template edits (openpyxl transform script)

Two structural variants:

**Pentad / decad** (`'1 пентада'` sheet):
- Insert 2 columns at the far left.
- New `A5:A6` = `№`, `B5:B6` = `Бассейн` (merged like neighbouring header
  cells), with matching style.
- Extend the title merges (rows 1, 2) and the row-7 separator to cover the new
  width.
- Delete the basin banner row (old `A7`).
- Data row: `A` = `{{DATA.BASIN_NO}}`, `B` = `{{DATA.BASIN_RU}}`; existing
  left/right data tags shift +2 columns.
- Set sensible widths for `№` (~5) and `Бассейн` (~18).

**Monthly / seasonal** (`'bulletin'` sheet, two blocks):
- Insert 2 columns at the far left.
- Main block: header row gets `№` / `Бассейн`; delete banner row; data row gets
  `{{DATA.BASIN_NO}}` / `{{DATA.BASIN_NAME}}`.
- Reservoir block: add `№` / `Бассейн` to its column-header row and its data row.
- Extend all title merges to the new width; fix column widths.

The script keeps a `.bak` copy of each edited file and is re-runnable
(idempotent: detect whether the `№` column already exists).

## Testing

- Unit: `_assign_basin_numbers` (sequential, resets per distinct basin, handles
  single-basin and empty lists).
- Unit: `_merge_basin_columns` (merges multi-station runs, leaves single-station
  rows unmerged, correct row spans, alignment).
- Integration: render a synthetic `is_tj` template with two basins (one multi-
  station, one single-station) and assert numbering + merged ranges; render the
  reservoir block and assert grouping.
- Regression: with org ≠ `tjhm`, the existing header-row behaviour and outputs
  are unchanged (no `№` column, banner row present).
- Existing bulletin tests (`test_bulletins_*`, `test_integration*`) stay green;
  formula/numeric-cell helpers already derive columns from tag positions, so the
  +2 shift is absorbed automatically.

All tests via `cd apps && SAPPHIRE_TEST_ENV=True bash run_tests.sh`.
