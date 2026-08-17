# Request to the iEasyHydro HF SDK developer: discharge norms for virtual stations

**Status**: **Sent 2026-08-17** — maintainer acknowledged and will look into it. Awaiting an answer
to the three questions below; PREPQ-014 stays open pending that reply. Retained here as the record
of what was asked.
**Requested by**: SAPPHIRE Forecast Tools
**Concerns**: `ieasyhydro-python-sdk` @ `2cc7953` (current `master` HEAD), `get_norm_for_site`
**Origin**: PREPQ-014 — see
[`doc/plans/issues/mid_prio_gi_draft_prepq_long_horizon_sdk_norm_path_none.md`](../plans/issues/mid_prio_gi_draft_prepq_long_horizon_sdk_norm_path_none.md)

---

## The ask, in one line

Can `get_norm_for_site` be extended to resolve norms for **virtual stations**, which today are
addressable for listing but not for norms?

## What we observe

Calling `get_norm_for_site(<virtual station code>, "discharge", norm_period=...)` raises:

```
ValueError: No path provided or the provided path is None
```

It fails identically for `norm_period` `"p"`, `"d"` and `"m"`, and it is deterministic rather than
intermittent.

## Why it happens (as we read the SDK)

`get_norm_for_site` resolves a per-site UUID before building the request path:

1. `_call_get_discharge_norm_for_site(...)` → `_call_get_norm_for_site(site_code, 'hydro', ...)`
   — `sdk_endpoint_definitions.py:137,111`
2. `_get_path()` calls `_get_site_uuid_for_site_code(site_code, 'hydro', 'M')`, which queries only
   `GET stations/{organization_uuid}/hydrological?station_code=<code>` — `:90-109`
3. A virtual station is not in that registry, so the UUID is `None`, `_get_path()` returns `None`
   via the `and site_uuid` guard (`:118-126`), and `_call_api` raises (`sdk_base.py:63-64`)

`_call_get_norm_for_site` accepts only `'hydro'` and `'meteo'` site types. There is no `'virtual'`
branch, and no fallback to `stations/{org}/virtual` — even though the SDK does expose
`get_virtual_sites()` against that endpoint, and virtual stations carry forecast flags and
associations.

So the SDK can *list* a virtual station and tell us it is forecast-enabled, but cannot return a norm
for it.

## Why it matters to us

Our work list is built from the hydrological **and** virtual registries, because both can be
forecast-enabled. The norm lookup can address only the first. Virtual stations therefore produce no
`norm` and no percent-of-norm, which affects bulletin display and every norm-relative product for
those sites.

We would like virtual stations to carry norms. We are treating the current behaviour as an upstream
gap rather than working around it client-side, because a workaround would mean suppressing the
signal instead of getting the data.

## Questions

1. **Is this intended?** Are norms conceptually defined for virtual stations, or is their absence a
   deliberate modelling decision on the iEH HF side? If deliberate, we will adapt and stop treating
   it as a gap.
2. **If they are defined**, could `_call_get_norm_for_site` gain a `'virtual'` branch resolving via
   `stations/{org}/virtual`?
3. **If norms for a virtual station are derived** from its constituent stations and weights rather
   than stored directly, would you expect that aggregation to happen server-side, or should clients
   compute it from the associations the SDK already returns? We would prefer server-side, so that
   every client agrees on the method.

## One unrelated observation, offered as a courtesy

While reading the code we noticed a possible station-identity hazard, independent of this request.
In `_get_site_uuid_for_site_code` (`sdk_endpoint_definitions.py:101-108`), when no row matches the
requested `station_type` the function falls back to `sites[0].get('uuid')` **without checking that
the row's station code matches the code that was requested**:

```python
for site in sites:
    if site.get('station_type') == station_type:
        return site.get('uuid')
# If no match with station_type, return first site's UUID as fallback
if sites:
    print(f"Warning: No site found with station_type={station_type}, using first available site")
    return sites[0].get('uuid')
```

If that endpoint can ever return rows for more than the queried `station_code`, this would attach
one station's norms to another — silently, since the warning goes to stdout. We have not observed it
happening and it may be impossible given how the endpoint filters; we mention it only because the
consequence would be quiet and data-corrupting rather than loud.

## What we are running

`ieasyhydro-sdk` 0.3.2 at commit `2cc795306c1b9333d6c0539fecaf0a36865391c8` — current `master` HEAD
as of 2026-08-17, installed identically across all 11 dependent modules. We are not behind; the
`fix_norm_retrieval` and `virtual_station_weights` branches are both already merged into what we
run.
