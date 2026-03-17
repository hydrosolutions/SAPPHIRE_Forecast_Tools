# Variable/Metric Codes Reference

## Hydrological Measurements

| Code | Description | Notes |
|------|-------------|-------|
| `WLD` | Water level daily | 8AM or 8PM values from KN-15 telegram |
| `WLDA` | Water level daily average | Calculated from 8AM and 8PM values |
| `WLDC` | Water level decadal | From KN-15 subgroup 966 |
| `WLDCA` | Water level decade average | Decadal average for a period |
| `WDD` | Water discharge daily | Morning reading, may be estimated from rating curve |
| `WDDA` | Water discharge daily average | From daily avg water level + rating curve |
| `WDFA` | Water discharge fiveday average | Pentadal average |
| `WDDCA` | Water discharge decade average | Decadal average |
| `WTO` | Water temperature observation | Daily from KN-15 telegram section 4 |
| `ATO` | Air temperature observation | Daily from KN-15 telegram section 4 |
| `IPO` | Ice phenomena observation | Complex: intensity + value code |
| `PD` | Precipitation daily | Complex: value + duration code |
| `WTDA` | Water temperature daily average | |
| `ATDA` | Air temperature daily average | |
| `RCSA` | River cross section area | From KN-15 subgroup 966 |

## Meteorological Measurements

| Code | Description | Notes |
|------|-------------|-------|
| `ATDCA` | Air temperature decade average | Manual entry or KN-15 subgroup 988 |
| `PDCA` | Precipitation decade average | Manual entry or KN-15 subgroup 988 |
| `ATMA` | Air temperature monthly average | Manual entry or KN-15 subgroup 988 |
| `PMA` | Precipitation monthly average | Manual entry or KN-15 subgroup 988 |

## Value Type Codes

| Code | Description |
|------|-------------|
| `M` | Manual measurement |
| `A` | Automatic measurement |
| `E` | Estimated value |
| `I` | Imported value |
| `U` | Unknown source |
| `O` | Override (manually entered by hydrologist) |
