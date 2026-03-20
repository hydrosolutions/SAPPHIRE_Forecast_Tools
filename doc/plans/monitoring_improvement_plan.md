# Monitoring Improvement Plan

## Current State

- **Email notifications exist** (pipeline module) but are ignored
- **Monitoring scripts exist** in `bin/monitoring/` for manual use
- **Docker monitoring daemons** exist but not deployed

## Core Problem

**Notification fatigue**: Email alerts are sent but not read. More emails won't help.

## Proposed Approach

### Option A: Daily Digest (Low Effort)

Replace constant alerts with a single daily summary:
- One email per day at fixed time (e.g., 07:00)
- Only if there are issues to report
- Clear, scannable format

**Pros**: Simple, reduces noise
**Cons**: Delayed awareness of critical issues

### Option B: Escalating Alerts (Medium Effort)

- Normal issues → logged only, included in daily digest
- Critical issues (container crash, no data for 24h+) → immediate alert
- Reduce alert frequency to only truly urgent issues

**Pros**: Important things still get immediate attention
**Cons**: Need to define what's "critical"

### Option C: Dashboard (Higher Effort)

- Simple status dashboard (could be static HTML updated by cron)
- Green/yellow/red indicators for each module
- Operators check dashboard instead of reading emails

**Pros**: Visual, quick to scan
**Cons**: Requires someone to check it

### Option D: Slack/Teams Integration (Medium Effort)

- Send alerts to a channel instead of email
- More visible, can be muted when needed
- Supports threading for related alerts

**Pros**: Better visibility than email
**Cons**: Requires Slack/Teams setup

## Recommendation

Start with **Option B (Escalating Alerts)** combined with a daily digest:

1. **Daily digest email** (07:00): Summary of last 24h
   - Sites processed, data freshness
   - Any warnings/errors (count only)
   - Quick "all OK" or "issues found" status

2. **Immediate alerts only for**:
   - Container crash (won't recover without intervention)
   - No data fetched for 48+ hours (forecast will fail)
   - Disk space critical

3. **No immediate alert for**:
   - Individual site fetch failures (logged, in digest)
   - Meteo-only sites (logged, in digest)
   - Recoverable errors

## Scripts to Update

| Script | Current | Proposed |
|--------|---------|----------|
| `docker.sh` | Alerts on every crash | Alert only + add to digest |
| `docker_log_watcher.sh` | Alerts on 404/ERROR patterns | Daily digest only |
| `preprunoff.sh` | Manual only | Add to daily digest |

## Next Steps

- [ ] Decide on approach (A, B, C, or D)
- [ ] Define "critical" vs "digest-only" thresholds
- [ ] Create daily digest script
- [ ] Update existing alert scripts

## Related

- `bin/monitoring/README.md` - Script documentation
- Pipeline email notification code (existing)
