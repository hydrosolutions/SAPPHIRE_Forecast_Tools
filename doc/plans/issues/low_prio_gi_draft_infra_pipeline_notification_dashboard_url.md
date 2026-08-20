# INFRA-036: Pipeline completion email sends a blob of host:port pairs instead of a dashboard link

**Status**: Draft
**Module**: infra — `apps/pipeline/pipeline_docker.py`
**Priority**: Low (cosmetic, but operator-facing and currently wrong in production)
**Labels**: `bug`, `pipeline`, `notifications`
**Found while**: mapping the origin allow-list for INFRA-032. Not fixed inline, per CLAUDE.md.

---

## Summary

`apps/pipeline/pipeline_docker.py:1377` reads `ieasyhydroforecast_url` and puts it into
the pipeline-completion email:

```python
dashboard_url = os.getenv("ieasyhydroforecast_url", "")
if dashboard_url:
    message += f"View the latest forecasts on the dashboard: {dashboard_url}\n\n"
```

The variable is designed to hold **one base URL**. On deployments that use it to carry
the WebSocket origin allow-list — a comma-separated list of `host:port` entries, which
is what kghm, taj and uzb all currently do — the email instead reads:

> View the latest forecasts on the dashboard: a:5006,b:5006,c:5006,d:5006

Not a link. Not clickable. Every completed pipeline run, to every recipient.

## Why it is not fixed by INFRA-032

INFRA-032 makes `ieasyhydroforecast_url_pentad` / `_decad` the place to configure
origins, which *allows* a deployment to stop overloading the base variable — but it does
not change any deployment's env file, and nothing forces the migration. Deployments that
keep the comma list in the base variable keep sending the broken notification. So this
survives that change and needs its own fix.

## Options

- **(a) Fix at the consumer.** Take the first comma-separated entry, and prepend a scheme
  if absent, before putting it in the message. Tolerates both the clean and the
  overloaded form. Smallest change, no deployment coordination.
- **(b) Introduce a dedicated variable** (e.g. `ieasyhydroforecast_dashboard_link`) that
  holds exactly the URL operators should click, and fall back to today's behaviour when
  unset. Cleanest semantics; needs an env-file edit per deployment to take effect.

Recommend (a) plus a note in `doc/configuration.md` that the base variable should hold a
single URL. (b) is worth it only if the notification text grows further.

## Note for whoever picks this up

Check whether anything else consumes `ieasyhydroforecast_url` expecting a single URL.
At the time of writing the consumers are: the derivation in
`bin/utils/common_functions.sh`, this notification, an `echo` at
`common_functions.sh:269`, a log line at `apps/forecast_dashboard/src/environment.py:78`,
and the `CMD` default in `apps/forecast_dashboard/Dockerfile:42` — the last of which
would also mis-handle a comma list, though both Compose files override that `CMD`.

## Acceptance criteria

- With `ieasyhydroforecast_url` holding a comma-separated list, the completion email
  contains exactly one usable URL.
- With it holding a single base URL, the email is unchanged from today.
- A test covers both forms. Use placeholder hosts only — no real hostnames or IPs.
