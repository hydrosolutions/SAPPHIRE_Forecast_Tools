# Monitoring Scripts

Scripts for monitoring SAPPHIRE forecast tools logs and health.

## Available Scripts

| Script | Description | Log File |
|--------|-------------|----------|
| `preprunoff.sh` | Monitor preprocessing_runoff maintenance logs | `/home/ubuntu/logs/sapphire_preprunoff_maintenance.log` |
| `docker.sh` | Monitor Docker container health and status | Docker daemon |
| `docker_log_watcher.sh` | Watch Docker container logs | Docker container logs |

## Usage

### Quick Summary
```bash
./bin/monitoring/preprunoff.sh
```

### Live Monitoring
```bash
./bin/monitoring/preprunoff.sh -t
```

### Check Specific Sites
```bash
./bin/monitoring/preprunoff.sh -c 16059 -c 15020
```

### Custom Log File
```bash
./bin/monitoring/preprunoff.sh -f /path/to/logfile.log
```

## Exit Codes

All monitoring scripts use consistent exit codes:

| Code | Meaning |
|------|---------|
| 0 | OK - No issues detected |
| 1 | Warning - Check output for details |
| 2 | Error - Errors found in logs |
| 3 | Log file not found or not readable |

## Adding New Monitoring Scripts

When adding a new monitoring script:
1. Use consistent naming: `<module>.sh`
2. Support common flags: `-f` (file), `-n` (lines), `-t` (tail), `-h` (help)
3. Use consistent exit codes (see above)
4. Update this README

## Alert email subjects and `SAPPHIRE_ALERT_ORG`

All alert emails use the subject form

    [SAPPHIRE <label> <CATEGORY>] <text>

where `<CATEGORY>` is `DOCKER`, `DASHBOARD`, `PIPELINE` or `HEALTHCHECK`.

`<label>` is resolved in this order (shell scripts): `SAPPHIRE_ALERT_ORG` in
the process environment (e.g. a systemd `Environment=` line), then a
`SAPPHIRE_ALERT_ORG=` line in the env file, then
`ieasyhydroforecast_organization`, then the hostname. Cron-run scripts such
as `email_healthcheck_report.sh` do not see systemd `Environment=` lines, so
on a demo/staging box put `SAPPHIRE_ALERT_ORG=kghm-demo` in the env file.
The pipeline (Python) uses the same order minus the hostname (it would be a
container id) and falls back to `unknown`. The env file is loaded into the
container environment, so the same line labels pipeline emails.
