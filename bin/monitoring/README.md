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
