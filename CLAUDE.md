# CLAUDE.md - Agent Instructions for SAPPHIRE Forecast Tools

## Project Context

This is the SAPPHIRE Forecast Tools repository - an open-source operational runoff forecasting toolkit. For detailed project information, **consult the Serena memories**.

## Required: Read Serena Memories

Before working on any task, read the relevant Serena memories using the MCP tools:

```
mcp__serena__list_memories()           # See available memories
mcp__serena__read_memory(memory_file_name="project_overview")
mcp__serena__read_memory(memory_file_name="code_style_conventions")
mcp__serena__read_memory(memory_file_name="suggested_commands")
mcp__serena__read_memory(memory_file_name="task_completion_checklist")
```

## Key Memories

| Memory | Contents |
|--------|----------|
| `project_overview` | Architecture, modules, tech stack, deployment info |
| `code_style_conventions` | Coding standards, patterns, naming conventions |
| `suggested_commands` | Common commands for testing, building, running |
| `task_completion_checklist` | Quality checks before completing tasks |

## Quick Reference

### Project Structure
- `apps/` - Main modules (each has `src/`, `tests/`, `pyproject.toml`)
- `apps/iEasyHydroForecast/` - Core library used by all other modules
- `apps/pipeline/` - Luigi orchestration for Docker containers
- `doc/plans/` - Planning documents and issue tracking

### Tech Stack
- Python 3.12, uv package manager
- Docker containers orchestrated by Luigi
- pytest for testing
- GitHub Actions CI/CD

### Current Migration
Python 3.11 → 3.12 with uv. See `doc/plans/uv_migration_plan.md`

### Known Issues
See `doc/plans/module_issues.md` for issue index, `doc/plans/issues/` for detailed plans.

## Module-Specific Notes

### preprocessing_runoff
- Fetches runoff data from iEasyHydro HF database
- Issue PR-001: Data not updating in Docker (date calculation bug)

### pipeline
- Runs as root (required for Docker socket access)
- Issue P-001: Marker files owned by root accumulate
- Marker cleanup task exists (`DeleteOldMarkerFiles`) but may have permission issues

### forecast_dashboard
- Integration tests disabled by default (TEST_LOCAL, TEST_PENTAD, TEST_DECAD env vars)
- Requires Playwright for browser testing

## Environment Variables

Key variables (see `.env` files in deployment):
- `ieasyforecast_intermediate_data_path` - Data storage path
- `ieasyhydroforecast_data_dir` - Root data directory
- `IN_DOCKER_CONTAINER` - Set to "True" when running in Docker
- `SAPPHIRE_TEST_ENV` - Set to "True" for test mode
