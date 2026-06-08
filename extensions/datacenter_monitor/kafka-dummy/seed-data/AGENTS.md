<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# seed-data

## Purpose
One-shot Iceberg seeder. `seed.py` generates a deterministic 600-second (10-minute) scenario of nested metrics for 13 boxes and writes them to the single Iceberg table `dc.metrics_seed` (created once; skipped if the table already exists). All history and replay in the demo derive from this one table. Node behavior is classified by name (Switch / DTN / HDD / NVMe / Control) with type-specific metric profiles.

## Key Files
| File | Description |
|------|-------------|
| `seed.py` | Builds `dc.metrics_seed` via `pyiceberg`. Scenario: `0–299s` WARNING ramp, `300–539s` WARNING+CRITICAL, `540–599s` recovery. Alarm thresholds: cpu_util >0.75/>0.90, sys_temp >70/>85°C |
| `Dockerfile` | `python:3.11-slim` + `pyiceberg[pyarrow,s3fs]`, `pyarrow`, `s3fs`, `boto3`; `CMD python seed.py` |

## For AI Agents

### Working In This Directory
- Idempotent by design: if `dc.metrics_seed` exists it skips. To force a reseed after schema changes: `docker compose down -v && docker compose up -d --build` (the `-v` drops MinIO volumes).
- The emitted nested-metrics schema (`cpu`/`mem`/`net`/`gpu`/`storage`) is the contract shared with `loop-producer`, `kafka-ingestor`, the extension parser, and `trino_client`. Change all together.
- The README notes 300s × 13 = 3,900 rows; the seeder's own docstring describes a 600s scenario — verify the actual row count from the table when it matters.

### Testing Requirements
- No unit tests. Verify with `docker compose logs -f seed-data` (container `dc_seed_data` should reach Exited 0) and then `/health` → `live_boxes: 13`.

### Common Patterns
- Single entry script; `os.getenv` defaults for catalog/S3; Korean docstring documents the scenario and `[수정]` reseed instructions.

## Dependencies

### Internal
- Produces Iceberg `dc.metrics_seed`, consumed by `loop-producer` and `query-server` (Trino).

### External
- `pyiceberg`, `pyarrow`, MinIO, Nessie.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
