<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# loop-producer

## Purpose
The live-metrics producer for the demo. Loads the `dc.metrics_seed` table that `seed-data` created and replays it to Kafka topic `datacenter.metrics` in `ts` order, preserving the original 1-second cadence but re-stamping each message to the current Unix-ms time, then loops forever. This is the data the extension's `KafkaSubscriber` and the query-server's `LiveCache` consume. In production, Spark Streaming / Flink replaces this.

## Key Files
| File | Description |
|------|-------------|
| `loop_producer.py` | Loads `dc.metrics_seed` (retries up to 5 min), sends nested-metrics messages (`cpu`/`mem`/`net`/`gpu`/`storage`), re-stamps `ts`, loops. Uses `confluent_kafka.Producer`; catalog/S3 via env |
| `Dockerfile` | Builds the producer image |

## For AI Agents

### Working In This Directory
- The emitted message schema (nested `metrics` with cpu/mem/net/gpu/storage) must match what `seed-data` writes and what `datacenter_monitor_python` parses. The `status` field is a placeholder — node coloring is driven by the separate node-state topic, not by this producer.
- Depends on `seed-data` having finished (Exited 0); compose ordering enforces this.

### Testing Requirements
- No unit tests. Verify via `docker compose logs -f loop-producer` and the `/metrics/latest` endpoint.

### Common Patterns
- `os.getenv(...)` defaults; Korean docstring documents the exact message format.

## Dependencies

### Internal
- Reads Iceberg `dc.metrics_seed`; produces `datacenter.metrics`.

### External
- `confluent-kafka`, `pyiceberg`, Nessie, MinIO.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
