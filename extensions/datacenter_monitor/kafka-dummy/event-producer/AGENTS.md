<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# event-producer

## Purpose
Reads the Iceberg events table and loops `HEALTH_TRANSITION` event messages to Kafka topic `datacenter.metrics.event`, preserving original `ts` gaps but re-stamping timestamps to current ISO-8601 UTC, then repeating forever. **The extension's consumer for this topic is currently disabled** (superseded by the canonical `datacenter.metrics.node-state.events` topic), so this producer feeds a legacy path retained for reference / Phase 2.

## Key Files
| File | Description |
|------|-------------|
| `event_producer.py` | Loads `dc.node_events` (creates/retries up to 5 min), sends each event by `ts` order, re-stamps `ts`, loops infinitely. Emits the HEALTH_TRANSITION envelope (`event_id`, `cluster`, `rack`, `node`, `severity`, `from`/`to`, `score`, `reason`, ...) |
| `config.py` | `os.getenv` defaults: `KAFKA_BROKER`, `KAFKA_TOPIC=datacenter.metrics.event`, Nessie/MinIO catalog, Iceberg `infra.node_events` |
| `requirements.txt` | `pyiceberg[pyarrow,s3fs]`, `pyarrow`, `s3fs`, `boto3`, `confluent-kafka` |
| `Dockerfile` | Builds the producer image |

## For AI Agents

### Working In This Directory
- This topic's extension consumer is commented out (see `extension.py::_on_update` and `datacenter_monitor_python/README.md`). Changing the event schema does **not** currently affect the live scene; coordinate with the node-state path if reviving it.
- Production catalog/broker defaults point at k8s DNS (`...svc.cluster.local`); override via env in compose.

### Testing Requirements
- No unit tests. Verify by running the stack and `docker compose logs -f event-producer`.

### Common Patterns
- Single entry script + `config.py` env defaults; Korean docstring documents the message format.

## Dependencies

### Internal
- Produces `datacenter.metrics.event` (legacy consumer in `datacenter_monitor_python`, currently unused).

### External
- `pyiceberg`, `confluent-kafka`, Nessie, MinIO.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
