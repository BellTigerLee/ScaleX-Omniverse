<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# kafka-ingestor

## Purpose
Consumes the Kafka `datacenter.metrics` topic and batch-loads it into the Iceberg `dc.metrics` table. In a real pipeline this role is performed by Spark Structured Streaming / Flink; here it is a lightweight `aiokafka` + `pyiceberg` stand-in. Note this is the **inverse** direction of `loop-producer` (Kafka → Iceberg, vs Iceberg → Kafka).

## Key Files
| File | Description |
|------|-------------|
| `ingestor.py` | `AIOKafkaConsumer` reads the nested metrics envelope (`cpu`/`mem`/`net`/`gpu`/`storage`), batches rows via PyArrow, writes to Iceberg `dc.metrics`. Catalog/S3 via env (`CATALOG_URI`, `S3_ENDPOINT`, MinIO creds, `WAREHOUSE`) |
| `Dockerfile` | Builds the ingestor image |

## For AI Agents

### Working In This Directory
- The expected Kafka message is the nested-metrics format documented in the file's Korean docstring — keep it aligned with `loop-producer` / `seed-data` output if you change the schema.
- Not all compose configurations enable this service; treat it as an optional demo component representing the streaming-ingest stage.

### Testing Requirements
- No unit tests. Verify via the running stack and `docker compose logs -f kafka-ingestor`.

### Common Patterns
- `os.getenv(...)` env defaults at module top; async consume loop; Korean docstring documents message format.

## Dependencies

### Internal
- Consumes `datacenter.metrics` (same topic the extension consumes); writes Iceberg `dc.metrics`.

### External
- `aiokafka`, `pyiceberg`, `pyarrow`, MinIO, Nessie.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
