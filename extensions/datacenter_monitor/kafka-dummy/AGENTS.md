<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# kafka-dummy

## Purpose
A self-contained Docker demo backend that produces the data the extension consumes, with no real datacenter. **This is the OLD demo backend bundled with the extension repo — it is distinct from the current standalone `ScaleX-QueryServer` repo.** `docker-compose.yaml` orchestrates Kafka (KRaft) → MinIO → Nessie (Iceberg REST catalog) → Trino, then one-shot `seed-data` → `loop-producer`, with a FastAPI `query-server` last. `seed.py` writes 300s × 13 boxes = 3,900 rows to Iceberg table `dc.metrics_seed`; the loop-producer replays them to Kafka `datacenter.metrics`; query-server serves the React dashboard.

## Key Files
| File | Description |
|------|-------------|
| `README.md` | Korean service-stack overview, startup order, quick-start, endpoint/port table, pipeline detail |
| `docker-compose.yaml` | The full stack: `kafka` (apache/kafka:4.0.0), `minio`/`minio-init`, `nessie`, `trino` (trinodb/trino:435), `seed-data`, `loop-producer`, `event-producer`, `query-server`. Containers prefixed `dc_` |
| `Dockerfile` | Image for `dummy_producer.py` (a CLI producer driven by `KAFKA_BROKER`/`KAFKA_TOPIC`/`INTERVAL`) |
| `dummy_producer.py` | Standalone CLI metric producer (alternative to loop-producer) |
| `OTel-Protocol.xlsx` | OpenTelemetry protocol reference (binary) |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `seed-data/` | One-shot Iceberg seeder `seed.py` → `dc.metrics_seed` (see `seed-data/AGENTS.md`) |
| `loop-producer/` | Reads `dc.metrics_seed`, loops it to Kafka `datacenter.metrics` (see `loop-producer/AGENTS.md`) |
| `event-producer/` | Reads Iceberg events table, loops HEALTH_TRANSITION events to `datacenter.metrics.event` (see `event-producer/AGENTS.md`) |
| `kafka-ingestor/` | Kafka→Iceberg ingestor (`datacenter.metrics` → `dc.metrics`); represents Spark/Flink in production (see `kafka-ingestor/AGENTS.md`) |
| `query-server/` | FastAPI backend: live cache, history via Trino, replay engine (see `query-server/AGENTS.md`) |
| `trino/` | Trino coordinator config + Iceberg/Nessie catalog properties (see `trino/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- This stack is **demo infrastructure, not part of the shipped extension** — and it is the *older* backend. The current production backend is the standalone `ScaleX-QueryServer` repo; don't confuse the two.
- Bring it up: `docker compose up -d --build`. Healthy when `curl http://localhost:8000/health` reports `live_boxes == 13`. First start takes ~1–2 min (seed-data must finish before loop-producer sends).
- Reseed after schema changes: `docker compose down -v && docker compose up -d --build`.
- Inside containers the services use k8s-style DNS names (`nessie`, `minio`, `kafka`); the production env defaults in `config.py` files point at the cluster (`...observability.svc.cluster.local`).

### Testing Requirements
- Only `query-server/` has unit tests (`cd query-server && pytest tests/`). The producers/ingestor/seeder are exercised by running the compose stack and checking logs (`docker compose logs -f <service>`).

### Common Patterns
- Each producer service: `Dockerfile` + a single Python entry script + (sometimes) `config.py` of `os.getenv(...)` defaults. Korean docstrings document the Kafka message format.

## Dependencies

### Internal
- Produces the topics consumed by `datacenter_monitor_python` (`KafkaSubscriber` etc.); `query-server` serves `ScaleX-Twin-Web`.

### External
- Docker Compose; Kafka, MinIO (S3), Project Nessie, Trino; `pyiceberg`, `confluent-kafka`, `aiokafka`, FastAPI, `trino`/SQLAlchemy.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
