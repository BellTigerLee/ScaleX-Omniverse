<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# trino

## Purpose
Configuration for the demo's Trino SQL engine, which queries the Iceberg `dc.metrics_seed` table for history and replay. Trino reads the same MinIO-backed Iceberg tables (via the Nessie REST catalog) that `seed-data` and the producers write.

## Key Files
None directly here — configuration lives under `etc/`.

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `etc/` | Trino node/coordinator config + catalog properties |

## For AI Agents

### Working In This Directory
- `etc/config.properties`: single-node coordinator, `http-server.http.port=8080`, discovery to localhost. `etc/jvm.config` / `etc/node.properties` are standard Trino bootstrap.
- `etc/catalog/iceberg.properties`: Iceberg connector using `iceberg.catalog.type=rest` → Nessie (`http://nessie:19120/iceberg/`), warehouse `s3://warehouse/`, MinIO S3 (`http://minio:9000`, `minioadmin`/`minioadmin`, path-style). Hostnames are compose-internal service names.
- Mounted read-only into the `trino` container by `docker-compose.yaml`. Edit here, then `docker compose restart trino`.

### Testing Requirements
- No tests. Verify via the Trino UI (`http://localhost:8080`) or query through the query-server `/metrics/*/history` endpoint.

### Common Patterns
- Plain Trino property files; comments are Korean.

## Dependencies

### Internal
- Backs `query-server/trino_client.py` history/replay queries.

### External
- Trino (`trinodb/trino:435`), Nessie, MinIO.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
