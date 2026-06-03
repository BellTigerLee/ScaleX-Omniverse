<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# query-server

## Purpose
The FastAPI backend bundled with the demo stack — the React dashboard's HTTP/WebSocket server. **This is the OLD demo backend; the current production backend is the standalone `ScaleX-QueryServer` repo.** `live_cache.py` keeps the latest metrics in memory (so `/metrics/latest` skips Trino), `trino_client.py` queries the Iceberg `dc.metrics_seed` history, and `replay_engine.py` replays seed data **simultaneously** to a Kafka replay topic and a `/ws/replay` WebSocket so the 3D scene and dashboard stay frame-aligned.

## Key Files
| File | Description |
|------|-------------|
| `main.py` | FastAPI app + routes: `/health`, `/topology`, `/metrics/latest`, `/metrics/{box_id}/history`, `/replay/{start,pause,resume,stop,status}`, `WS /ws/replay`. (`/turn-credentials` is commented out — direct WebRTC only) |
| `config.py` | `os.getenv` defaults: Kafka broker/topics (`datacenter.metrics`, `.replay`, `.event`, `.replay.event`, `.node-state.events`), Trino host/catalog/schema, `LIVE_CACHE_SIZE` |
| `live_cache.py` | `LiveCache` — `aiokafka` consumer keeping per-box recent message deque in memory |
| `replay_engine.py` | `ReplayEngine` — loads a Trino range, replays at original interval / `speed`, dual-publishes to Kafka replay topic + WebSocket with `playback_ts`/`original_ts` |
| `trino_client.py` | Trino/SQLAlchemy queries against `dc.metrics_seed`; `DEMO_EPOCH=1735689600` offset conversion (demo `ts` = 0–599s offsets) |
| `node_state_cache.py` | `NodeStateCache` — consumes `datacenter.metrics.node-state.events`, keeps latest envelope per (cluster, node), overwrite-only, no TTL |
| `topology_db.py` / `topology_seed.py` | PostgreSQL topology helpers (seed from `topology.json`; `get_topology`/`get_prim_for_node`). **Note: Postgres is not in this compose file** — these are an optional/aspirational path |
| `requirements.txt` | FastAPI, uvicorn, `aiokafka[zstd]`, `confluent-kafka`, `trino`, SQLAlchemy(+trino), `httpx`, `psycopg2-binary` |
| `Dockerfile` | Builds the server image |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `tests/` | Pytest for `node_state_cache` (flat imports) (see `tests/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- Modules import each other **flat** (e.g. `from config import ...`, `from node_state_cache import ...`) — run/test from this directory, not the repo root.
- This is the demo backend. For production behavior consult the standalone `ScaleX-QueryServer` repo (its `CLAUDE.md`); avoid conflating the two when reasoning about replay/topology contracts.
- Replay is dual-published (Kafka + WebSocket) — keep both legs in sync if you touch the cadence/payload.

### Testing Requirements
- `cd query-server && pytest tests/` (or `pytest tests/test_node_state_cache.py::test_name`). No `pytest.ini`; tests rely on flat imports from this directory.

### Common Patterns
- `os.getenv` config defaults; async Kafka/Trino; Korean module docstrings document routes and data flow.

## Dependencies

### Internal
- Consumes Kafka from the demo producers; serves `ScaleX-Twin-Web`; replay topic is consumed by `datacenter_monitor_python` via `switch_topic`.

### External
- FastAPI/uvicorn, `aiokafka`, `confluent-kafka`, Trino + SQLAlchemy, optional PostgreSQL.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
