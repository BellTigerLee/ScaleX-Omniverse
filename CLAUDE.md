# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single NVIDIA **Omniverse Kit extension** — `extensions/datacenter_monitor/` — that renders a real-time datacenter digital twin. It consumes Kafka metrics/events, recolors USD scene prims by node health, and exchanges WebRTC messages with a React dashboard. It is **USD Viewer based, with no Isaac Sim / omni.physx dependency** (the code repeatedly notes this — do not add physics, timeline play/stop, or `isaacsim.*` imports).

Most code comments and the per-directory READMEs are in Korean. Match that when editing existing files.

The repo root is mostly empty; everything lives under `extensions/datacenter_monitor/`:
- `datacenter_monitor_python/` — the Kit extension Python module (loaded inside Omniverse).
- `kafka-dummy/` — a self-contained Docker demo backend (Kafka + MinIO + Nessie + Trino + a FastAPI `query-server`) that produces the data the extension consumes. This is **demo infrastructure**, not part of the shipped extension.
- `config/` — endpoint profiles (see below).
- `import_python.py` — a standalone Omniverse Script Editor snippet (UV-animated emissive cylinder); not imported by the extension.

## Commands

Two independent pytest suites. Both run as **plain Python** (the extension suite stubs out `carb`/Kit so Omniverse is not required) — use the project's venv / Kit Python, since system Python lacks `pytest`.

```bash
# Extension unit tests — run from the extension root (pytest.ini lives there)
cd extensions/datacenter_monitor && pytest
pytest datacenter_monitor_python/tests/test_config_loader.py::test_name   # single test

# Query-server unit tests — run from the query-server dir (tests import modules flat, e.g. `from node_state_cache import ...`)
cd extensions/datacenter_monitor/kafka-dummy/query-server && pytest tests/
```

```bash
# Demo backend — full stack
cd extensions/datacenter_monitor/kafka-dummy && docker compose up -d --build
curl http://localhost:8000/health        # healthy when live_boxes == 13
docker compose logs -f loop-producer
```

The extension itself is not "run" from the CLI — it is loaded inside an Omniverse Kit app via the Extension Manager pointed at `extensions/datacenter_monitor/`. Requires `kafka-python` (or `confluent-kafka`) installed into the Kit Python. See `extensions/datacenter_monitor/README.md`.

## Architecture

### Extension runtime (the critical threading model)

`extension.py` (`omni.ext.IExt`) is the entry point and the only place that touches Kit lifecycle. The central rule everything is built around: **USD may only be mutated on the Kit main thread.** So:

- Three background **Kafka consumer threads** (`KafkaSubscriber`, `NodeStateSubscriber`, `ClusterRankSubscriber` in `kafka_subscriber.py`) only parse messages and push dicts into thread-safe `queue.Queue`s. They never touch USD.
- `extension.py::_on_update` runs every frame on the main thread, drains those queues (bounded per-frame to avoid frame drops), and calls into `SceneManager`. This is the one place data crosses from Kafka threads into the scene.

When changing data flow, preserve this queue boundary — do not call `SceneManager` from a Kafka thread.

### SceneManager — mixin facade

`scene_manager.py` is a thin facade. All real USD logic lives in mixins under `scene/`, composed via multiple inheritance:

| Mixin file | Responsibility |
|---|---|
| `scene/camera.py` | camera position + zoom animation (per-frame tick) |
| `scene/topology.py` | discover/index the USD cluster→rack→node hierarchy |
| `scene/node_visibility.py` | visibility + Stage A/B/C/D transitions, node pop-forward/dim |
| `scene/material.py` | glass-cube overlay creation, health-color updates |
| `scene/node_state_pulse.py` | emissive breathing pulse driven by node-state messages |
| `scene/node_index.py` | (cluster, node) → prim-name resolution from the Topology API |
| `scene/alert.py`, `scene/event_alert.py`, `scene/net_line_anim.py` | rack alert markers, transient event ImagePanels, network line animation |

Each mixin owns an `_init_*()` called from `SceneManager.__init__`, and its caches are cleared in `initialize()`/`cleanup()`. Add new scene behavior as a mixin following this pattern rather than fattening `scene_manager.py`.

### View stages (A→B→C→D)

The whole interaction model is a 4-level zoom: **A** full scene → **B** cluster focus → **C** rack focus → **D** node inspect. `extension.py` holds `self._view_stage = {"stage": ...}` as the single source of truth; `MessageHandler` decides transitions from React messages and reports back via the `on_view_stage_change` callback.

### React ↔ Kit messaging

`message_handler.py` bridges WebRTC. Inbound: React `AppStreamer.sendMessage` → `omni.kit.livestream.messaging` → `carb.eventdispatcher` event `"datacenter_monitor"` → `MessageHandler`. Outbound: `omni.kit.app.queue_event("omni.kit.livestream.send_message", ...)`. Message-type tables (`cluster_focus`, `rack_focus`, `node_inspect`, `replay_start/stop`, `get_topology`, etc. inbound; `selection_changed`, `scene_manifest`, `cluster_rank` outbound) are documented in `datacenter_monitor_python/README.md` — keep that table in sync when adding messages.

### Configuration — endpoint profiles

Kafka endpoint (and optional Topology API URL) is **never hardcoded**. `config_loader.py::load_profile()` reads a flat `KEY=VALUE` file from `config/`, resolved in order: `DC_PROFILE` env var (`config/env.<DC_PROFILE>`) → `config/active` symlink → `config/env.default`. Required keys: `CLUSTER_HOST`, `KAFKA_NODEPORT`. `global_variables.py` consumes this at import time. The extension connects as an **external** Kafka client via a k8s NodePort — k8s-internal DNS does not apply.

### global_variables.py — the tuning surface

This file is the single place for all tunables and, importantly, the **assumed USD scene hierarchy** (`/World/SCENT_Multi_POD_Module/ScaleX_POD/{Name}_Cluster/Rack_{Name}/Box_{Name}|Server_{Name}`) plus the prim-name prefixes (`CLUSTER_SUFFIX`, `RACK_PREFIX`, `BOX_PREFIX`, `SERVER_PREFIX`) that `scene/topology.py` matches on. If the USD asset's naming changes, this is what you edit — there is no other config for it. It also holds color/emissive thresholds, camera offsets, pulse parameters, and `MAIN_STAGE_USD_PATH` (auto-loaded on startup if it exists).

### Kafka topics

| Topic | Purpose | Consumer in extension |
|---|---|---|
| `datacenter.metrics` | live metrics (live cache; color path currently disabled) | `KafkaSubscriber` |
| `datacenter.metrics.node-state.events` | canonical HEALTHY/DISCONNECTED node-state envelope (Flink) — drives color + pulse | `NodeStateSubscriber` → `apply_node_state` → `tick_pulse` |
| `datacenter.metrics.stageab` | per-cluster CPU rank, forwarded to React | `ClusterRankSubscriber` |
| `datacenter.metrics.replay[.event]` | replay topics produced by query-server's ReplayEngine | topic-switch on `replay_start/stop` |
| `datacenter.metrics.event` | legacy HEALTH_TRANSITION events — **superseded, do not wire up** | unused |

Node coloring is driven by `node-state.events`, **not** by the `status` field on `datacenter.metrics` (that path is intentionally commented out in `_on_update` — the dashboard `status` is a placeholder). Schema details and the canonical envelope fields live in `datacenter_monitor_python/README.md` and `ScaleX-POD.md`.

### Demo backend (`kafka-dummy/`)

`docker-compose.yaml` orchestrates the stack; startup order is `kafka → minio → nessie → trino`, then `seed-data` (one-shot, writes 3,900 rows to Iceberg table `dc.metrics_seed`) → `loop-producer`, and `query-server` last. The FastAPI `query-server/` serves the React dashboard: `live_cache.py` keeps latest metrics in memory (so `/metrics/latest` skips Trino), `trino_client.py` queries Iceberg history, and `replay_engine.py` replays seed data simultaneously to a Kafka replay topic and a `/ws/replay` WebSocket. Endpoint list is in `kafka-dummy/README.md`.
