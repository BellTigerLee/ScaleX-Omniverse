<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# datacenter_monitor_python

## Purpose
The Kit extension Python package loaded inside Omniverse. `extension.py` (`omni.ext.IExt`) is the entry point and the only place that touches Kit lifecycle. It starts three background Kafka subscriber threads (metrics, node-state, cluster-rank) that push parsed dicts into thread-safe queues; `_on_update` runs every frame on the Kit main thread, drains those queues (bounded per-frame), and calls into `SceneManager`. `MessageHandler` bridges WebRTC messages to/from the React dashboard and decides A→B→C→D view-stage transitions. **USD may only be mutated on the Kit main thread** — Kafka threads never touch USD.

## Key Files
| File | Description |
|------|-------------|
| `README.md` | Korean architecture doc + **the WebRTC message-type tables** (React→Kit, Kit→React) + Kafka message schemas + topic table. **Mirror of the React `OmniverseViewer.jsx` tables — change both** |
| `extension.py` | Entry point: starts/stops `KafkaSubscriber`/`NodeStateSubscriber`/`ClusterRankSubscriber`, owns `self._view_stage` (single source of truth for stage), per-frame `_on_update` queue drain, stage-ready retry, replay topic switching, selection→React forwarding |
| `global_variables.py` | The tuning surface: USD hierarchy (`/World/SCENT_Multi_POD_Module/ScaleX_POD/{Name}_Cluster/Rack_{Name}/Box_/Server_`), prim prefixes (`CLUSTER_SUFFIX`/`RACK_PREFIX`/`BOX_PREFIX`/`SERVER_PREFIX`), Kafka topic names, `MAIN_STAGE_USD_PATH`, color/emissive thresholds, camera offsets, pulse params. Reads `config/` at import time |
| `config_loader.py` | Endpoint-profile loader: `resolve_profile_path()` / `parse_profile()` / `load_profile()`. Stdlib-only (importable in tests without Kit) |
| `kafka_subscriber.py` | Three consumer thread classes + pure parsers `parse_node_state_message` / `parse_cluster_rank_message`; confluent-kafka preferred, kafka-python fallback; `switch_topic()` for live↔replay |
| `message_handler.py` | WebRTC router: inbound via `carb.eventdispatcher` event `"datacenter_monitor"`; outbound via `omni.kit.app.queue_event("omni.kit.livestream.send_message", ...)`. Decides stage transitions |
| `scene_manager.py` | Thin facade composing the `scene/` mixins; owns `__init__`/`initialize()`/`cleanup()`/`scene_reset()` and the public API surface |
| `__init__.py` | `import carb` + re-export of `Extension` (Omniverse-only; stubbed out in tests) |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `scene/` | All real USD logic as `SceneManager` mixins (camera, topology, visibility/stages, material, pulse, alerts, net-line) + pure parsers (see `scene/AGENTS.md`) |
| `tests/` | Pytest unit tests for the stdlib/pure-Python paths (see `tests/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- Preserve the **queue boundary**: Kafka threads parse + enqueue only; scene mutation happens solely in `_on_update`. Three queues exist (`_kafka_queue` maxsize 500, `_event_queue`, `_cluster_rank_queue`).
- Node coloring is driven by `node-state.events` → `SceneManager.apply_node_state` → `tick_pulse`. The `update_node_color_from_kafka` path on `datacenter.metrics` is **intentionally commented out** in `_on_update`; do not re-enable without intent. Legacy `EventKafkaSubscriber`/`show_event_panel` (HEALTH_TRANSITION severity) path is also commented out (Phase 2 reasons-driven UI planned).
- Replay: `_on_replay_start/_stop` clear queues then `switch_topic()` to `datacenter.metrics.replay[.event]` and back to live.
- Stage readiness uses a frame-delayed retry loop (`_schedule_stage_ready` / `_process_stage_ready_retry`) to avoid racing USD reference loading before topology + glass-cube creation succeed.
- Add new scene behavior as a `scene/` mixin (each owns `_init_*()` called from `SceneManager.__init__`); do not fatten `scene_manager.py` or `extension.py`.

### Testing Requirements
- `cd ..` (extension root) then `pytest`. Pure modules (`config_loader`, `kafka_subscriber` parsers, `scene/node_metrics`, `scene/node_state_pulse`, `scene/node_index`) are import-safe without Kit; `carb`/Kit imports are stubbed by the root `conftest.py`.

### Common Patterns
- Korean comments + `[수정]` markers; `print("[Datacenter Monitor] ...")` logging.
- View stages: `"A"` full scene → `"B"` cluster → `"C"` rack → `"D"` node inspect, stored in `self._view_stage["stage"]`.

## Dependencies

### Internal
- `config/` profiles (via `config_loader`/`global_variables`); `scene/` mixins; assets at `MAIN_STAGE_USD_PATH`.

### External
- `omni.ext`, `omni.kit.app`, `omni.usd`, `carb`, `pxr`; `confluent-kafka`/`kafka-python`.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
