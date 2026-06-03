<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# datacenter_monitor

## Purpose
The main NVIDIA Omniverse Kit extension. It renders a real-time datacenter digital twin: three background Kafka consumer threads parse messages into thread-safe queues, the Kit per-frame update drains them on the main thread and drives `SceneManager` (USD recoloring, camera animation, node pop-forward, alert markers), and a `MessageHandler` bridges WebRTC messages to/from the React dashboard. Node coloring is driven by the canonical `datacenter.metrics.node-state.events` topic — **not** by the `status` field on `datacenter.metrics` (that path is intentionally commented out). USD Viewer based; no Isaac Sim / omni.physx.

## Key Files
| File | Description |
|------|-------------|
| `README.md` | English operator guide: prerequisites, endpoint-profile setup/activation, run steps, troubleshooting table |
| `conftest.py` | Root test conftest — pre-registers `datacenter_monitor_python` as a stub so pytest never executes the real `__init__.py` (which `import carb`); adds the package dir to `sys.path` |
| `pytest.ini` | `--import-mode=importlib`; `testpaths = datacenter_monitor_python/tests` |
| `topology.json` | Seed topology (cluster/box/node) consumed by the demo backend's `topology_seed.py` |
| `import_python.py` | Standalone Script Editor snippet (UV-animated cylinder); not imported by the extension |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `config/` | `extension.toml` (Kit metadata + dependencies) and endpoint profiles `env.*` (see `config/AGENTS.md`) |
| `datacenter_monitor_python/` | The Kit extension Python package — entry point, Kafka subscribers, SceneManager, message handler (see `datacenter_monitor_python/AGENTS.md`) |
| `kafka-dummy/` | Self-contained Docker demo backend (Kafka/MinIO/Nessie/Trino/FastAPI) — OLD demo, distinct from the standalone `ScaleX-QueryServer` repo (see `kafka-dummy/AGENTS.md`) |
| `nginx/` | Reverse-proxy config fronting React + query-server + Omniverse WebRTC signaling (see `nginx/AGENTS.md`) |
| `docs/` | `superpowers/` agent scratch (specs/plans) — NOT documented; `CHANGELOG.md`/`README.md` referenced by extension.toml |
| `assets/` | USD art + materials (`ScaleX_POD_Project/`) — binary, NOT documented |

## For AI Agents

### Working In This Directory
- **The central threading rule:** USD may only be mutated on the Kit main thread. Kafka threads only parse and enqueue dicts; `extension.py::_on_update` is the single place data crosses into the scene. Do not call `SceneManager` from a Kafka thread.
- Kafka endpoints are **never hardcoded** — they come from `config/env.<profile>` via `config_loader.py`. See `config/AGENTS.md`.
- WebRTC message-type tables in `datacenter_monitor_python/README.md` are mirrored in the React `OmniverseViewer.jsx` — change both.
- USD Viewer based: no physics, no timeline play/stop, no `isaacsim.*`.

### Testing Requirements
- `cd extensions/datacenter_monitor && pytest` runs the suite without Omniverse (stubbed by `conftest.py`). Single test: `pytest datacenter_monitor_python/tests/test_config_loader.py::test_name`.
- Add tests beside the behavior changed (config parsing, message parsing, topology mapping, node-metrics cache, pulse mapping). Use `tmp_path` / `monkeypatch` fixtures for filesystem and env-profile cases.

### Common Patterns
- Korean comments + `[수정]` markers; `print("[Datacenter Monitor] ...")` logging.
- All tunables, USD hierarchy assumptions, prim-name prefixes, Kafka topic names, color/emissive thresholds, and camera offsets live in `datacenter_monitor_python/global_variables.py`.

## Dependencies

### Internal
- Consumes Kafka from `kafka-dummy/query-server` (demo) or `ScaleX-QueryServer` (production); talks WebRTC to `ScaleX-Twin-Web`.

### External
- Omniverse Kit, USD (`pxr`), `confluent-kafka`/`kafka-python`, `pytest`.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
