<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# ScaleX-Omniverse

## Purpose
This git repo holds the NVIDIA **Omniverse Kit extensions** that render the ScaleX datacenter digital twin. The primary extension, `datacenter_monitor`, consumes Kafka metrics/events, recolors USD scene prims by node health, animates cameras through a 4-stage zoom (A→B→C→D), and exchanges WebRTC messages with the React dashboard (`ScaleX-Twin-Web`). A second, smaller extension, `scalex_dynamic_props`, decorates the same USD stage at runtime using a session layer so the on-disk `.usd` is never modified. The extensions live **outside** `kit-app-template/` and are wired in via the launcher's `--ext-folder` flag. It is **USD Viewer based — no Isaac Sim / omni.physx** (no physics, no timeline play/stop, no `isaacsim.*` imports). Most comments, READMEs, and log strings are in **Korean**; `[수정]` ("modify") markers flag intended extension points — match the existing language and style.

## Key Files
| File | Description |
|------|-------------|
| `CLAUDE.md` | Authoritative architecture doc: extension threading model, SceneManager mixin facade, view stages, React↔Kit messaging, config profiles, Kafka topics, demo backend |
| `AGENTS.md` | This file (regenerated; folds in the prior "Repository Guidelines") |
| `README.md` | One-line stub |
| `ScaleX-POD.md` | USD scene hierarchy / node-state schema reference (`datax`/`twinx`/etc. clusters, box prim naming) |
| `import_python.py` | Standalone Omniverse Script Editor snippet (UV-animated emissive cylinder); not imported by any extension |
| `invisible_bug.png`, `prim_bug.png` | Debug screenshots (binary; ignore) |
| `.gitignore` | Ignores `config/active` symlink, generated data, caches |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `extensions/` | The two Kit extensions discovered via `--ext-folder` (see `extensions/AGENTS.md`) |
| `assets/` | USD art (`ScaleX_POD_Project/ScaleX_Twin.usd`), materials, `.thumbs` — binary, NOT documented |

## For AI Agents

### Working In This Directory
- The extension is **USD Viewer based — no Isaac Sim / omni.physx**. Do not add physics, timeline play/stop, or `isaacsim.*` imports. The code repeatedly asserts this.
- Two independent extensions live under `extensions/`. They are *not* merged: `datacenter_monitor` owns all Kafka/WebRTC/scene logic; `scalex_dynamic_props` only authors session-layer decoration on top of the same stage.
- The WebRTC message-type tables are **mirrored** between `extensions/datacenter_monitor/datacenter_monitor_python/README.md` and the React app's `OmniverseViewer.jsx` — change both when adding a message type.
- The **A→B→C→D view-stage state machine** (full scene → cluster → rack → node) is implemented independently here (`extension.py` / `message_handler.py`) and in the browser (`OmniverseViewer.jsx`); keep transition logic consistent across both.
- This is its own git repo: `cd` here (or deeper) before committing. Contract-spanning changes also touch `ScaleX-QueryServer` and/or `ScaleX-Twin-Web` — commit each side in its own repo.

### Build, Test, and Development Commands
- The extension is **not run from the CLI** — it is loaded inside a Kit app. From the workspace root: `~/workspace/start_datacenter.sh [--streaming|--composer]` launches the Kit app with `--ext-folder ~/workspace/ScaleX-Omniverse/extensions --enable datacenter_monitor` (and `--enable scalex_dynamic_props`).
- Two independent pytest suites, both run as **plain Python** (Omniverse not required — `conftest.py` stubs `carb`/Kit). Use the project venv / Kit Python; system Python lacks `pytest`:
  ```bash
  cd extensions/datacenter_monitor && pytest                                # extension tests (pytest.ini lives here)
  pytest datacenter_monitor_python/tests/test_config_loader.py::test_name   # single test
  cd extensions/datacenter_monitor/kafka-dummy/query-server && pytest tests/ # demo query-server tests (flat imports)
  ```
- Demo backend (old, self-contained): `cd extensions/datacenter_monitor/kafka-dummy && docker compose up -d --build`; health: `curl http://localhost:8000/health` (healthy when `live_boxes == 13`).
- Requires `kafka-python` or `confluent-kafka` installed into the Kit Python for live Kafka consumption.

### Common Patterns
- Python 3, 4-space indentation, `snake_case` files/functions, type hints where they clarify. No project-wide formatter/linter — match nearby code. Test files: `test_*.py`, placed beside the behavior under test.
- Prefer small focused helpers in `datacenter_monitor_python/scene/` (mixin modules) over expanding `extension.py` or `scene_manager.py`.
- Korean comments + `[수정]` extension-point markers throughout; `print("[<Title>] ...")` logging convention.

## Dependencies

### Internal
- Consumes Kafka topics produced by `ScaleX-QueryServer` (live `datacenter.metrics`, replay topics, plus Flink-produced `node-state.events` / `stageab`).
- Exchanges WebRTC messages with `ScaleX-Twin-Web` (the React dashboard renders the streamed 3D scene).
- Loaded by `kit-app-template` via `--ext-folder`.

### External
- NVIDIA Omniverse Kit (`omni.usd`, `omni.kit.viewport.utility`, `omni.kit.commands`, `omni.kit.livestream.webrtc`), USD (`pxr`).
- `confluent-kafka` (preferred) / `kafka-python` (fallback) inside Kit Python.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
