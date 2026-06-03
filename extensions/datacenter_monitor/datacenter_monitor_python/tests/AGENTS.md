<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# tests

## Purpose
Pytest unit tests for the extension's pure / stdlib-only logic — the paths that can run without Omniverse. The root `conftest.py` (at the extension root) stubs the `datacenter_monitor_python` package so importing it never triggers `import carb`; the local `conftest.py` here adds the package dir to `sys.path` so modules like `config_loader` import by name.

## Key Files
| File | Covers |
|------|--------|
| `conftest.py` | Inserts the parent package dir into `sys.path` for name-based imports |
| `test_config_loader.py` | `config_loader.resolve_profile_path` — `DC_PROFILE` / `active` symlink / missing-profile / error paths (uses `tmp_path`, `monkeypatch`) |
| `test_node_state_parse.py` | `kafka_subscriber.parse_node_state_message` — canonical envelope validation (required fields, 5-enum status) |
| `test_cluster_rank_parse.py` | `kafka_subscriber.parse_cluster_rank_message` — `stageab` cluster-rank parser |
| `test_node_index.py` | `scene.node_index.parse_topology_response` — Topology API response → prim mapping |
| `test_node_metrics_cache.py` | `scene.node_metrics._NodeMetricsMixin` — prim-keyed metrics cache (via a test harness subclass) |
| `test_pulse_mapping.py` | `scene.node_state_pulse` — `pulse_params` & `compute_emissive` (sin rise / exp decay) |

## For AI Agents

### Working In This Directory
- Tests must remain runnable in plain Python (no Kit). Only import modules that have no `omni`/`carb`/USD imports at module top level — `config_loader`, the `kafka_subscriber` parser functions, and the pure `scene/` modules (`node_index`, `node_metrics`, `node_state_pulse`).
- For mixin logic that needs instance state, build a small harness subclass in the test (see `test_node_metrics_cache.py`) rather than instantiating the full `SceneManager`.

### Testing Requirements
- Run from the extension root: `cd extensions/datacenter_monitor && pytest`. Single: `pytest datacenter_monitor_python/tests/test_config_loader.py::test_name`.
- `pytest.ini` sets `--import-mode=importlib` and `testpaths`. Name new files `test_*.py` and place them beside the behavior under test.

### Common Patterns
- `tmp_path` + `monkeypatch` for filesystem / env-profile cases; sample envelopes/responses inlined as fixtures. Test docstrings are in English; production code comments are Korean.

## Dependencies

### Internal
- Imports from `config_loader`, `kafka_subscriber`, and `datacenter_monitor_python.scene.*`.

### External
- `pytest`.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
