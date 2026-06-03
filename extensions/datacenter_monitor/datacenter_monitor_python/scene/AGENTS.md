<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# scene

## Purpose
All real USD scene logic, split into focused mixins that `SceneManager` composes via multiple inheritance. Each mixin owns an `_init_*()` called from `SceneManager.__init__`, and its caches are cleared in `initialize()`/`cleanup()`. Two modules (`node_metrics.py`, `node_state_pulse.py`, plus the parser in `node_index.py`) contain pure/USD-independent logic that is unit-tested. **All USD mutation here must run on the Kit main thread.**

## Key Files
| File | Mixin / role |
|------|--------------|
| `__init__.py` | Package marker (Korean comment) |
| `camera.py` | `_CameraControllerMixin` — camera pos/target read-write, BBox-based rack camera, smoothstep animation tick |
| `topology.py` | `_TopologyMixin` — discover/index the USD cluster→rack→box(node) hierarchy from `SCALE_POD_PATH`; flat fallback; node X-position normalization |
| `node_visibility.py` | `_NodeVisibilityMixin` — visibility + Stage A↔B↔C↔D transitions, node pop-forward, sibling dim/restore |
| `material.py` | `_MaterialMixin` — overlay glass-cube creation sized to BBox, HEALTHY/WARNING/CRITICAL color updates, `update_node_color_from_kafka` (currently unused path) |
| `node_metrics.py` | `_NodeMetricsMixin` — prim-keyed cache of latest `datacenter.metrics` per node (pure; tested) |
| `node_state_pulse.py` | `compute_emissive()` one-shot emissive breathing pulse (pure functions, sin rise + exp decay; tested) |
| `node_index.py` | `parse_topology_response()` — Topology API response → (cluster, node)→prim_name map (pure; tested) |
| `alert.py` | `_AlertMixin` — rack alert marker (emissive sphere) create/hide |
| `event_alert.py` | `_EventAlertMixin` — transient ImagePanel.usd panels on rack/node, auto-expire after `EVENT_PANEL_LIFETIME_SEC` |
| `net_line_anim.py` | `_NetLineAnimMixin` — per-rack NetLine UV-pulse material, speed driven by Kafka `net.in/out_mbps` |

## For AI Agents

### Working In This Directory
- Add new scene behavior as a new mixin following the `_init_*()` + cache-clear pattern, then add it to the `SceneManager(...)` base list and `__init__`/`initialize`/`cleanup`. Do not move logic up into `scene_manager.py`.
- Cross-mixin calls exist (e.g. `_MaterialMixin` calls `create_alert_decal`/`hide_alert_decal` on `_AlertMixin`); shared state (`_stage`, caches) lives on the composed `SceneManager` instance.
- USD Viewer only — no physics/timeline/`isaacsim.*`. Glass-cube overlays + emissive materials are the visualization primitives; node coloring is driven by `apply_node_state` (node-state events), not raw metrics.
- Keep `node_metrics.py`, `node_state_pulse.py`, and `node_index.py` import-safe (no `omni`/`carb`/USD at module top level) so they remain unit-testable.

### Testing Requirements
- Pure modules are covered by `../tests/test_node_metrics_cache.py`, `test_pulse_mapping.py`, `test_node_index.py`. Run from the extension root with `pytest`. USD-touching mixins are not unit-tested — verify visually via the Kit app.

### Common Patterns
- Korean module docstrings list each public method; `[수정]` markers flag tunable/extension points (e.g. decal style in `alert.py`).

## Dependencies

### Internal
- Imports tunables from `../global_variables.py`; composed by `../scene_manager.py`.

### External
- `pxr` (`Usd`, `UsdGeom`, `UsdShade`, `Gf`, `Sdf`), `omni.usd`, `omni.kit.viewport.utility`.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
