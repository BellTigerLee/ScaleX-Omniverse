<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# scalex_dynamic_props_python

## Purpose
The Python package for the `scalex_dynamic_props` extension. `extension.py` subscribes to USD stage events and, on `ASSETS_LOADED`, calls `SceneBuilder.rebuild()` once per stage. `scene_builder.py` does all USD authoring inside a `Usd.EditContext` targeting an anonymous sublayer of the session layer, so the on-disk `.usd` is never modified. `global_variables.py` holds every path, color, and target rack. USD Viewer based — no Isaac Sim / omni.physx; stage-event callbacks run on the Kit main thread.

## Key Files
| File | Description |
|------|-------------|
| `__init__.py` | `from .extension import Extension` |
| `extension.py` | `omni.ext.IExt` entry point. Stage-event subscription; per-stage build guard keyed by `id(stage):root_layer_identifier`; `_try_build` (skips if already built, tears down overlay when disabled), `_teardown_stage_overlay`; `on_shutdown` clears the overlay and detaches the session sublayer |
| `scene_builder.py` | `SceneBuilder` — inserts the anonymous overlay into `session.subLayerPaths`, authors per-cluster `Darker_Chassis_Metal` MDL materials + `FloorPanel_Bottom_Under_Front` meshes (geometry copied from the source floor panel) under each target rack; `rebuild()` clears + re-authors (idempotent); `teardown()` removes prims + detaches the overlay |
| `global_variables.py` | `EXTENSION_TITLE`, `SCALEX_DYNAMIC_PROPS_ENABLED` env handling (`is_dynamic_props_enabled()`), `OVERLAY_TAG`, asset paths (`DC_ASSETS`, `MDL_DARKER`), `DYNAMIC_ROOT`/`LOOKS_SCOPE`, `POD_BASE`, floor-panel relative path/names, `UNDER_PANEL_TRANSLATE=(0,0,-5)`, and `CLUSTER_PANELS` (target cluster→rack→sRGB color list) |

## For AI Agents

### Working In This Directory
- **Never author outside the session-layer overlay.** All `stage` mutations must happen within the `Usd.EditContext(stage, Usd.EditTarget(overlay))` block. This is the pollution-prevention contract.
- Build only on `ASSETS_LOADED` (so references/payloads, hence the source floor-panel mesh, are loaded). The per-stage guard prevents repeated `Clear()`+re-author churn when `ASSETS_LOADED` fires multiple times (e.g. MDL load).
- Copy the source mesh geometry at runtime (`points`/`faceVertexCounts`/`faceVertexIndices`/`normals`/`extent`/`st`/`subdivisionScheme`) — do **not** hardcode the 768-point geometry.
- Colors are sRGB values used directly (÷255), no linear conversion. To change targets/colors, edit `CLUSTER_PANELS` in `global_variables.py` (`[수정]` marker).
- Reuse `datacenter_monitor`'s MDL by path (`DC_ASSETS`); do not duplicate assets.

### Testing Requirements
- No unit tests. Verify visually: `~/workspace/start_datacenter.sh --composer`, confirm colored under-panels, confirm Save leaves the `.usd` unchanged, confirm re-open is idempotent.

### Common Patterns
- Korean comments + `[수정]` markers; `_log()` wraps `print("[ScaleX Dynamic Props] ...")`. Mirrors `datacenter_monitor`'s naming conventions.

## Dependencies

### Internal
- Reads MDL from `../../datacenter_monitor/assets/ScaleX_POD_Project/materials/`; targets the same stage hierarchy (`POD_BASE`) that `datacenter_monitor` opens.

### External
- `omni.ext`, `omni.usd`, USD (`pxr`: `Usd`, `UsdGeom`, `UsdShade`, `Sdf`, `Gf`).

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
