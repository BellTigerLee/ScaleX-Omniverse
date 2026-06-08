<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# scalex_dynamic_props

## Purpose
A small, independent Kit extension that decorates the `ScaleX_Twin.usd` stage at runtime **without polluting the on-disk `.usd`**. It authors all prims/materials into an anonymous sublayer of the stage's **session layer**, so "Save" never writes them to the original asset. Specifically it creates a cluster-colored `FloorPanel_Bottom_Under_Front` mesh under each target rack (copying the source floor-panel geometry) and binds a per-cluster `Darker_Chassis_Metal.mdl` material. It runs on top of the stage that `datacenter_monitor` opens and reuses that extension's MDL assets by path (no copying). USD Viewer based — no Isaac Sim / omni.physx.

## Key Files
| File | Description |
|------|-------------|
| `scalex_dynamic_props_python/` | The Python package — entry point + session-layer build logic + constants (see below) |

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `config/` | `extension.toml` (Kit metadata; deps `omni.usd`, `omni.kit.viewport.utility`; module `scalex_dynamic_props_python`) (see `config/AGENTS.md`) |
| `scalex_dynamic_props_python/` | `extension.py`, `scene_builder.py`, `global_variables.py` (see `scalex_dynamic_props_python/AGENTS.md`) |
| `docs/` | `SPEC.md` (extracted-from-USD spec), `README.md`, `CHANGELOG.md` (see `docs/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- **Pollution ban is the core constraint:** author only into the session-layer anonymous overlay via `Usd.EditContext`. Never author to the root layer. The disk `.usd` must be byte-identical after a Composer "Save".
- Idempotent lifecycle: build runs once per stage on `ASSETS_LOADED` (references/payloads loaded → source mesh safe to read); the guard resets only on `OPENED`/`CLOSING`/`CLOSED`. Re-opening a stage produces the same result with no duplicate prims.
- `SCALEX_DYNAMIC_PROPS_ENABLED` (default on). Set `0`/`false`/`off`/`no` to author nothing and detach the existing overlay (use when you need a clean save).
- This extension is launched alongside `datacenter_monitor` via `--enable scalex_dynamic_props` in `start_datacenter.sh`; `--ext-folder` already points at `extensions/`.
- Do not merge this into `datacenter_monitor` — separation is intentional.

### Testing Requirements
- No automated tests. Verify with `~/workspace/start_datacenter.sh --composer`: colored under-panels appear under DataX/TwinX/MobileX/AutoX racks; after Save the `.usd` is unchanged on disk (pollution = 0); re-open is idempotent.

### Common Patterns
- Korean comments + `[수정]` markers; `print("[ScaleX Dynamic Props] ...")` logging; all paths/colors/targets centralized in `global_variables.py`.

## Dependencies

### Internal
- References `../datacenter_monitor/assets/ScaleX_POD_Project/materials/Darker_Chassis_Metal.mdl` by path; operates on the stage `datacenter_monitor` opens.

### External
- Omniverse Kit (`omni.ext`, `omni.usd`), USD (`pxr`: `Usd`, `UsdGeom`, `UsdShade`, `Sdf`, `Gf`).

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
