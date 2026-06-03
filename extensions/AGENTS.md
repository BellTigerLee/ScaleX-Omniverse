<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# extensions

## Purpose
Holds the Omniverse Kit extensions discovered at launch via `--ext-folder ~/workspace/ScaleX-Omniverse/extensions`. The launcher (`~/workspace/start_datacenter.sh`) passes `--enable datacenter_monitor` and `--enable scalex_dynamic_props`, so any well-formed extension folder placed here is auto-discoverable. Both extensions are USD Viewer based (no Isaac Sim / omni.physx). Each follows the Kit convention: folder name = extension id, Python package = `<folder>_python`, metadata in `config/extension.toml`.

## Key Files
None at this level — only the two extension subdirectories.

## Subdirectories
| Directory | Purpose |
|-----------|---------|
| `datacenter_monitor/` | The main extension — Kafka-driven 3D digital twin, WebRTC messaging, USD recoloring, A→B→C→D view stages (see `datacenter_monitor/AGENTS.md`) |
| `scalex_dynamic_props/` | Decorates the same stage at runtime via a session-layer overlay so the on-disk `.usd` is never modified; reuses `datacenter_monitor`'s MDL assets (see `scalex_dynamic_props/AGENTS.md`) |

## For AI Agents

### Working In This Directory
- The two extensions are independent and must not be merged. `scalex_dynamic_props` is intentionally a separate extension so that runtime decoration is isolated from the data-driven monitor logic.
- To add a new extension, create `<name>/config/extension.toml` with a `[[python.module]] name = "<name>_python"` block and a `<name>_python/` package; then add `--enable <name>` to `start_datacenter.sh`.
- Both extensions are USD Viewer based — no physics, no timeline, no `isaacsim.*`.

### Testing Requirements
- `datacenter_monitor` has a pytest suite (`cd datacenter_monitor && pytest`). `scalex_dynamic_props` has no test suite — verify visually with `~/workspace/start_datacenter.sh --composer`.

### Common Patterns
- Korean comments + `[수정]` markers; `print("[<Title>] ...")` logging; all tunables/paths centralized in each package's `global_variables.py`.

## Dependencies

### Internal
- `scalex_dynamic_props` references `datacenter_monitor/assets/ScaleX_POD_Project/materials/Darker_Chassis_Metal.mdl` by path (it does not copy assets) and operates on the same `ScaleX_Twin.usd` stage that `datacenter_monitor` opens.

### External
- NVIDIA Omniverse Kit extension system, USD (`pxr`).

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
