<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# config

## Purpose
Kit extension metadata for `scalex_dynamic_props`. There are no endpoint profiles here (unlike `datacenter_monitor/config/`) — this extension takes no network configuration; its only runtime toggle is the `SCALEX_DYNAMIC_PROPS_ENABLED` environment variable read in `global_variables.py`.

## Key Files
| File | Description |
|------|-------------|
| `extension.toml` | Package metadata (version 1.0.0, "ScaleX Dynamic Props"); `[dependencies]` `omni.usd` + `omni.kit.viewport.utility`; `[[python.module]] name = "scalex_dynamic_props_python"`. Header `[수정]` note explains USD-Viewer basis, session-layer authoring, and asset reuse from `datacenter_monitor` |

## For AI Agents

### Working In This Directory
- Dependencies are minimal on purpose (no WebRTC/livestream, no Kafka). Keep it USD-Viewer-only.
- The module name must stay `scalex_dynamic_props_python` to match the package folder.

### Testing Requirements
- No tests. The toml is validated implicitly when Kit loads the extension at launch.

### Common Patterns
- Korean comments; mirrors `datacenter_monitor/config/extension.toml` structure minus the optional livestream dependency.

## Dependencies

### Internal
- Declares the `scalex_dynamic_props_python` module.

### External
- Omniverse Kit extension system.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
