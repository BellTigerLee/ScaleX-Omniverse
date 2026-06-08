<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# docs

## Purpose
Documentation for the `scalex_dynamic_props` extension, referenced by `config/extension.toml` (`readme = "docs/README.md"`, `changelog = "docs/CHANGELOG.md"`). `SPEC.md` is the authoritative implementation spec, with all numeric values extracted by reading the saved `ScaleX_Twin.usd` directly with `usd-core`.

## Key Files
| File | Description |
|------|-------------|
| `SPEC.md` | Full spec: pollution ban / session-layer isolation, folder layout, launcher wiring, the cluster→rack→color table (DataX `#9cc2e5`, TwinX `#a8d08c`, MobileX `#ffd766`, AutoX `#bfbfbf`), source-mesh copy + MDL binding, idempotent lifecycle, verification scenarios |
| `README.md` | Korean overview: why a separate extension, what it builds, run/wiring, `SCALEX_DYNAMIC_PROPS_ENABLED`, asset reuse, idempotent lifecycle |
| `CHANGELOG.md` | 1.0.0 → 1.0.2 (1.0.1 added the once-per-stage build guard + enable env var; 1.0.2 removed the card-plane feature, keeping only the under-panel) |

## For AI Agents

### Working In This Directory
- Treat `SPEC.md` as the source of truth for target racks, colors, paths, and the `(0,0,-5)` under-panel translate. Update it (and `CHANGELOG.md`) when behavior changes.
- The spec notes the card/plane feature was removed in 1.0.2 — only the colored `FloorPanel_Bottom_Under_Front` under-panel remains. Some older spec/README phrasing may still mention the plane; the CHANGELOG is authoritative on what currently ships.
- Color values are sRGB used directly (no linear conversion) — keep doc and `global_variables.py::CLUSTER_PANELS` in sync.

### Testing Requirements
- Documentation only; no tests. The verification scenarios in `SPEC.md` §6 are the manual acceptance checks.

### Common Patterns
- Korean prose with embedded tables and code blocks; `[수정]` markers indicate tunable points.

## Dependencies

### Internal
- Describes `../scalex_dynamic_props_python/` and is linked from `../config/extension.toml`.

### External
- None.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
