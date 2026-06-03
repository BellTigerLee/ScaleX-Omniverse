<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# tests

## Purpose
Pytest unit tests for the demo query-server. Currently covers the in-memory node-state cache.

## Key Files
| File | Description |
|------|-------------|
| `__init__.py` | Marks the package (single newline) |
| `test_node_state_cache.py` | Exercises `NodeStateCache` — overwrite-only latest envelope per (cluster, node), required-field validation |

## For AI Agents

### Working In This Directory
- Imports are **flat** (`from node_state_cache import ...`), so tests must be run from the `query-server/` directory, not the repo root.
- Keep tested logic free of live Kafka/Trino connections — `NodeStateCache` accepts envelopes directly, so test the parse/cache logic without a broker.

### Testing Requirements
- `cd ..` (to `query-server/`) then `pytest tests/`. Single test: `pytest tests/test_node_state_cache.py::test_name`. No `pytest.ini` in this tree.

### Common Patterns
- Inline sample envelopes as fixtures; assert cache contents after feeding messages.

## Dependencies

### Internal
- `node_state_cache.py` in the parent directory.

### External
- `pytest`.

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
