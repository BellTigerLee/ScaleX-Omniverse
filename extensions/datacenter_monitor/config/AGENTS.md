<!-- Parent: ../AGENTS.md -->
<!-- Generated: 2026-06-03 | Updated: 2026-06-03 -->

# config

## Purpose
Kit extension metadata plus the **endpoint profiles** that keep the Kafka broker address (and optional Topology API URL) out of code. `config_loader.py::load_profile()` reads a flat `KEY=VALUE` file from here. Switching clusters is a one-action operation (edit the `active` symlink or set `DC_PROFILE`).

## Key Files
| File | Description |
|------|-------------|
| `extension.toml` | Kit package metadata + `[dependencies]` (`omni.usd`, `omni.kit.viewport.utility`, `omni.kit.commands`, optional `omni.kit.livestream.webrtc`) + `[[python.module]] name = "datacenter_monitor_python"`. `[수정]` marks the livestream-dependency tweak point |
| `env.example` | Committed template documenting all keys: `CLUSTER_HOST`, `KAFKA_NODEPORT`, optional `TOPOLOGY_URL`, optional `DEV_FAKE_NODE_MAPPING` |
| `env.cluster-poc` | PoC cluster profile (`CLUSTER_HOST=10.30.0.233`, `KAFKA_NODEPORT=9092`, `TOPOLOGY_URL=...:8000/topology`) |
| `env.cluster-dev` | Dev cluster profile; sets `DEV_FAKE_NODE_MAPPING=true` (local Flink emits node names like `work2..work8` not in topology) |
| `active` | Symlink to the active profile (gitignored). Currently → `env.cluster-poc` |

## For AI Agents

### Working In This Directory
- **Resolution order** (`config_loader.py`): `DC_PROFILE` env var (`env.<DC_PROFILE>`) → `active` symlink → `env.default` → raise `FileNotFoundError` with guidance.
- **Required keys:** `CLUSTER_HOST`, `KAFKA_NODEPORT` (the latter must parse as `int`). Optional: `TOPOLOGY_URL` (→ `NODE_INDEX_URL`; unset = prim-name heuristic fallback), `DEV_FAKE_NODE_MAPPING` (`true|1|yes|on`).
- Add a new cluster as `env.<name>`, never hardcode endpoints in Python. Activate with `ln -sfn env.<name> active` or `DC_PROFILE=<name>`.
- `DEV_FAKE_NODE_MAPPING` must stay empty/false on PoC/production profiles — it stable-hashes unregistered node names onto arbitrary topology prims for dev visualization only.
- The extension connects as an **external** Kafka client via a k8s NodePort — k8s-internal DNS does not apply.

### Testing Requirements
- Profile parsing/resolution is covered by `datacenter_monitor_python/tests/test_config_loader.py`. Use `tmp_path` + `monkeypatch` to exercise `DC_PROFILE` / symlink / missing-key / bad-int cases.

### Common Patterns
- Flat `KEY=VALUE`, `#` comments and blank lines ignored, whitespace trimmed, last duplicate wins. Korean inline comments.

## Dependencies

### Internal
- Consumed at import time by `datacenter_monitor_python/global_variables.py` (builds `KAFKA_BOOTSTRAP_SERVERS` and `NODE_INDEX_URL`).

### External
- None beyond the standard library (`os`, `pathlib`).

<!-- MANUAL: Any manually added notes below this line are preserved on regeneration -->
