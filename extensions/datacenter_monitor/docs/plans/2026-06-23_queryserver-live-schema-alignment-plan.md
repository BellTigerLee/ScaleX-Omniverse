# Omniverse QueryServer Live Schema Alignment Plan

Date: 2026-06-23
Status: REVISED PLAN
Owner component: `ScaleX-Omniverse/extensions/datacenter_monitor`
Upstream basis: current uncommitted `ScaleX-QueryServer` VM bridge implementation on 2026-06-23

## 1. Why This Plan Was Revised

The QueryServer side is no longer only a design. Current changes under `ScaleX-QueryServer` now include:

- `query-server/victoriametrics_bridge.py`
- `.env` loading in `query-server/config.py`
- `QS_VM_BRIDGE_ENABLED` lifecycle integration in `main.py`
- `NodeStateCache.VALID_STATUSES = {HEALTHY, DISCONNECTED, MISSING}`
- VM bridge tests covering status rules, VM metric mapping, readiness label normalization, and node-state envelope construction

Therefore Omniverse should align with the actual QueryServer payloads, not just the earlier conceptual plan.

## 2. Goal

Omniverse should consume the canonical Kafka payloads emitted by QueryServer and stop relying on dummy/loop-producer-specific assumptions.

```text
QueryServer VictoriaMetricsBridge
  -> Kafka datacenter.metrics
  -> Omniverse metric cache

QueryServer VictoriaMetricsBridge
  -> Kafka datacenter.metrics.node-state.events
  -> Omniverse node-state visual
```

Omniverse must not branch on QueryServer's internal source. Today the source is VictoriaMetrics. Later it may include ClickHouse, VictoriaLogs, or a feature store. If QueryServer emits the same Kafka schema, Omniverse should continue to work.

## 3. Explicit Non-Goal: Glow And Pulse

Glow/pulse behavior is not part of this redesign.

- Current operation assumes glow/pulse is disabled.
- `DC_GLASS_CUBE_PULSE=0` or equivalent is the expected mode.
- No new emissive, breathing, or glow intensity behavior should be introduced for this phase.
- Existing pulse/glow code may remain dormant, but this plan must not depend on it.
- This phase is about schema alignment, status interpretation, topology mapping, and removing dummy assumptions.

## 4. Actual QueryServer Kafka Payloads

### 4.1 Metric Snapshot Payload

Current QueryServer builder: `victoriametrics_bridge.build_metric_snapshot()`.

It emits this shape to `datacenter.metrics`:

```json
{
  "schema_version": 1,
  "kind": "node_metrics_snapshot",
  "ts": 1782194209000,
  "cluster": "ecclab",
  "node": "work2",
  "status": "HEALTHY",
  "metrics": {
    "cpu": {
      "util": 0.3,
      "cores": 32.0,
      "load1": 4.1,
      "load5": 0.0,
      "load15": 0.0
    },
    "mem": {
      "util": 0.52,
      "total_gb": 128.0,
      "avail_gb": 61.4,
      "oom_cnt": 0
    },
    "net": {
      "in_mbps": 125.0,
      "out_mbps": 88.0,
      "retrans": 0.0,
      "err_sum": 0.0,
      "nic_err_sum": 0.0,
      "nic_drop_sum": 0.0
    },
    "gpu": {
      "util": 0.0,
      "mem_util": 0.0,
      "mem_used_gb": 0.0,
      "total_gb": 0.0,
      "temp": 0.0,
      "pwr": 0.0
    },
    "storage": {
      "util": 0.0,
      "read_mbps": 12.0,
      "write_mbps": 0.0,
      "io_mbps": 0.0
    }
  },
  "telemetry": {
    "source": "victoriametrics",
    "rule_interval_sec": 15,
    "scrape_interval_sec": 30,
    "lag_sec": 10.1,
    "missing_after_sec": 120.0
  },
  "kubernetes": {
    "ready": true
  },
  "debug_ts": 1782194209000
}
```

Important current behavior:

- Ratio fields are clamped to `0..1` by QueryServer.
- All metric groups are present with numeric defaults, even when a node has no GPU or a field was absent from VM.
- `status` is included for compatibility, but Omniverse must not drive visual health from `datacenter.metrics`.
- `telemetry.source` currently equals `victoriametrics`, but Omniverse should treat it as informational only.
- Healthy readiness-only nodes are not published as zero-metric snapshots.
- Readiness-only NotReady nodes are published as `DISCONNECTED` so fault state is not hidden.

### 4.2 Node-State Envelope Payload

Current QueryServer builder: `victoriametrics_bridge.build_node_state_envelope()`.

It emits this shape to `datacenter.metrics.node-state.events`:

```json
{
  "schema_version": 1,
  "kind": "snapshot",
  "scope": "node",
  "cluster": "ecclab",
  "node": "work2",
  "status": "MISSING",
  "reasons": ["telemetry_missing_120s"],
  "ts": 1782194209000,
  "state_since": 1782194209000,
  "previous_status": "HEALTHY",
  "last_seen_at": 1782194089000,
  "gap_sec": 120.0,
  "node_ready": true,
  "telemetry_lag_sec": 120.0
}
```

Current reason strings:

| Status | `reasons` |
| --- | --- |
| `HEALTHY` | `[]` |
| `DISCONNECTED` | `["node_not_ready"]` |
| `MISSING` | `["telemetry_missing_120s"]` when threshold is 120 |

Important current behavior:

- QueryServer emits `kind="snapshot"` for the VM bridge path.
- `previous_status` is present.
- `state_since` is preserved while status remains unchanged and reset on transition.
- `last_seen_at = ts - telemetry_lag_sec * 1000`.
- `node_ready` and `telemetry_lag_sec` are present and should be preserved for future UI.

## 5. Status Contract

Omniverse must accept exactly these public node statuses:

```text
HEALTHY | DISCONNECTED | MISSING
```

| Status | Meaning | Omniverse interpretation |
| --- | --- | --- |
| `HEALTHY` | Kubernetes node is Ready and telemetry is fresh. | Normal node state. |
| `DISCONNECTED` | Kubernetes node is NotReady, unknown, or unreachable. | Node health/fault state. |
| `MISSING` | Kubernetes node is Ready but telemetry is missing for at least `missing_after_sec`. | Telemetry-quality state, distinct from node NotReady. |

Status precedence is owned by QueryServer:

```text
DISCONNECTED > MISSING > HEALTHY
```

Omniverse should not recompute this precedence from raw metrics.

## 6. What Status Is Not

Resource pressure is not node health status.

```text
high CPU != DISCONNECTED
high GPU temperature != MISSING
high storage usage != node health failure
network errors != node status by themselves
```

Metric values remain evidence for React panels, node inspect, and future explanations. They should not drive Omniverse node health visuals unless QueryServer emits a node-state envelope.

## 7. Topic Responsibilities

### 7.1 `datacenter.metrics`

Omniverse responsibilities:

1. Parse the metric snapshot.
2. Require at minimum `cluster`, `node`, `ts`, and `metrics`.
3. Prefer `schema_version=1` and `kind=node_metrics_snapshot` when present, but keep compatibility with older metric snapshots if the minimal contract is satisfied.
4. Resolve `(cluster,node)` to a USD prim.
5. Cache the full original message for node inspect.
6. Preserve `telemetry`, `kubernetes`, and future fields.
7. Never use this topic as the health visual source.

### 7.2 `datacenter.metrics.node-state.events`

Omniverse responsibilities:

1. Parse and validate the node-state envelope.
2. Require the existing canonical fields: `kind`, `scope`, `cluster`, `node`, `status`, `ts`, `state_since`, `last_seen_at`, `gap_sec`, `reasons`.
3. Accept only `HEALTHY`, `DISCONNECTED`, and `MISSING`.
4. Resolve `(cluster,node)` to a USD prim.
5. Apply the non-glow node visual state.
6. Preserve `previous_status`, `node_ready`, and `telemetry_lag_sec` for future UI/logging.

## 8. Identity And Topology Alignment

Current QueryServer VM bridge emits raw VM identities such as:

```text
cluster="ecclab"
node="work2" ... "work8"
```

Omniverse can only visualize these if the configured QueryServer topology maps the same `(cluster,node)` pairs to USD `prim_name` values.

This is the most important deployment dependency.

Production/PoC policy:

- `TOPOLOGY_URL` must point to the QueryServer instance that knows the same nodes QueryServer publishes to Kafka.
- `DEV_FAKE_NODE_MAPPING=false` for PoC/production.
- Unknown `(cluster,node)` must be logged and dropped, not arbitrarily assigned.
- If `ecclab/workN` does not exist in `/topology`, fix QueryServer topology or add an explicit QueryServer-owned identity mapping. Do not use fake mapping as a production workaround.

Expected profile shape:

```text
CLUSTER_HOST=10.32.161.108
KAFKA_NODEPORT=30892
TOPOLOGY_URL=http://<queryserver-host>:8000/topology
DEV_FAKE_NODE_MAPPING=false
```

The exact QueryServer host depends on deployment. The Kafka host/port above reflects the current QueryServer `.env.example` broker value.

## 9. Implementation Plan

### Phase 1: Parser Alignment

Update `datacenter_monitor_python/kafka_subscriber.py`.

Current node-state parser accepts:

```text
HEALTHY | WARNING | CRITICAL | DISCONNECTED | UNKNOWN
```

Target parser accepts:

```text
HEALTHY | DISCONNECTED | MISSING
```

Recommended parser structure:

```text
parse_metric_snapshot(raw: bytes) -> dict | None
parse_node_state_message(raw: bytes) -> dict | None
normalize_node_status(status: str) -> str | None
```

Rules:

- Reject `WARNING`, `CRITICAL`, and `UNKNOWN` on node-state topic.
- Reject lowercase/mixed-case status unless a later schema version explicitly allows normalization.
- Preserve all extra fields.
- Keep metric snapshot parser permissive enough for existing dummy messages, but document QueryServer schema as the production path.

### Phase 2: Non-Glow Visual State Handling

Update `datacenter_monitor_python/scene/material.py`.

Current issues:

- `MISSING` is not represented.
- Comments and helper names still mention old `WARNING/CRITICAL` flows.
- With pulse disabled, `apply_node_state()` may hide non-HEALTHY states instead of leaving a persistent visible status.

Target behavior without glow:

| Status | Behavior |
| --- | --- |
| `HEALTHY` | Clear or normal visual state. |
| `DISCONNECTED` | Persistent NotReady/fault visual state. |
| `MISSING` | Persistent telemetry-missing visual state, visually distinct from `DISCONNECTED`. |

Add or revise constants in `global_variables.py`:

```text
GLASS_CUBE_HEALTHY_COLOR
GLASS_CUBE_DISCONNECTED_COLOR
GLASS_CUBE_MISSING_COLOR
```

Do not add new glow intensity or pulse parameters for this phase.

### Phase 3: Metric Cache Must Preserve QueryServer Fields

Update `datacenter_monitor_python/scene/node_metrics.py`.

The cache should keep the original message intact:

```text
_node_metrics_cache[prim_path][node] = msg
```

Do not strip:

- `schema_version`
- `kind`
- `telemetry`
- `kubernetes`
- future `evidence`
- future `logs`
- unknown source-specific fields

Fallback aliases may be kept for compatibility:

```text
cluster = msg.get("cluster") or msg.get("cluster_id")
node = msg.get("node") or msg.get("node_id") or msg.get("box_id")
```

Production QueryServer emits `cluster` and `node`, so fallback aliases are compatibility only.

### Phase 4: Profile And Topology Cleanup

Update docs and, later, profiles to separate production from development behavior.

Production/PoC:

```text
DEV_FAKE_NODE_MAPPING=false
TOPOLOGY_URL=http://<queryserver-host>:8000/topology
```

Development-only:

```text
DEV_FAKE_NODE_MAPPING=true
```

If QueryServer publishes `ecclab/workN`, `/topology` must expose `ecclab` and those node names. If `/topology` still exposes only older demo clusters like `datax` or `twinx`, Omniverse should not paper over the mismatch.

### Phase 5: Documentation Cleanup

Update:

- `README.md`
- `datacenter_monitor_python/README.md`
- `config/env.example`
- possibly `datacenter_monitor_python/scene/AGENTS.md` if implementation changes the role description

Docs should say:

- QueryServer is the production Kafka producer.
- `kafka-dummy` is old/local demo infrastructure only.
- `datacenter.metrics` is metric cache input.
- `datacenter.metrics.node-state.events` is the visual status source.
- accepted statuses are `HEALTHY`, `DISCONNECTED`, and `MISSING`.
- glow/pulse is disabled and out of scope for this plan.

### Phase 6: Event/Log Future Compatibility

Current QueryServer changes also add ClickHouse/K8s event cache helpers and VictoriaLogs config, but the VM bridge does not publish those as Omniverse visual events yet.

Omniverse should not overload node-state parsing for future logs/events.

Future path:

```text
QueryServer event/log source
  -> datacenter.metrics.event or a new evidence topic
  -> separate Omniverse event/evidence parser
```

Node-state remains only for `HEALTHY/DISCONNECTED/MISSING`.

### Phase 7: Replay Follow-Up

Replay is not part of this live schema alignment implementation.

Current risk:

- Omniverse switches node-state subscriber to `datacenter.metrics.replay.event` during replay.
- QueryServer replay event payload may not be a node-state envelope.
- Once Omniverse parser rejects legacy statuses, replay-event messages may be dropped.

Short-term decision:

- Keep this plan focused on live QueryServer VM bridge topics.
- Record replay node-state alignment as a follow-up.

Future options:

| Option | Meaning |
| --- | --- |
| A | QueryServer replay event topic emits node-state envelopes. |
| B | Omniverse uses a separate replay-event parser. |
| C | Replay omits node-state visuals until replay schema is redesigned. |

Recommended later decision: A if replay should preserve live visual semantics.

## 10. Tests To Update Later

No build or test run is required for this planning update. When implementing later, update plain-Python tests.

### 10.1 Node-State Parser

File: `datacenter_monitor_python/tests/test_node_state_parse.py`

Required changes:

- Accept `HEALTHY`, `DISCONNECTED`, and `MISSING`.
- Reject `WARNING`, `CRITICAL`, and `UNKNOWN`.
- Verify QueryServer fields `schema_version`, `previous_status`, `node_ready`, and `telemetry_lag_sec` are preserved when present.
- Keep `previous_status` optional because the minimal required field list does not require it, even though current QueryServer emits it.

### 10.2 Metric Parser

Add tests for QueryServer metric snapshots:

- Accept `schema_version=1`, `kind=node_metrics_snapshot` payload.
- Accept nested metric groups with zero defaults.
- Preserve `telemetry` and `kubernetes` objects.
- Preserve unknown future fields.
- Reject missing `cluster`, `node`, `ts`, or `metrics`.

### 10.3 Metric Cache

File: `datacenter_monitor_python/tests/test_node_metrics_cache.py`

Add checks:

- QueryServer extra fields survive cache roundtrip.
- compatibility aliases do not break current behavior if retained.

### 10.4 Topology/Identity

Add or extend topology tests:

- QueryServer `/topology` response maps `(cluster,node)` to `prim_name`.
- unknown `ecclab/workN` style identities are dropped when no mapping exists and fake mapping is disabled.

### 10.5 Pulse Tests

No new pulse/glow tests are required for this plan because glow/pulse is out of scope.

Existing pulse tests should keep passing if dormant code remains untouched.

## 11. Manual Verification Later

When implementation work starts later, verify manually:

1. QueryServer `/health` reports `vm_bridge_enabled=true` when enabled.
2. Kafka `datacenter.metrics` receives `kind=node_metrics_snapshot` messages.
3. Omniverse caches those metric messages and does not use them for health visuals.
4. `node_inspect` sends the cached QueryServer payload to React.
5. Kafka `datacenter.metrics.node-state.events` receives `kind=snapshot` messages.
6. Omniverse accepts `HEALTHY`, `DISCONNECTED`, and `MISSING`.
7. Omniverse rejects `WARNING`, `CRITICAL`, and `UNKNOWN` on node-state topic.
8. `DISCONNECTED` and `MISSING` are visually distinct without glow.
9. Unknown `(cluster,node)` identities are dropped when fake mapping is disabled.
10. The configured `/topology` contains the same node identities QueryServer publishes.

## 12. Open Decisions

1. Exact non-glow visual style for `DISCONNECTED`.
2. Exact non-glow visual style for `MISSING`.
3. Whether `MISSING` should be color-only, outline/ring, label, or UI annotation.
4. Whether QueryServer should canonicalize VM identities before Kafka publish or `/topology` should be updated to match raw VM identities.
5. Whether replay event topic should become node-state-envelope compatible.

Recommended direction:

- `DISCONNECTED`: strong fault visual, preferably red or dark red.
- `MISSING`: telemetry-quality visual, preferably blue, purple, or amber, clearly distinct from `DISCONNECTED`.
- `HEALTHY`: normal/default appearance.
- Identity should be fixed at QueryServer/topology contract level, not by Omniverse fake mapping.

## 13. Done Criteria For Future Implementation

This plan is implemented when:

- Omniverse accepts QueryServer's current VM bridge Kafka payloads.
- Omniverse no longer relies on dummy/loop-producer-specific status assumptions.
- Node visual state is driven only by `datacenter.metrics.node-state.events`.
- Accepted node-state status enum is exactly `HEALTHY`, `DISCONNECTED`, `MISSING`.
- Metric snapshots preserve future QueryServer fields.
- Production/PoC profiles use QueryServer topology and do not enable fake node mapping.
- Glow/pulse remains disabled and non-required.
