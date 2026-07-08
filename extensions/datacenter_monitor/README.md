# datacenter_monitor — ScaleX Omniverse Extension

## Purpose

`datacenter_monitor`는 ScaleX Datacenter Digital Twin을 Omniverse/USD Viewer에서 표시하는 Kit extension입니다.

현재 production live path는 `kafka-dummy`가 아니라 standalone `ScaleX-QueryServer`가 Kafka로 발행하는 canonical payload를 소비합니다.

```text
ScaleX-QueryServer
  -> Kafka datacenter.metrics
     -> Omniverse metric cache

ScaleX-QueryServer
  -> Kafka datacenter.metrics.node-state.events
     -> Omniverse node state visual

ScaleX-QueryServer /topology
  -> Omniverse (cluster,node) -> USD prim mapping
```

QueryServer 내부 source는 현재 VictoriaMetrics bridge입니다. 이후 ClickHouse, VictoriaLogs, feature-store가 추가되어도 Kafka schema가 유지되면 Omniverse 쪽은 source-specific 분기 없이 동작하도록 설계합니다.

## Current Contract

### Topics

| Topic | Role | Omniverse behavior |
| --- | --- | --- |
| `datacenter.metrics` | Numeric node metric snapshot | Parse, resolve `(cluster,node)`, cache original payload, forward to React on node inspect. |
| `datacenter.metrics.node-state.events` | Canonical node status | Parse, resolve `(cluster,node)`, apply node visual state. |
| `datacenter.metrics.stageab` | Cluster rank payload | Forward `cluster_rank` to React. |
| `datacenter.metrics.replay` | Replay metric frames | Used only when React starts replay. |
| `datacenter.metrics.replay.event` | Replay event frames | Existing replay path; live QueryServer status alignment is a separate follow-up. |

### Node Status

Only these public node statuses are accepted on `datacenter.metrics.node-state.events`:

```text
HEALTHY | DISCONNECTED | MISSING
```

| Status | Meaning |
| --- | --- |
| `HEALTHY` | Kubernetes node is Ready and telemetry is fresh. |
| `DISCONNECTED` | Kubernetes node is NotReady, unknown, or unreachable. |
| `MISSING` | Kubernetes node is Ready but telemetry is missing for at least the QueryServer threshold, currently 120 seconds. |

High CPU, high GPU temperature, storage saturation, and network pressure do not become node status by themselves. They remain metric evidence.

### Glow/Pulse

Glow/pulse is not required for the current live path.

Set this when launching Omniverse if you want the non-glow behavior explicitly:

```bash
export DC_GLASS_CUBE_PULSE=0
```

`DISCONNECTED` and `MISSING` use persistent non-glow visual states. `HEALTHY` clears the overlay in non-glow mode.

## Installation

### 1. Install Kafka Python Client In Kit Python

Use the Python executable bundled with your Omniverse Kit app.

```bash
<kit_python> -m pip install confluent-kafka
```

If `confluent-kafka` is unavailable, install the fallback client:

```bash
<kit_python> -m pip install kafka-python
```

### 2. Register The Extension Folder

In Omniverse USD Composer or your Kit app:

1. Open `Window -> Extensions`.
2. Add this folder as an extension search path:

```text
/home/work8/workspace/ScaleX-Omniverse/extensions
```

3. Enable extension id:

```text
datacenter_monitor
```

The workspace launcher may already pass this folder with `--ext-folder` and enable the extension.

### 3. Select A QueryServer Live Profile

A QueryServer-oriented profile is provided:

```text
config/env.queryserver-live
```

Activate it for one launch:

```bash
export DC_PROFILE=queryserver-live
export DC_GLASS_CUBE_PULSE=0
```

Or activate it persistently:

```bash
cd /home/work8/workspace/ScaleX-Omniverse/extensions/datacenter_monitor/config
ln -sfn env.queryserver-live active
```

Profile values:

```text
CLUSTER_HOST=10.32.161.108
KAFKA_NODEPORT=30892
TOPOLOGY_URL=http://10.32.161.108:8000/topology
DEV_FAKE_NODE_MAPPING=false
```

Adjust `TOPOLOGY_URL` if QueryServer runs on another host. Kafka bootstrap values must be `host:port` without `http://`.

### 4. Start QueryServer Live Bridge

QueryServer must publish the live Kafka topics before Omniverse can show real data.

At QueryServer side, use the VM bridge mode:

```text
QS_VM_BRIDGE_ENABLED=1
KAFKA_BROKER=10.32.161.108:30892
VICTORIA_METRICS_URL=http://10.32.162.115/select/99/prometheus
```

Check QueryServer health:

```bash
curl http://<queryserver-host>:8000/health
```

Expected signal:

```json
{
  "vm_bridge_enabled": true
}
```

## Uninstallation

### Disable The Extension

In Omniverse Extension Manager, disable:

```text
datacenter_monitor
```

### Remove The Extension Search Path

Remove this path from the Kit extension search paths if it was added manually:

```text
/home/work8/workspace/ScaleX-Omniverse/extensions
```

### Remove Active Profile Symlink

```bash
rm -f /home/work8/workspace/ScaleX-Omniverse/extensions/datacenter_monitor/config/active
```

### Optional: Remove Installed Python Packages

Use the same Kit Python used for installation:

```bash
<kit_python> -m pip uninstall confluent-kafka kafka-python
```

Only uninstall these if no other Kit extension depends on them.

## Feature Description

### QueryServer Live Metrics

The extension consumes `datacenter.metrics` messages such as:

```json
{
  "schema_version": 1,
  "kind": "node_metrics_snapshot",
  "ts": 1782194209000,
  "cluster": "ecclab",
  "node": "work2",
  "status": "HEALTHY",
  "metrics": {
    "cpu": { "util": 0.3 },
    "mem": { "util": 0.52 },
    "net": { "in_mbps": 125.0, "out_mbps": 88.0 },
    "gpu": { "util": 0.0 },
    "storage": { "util": 0.0 }
  },
  "telemetry": { "source": "victoriametrics", "lag_sec": 10.1 },
  "kubernetes": { "ready": true }
}
```

The full payload is cached. Extra fields from future QueryServer sources are preserved.

### Canonical Node State

The extension consumes `datacenter.metrics.node-state.events` messages such as:

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

This topic is the only live node visual source.

### Topology Mapping

The extension reads `TOPOLOGY_URL` and builds a `(cluster,node) -> prim_name` index. QueryServer must publish Kafka node identities that exist in `/topology`.

If QueryServer emits `ecclab/work2`, `/topology` must include `cluster=ecclab` and `node=work2` mapped to a USD box prim.

Production rule:

```text
DEV_FAKE_NODE_MAPPING=false
```

Unknown nodes are logged and dropped rather than assigned to arbitrary prims.

### React Integration

When React requests `node_inspect`, Omniverse returns cached `datacenter.metrics` payloads through the existing WebRTC custom event path:

```text
event_type=node_metrics
```

Cluster rank messages from `datacenter.metrics.stageab` are forwarded to React as:

```text
event_type=cluster_rank
```

### Replay

Replay topic switching remains present, but live QueryServer VM bridge alignment is the primary supported path in this implementation. Replay event schema alignment should be handled in a separate follow-up if replay needs the same `HEALTHY/DISCONNECTED/MISSING` semantics.

## Development And Verification

Run pure Python tests from this directory:

```bash
cd /home/work8/workspace/ScaleX-Omniverse/extensions/datacenter_monitor
eval "$(pyenv init -)"
pyenv activate .venv
python -m pytest datacenter_monitor_python/tests
```

Useful focused checks:

```bash
python -m pytest datacenter_monitor_python/tests/test_node_state_parse.py
python -m pytest datacenter_monitor_python/tests/test_metric_snapshot_parse.py
python -m pytest datacenter_monitor_python/tests/test_node_metrics_cache.py
python -m pytest datacenter_monitor_python/tests/test_config_loader.py
```

Syntax-only check for USD-touching modules:

```bash
python -m py_compile datacenter_monitor_python/scene/material.py datacenter_monitor_python/scene_manager.py
```

## Troubleshooting

| Symptom | Likely cause | Check |
| --- | --- | --- |
| Kafka connected but no nodes update | QueryServer not publishing live bridge topics | QueryServer `/health`, Kafka topic contents |
| Messages are dropped as unknown node | `/topology` does not map the Kafka `(cluster,node)` identity | QueryServer `/topology` vs Kafka payload `cluster/node` |
| `WARNING/CRITICAL/UNKNOWN` dropped | Legacy node-state statuses are no longer valid | QueryServer should emit only `HEALTHY/DISCONNECTED/MISSING` |
| `MISSING` looks same as `DISCONNECTED` | Visual colors need tuning in `global_variables.py` | `GLASS_CUBE_DISCONNECTED_COLOR`, `GLASS_CUBE_MISSING_COLOR` |
| Pulse/glow still appears | Pulse env not disabled | Set `DC_GLASS_CUBE_PULSE=0` before launch |

## Related Documents

- QueryServer live bridge plan: `/home/work8/workspace/ScaleX-QueryServer/docs/plans/2026-06-23_victoriametrics-live-bridge-status-spec-plan.md`
- Omniverse implementation plan: `docs/plans/2026-06-23_queryserver-live-schema-alignment-plan.md`
- Old local demo stack: `kafka-dummy/README.md`
