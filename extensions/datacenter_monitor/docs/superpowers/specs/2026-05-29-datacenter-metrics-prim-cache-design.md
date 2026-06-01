# datacenter.metrics → prim 캐시 → node_inspect 시 전송 — 설계

- 날짜: 2026-05-29
- 대상 익스텐션: `extensions/datacenter_monitor/`
- 관련 스키마: `ScaleX-POD.md` (`datacenter.metrics`)

## 1. 배경 / 문제

`datacenter.metrics` 토픽은 `key = cluster|node` (예: `ecclab|work7`) 로 **upsert(compacted)** 되고 있어, 키마다 노드의 최신 메트릭 스냅샷이 유지된다. 그러나 현재 익스텐션은 이 토픽을 `KafkaSubscriber → _kafka_queue` 로 받아 `extension.py::_on_update` 에서 drain 하기만 하고 **그대로 버린다** (`update_node_color_from_kafka` 호출이 주석 처리됨, 2026-04-17 node-state.events 로 색상 경로 이관).

목표: 들어오는 `datacenter.metrics` 메시지를 **해당 노드의 USD prim 에 대응**시켜 prim 별 최신 메트릭을 캐시하고, React 가 노드를 inspect(Stage D 진입)할 때 그 prim 의 최신 메트릭을 디테일 패널용으로 전송한다.

## 2. 범위 / 비범위

**범위**
- `datacenter.metrics` 의 `(cluster, node)` → prim 해석 및 prim 별 최신 메트릭 캐시.
- `node_inspect` 시 해당 prim 의 최신 메트릭을 React 로 1회 전송.

**비범위 (변경하지 않음)**
- 노드 색/상태 시각화: 계속 `datacenter.metrics.node-state.events` (`NodeStateSubscriber → apply_node_state → tick_pulse`) 가 담당. 이 경로는 손대지 않는다.
- `datacenter.metrics` 의 `status` 필드는 색상 결정에 쓰지 않는다 (기존 정책 유지).
- Replay 엔진, cluster-rank, alert 패널 등 기타 경로.

## 3. 핵심 결정 (확정)

| 항목 | 결정 |
|------|------|
| prim 매핑 소스 | **Topology API** — 기존 `_resolve_prim_path(cluster, node)` 재사용 |
| metrics 의 용도 | 색상은 node-state 가 유지, metrics 는 **prim 별 세부값 캐시** 용 |
| 소비 범위 | **항상 전체 노드, 매 메시지** (stage 무관) — 기존 `_kafka_queue` drain 재사용 |
| React 전송 시점 | **node_inspect(Stage D 진입) 시 on-demand 1회** |
| 전송 페이로드 | **메시지 전체** 를 그대로 (필드 선별 안 함) |
| 1:N (prim 하나에 노드 여럿) | **배열**(`nodes: [...]`) 로 전송. 1:1 이면 길이 1 |

## 4. 데이터 흐름

```
datacenter.metrics (upsert, key=cluster|node)
   │  KafkaSubscriber → _kafka_queue            ← 변경 없음
   ▼
extension.py::_on_update  (매 프레임, 전체 노드 drain)
   │  기존: drain 후 discard
   │  변경: 각 메시지를 SceneManager.cache_node_metrics(msg) 로 전달
   ▼
SceneManager.cache_node_metrics(msg)
   │  _resolve_prim_path(msg["cluster"], msg["node"])   ← 기존 해석기 재사용
   │     (Topology API 인덱스 → Box_ 휴리스틱 → dev fake mapping 순)
   ▼
_node_metrics_cache[prim_path][node] = msg   (전체 노드, 항상 최신으로 갱신)

[별개 경로 — 변경 없음]
   node-state.events → 노드 색/상태 (전체 노드 계속 처리)

React → { type: "node_inspect", prim: "<prim_path>" }
   ▼
MessageHandler._on_incoming_message  (node_inspect 분기)
   │  기존: scene_manager.node_inspect(prim_path) + stage 전환
   │  추가: nodes = scene_manager.get_node_metrics(prim_path)
   │        send_node_metrics(prim_path, nodes)
   ▼
Kit → React:  event_type = "node_metrics"
```

핵심: **소비·캐시는 항상 전체 노드**. `node_inspect` 는 전송 트리거일 뿐, 다른 노드의 소비/색상에 영향 없음.

## 5. 컴포넌트 설계

### 5.1 새 mixin — `datacenter_monitor_python/scene/node_metrics.py`

기존 SceneManager mixin 패턴(`scene/material.py` 등)을 따른다. 색상 로직과 섞지 않기 위해 별도 mixin `_NodeMetricsMixin` 으로 분리한다.

```python
class _NodeMetricsMixin:
    def _init_node_metrics(self):
        # prim_path → { node_name(str) → 최신 metrics dict }
        self._node_metrics_cache: dict[str, dict] = {}

    def cache_node_metrics(self, msg: dict) -> None:
        """datacenter.metrics 1건을 prim 으로 해석해 최신값으로 캐시.
        해석 실패(None) 시 조용히 skip — upsert 라 다음 메시지가 곧 채운다."""
        if not isinstance(msg, dict):
            return
        cluster = msg.get("cluster", "")
        node    = msg.get("node", "")
        if not node:
            return
        prim_path = self._resolve_prim_path(cluster, node)   # _MaterialMixin 제공
        if prim_path is None:
            return
        self._node_metrics_cache.setdefault(prim_path, {})[node] = msg

    def get_node_metrics(self, prim_path: str) -> list:
        """해당 prim 의 노드별 최신 메트릭 메시지 리스트. 없으면 []."""
        by_node = self._node_metrics_cache.get(prim_path, {})
        return list(by_node.values())
```

- `_resolve_prim_path` 는 `_MaterialMixin` 에 이미 존재하므로 mixin 조합 시 그대로 호출 가능.
- 캐시는 `prim_path → {node: msg}` 2단 구조로 1:N 을 자연스럽게 수용.

### 5.2 `scene_manager.py` 변경

- `SceneManager` 의 mixin 목록에 `_NodeMetricsMixin` 추가.
- `__init__` 에서 `self._init_node_metrics()` 호출.
- `initialize()` 와 `cleanup()` 에서 `self._node_metrics_cache.clear()` (다른 캐시들과 동일하게 stage 열림/닫힘 시 초기화).

### 5.3 `extension.py::_on_update` 변경

metrics 큐 drain 루프에서 현재 주석/discard 자리에 캐시 호출 추가:

```python
while not self._kafka_queue.empty() and processed < 20:
    try:
        kafka_msg = self._kafka_queue.get_nowait()
        self._scene_manager.cache_node_metrics(kafka_msg)   # ← 추가
        processed += 1
    except queue.Empty:
        break
```

기존 주석 처리된 `update_node_color_from_kafka` 는 그대로 둔다(색상 경로 비활성 유지).

### 5.4 `message_handler.py` 변경

**새 송신 메서드**:

```python
def send_node_metrics(self, prim_path: str, nodes: list):
    self._send_to_client({
        "event_type": "node_metrics",
        "payload": { "primPath": prim_path, "nodes": nodes },
    })
```

**`node_inspect` 분기에 훅 추가** — 기존 `node_inspect(prim_path)` + stage 전환 직후:

```python
nodes = self._scene_manager.get_node_metrics(prim_path)
self.send_node_metrics(prim_path, nodes)
```

## 6. 메시지 프로토콜 (Kit → React)

`event_type: "node_metrics"`. 전송 시점: `node_inspect` 처리 시.

```json
{
  "event_type": "node_metrics",
  "payload": {
    "primPath": "/World/.../Box_4U_HDD_1",
    "nodes": [
      {
        "ts": 1780042500000,
        "cluster": "ecclab",
        "node": "work7",
        "status": "HEALTHY",
        "metrics": { "cpu": {...}, "mem": {...}, "net": {...}, "gpu": {...}, "storage": {...} },
        "conditions": {...},
        "debug_ts": 1780042634586
      }
    ]
  }
}
```

- `nodes` 항목은 `datacenter.metrics` 메시지 **원본 그대로**(필드 선별 없음).
- prim 에 캐시된 메트릭이 없으면 `"nodes": []` 전송 → React 가 "데이터 없음" 처리.

## 7. 엣지 케이스

- **topology/stage 준비 전 metrics 도착**: `_resolve_prim_path` 실패 → skip. metrics 는 upsert 라 다음 메시지가 곧 캐시를 채운다.
- **캐시에 없는 prim 을 inspect**: `nodes: []` 전송.
- **Replay 중**: `_kafka_queue` 가 replay 토픽으로 전환되면 캐시도 replay 메트릭이 된다(의도적 허용). `replay_stop` 시 live 로 복원.
- **dev fake mapping (`env.cluster-dev`, `DEV_FAKE_NODE_MAPPING=true`)**: `cache_node_metrics` 가 `_resolve_prim_path` 를 재사용하므로 dev fake mapping 을 그대로 상속한다. `_dev_fake_assigned` 가 **node 이름 기준 sticky** 캐시라, 같은 노드(예: `work7`)에 대해 metrics 경로와 node-state 경로가 **동일 prim 으로** 해석된다 → 색을 칠한 prim 에 metrics 도 캐시되어 일관성이 보장된다. (단, dev fake 후보 prim 풀은 Topology API 인덱스에서 나오므로 dev 에서도 `TOPOLOGY_URL` 이 살아 있어야 한다.)
- **stage 닫힘**: `cleanup()` 에서 캐시 clear → stale prim_path 잔존 방지.

## 8. 테스트 (plain-python, carb stub)

기존 `datacenter_monitor_python/tests/` 패턴(예: `test_node_index.py`)을 따른다. `cache_node_metrics`/`get_node_metrics` 는 `_cluster_node_to_prim`·`_cluster_box_index` dict 만 채우면 USD/Kit 없이 단위 테스트 가능:

- **1:1 매핑**: 노드 1개 → prim 1개 캐시 후 `get_node_metrics` 가 길이 1 리스트 반환.
- **1:N 매핑**: 같은 prim 에 노드 2개(`cp2`, `cp3`) 캐시 후 길이 2 리스트 반환.
- **upsert 덮어쓰기**: 같은 `(prim, node)` 에 새 메시지 → 최신값만 유지.
- **해석 실패**: 미등록 노드 → 캐시에 들어가지 않음(skip).
- **미존재 prim inspect**: `get_node_metrics(없는 경로)` → `[]`.

## 9. 변경 파일 요약

| 파일 | 변경 |
|------|------|
| `datacenter_monitor_python/scene/node_metrics.py` | **신규** — `_NodeMetricsMixin` |
| `datacenter_monitor_python/scene_manager.py` | mixin 등록, `_init_node_metrics()`, `initialize`/`cleanup` 에서 clear |
| `datacenter_monitor_python/extension.py` | `_on_update` metrics drain 루프에서 `cache_node_metrics(msg)` 호출 |
| `datacenter_monitor_python/message_handler.py` | `send_node_metrics()` 추가, `node_inspect` 분기에 전송 훅 |
| `datacenter_monitor_python/tests/test_node_metrics_cache.py` | **신규** — 단위 테스트 |

node-state.events / 색상 / pulse 경로는 일절 변경하지 않는다.
