# Cluster Rank Consumer POC — Design Spec

- **Date**: 2026-04-22
- **Scope**: `extensions/datacenter_monitor` + `react-dev-tools`
- **Producer spec (upstream)**: `flink/docs/superpowers/specs/2026-04-22-cluster-cpu-rank-producer-design.md`
- **Status**: Draft (awaiting user review)

---

## §1. Goal

Flink 프로듀서가 `datacenter.metrics.stageab` 토픽으로 publish 중인
C-axis cluster CPU rank 메시지를, **Omniverse Extension 이 구독하여
React 에 포워딩하고, React 는 브라우저 콘솔에 `console.log` 로 출력**하는
end-to-end 파이프라인 최소 POC.

시각 채널(halo mesh / emissive amplitude) 결정, Redis 캐시 연동,
query-server 엔드포인트, Replay 지원, A축 reasons 분기, 여러 cluster 의
global Top-K 재정렬 — 모두 범위 밖.

## §2. Non-goals

- **시각화** — React 에서 DashboardOverlay 패널·3D halo mesh 등 UI 렌더링
  없음. 오직 `console.log` 만.
- **Redis / query-server** — Extension → React 직접 포워딩. query-server
  REST/WS 경유 안 함.
- **Replay 토픽 전환** — 프로듀서가 replay 변형을 publish 하지 않음.
  `switch_topic()` 와이어링 생략.
- **id 확장 대응** — 현재 토픽에는 `id="cluster-rank"` 메시지만 흐름.
  향후 `cluster-severity`, `cluster-rank-mem` 등이 추가되면 Extension
  필터에 분기 추가하는 방식(별도 트랙).

## §3. Input contract (producer 이 이미 보장)

- **토픽**: `datacenter.metrics.stageab`
- **Kafka key**: `cluster-rank:<cluster>` (Extension 은 key 를 사용하지 않음)
- **Cadence**: 30초마다 per cluster
- **Payload (UTF-8 JSON bytes)**:
  ```json
  {
    "id": "cluster-rank",
    "cluster": "datax",
    "ts": 1776830800669,
    "ranking": [
      {"node": "datax-dtn-1", "rank": 1, "cpu_util": 0.1255...},
      {"node": "datax-hdd-3", "rank": 2, "cpu_util": 0.1233...}
    ]
  }
  ```
- **Empty-terminal** (모든 노드 stale): `ranking: []` 1회 발행.

## §4. Architecture

```
Kafka datacenter.metrics.stageab
   │
   ▼  (background thread, confluent-kafka 우선)
ClusterRankSubscriber._parse_cluster_rank(raw)
   │   - JSON decode + dict 검증
   │   - 필수 필드: id, cluster, ts, ranking
   │   - id != "cluster-rank" → drop (None)
   │
   ▼  queue.Queue(maxsize=100)
Extension._on_update() 매 프레임
   │   최대 10개 drain → message_handler.send_cluster_rank(payload)
   │
   ▼  omni.kit.app.queue_event("omni.kit.livestream.send_message", ...)
WebRTC 스트리밍 레이어 (기존)
   │
   ▼  React OmniverseViewer.handleCustomEvent(ev)
   │   event_type === "cluster_rank" 분기
   │
   ▼  console.log("[cluster_rank]", innerData)
```

## §5. Components — Extension 쪽 변경

### §5.1 `global_variables.py`

2개 상수 추가:

```python
KAFKA_TOPIC_CLUSTER_RANK = "datacenter.metrics.stageab"
# group_id 는 KAFKA_GROUP_ID + "-cluster-rank" 를 호출 시점에 조립.
```

### §5.2 `kafka_subscriber.py`

모듈 수준에 파서 함수 추가:

```python
_CLUSTER_RANK_REQUIRED_FIELDS = ("id", "cluster", "ts", "ranking")

def parse_cluster_rank_message(raw: bytes):
    """
    stageab 토픽 메시지 파싱. 실패 또는 id 불일치 시 None.
    - JSON UTF-8 decode
    - dict 검증
    - 필수 필드 검증
    - id == "cluster-rank" 검증
    - ranking 이 list 검증
    """
    ...
```

`ClusterRankSubscriber` 클래스 추가 — `NodeStateSubscriber` 를 거의 그대로
복제. 차이점은 오직 `_parse_event()` → `parse_cluster_rank_message()` 호출.
`confluent-kafka` → `kafka-python` 이중 백엔드, 5초 재연결 루프, queue
overflow 시 oldest 드롭 패턴 모두 동일.

### §5.3 `message_handler.py`

신규 메서드 1개:

```python
def send_cluster_rank(self, payload: dict):
    """stageab cluster-rank 메시지를 React 로 포워딩."""
    self._send_to_client({
        "event_type": "cluster_rank",
        "payload": payload,
    })
```

### §5.4 `extension.py`

`on_startup`:
- `self._cluster_rank_queue = queue.Queue(maxsize=100)`
- `self._cluster_rank_subscriber = ClusterRankSubscriber(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    topic=KAFKA_TOPIC_CLUSTER_RANK,
    group_id=KAFKA_GROUP_ID + "-cluster-rank",
    data_queue=self._cluster_rank_queue,
  )` + `.start()`

`on_shutdown`:
- `self._cluster_rank_subscriber.stop()`

`_on_update`:
- 다른 큐들 drain 뒤에 cluster-rank 큐 drain 블록 추가 (최대 10개/frame):
  ```python
  rank_processed = 0
  while not self._cluster_rank_queue.empty() and rank_processed < 10:
      try:
          env = self._cluster_rank_queue.get_nowait()
          self._message_handler.send_cluster_rank(env)
          rank_processed += 1
      except queue.Empty:
          break
  ```

Replay 토픽 전환(`_on_replay_start` / `_on_replay_stop`) 는 **건드리지
않음** — 프로듀서가 replay 변형 없음.

## §6. Components — React 쪽 변경

### §6.1 `react-dev-tools/src/OmniverseViewer.js`

`handleCustomEvent` 내부, 기존 `scene_manifest` 분기 뒤에 3줄 추가:

```js
if (eventType === "cluster_rank") {
  console.log("[cluster_rank]", innerData);
}
```

다른 상태(State) 변경 없음. DashboardOverlay 에 prop 전달 없음.

## §7. Error handling

| 조건 | 동작 |
|---|---|
| Kafka 미연결 / 브로커 다운 | 5초 재시도 루프 (기존 패턴) |
| JSON decode 실패 | 경고 print 후 메시지 드롭 |
| 필수 필드 누락 | 경고 print 후 드롭 |
| `id != "cluster-rank"` | 조용히 드롭 (정상 경로) |
| `ranking` 이 list 아님 | 경고 print 후 드롭 |
| Queue full | 가장 오래된 메시지 제거 후 신규 삽입 (기존 패턴) |
| React 미연결 | `omni.kit.app.queue_event` 내부에서 자체 처리 (기존 패턴) |

## §8. Testing

### §8.1 단위 테스트 (Extension)

신규 파일 `extensions/datacenter_monitor/datacenter_monitor_python/tests/test_cluster_rank_parse.py` —
3 케이스:

1. **Happy path**: 스펙 §3 예시 페이로드 → dict 반환, 필드 보존 확인.
2. **`id` 불일치 드롭**: `id="cluster-severity"` → `None`.
3. **JSON 오류 드롭**: `b"not-json"` → `None`.

테스트는 기존 `test_node_state_parse.py` 의 스타일을 그대로 따름 (pytest,
모듈 함수만 직접 호출, Kafka / queue 의존성 없음).

### §8.2 수동 E2E 검증 (lab1 + React dev server)

1. lab1 Flink 가 `datacenter.metrics.stageab` 를 이미 publish 중인지
   kafbat UI 로 확인 (프로듀서 스펙 I1~I4 완료 전제).
2. Omniverse Extension 기동 → Extension 로그에
   `[ClusterRankSubscriber] ✅ confluent-kafka 연결 성공` 확인.
3. React 앱을 dev server 로 띄우고 브라우저 콘솔 오픈.
4. 30초 cadence 로 `[cluster_rank] {id: "cluster-rank", cluster: "...", ts: ..., ranking: [...]}` 가 출력되면 성공.
5. Empty-terminal 재현은 선택 — 모든 노드 stale 시 `ranking: []` 가 1회 찍히면 통과.

## §9. Out of scope (follow-up roadmap)

후속 세션 주제 (본 POC 의존):

- **§9.1 시각 채널 선택** (프로듀서 스펙 Q7 에서 보류) — halo mesh /
  emissive amplitude / 하이브리드 중 결정 후 Extension `scene_manager` 에
  rank → visual 매핑 구현.
- **§9.2 React DashboardOverlay 패널** — `liveMetrics` 처럼 상태로 승격,
  per-cluster TopK 패널 렌더. query-server Redis 트랙과 시점 맞춤.
- **§9.3 `id` 확장 대응** — `cluster-severity`, `cluster-rank-mem` 등
  다른 aggregate 가 토픽에 추가되면 `ClusterRankSubscriber` 를 generic
  `StageAbSubscriber` 로 리네임하고 `id` 별 분기 추가.
- **§9.4 Replay 대응** — 프로듀서가 `.replay.stageab` 변형을 지원하면
  `switch_topic()` 와이어링 추가.

## §10. References

- Producer spec: `flink/docs/superpowers/specs/2026-04-22-cluster-cpu-rank-producer-design.md`
- 기존 subscriber 패턴: `extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py` — `NodeStateSubscriber`
- 기존 Kit→React 포워딩 패턴: `extensions/datacenter_monitor/datacenter_monitor_python/message_handler.py` — `_send_to_client`, `send_scene_manifest`
- React 수신 디스패치: `react-dev-tools/src/OmniverseViewer.js` — `handleCustomEvent`
