# Datacenter Digital Twin — Omniverse Kit Extension

Omniverse Kit(USD Viewer)용 Python Extension.
Kafka 메트릭을 실시간으로 구독하여 USD 씬의 서버 색상을 자동 업데이트하고,
React 대시보드와 WebRTC로 양방향 메시지를 교환합니다.

---

## 아키텍처 개요

```
┌─────────────────────────────────────────────────────────────────┐
│                 Omniverse Kit Extension                         │
│                                                                 │
│  extension.py          — 메인 진입점, 생명주기 관리              │
│  ├── KafkaSubscriber   — 백그라운드 Kafka 소비 스레드            │
│  ├── SceneManager      — USD 씬 조작 (색상, 카메라, topology)    │
│  └── MessageHandler    — React ↔ Kit WebRTC 메시지 라우터        │
│                                                                 │
│  ←── Kafka ────── loop-producer / replay-engine                 │
│  ←── WebRTC ───── React Dashboard (OmniverseViewer.js)          │
└─────────────────────────────────────────────────────────────────┘
```

---

## 파일 구조

| 파일 | 역할 |
|------|------|
| `extension.py` | Kit Extension 진입점 (`on_startup` / `on_shutdown`) |
| `global_variables.py` | Kafka 설정, USD 경로, 색상 임계값 등 모든 설정값 |
| `kafka_subscriber.py` | Kafka 소비 백그라운드 스레드, 토픽 전환(switch_topic) |
| `message_handler.py` | React → Extension 메시지 파싱 및 라우팅 |
| `scene_manager.py` | USD Prim 탐색, 색상 변경, 카메라 애니메이션, topology 조회 |

---

## 동작 흐름

### 실시간 색상 업데이트

```
Kafka (datacenter.metrics)
  ↓  KafkaSubscriber (백그라운드 스레드)
Queue (thread-safe, maxsize=500)
  ↓  Extension._on_update() — Kit 메인 스레드 (매 프레임)
SceneManager.update_node_color_from_kafka(msg, view_stage)
  ↓
USD Prim Front Panel Material emissive 색상 변경
  normal   → 기본 색 (발광 없음)
  warning  → 노란색 발광 (500 nit HDR)
  critical → 빨간색 발광 (3000 nit HDR) + Alert Decal
```

> USD 조작은 반드시 Kit 메인 스레드에서만 가능합니다. 큐를 통해 Kafka 스레드에서 메인 스레드로 데이터를 전달합니다.

### Replay 토픽 전환

```
React: replay_start 메시지 전송
  ↓  MessageHandler._on_incoming_message("replay_start")
  ↓  Extension._on_replay_start()
KafkaSubscriber.switch_topic("datacenter.metrics.replay")
  ↓  consumer 재시작 → replay 토픽 구독
Replay 데이터로 씬 색상 업데이트

React: replay_stop 메시지 전송
  ↓  Extension._on_replay_stop()
KafkaSubscriber.switch_topic("datacenter.metrics")   ← live 복원
```

### WebRTC 통신 흐름

```
React → Kit:
  AppStreamer.sendMessage({ event_type: "datacenter_monitor", payload: {...} })
  → omni.kit.livestream.messaging 확장 → carb.eventdispatcher
  → MessageHandler._on_incoming_raw() → _on_incoming_message()

Kit → React:
  omni.kit.app.queue_event("omni.kit.livestream.send_message", {"message": json})
  → WebRTC 스트리밍 레이어 → React onCustomEvent()
```

---

## 메시지 프로토콜

### React → Extension (수신)

| type | 파라미터 | 동작 |
|------|---------|------|
| `cluster_focus` | `prim`, `hide_others` | Stage A→B, 다른 클러스터 숨김, 카메라 이동 |
| `rack_focus` | `prim`, `hide_others` | Stage B→C, 다른 랙 숨김, 카메라 이동 |
| `node_inspect` | `prim` | Stage C→D, 노드 pop-forward |
| `node_deselect` | `return_rack` | Stage D→C, 노드 복귀 |
| `rack_deselect_to_cluster` | `return_cluster` | Stage C→B, 랙 복귀 |
| `scene_reset` | — | Stage →A, 전체 씬 복원 |
| `get_topology` | — | scene_manifest 전송 |
| `replay_start` | — | Kafka 토픽 → replay 전환 |
| `replay_stop` | — | Kafka 토픽 → live 복원 |

### Extension → React (송신)

| event_type | payload | 전송 시점 |
|------------|---------|----------|
| `selection_changed` | `{ active, selected }` | 뷰포트에서 prim 클릭 시 |
| `scene_manifest` | `{ clusters, racks }` | Stage 로드 완료 시 / get_topology 요청 시 |
| `alert_notification` | `{ rack_id, node_id, status, metrics }` | 경고 발생 시 |

---

## 씬 View State (4단계)

| Stage | 상태 | Extension 동작 |
|-------|------|---------------|
| **A** | 전체 씬 | 모든 prim 표시, overview 카메라 |
| **B** | 클러스터 포커스 | 선택 클러스터만 표시, 나머지 숨김 |
| **C** | 랙 포커스 | 선택 랙만 표시, 랙 카메라 (BBox 자동 계산) |
| **D** | 노드 인스펙션 | 선택 노드 pop-forward (`NODE_POP_DISTANCE`) |

---

## Kafka 메시지 스키마

### 메트릭 메시지 (`datacenter.metrics` / `datacenter.metrics.replay`)

```jsonc
{
  "ts":       1775454754796,           // epoch ms
  "cluster":  "datax",                 // USD prim 매칭 (대소문자 무관)
  "node":     "work5",                 // USD prim 매칭
  "status":   "HEALTHY",               // "HEALTHY" | "WARNING" | "CRITICAL"
  "metrics": {
    "cpu":     { "util": 0.035, "cores": 12.0, "load1": 0.38,
                 "load5": 0.41, "load15": 0.43, "eff": 0.053 },
    "mem":     { "util": 0.201, "total_gb": 16.62,
                 "avail_gb": 13.29, "oom_cnt": 0 },
    "net":     { "retrans": 0.0, "in_mbps": 2.62, "out_mbps": 2.89,
                 "nic_err_sum": 0.0, "nic_drop_sum": 0.0,
                 "netstat_err": 0.0, "err_sum": 0.0 },
    "gpu":     { "util": 0.0, "temp": 38.0, "pwr": 24.868,
                 "mem_util": 0.0, "mem_used_gb": 0.0, "total_gb": 23.525 },
    "storage": { "util": 0.274, "read_mbps": 0.0,
                 "write_mbps": 0.171, "io_mbps": 0.171 }
  },
  "debug_ts": 1775454757851            // 디버그용
}
```

**소비자:** `KafkaSubscriber` → `SceneManager.update_node_color_from_kafka()`

### 이벤트 메시지 (`datacenter.metrics.event` / `datacenter.metrics.replay.event`)

```jsonc
{
  "event_id":   "uuid4",              // 고유 이벤트 ID
  "ts":         "2026-03-13T10:15:07Z", // ISO 8601 UTC (발행 시각)
  "cluster":    "datax",               // 클러스터 이름
  "rack":       "Rack_42U_A3",         // 랙 이름
  "node":       "Box_4U_HDD_1",        // 노드(Box) 이름
  "scope":      "node",               // 이벤트 범위: "node" | "rack" | "cluster"
  "source":     "flink-health-engine", // 이벤트 발생 소스
  "event_type": "HEALTH_TRANSITION",   // 이벤트 유형
  "category":   "health",             // 카테고리
  "severity":   "WARNING",            // "INFO" | "WARNING" | "CRITICAL"
  "status":     "OPEN",               // 이벤트 상태
  "from":       "HEALTHY",            // 이전 상태
  "to":         "WARNING",            // 전환 상태
  "score":      0.63,                 // 건강 점수 (0.0 ~ 1.0)
  "message":    "Node health changed from HEALTHY to WARNING",
  "reason":     { "cpu_util": 0.91 }, // 원인 (cpu_util, mem_util 중 해당 값만 포함)
  "original_ts": 1706000000           // 시드 데이터 원본 타임스탬프 (replay 동기화용)
}
```

**소비자 (legacy — 주석 처리됨, 2026-04-17):** 이전에는 `EventKafkaSubscriber` → `SceneManager.show_event_panel()` 경로로 `severity`에 따라 ImagePanel 경고를 띄웠으나, `datacenter.metrics.node-state.events` canonical 스키마로 이관되면서 `EventKafkaSubscriber` 는 `NodeStateSubscriber` 로 개명되고 이 경로의 호출부는 `extension.py::_on_update` 에서 주석 처리되었다. reasons-driven 알림 UI 는 Phase 2 에서 재설계 예정.

### Kafka 토픽 요약

| 토픽 | 용도 | Producer | Consumer |
|------|------|----------|----------|
| `datacenter.metrics` | 실시간 메트릭 | loop-producer | Extension (`KafkaSubscriber`) |
| `datacenter.metrics.node-state.events` | canonical node-state (HEALTHY/DISCONNECTED) | Flink workload-process | Extension (`NodeStateSubscriber` → `SceneManager.apply_node_state` → `tick_pulse`) |
| `datacenter.metrics.event` | (legacy) HEALTH_TRANSITION 이벤트 | event-producer | 현재 미사용 — 위 node-state.events 토픽이 대체 |
| `datacenter.metrics.replay` | 리플레이 메트릭 | query-server (ReplayEngine) | Extension (토픽 전환) |
| `datacenter.metrics.replay.event` | 리플레이 이벤트 | query-server (ReplayEngine) | Extension (토픽 전환) |

---

## 색상 / 발광 임계값

`global_variables.py`에서 수정:

```python
TEMP_NORMAL   = 70.0    # °C 미만: 정상 (발광 없음)
TEMP_WARNING  = 85.0    # °C 미만: warning (노란색 HDR 발광)
                        # °C 이상: critical (빨간색 HDR 발광 + Alert Decal)

EMISSIVE_WARNING_INTENSITY  = 500.0   # nit (HDR)
EMISSIVE_CRITICAL_INTENSITY = 3000.0  # nit (HDR Bloom 효과)
```

> 현재는 `status` 필드(`normal`/`warning`/`critical`)를 직접 사용합니다.
> 온도 기반 계산이 필요하다면 `global_variables.py`의 임계값과 `scene_manager.py`를 참고하세요.

## Kafka 라이브러리 설치

confluent-kafka (권장):

```bash
# Kit Python 경로 확인 후 실행
<kit_python> -m pip install confluent-kafka

# 예시
C:/Users/.../kit-app-template/_build/windows-x86_64/release/kit/python/python.exe -m pip install confluent-kafka
```

kafka-python (fallback, confluent-kafka 없을 때 자동 사용):

```bash
<kit_python> -m pip install kafka-python
```

