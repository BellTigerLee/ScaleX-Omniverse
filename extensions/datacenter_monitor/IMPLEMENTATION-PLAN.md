# datacenter_monitor (Omniverse) 구현 설계 / 갭 추적

> 상위 문서: [`/INTEGRATION-GAP-ANALYSIS.md`](../../../INTEGRATION-GAP-ANALYSIS.md) — `[GAP-n]` ID 공유.
> 대상: `ScaleX-Omniverse/extensions/datacenter_monitor` (Kit extension, Python)
> 역할: 3D 디지털트윈 시각화. Kafka(node-state/metrics/stageab) 소비 → USD prim 색상/펄스/랭킹.

---

## 1. 현재 구현 요약 (As-Is)

| 영역 | 상태 | 위치 |
|---|---|---|
| Kafka 3토픽 소비(metrics/node-state/stageab) | ✅ ACTIVE | `kafka_subscriber.py` (3 subscriber 스레드) |
| 색상/펄스 (canonical 상태 기반) | ✅ ACTIVE | `scene/material.py` `apply_node_state` |
| topology 발견(USD walk) + node↔prim 3단 해석 | ✅ ACTIVE | `scene/topology.py`, `scene/material.py:140-196` |
| cluster rank → React 전달 | ✅ ACTIVE | `ClusterRankSubscriber` → `MessageHandler.send_cluster_rank()` |
| 4-stage 네비/카메라/replay 토픽 스위치 | ✅ ACTIVE | scene mixins, `extension.py` |
| `datacenter.metrics.status` 색상 경로 | ❌ 의도적 비활성 | `extension.py:195-199` (status=placeholder) |
| 이벤트 알림 UI(ImagePanel, severity) | ❌ 비활성(Phase 2) | `extension.py:224-241` |
| FrontPanel material 캐싱 | ⚠ 레거시(glass cube로 대체) | `scene/material.py:202-228` |
| 상태 실린더 인디케이터 | ❌ 미구현(주석) | `scene/topology.py:120` |

### 소비 계약 (상위 §2 정본 동일)
- 색상 정본 = `node-state.events` (`status`/`reasons`). `datacenter.metrics`의 `status`는 무시(placeholder).
- cluster rank = `stageab` (`id:"cluster-rank"` 아닌 레코드는 drop).

---

## 2. 구현 요소 (Backlog)

### §visual-todo — `[GAP-6]` 미시각화 메트릭 🟠
수신·캐시되나 화면 표현 없음:
- [ ] **Power(`gpu.pwr`)**: HDR 강도/색으로 전력 부하 표현(임계 정의 필요).
- [ ] **Network 지연(`net.rtt_p50/p95/p99_ms`)**: 노드 간 링크/배지 시각화.
- [ ] **Storage util**: 색/마커 피드백.
- [ ] **시계열 추세**: 현재 latest만. QueryServer `/metrics/{box}/history` 연동한 미니 추세 표시 검토.

### §alert-ui — `[GAP-1]` reasons 기반 알림 재설계 🔴(Phase 2)
- [ ] `node-state.events`의 `reasons[]` 기반 알림 UI 신규 spec(기존 severity/ImagePanel 경로는 canonical envelope에 severity 없어 비활성됨, `extension.py:226`).
- [ ] QueryServer events 파이프라인 결정(`[GAP-1]`)과 정합 — 알림 소스를 node-state 전이로 통일.

### §topology-mapping — `[GAP-4]` 매핑 견고화 🟠
- [ ] node↔prim 3단 해석(REST topology → BOX_ heuristic → `DEV_FAKE_NODE_MAPPING`) 중 **dev fake 의존 제거** 경로 확보.
- [ ] `TOPOLOGY_URL`(QueryServer `/topology`)이 동적화(`[GAP-4]`)되면 heuristic 폴백 의존도 감소 — 연동 확인.
- [ ] USD prim 이름 ↔ Kafka node 이름 ↔ topology.json 3자 동기화 규약 문서화.

### §scene-hardcoded — `[GAP-7]` USD 가정 외부화 🟡
하드코딩(불일치 시 graceful fallback 없음):
- [ ] `SCALE_POD_PATH="/World/SCENT_Multi_POD_Module/ScaleX_POD"`, `CLUSTER_SUFFIX/RACK_PREFIX/BOX_PREFIX` (`global_variables.py:27-36`) → 설정/프로파일화 또는 누락 시 경고+부분동작.
- [ ] 카메라 좌표(`global_variables.py:130-131`) → 스테이지에서 읽되 실패 시만 기본값.
- [ ] 노드 X 정규화 특수 케이스(DGX 등 `global_variables.py:175-182`) 일반화.
- [ ] `kafka-dummy/` 로컬 데모(localhost 하드코딩, seed 28노드/2클러스터, health `live_boxes==13` 가정)는 데모 전용 명시 — 프로덕션 프로파일과 분리 유지.

---

## 3. 검증 기준 (Done = 증거)
- [ ] reasons 기반 알림: WARNING/CRITICAL 전이 시 UI 표시(스크린샷/녹화).
- [ ] power/rtt/storage 시각화 동작 확인.
- [ ] dev fake 끈 상태에서 실 topology로 전 노드 매핑(미매핑 0).
- [ ] `python -m pytest` 통과.

---

## 4. 참고 (코드 좌표)
- 엔트리/루프: `datacenter_monitor_python/extension.py`
- 설정/하드코딩: `datacenter_monitor_python/global_variables.py`, `config/env.*`, `config_loader.py`
- Kafka: `datacenter_monitor_python/kafka_subscriber.py`
- 씬/색상/topology: `datacenter_monitor_python/scene/` (material/topology/node_state_pulse 등)
- React 브리지: `datacenter_monitor_python/message_handler.py`
- 데모 인프라: `kafka-dummy/` (docker-compose, seed-data, loop-producer)
