# Datacenter Digital Twin — Demo Infrastructure (kafka-dummy)

데모용 데이터 파이프라인 전체 스택.
실제 데이터센터 없이 **시뮬레이션 메트릭**을 생성하여 Kafka, Iceberg, Trino를 통해 서빙합니다.

---

## 서비스 구성

```
┌─────────────────────────────────────────────────────────────────┐
│                     docker-compose.yaml                         │
│                                                                 │
│  kafka      :9092   KRaft 모드, 토픽 자동 생성                    │
│  minio      :9000   S3 호환 오브젝트 스토리지 (Parquet 파일 저장)  │
│  minio-init         warehouse 버킷 초기화 (one-shot)             │
│  nessie     :19120  Iceberg REST Catalog (ProjectNessie)        │
│  trino      :8080   SQL 쿼리 엔진 (Iceberg 테이블 조회)           │
│  seed-data          시드 데이터 생성 (one-shot)                   │
│  loop-producer      Kafka 실시간 메시지 발행 (무한 루프)           │
│  query-server :8000 FastAPI 백엔드 (React 대시보드용)             │
└─────────────────────────────────────────────────────────────────┘
```

### 서비스 기동 순서

```
kafka → minio → minio-init → nessie → trino
                            ↘ seed-data → loop-producer
kafka + trino → query-server
```

---

## 빠른 시작

```bash
cd extension/datacenter_monitor/kafka-dummy
docker compose up -d --build
```

| 서비스 | 주소 |
|--------|------|
| Query Server API | http://localhost:8000 |
| Swagger UI | http://localhost:8000/docs |
| Kafka | localhost:9092 |
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin) |
| Nessie API | http://localhost:19120 |
| Trino UI | http://localhost:8080 |

> **참고**: seed-data 컨테이너(dc_seed_data)가 완료(Exited 0)된 후 loop-producer가 데이터 전송을 시작합니다. 처음 시작 시 약 1~2분 소요됩니다.

### 상태 확인

```bash
# Query Server 헬스체크 (live_boxes: 13 이면 정상)
curl.exe http://localhost:8000/health

# 컨테이너 상태
docker compose ps

# 각 컨테이너 로그
docker compose logs -f loop-producer
docker compose logs -f seed-data
docker compose logs -f query-server
sudo docker exec dc_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## 데이터 파이프라인 상세

### 1. seed-data — Iceberg 초기 데이터 생성

**파일**: `seed-data/seed.py`

300초 × 13개 박스 = **3,900행**을 Iceberg 테이블 `dc.metrics_seed`에 한 번 적재합니다.
테이블이 이미 존재하면 스킵합니다.

**시나리오**:
- `t = 0 ~ 119s`: 정상 (normal) — 모든 지표 baseline 수준
- `t = 120 ~ 239s`: 경보 상승 — CPU/온도 상승, warning → critical
- `t = 240 ~ 299s`: 회복 구간ㅉ

### 3. query-server — FastAPI 백엔드

**파일**: `query-server/`

React 대시보드의 모든 HTTP/WebSocket 요청을 처리합니다.

#### 구성 모듈

| 모듈 | 역할 |
|------|------|
| `main.py` | FastAPI 앱, 엔드포인트 라우팅 |
| `live_cache.py` | Kafka Consumer → 메모리 캐시 (최신 메트릭 유지) |
| `replay_engine.py` | Iceberg 데이터 재생 (Kafka + WebSocket 동시 발행) |
| `trino_client.py` | Trino SQL 쿼리 (히스토리, Replay 데이터 조회) |
| `config.py` | 환경변수 설정 |

#### API 엔드포인트

| 메서드 | 경로 | 설명 |
|--------|------|------|
| GET | `/health` | 서버 상태 + live_boxes 수 |
| GET | `/topology` | 클러스터/박스 목록 |
| GET | `/metrics/latest` | 전체 박스 최신 메트릭 (LiveCache, Trino 미사용) |
| GET | `/metrics/range` | 데이터 가용 시간 범위 (Replay 피커용) |
| GET | `/metrics/{box_id}/history` | 특정 박스 시계열 히스토리 (Trino) |
| POST | `/replay/start` | Replay 시작 `{ from_ts, to_ts, speed }` |
| POST | `/replay/pause` | Replay 일시정지 |
| POST | `/replay/resume` | Replay 재개 |
| POST | `/replay/stop` | Replay 중지 |
| GET | `/replay/status` | Replay 현재 상태 |
| WS | `/ws/replay` | Replay 데이터 실시간 push |

#### LiveCache 동작

Kafka `datacenter.metrics` 토픽을 구독하여 box_id별 **최신 메시지**를 메모리에 보관합니다.
`/metrics/latest`는 이 캐시를 즉시 반환하므로 Trino 없이 저지연으로 서빙됩니다.

#### ReplayEngine 동작

```
POST /replay/start { from_ts, to_ts, speed }
  ↓
trino_client.get_replay_rows(from_ts, to_ts)  → Iceberg 데이터 조회
  ↓
asyncio Task: 원본 간격 / speed 만큼 sleep하며 순서대로 발행
  ├── Kafka: datacenter.metrics.replay 토픽
  └── WebSocket: /ws/replay에 연결된 클라이언트 전체
```