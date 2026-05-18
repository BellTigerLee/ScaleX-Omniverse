"""
ReplayEngine
────────────────────────────────────────────────────────────────────────────
Iceberg 데이터를 시간 순서대로 재생합니다.

동작:
  1. Trino에서 지정 구간 데이터 로드
  2. 원본 타임스탬프 간격 / speed 속도로 재생
  3. 각 스텝마다 동시에:
       - Kafka replay 토픽 → Omniverse Extension (switch_topic 으로 전환됨)
       - WebSocket → React 대시보드

타임코드 동기화:
  모든 메시지에 playback_ts(현재 서버시각) + original_ts(원본 시각) 포함.
  React와 Omniverse 모두 동일한 original_ts를 기준 축으로 사용합니다.
"""

import asyncio
import json
import time
from collections import defaultdict
from enum import Enum
from typing import Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from config import KAFKA_BROKER, KAFKA_REPLAY_TOPIC, KAFKA_EVENT_TOPIC, KAFKA_REPLAY_EVENT_TOPIC


class ReplayState(str, Enum):
    IDLE    = "idle"
    PLAYING = "playing"
    PAUSED  = "paused"


class ReplayEngine:
    def __init__(self):
        self._state:      ReplayState = ReplayState.IDLE
        self._from_ts:    int   = 0
        self._to_ts:      int   = 0
        self._current_ts: int   = 0
        self._speed:      float = 1.0

        self._task:       Optional[asyncio.Task] = None
        self._ws_clients: set = set()

        self._producer = Producer({"bootstrap.servers": KAFKA_BROKER})
        self._ensure_topic(KAFKA_REPLAY_TOPIC)
        self._ensure_topic(KAFKA_EVENT_TOPIC)
        self._ensure_topic(KAFKA_REPLAY_EVENT_TOPIC)

    def _ensure_topic(self, topic_name: str):
        """서버 기동 시 토픽이 없으면 즉시 생성."""
        admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
        existing = admin.list_topics(timeout=5).topics
        if topic_name not in existing:
            fs = admin.create_topics(
                [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
            )
            for topic, f in fs.items():
                try:
                    f.result()
                    print(f"[ReplayEngine] 토픽 생성 완료: {topic}")
                except Exception as e:
                    print(f"[ReplayEngine] 토픽 생성 실패 (이미 존재할 수 있음): {e}")
        else:
            print(f"[ReplayEngine] 토픽 이미 존재: {topic_name}")

    # ── WebSocket 클라이언트 관리 ─────────────────────────────────────────────

    def add_ws(self, ws):
        self._ws_clients.add(ws)

    def remove_ws(self, ws):
        self._ws_clients.discard(ws)

    # ── 상태 조회 ─────────────────────────────────────────────────────────────

    def status(self) -> dict:
        return {
            "state":      self._state,
            "from_ts":    self._from_ts,
            "to_ts":      self._to_ts,
            "current_ts": self._current_ts,
            "speed":      self._speed,
        }

    # ── 제어 ─────────────────────────────────────────────────────────────────

    async def start(self, rows: list[dict], from_ts: int, to_ts: int, speed: float = 1.0,
                    event_rows: list[dict] | None = None):
        await self._stop_task()
        self._from_ts    = from_ts
        self._to_ts      = to_ts
        self._current_ts = from_ts
        self._speed      = speed
        self._state      = ReplayState.PLAYING
        self._task = asyncio.create_task(self._run(rows, event_rows or []))
        print(f"[ReplayEngine] 시작 from={from_ts} to={to_ts} speed={speed}x  "
              f"rows={len(rows)} event_rows={len(event_rows or [])}")

    async def pause(self):
        if self._state == ReplayState.PLAYING:
            self._state = ReplayState.PAUSED
            print("[ReplayEngine] 일시정지")

    async def resume(self):
        if self._state == ReplayState.PAUSED:
            self._state = ReplayState.PLAYING
            print("[ReplayEngine] 재개")

    async def stop(self):
        await self._stop_task()
        print("[ReplayEngine] 정지")

    async def _stop_task(self):
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._state = ReplayState.IDLE

    # ── 재생 루프 ─────────────────────────────────────────────────────────────

    async def _run(self, rows: list[dict], event_rows: list[dict]):
        # metrics ts별 그룹화
        groups: dict[int, list] = defaultdict(list)
        for row in rows:
            groups[row["event_ts"]].append(row)
        ts_keys = sorted(groups.keys())

        # events ts별 그룹화
        event_groups: dict[int, list] = defaultdict(list)
        for ev in event_rows:
            event_groups[ev["event_ts"]].append(ev)

        if not ts_keys:
            print("[ReplayEngine] 데이터 없음")
            self._state = ReplayState.IDLE
            return

        prev_ts = ts_keys[0]

        for ts in ts_keys:
            # pause 대기
            while self._state == ReplayState.PAUSED:
                await asyncio.sleep(0.2)
            if self._state == ReplayState.IDLE:
                break

            self._current_ts = ts

            # 원본 간격 / speed 만큼 대기
            gap = (ts - prev_ts) / self._speed
            if gap > 0:
                await asyncio.sleep(gap)
            prev_ts = ts

            # metrics 배치 발행
            await self._publish_batch(groups[ts])

            # 해당 ts 구간의 이벤트 발행
            if ts in event_groups:
                await self._publish_events(event_groups[ts])

        self._state = ReplayState.IDLE
        print("[ReplayEngine] 재생 완료")

    async def _publish_batch(self, batch: list[dict]):
        now_ms = int(time.time() * 1000)
        dead_ws: set = set()

        for row in batch:
            status = row.get("status", "HEALTHY")
            msg = {
                "type":       "metrics",
                # live 메시지 포맷과 동일 (loop_producer 호환)
                "cluster":    row["cluster_id"],
                "node":       row["box_id"],
                "status":     status,
                "metrics": {
                    "cpu": {
                        "util":  round(row.get("cpu_util",  0.0), 4),
                        "cores": row.get("cpu_cores", 0.0),
                        "load5": round(row.get("cpu_load5", 0.0), 2),
                        "eff":   round(row.get("cpu_eff",   0.0), 4),
                    },
                    "mem": {
                        "util":     round(row.get("mem_util",     0.0), 4),
                        "total_gb": row.get("mem_total_gb", 0.0),
                        "avail_gb": round(row.get("mem_avail_gb", 0.0), 1),
                        "oom_cnt":  int(row.get("mem_oom_cnt", 0)),
                    },
                    "net": {
                        "in_mbps":  row.get("net_in_mbps",  0.0),
                        "out_mbps": row.get("net_out_mbps", 0.0),
                        "retrans":  round(row.get("net_retrans",  0.0), 1),
                        "err_sum":  int(row.get("net_err_sum", 0)),
                    },
                    "gpu": {
                        "util":     round(row.get("gpu_util",     0.0), 4),
                        "mem_util": round(row.get("gpu_mem_util", 0.0), 4),
                        "temp":     round(row.get("gpu_temp",     0.0), 1),
                        "pwr":      round(row.get("gpu_pwr",      0.0), 1),
                    },
                    "storage": {
                        "util":    round(row.get("storage_util",    0.0), 4),
                        "io_mbps": row.get("storage_io_mbps", 0.0),
                    },
                },
                "ts":          now_ms,
                "original_ts": row["event_ts"],
                "playback_ts": now_ms,
            }
            payload = json.dumps(msg).encode("utf-8")

            # Kafka → Omniverse Extension
            self._producer.produce(
                KAFKA_REPLAY_TOPIC,
                key=f"{row['cluster_id']}/{row['box_id']}",
                value=payload,
            )

            # WebSocket → React
            for ws in self._ws_clients:
                try:
                    await ws.send_text(json.dumps(msg))
                except Exception:
                    dead_ws.add(ws)

        self._producer.flush()
        self._ws_clients -= dead_ws

    async def _publish_events(self, events: list[dict]):
        """이벤트 배치를 Kafka event 토픽과 WebSocket으로 발행."""
        now_ms = int(time.time() * 1000)
        dead_ws: set = set()

        for ev in events:
            msg = {
                "type":        "event",
                "event_id":    ev.get("event_id", ""),
                "ts":          now_ms,
                "original_ts": ev["event_ts"],
                "playback_ts": now_ms,
                "cluster":     ev.get("cluster", ""),
                "rack":        ev.get("rack", ""),
                "node":        ev.get("node", ""),
                "scope":       ev.get("scope", "node"),
                "source":      ev.get("source", ""),
                "event_type":  ev.get("event_type", ""),
                "category":    ev.get("category", ""),
                "severity":    ev.get("severity", ""),
                "status":      ev.get("status", ""),
                "from":        ev.get("from_state", ""),
                "to":          ev.get("to_state", ""),
                "score":       ev.get("score", 0.0),
                "message":     ev.get("message", ""),
                "reason": {
                    k: v for k, v in {
                        "cpu_util": ev.get("reason_cpu"),
                        "mem_util": ev.get("reason_mem"),
                    }.items() if v is not None
                },
            }
            payload = json.dumps(msg).encode("utf-8")

            self._producer.produce(
                KAFKA_REPLAY_EVENT_TOPIC,
                key=f"{ev.get('cluster', '')}/{ev.get('rack', '')}/{ev.get('node', '')}",
                value=payload,
            )

            for ws in self._ws_clients:
                try:
                    await ws.send_text(json.dumps(msg))
                except Exception:
                    dead_ws.add(ws)

        self._producer.flush()
        self._ws_clients -= dead_ws
