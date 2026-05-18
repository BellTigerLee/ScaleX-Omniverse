"""
LiveCache
────────────────────────────────────────────────────────────────────────────
Kafka에서 실시간 메트릭을 소비해 in-memory에 보관합니다.
/metrics/latest 엔드포인트는 Trino를 거치지 않고 여기서 직접 응답합니다.

실시간 동기화:
  Omniverse Extension도 같은 Kafka 토픽을 구독하므로
  두 클라이언트 모두 동일한 message.timestamp를 기준으로 사용합니다.
  LiveCache의 ~1초 지연은 대시보드 특성상 허용 범위입니다.
"""

import asyncio
import json
from collections import defaultdict, deque
from typing import Optional

from aiokafka import AIOKafkaConsumer
from config import KAFKA_BROKER, KAFKA_TOPIC, KAFKA_GROUP_ID, LIVE_CACHE_SIZE, KAFKA_EVENT_TOPIC

class LiveCache:
    def __init__(self):
        # box_id → deque (최근 LIVE_CACHE_SIZE개 메시지)
        self._history: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=LIVE_CACHE_SIZE)
        )
        # box_id → 가장 최근 메시지 1개
        self._latest: dict[str, dict] = {}

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task:     Optional[asyncio.Task]     = None
        self._running = False

    async def start(self):
        self._running  = True
        print(f"[LiveCache] 브로커 연결 시도: {KAFKA_BROKER}, 토픽: {KAFKA_TOPIC}")
        self._consumer = AIOKafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{KAFKA_GROUP_ID}-live",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
        )
        await self._consumer.start()
        print(f"[LiveCache] 브로커 연결 OK, 파티션 할당: {self._consumer.assignment()}")
        self._task = asyncio.create_task(self._consume())
        self._task.add_done_callback(self._on_consume_done)
        print("[LiveCache] 시작")

    def _on_consume_done(self, task: asyncio.Task):
        """_consume task 종료 시 호출 — 예외를 로그에 기록."""
        if task.cancelled():
            print("[LiveCache] _consume task 취소됨")
        elif task.exception():
            import traceback
            print("[LiveCache] _consume task 예외 종료:")
            traceback.print_exception(type(task.exception()), task.exception(), task.exception().__traceback__)

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()

    async def _consume(self):
        print("[LiveCache] _consume 루프 시작")
        msg_count = 0
        try:
            async for msg in self._consumer:
                if not self._running:
                    break
                data   = msg.value
                box_id = data.get("node") or data.get("box_id") or data.get("node_id", "unknown")
                self._history[box_id].append(data)
                self._latest[box_id] = data
                msg_count += 1
                if msg_count <= 3:
                    print(f"[LiveCache] 메시지 수신 #{msg_count}: box={box_id} topic={msg.topic} partition={msg.partition} offset={msg.offset}")
        except Exception as e:
            print(f"[LiveCache] _consume 예외: {type(e).__name__}: {e}")
            raise
        finally:
            print(f"[LiveCache] _consume 루프 종료 (총 {msg_count}건)")

    # ── 조회 API ────────────────────────────────────────────────────────────

    def get_latest_all(self) -> list[dict]:
        """모든 박스의 최신 상태를 반환."""
        return list(self._latest.values())

    def get_latest(self, box_id: str) -> Optional[dict]:
        return self._latest.get(box_id)

    def get_in_memory_history(self, box_id: str) -> list[dict]:
        """인메모리 링버퍼의 히스토리 (단기, 최대 LIVE_CACHE_SIZE개)."""
        return list(self._history.get(box_id, []))


class EventCache:
    """
    datacenter.metrics.event 토픽을 소비해 최근 이벤트를 in-memory 보관.
    최대 EVENT_CACHE_SIZE개 이벤트를 링버퍼에 유지.
    """

    EVENT_CACHE_SIZE = 200

    def __init__(self):
        self._events: deque = deque(maxlen=self.EVENT_CACHE_SIZE)
        # node → 해당 노드의 최근 이벤트 deque
        self._by_node: dict[str, deque] = defaultdict(lambda: deque(maxlen=50))

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task:     Optional[asyncio.Task]     = None
        self._running = False

    async def start(self):
        self._running  = True
        self._consumer = AIOKafkaConsumer(
            KAFKA_EVENT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{KAFKA_GROUP_ID}-events",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume())
        print("[EventCache] 시작")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()

    async def _consume(self):
        async for msg in self._consumer:
            if not self._running:
                break
            data = msg.value
            self._events.append(data)
            node = data.get("node", "unknown")
            self._by_node[node].append(data)

    # ── 조회 API ─────────────────────────────────────────────────────────────

    def get_recent(self, n: int = 50) -> list[dict]:
        """최근 n개 이벤트 반환 (최신순)."""
        events = list(self._events)
        return list(reversed(events[-n:]))

    def get_by_node(self, node_id: str) -> list[dict]:
        """특정 노드의 최근 이벤트 반환 (최신순)."""
        return list(reversed(list(self._by_node.get(node_id, []))))
