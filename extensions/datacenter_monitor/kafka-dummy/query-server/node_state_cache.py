"""
NodeStateCache
────────────────────────────────────────────────────────────────────────────
datacenter.metrics.node-state.events 토픽을 소비해 (cluster, node) 별 최신
node-state envelope 1개를 in-memory 보관합니다.

캐시는 덮어쓰기 only이며 TTL은 없습니다. HEALTHY transition envelope이
들어오면 client 필터에서 unhealthy 표에서 제외됩니다.
"""

import asyncio
import json
from typing import Optional

from config import KAFKA_BROKER, KAFKA_GROUP_ID, KAFKA_NODE_STATE_TOPIC

try:
    from aiokafka import AIOKafkaConsumer
except ImportError:
    AIOKafkaConsumer = None


class NodeStateCache:
    REQUIRED_FIELDS = (
        "kind", "scope", "cluster", "node", "status",
        "ts", "state_since", "last_seen_at", "gap_sec", "reasons",
    )
    VALID_STATUSES = frozenset(
        ("HEALTHY", "WARNING", "CRITICAL", "DISCONNECTED", "UNKNOWN")
    )

    def __init__(self):
        # key = f"{cluster}/{node}", value = envelope dict
        self._latest: dict[str, dict] = {}

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        if AIOKafkaConsumer is None:
            raise RuntimeError("aiokafka is not installed")
        self._running = True
        print(f"[NodeStateCache] 브로커 연결 시도: {KAFKA_BROKER}, 토픽: {KAFKA_NODE_STATE_TOPIC}")
        self._consumer = AIOKafkaConsumer(
            KAFKA_NODE_STATE_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            group_id=f"{KAFKA_GROUP_ID}-node-state",
            auto_offset_reset="latest",
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
            max_poll_interval_ms=300000,
        )
        await self._consumer.start()
        print(f"[NodeStateCache] 브로커 연결 OK, 파티션 할당: {self._consumer.assignment()}")
        self._task = asyncio.create_task(self._consume())
        self._task.add_done_callback(self._on_consume_done)
        print("[NodeStateCache] 시작")

    def _on_consume_done(self, task: asyncio.Task):
        if task.cancelled():
            print("[NodeStateCache] _consume task 취소됨")
        elif task.exception():
            import traceback
            print("[NodeStateCache] _consume task 예외 종료:")
            traceback.print_exception(
                type(task.exception()),
                task.exception(),
                task.exception().__traceback__,
            )

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
        print("[NodeStateCache] _consume 루프 시작")
        msg_count = 0
        try:
            async for msg in self._consumer:
                if not self._running:
                    break
                data = msg.value
                if not self._validate(data):
                    continue
                key = f"{data['cluster']}/{data['node']}"
                self._latest[key] = data
                msg_count += 1
                if msg_count <= 3:
                    print(
                        f"[NodeStateCache] 메시지 수신 #{msg_count}: "
                        f"key={key} status={data['status']} reasons={data['reasons']}"
                    )
        except Exception as e:
            print(f"[NodeStateCache] _consume 예외: {type(e).__name__}: {e}")
            raise
        finally:
            print(f"[NodeStateCache] _consume 루프 종료 (총 {msg_count}건)")

    def _validate(self, data) -> bool:
        if not isinstance(data, dict):
            print(f"[NodeStateCache] envelope drop - not dict: {type(data).__name__}")
            return False
        missing = [f for f in self.REQUIRED_FIELDS if f not in data]
        if missing:
            print(f"[NodeStateCache] envelope drop - missing fields {missing}")
            return False
        if data["status"] not in self.VALID_STATUSES:
            print(f"[NodeStateCache] envelope drop - unknown status {data['status']!r}")
            return False
        if not isinstance(data["reasons"], list):
            print(
                f"[NodeStateCache] envelope drop - reasons not list: "
                f"{type(data['reasons']).__name__}"
            )
            return False
        return True

    def get_latest_all(self) -> list[dict]:
        return list(self._latest.values())

    def get_latest(self, cluster: str, node: str) -> Optional[dict]:
        return self._latest.get(f"{cluster}/{node}")
