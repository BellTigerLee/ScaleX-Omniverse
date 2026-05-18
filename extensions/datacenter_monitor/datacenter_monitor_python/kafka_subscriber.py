"""
KafkaSubscriber
Kafka 메시지를 백그라운드 스레드에서 소비하여 thread-safe 큐에 넣습니다.
USD 조작은 Kit 메인 스레드에서만 가능하므로 직접 조작하지 않습니다.

라이브러리 우선순위:
  1. confluent-kafka  (성능 우수, 권장)
  2. kafka-python     (fallback)

설치 (kit-app-template 기준):
  C:/Users/.../kit-app-template/_build/windows-x86_64/release/kit/python/python.exe -m pip install confluent-kafka
"""

import json
import queue
import threading
import time


# ── Canonical node-state envelope 파서 ────────────────────────────────────
# 2026-04-17-node-state-message-schema-design.md §5 envelope 를 그대로 소비.

_NODE_STATE_REQUIRED_FIELDS = (
    "kind", "scope", "cluster", "node", "status",
    "ts", "state_since", "last_seen_at", "gap_sec", "reasons",
)
_NODE_STATE_VALID_STATUSES = frozenset(
    ("HEALTHY", "WARNING", "CRITICAL", "DISCONNECTED", "UNKNOWN")
)


def parse_node_state_message(raw: bytes):
    """
    Flink canonical node-state envelope (UTF-8 JSON bytes) → dict 또는 None.

    검증:
      - JSON 디코딩 성공
      - 필수 필드(_NODE_STATE_REQUIRED_FIELDS) 전부 존재
      - status 값이 5-enum 중 하나
    실패 시 경고 로그를 남기고 None 반환 (호출자가 드롭).
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[NodeStateParser] JSON/UTF-8 디코드 실패: {e}")
        return None

    if not isinstance(data, dict):
        print(f"[NodeStateParser] 최상위가 dict 아님: {type(data).__name__}")
        return None

    missing = [f for f in _NODE_STATE_REQUIRED_FIELDS if f not in data]
    if missing:
        print(f"[NodeStateParser] 필수 필드 누락 {missing}, 드롭: {list(data.keys())}")
        return None

    status = data["status"]
    if status not in _NODE_STATE_VALID_STATUSES:
        print(f"[NodeStateParser] 알 수 없는 status 값 '{status}', 드롭")
        return None

    return data


# ── Cluster rank (stageab) 페이로드 파서 ──────────────────────────────
# 2026-04-22-cluster-rank-consumer-poc-design.md §5.2 참고.
# 토픽은 향후 다른 id (cluster-severity 등) 도 수용하므로 id=="cluster-rank" 로 필터.

_CLUSTER_RANK_REQUIRED_FIELDS = ("id", "cluster", "ts", "ranking")


def parse_cluster_rank_message(raw: bytes):
    """
    Flink stageab cluster-rank 페이로드 (UTF-8 JSON bytes) → dict 또는 None.

    검증:
      - JSON / UTF-8 디코딩 성공
      - top-level 이 dict
      - 필수 필드(_CLUSTER_RANK_REQUIRED_FIELDS) 전부 존재
      - id == "cluster-rank"   (그 외 aggregate 는 조용히 드롭)
      - ranking 이 list
    실패 시 필요하면 경고 로그 후 None 반환.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[ClusterRankParser] JSON/UTF-8 디코드 실패: {e}")
        return None

    if not isinstance(data, dict):
        print(f"[ClusterRankParser] 최상위가 dict 아님: {type(data).__name__}")
        return None

    missing = [f for f in _CLUSTER_RANK_REQUIRED_FIELDS if f not in data]
    if missing:
        print(f"[ClusterRankParser] 필수 필드 누락 {missing}, 드롭: {list(data.keys())}")
        return None

    if data["id"] != "cluster-rank":
        # 같은 토픽의 다른 aggregate — 정상 경로, 로그 없이 드롭.
        return None

    if not isinstance(data["ranking"], list):
        print(f"[ClusterRankParser] ranking 이 list 아님: {type(data['ranking']).__name__}")
        return None

    return data


# ── 라이브러리 감지 ─────────────────────────────────────────────────────────
_BACKEND = None

try:
    from confluent_kafka import Consumer as _ConfluentConsumer, KafkaError, KafkaException
    _BACKEND = "confluent"
except ImportError:
    try:
        from kafka import KafkaConsumer as _KafkaConsumer
        from kafka.errors import NoBrokersAvailable
        _BACKEND = "kafka-python"
    except ImportError:
        _BACKEND = None


class KafkaSubscriber:
    """
    백그라운드 스레드에서 Kafka 토픽을 구독합니다.

    [수정 포인트 - KAFKA MESSAGE PARSING]
    _parse_message() 메서드에서 실제 Kafka 메시지 형식에 맞게
    파싱 로직을 수정하세요.
    """

    def __init__(
        self,
        bootstrap_servers: list,
        topic: str,
        group_id: str,
        data_queue: queue.Queue,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._queue = data_queue

        self._thread: threading.Thread | None = None
        self._running = False
        self._connected = False

        # Replay 모드용 토픽 전환
        self._current_topic = topic

        if _BACKEND is None:
            print(
                "[KafkaSubscriber] ❌ Kafka 라이브러리 미설치.\n"
                "  설치 명령:\n"
                "  <kit_python> -m pip install confluent-kafka\n"
                "  예) C:/Users/.../kit-app-template/_build/windows-x86_64/release/kit/python/python.exe -m pip install confluent-kafka"
            )
        else:
            print(f"[KafkaSubscriber] 백엔드: {_BACKEND}")

    # ── 제어 ─────────────────────────────────────────────────────────────────

    def start(self):
        if _BACKEND is None:
            return
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop, name="KafkaSubscriberThread", daemon=True
        )
        self._thread.start()
        print(f"[KafkaSubscriber] 시작 — topic: {self._current_topic}")

    def stop(self):
        self._running = False
        print("[KafkaSubscriber] 정지 요청")

    def is_connected(self) -> bool:
        return self._connected

    # ── [수정 포인트 - REPLAY MODE] ───────────────────────────────────────────
    # Replay 모드에서 Query Server가 Replay 토픽으로 publish하면
    # 이 메서드를 호출하여 consumer 토픽을 전환하세요.
    def switch_topic(self, new_topic: str):
        """
        소비 토픽을 전환합니다 (live ↔ replay).
        Kit 메인 스레드에서 호출해도 안전하도록 blocking sleep 없음.
        consume loop가 토픽 변경을 감지해 자연 종료 → 새 스레드 즉시 시작.
        """
        if self._current_topic != new_topic:
            self._current_topic = new_topic   # 구 스레드 inner loop 탈출 트리거
            self._thread = threading.Thread(
                target=self._consume_loop, name="KafkaSubscriberThread", daemon=True
            )
            self._thread.start()
            print(f"[KafkaSubscriber] 토픽 전환 → {new_topic}")

    # ── 내부: 소비 루프 ───────────────────────────────────────────────────────

    def _consume_loop(self):
        if _BACKEND == "confluent":
            self._consume_loop_confluent()
        elif _BACKEND == "kafka-python":
            self._consume_loop_kafka_python()

    def _consume_loop_confluent(self):
        """confluent-kafka Consumer 루프"""
        my_topic = self._current_topic
        while self._running:
            consumer = None
            try:
                conf = {
                    # [수정 포인트 - KAFKA BOOTSTRAP]
                    "bootstrap.servers": ",".join(str(s) for s in self._bootstrap_servers),
                    "group.id": self._group_id,
                    # [수정 포인트 - KAFKA CONSUMER OPTIONS]
                    "auto.offset.reset": "latest",       # 최신부터 소비
                    "enable.auto.commit": True,
                    "session.timeout.ms": 10000,
                }
                consumer = _ConfluentConsumer(conf)
                consumer.subscribe([my_topic])
                self._connected = True
                print(f"[KafkaSubscriber] ✅ confluent-kafka 연결 성공 — {self._bootstrap_servers}")

                while self._running and self._current_topic == my_topic:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())
                    parsed = self._parse_message(msg.value())
                    if parsed is not None:
                        # 큐가 가득 찼으면 가장 오래된 것 버리고 새것 삽입
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        # Kit 메인 스레드가 GIL을 확보하여 _on_update()를 실행하도록
                        # time.sleep(0)은 2개 Kafka 스레드 경쟁 시 불충분 → 1ms 보장
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[KafkaSubscriber] ⚠️ confluent-kafka 오류: {e}")
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._current_topic != my_topic:
                break
            if self._running:
                print("[KafkaSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[KafkaSubscriber] 정지됨")

    def _consume_loop_kafka_python(self):
        """kafka-python KafkaConsumer 루프"""
        my_topic = self._current_topic
        while self._running:
            consumer = None
            try:
                consumer = _KafkaConsumer(
                    my_topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._group_id,
                    # [수정 포인트 - KAFKA CONSUMER OPTIONS]
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda v: v,    # raw bytes
                    request_timeout_ms=10000,
                    session_timeout_ms=10000,
                )
                self._connected = True
                print(f"[KafkaSubscriber] ✅ kafka-python 연결 성공 — {self._bootstrap_servers}")

                for msg in consumer:
                    if not self._running:
                        break
                    if self._current_topic != my_topic:
                        break
                    parsed = self._parse_message(msg.value)
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[KafkaSubscriber] ⚠️ kafka-python 오류: {e}")
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._current_topic != my_topic:
                break
            if self._running:
                print("[KafkaSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[KafkaSubscriber] 정지됨")

    def _parse_message(self, raw: bytes) -> dict | None:
        """
        Kafka raw bytes → Python dict 변환.

        [수정 포인트 - MESSAGE FORMAT]
        실제 Kafka 메시지 형식에 맞게 이 메서드를 수정하세요.

        현재 가정하는 형식:
        {
            'ts': 1775454754796,
            'cluster': 'datax',
            'node': 'work5',
            'status': 'HEALTHY',
            'metrics': {
                'cpu': {
                    'util': 0.035,
                    'cores': 12.0,
                    'load1': 0.38,
                    'load5': 0.41,
                    'load15': 0.43,
                    'eff': 0.053
                },
                'mem': {
                    'util': 0.201,
                    'total_gb': 16.62,
                    'avail_gb': 13.29,
                    'oom_cnt': 0
                },
                'net': {
                    'retrans': 0.0,
                    'in_mbps': 2.62,
                    'out_mbps': 2.89,
                    'nic_err_sum': 0.0,
                    'nic_drop_sum': 0.0,
                    'netstat_err': 0.0,
                    'err_sum': 0.0
                },
                'gpu': {
                    'util': 0.0,          # GPU 사용률 (0~1)
                    'temp': 38.0,         # GPU 온도 (°C)
                    'pwr': 24.868,        # GPU 전력 (W)
                    'mem_util': 0.0,      # GPU 메모리 사용률 (0~1)
                    'mem_used_gb': 0.0,   # GPU 메모리 사용량 (GB)
                    'total_gb': 23.525    # GPU 메모리 총량 (GB) — GPU 없는 노드도 0이 아닌 온보드 값
                },
                'storage': {
                    'util': 0.274,
                    'read_mbps': 0.0,
                    'write_mbps': 0.171,
                    'io_mbps': 0.171
                }
            },
            'debug_ts': 1775454757851  # 디버그용
        }
        """
        try:
            data = json.loads(raw.decode("utf-8"))


            # cluster 또는 node 중 하나는 있어야 함 (신 포맷 우선, 구 포맷 fallback)
            if not data.get("cluster") and not data.get("cluster_id") \
               and not data.get("node") and not data.get("box_id") \
               and not data.get("server_id") and not data.get("node_id"):
                print(f"[KafkaSubscriber] ⚠️ 필수 필드 없음, 메시지 드롭: {list(data.keys())}")
                return None

            cluster = data.get("cluster") or data.get("cluster_id")
            node    = data.get("node") or data.get("box_id", "-")
            status  = data.get("status") or data.get("metrics", {}).get("status", "?")
            temp    = (data.get("metrics", {}).get("gpu", {}).get("temp")
                       or data.get("metrics", {}).get("temperature", "?"))
            # print(
            #     f"[KafkaSubscriber] ✅ 수신 cluster={cluster} "
            #     f"rack={data.get('rack_id','-')} node={node} "
            #     f"status={status} temp={temp}°C"
            # )
            time.sleep(0.001)

            return data

        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            print(f"[KafkaSubscriber] 메시지 파싱 실패: {e}")
            return None


# ── NodeStateSubscriber ──────────────────────────────────────────────────────

class NodeStateSubscriber:
    """
    datacenter.metrics.node-state.events 토픽 전용 구독자.
    KafkaSubscriber 와 동일한 confluent / kafka-python 이중 백엔드.
    switch_topic() 으로 replay 토픽(준비되면) 전환 가능.

    수신 envelope 포맷은 parse_node_state_message() 참고.
    본 클래스는 2026-04-17-node-state-pulse-extension-design.md 의
    Legacy 코드 보존 정책에 따라 기존 HEALTH_TRANSITION 검증 블록을
    삭제하지 않고 주석 처리한다.
    """

    def __init__(
        self,
        bootstrap_servers: list,
        topic: str,
        group_id: str,
        data_queue: queue.Queue,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._queue             = data_queue

        self._thread:    threading.Thread | None = None
        self._running  = False
        self._connected = False

        if _BACKEND is None:
            print("[NodeStateSubscriber] ❌ Kafka 라이브러리 미설치.")
        else:
            print(f"[NodeStateSubscriber] 백엔드: {_BACKEND}")

    # ── 제어 ─────────────────────────────────────────────────────────────────

    def start(self):
        if _BACKEND is None:
            return
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop, name="NodeStateSubscriberThread", daemon=True
        )
        self._thread.start()
        print(f"[NodeStateSubscriber] 시작 — topic: {self._topic}")

    def stop(self):
        self._running = False
        print("[NodeStateSubscriber] 정지 요청")

    def is_connected(self) -> bool:
        return self._connected

    def switch_topic(self, new_topic: str):
        """
        소비 토픽을 전환합니다 (live.event ↔ replay.event).
        Kit 메인 스레드에서 호출해도 안전. consume loop 토픽 체크로 구 스레드 자연 종료.
        """
        if self._topic != new_topic:
            self._topic = new_topic   # 구 스레드 inner loop 탈출 트리거
            t = threading.Thread(
                target=self._consume_loop, name="NodeStateSubscriberThread", daemon=True
            )
            t.start()
            self._thread = t
            print(f"[NodeStateSubscriber] 토픽 전환 → {new_topic}")

    # ── 내부: 소비 루프 ───────────────────────────────────────────────────────

    def _consume_loop(self):
        if _BACKEND == "confluent":
            self._consume_loop_confluent()
        elif _BACKEND == "kafka-python":
            self._consume_loop_kafka_python()

    def _consume_loop_confluent(self):
        my_topic = self._topic
        while self._running:
            consumer = None
            try:
                conf = {
                    "bootstrap.servers": ",".join(str(s) for s in self._bootstrap_servers),
                    "group.id":           self._group_id,
                    "auto.offset.reset":  "latest",
                    "enable.auto.commit": True,
                    "session.timeout.ms": 10000,
                }
                consumer = _ConfluentConsumer(conf)
                consumer.subscribe([my_topic])
                self._connected = True
                print(f"[NodeStateSubscriber] ✅ confluent-kafka 연결 성공 — {self._bootstrap_servers}")

                while self._running and self._topic == my_topic:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())
                    parsed = self._parse_event(msg.value())
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[NodeStateSubscriber] ⚠️ confluent-kafka 오류: {e}")
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._topic != my_topic:
                break
            if self._running:
                print("[NodeStateSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[NodeStateSubscriber] 정지됨")

    def _consume_loop_kafka_python(self):
        my_topic = self._topic
        while self._running:
            consumer = None
            try:
                consumer = _KafkaConsumer(
                    my_topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda v: v,
                    request_timeout_ms=10000,
                    session_timeout_ms=10000,
                )
                self._connected = True
                print(f"[NodeStateSubscriber] ✅ kafka-python 연결 성공 — {self._bootstrap_servers}")

                for msg in consumer:
                    if not self._running:
                        break
                    if self._topic != my_topic:
                        break
                    parsed = self._parse_event(msg.value)
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[NodeStateSubscriber] ⚠️ kafka-python 오류: {e}")
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._topic != my_topic:
                break
            if self._running:
                print("[NodeStateSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[NodeStateSubscriber] 정지됨")

    def _parse_event(self, raw: bytes) -> dict | None:
        """Canonical node-state envelope 파싱. 실패 시 None."""
        return parse_node_state_message(raw)

    # legacy: HEALTH_TRANSITION schema — never emitted by Flink,
    #        superseded by parse_node_state_message() (2026-04-17).
    #
    # def _parse_event_legacy_health_transition(self, raw: bytes) -> dict | None:
    #     """
    #     event 메시지 파싱 + 디버그 print.
    #     필수 필드(cluster, rack, node, event_type, severity) 누락 시 drop.
    #     """
    #     try:
    #         data = json.loads(raw.decode("utf-8"))
    #         cluster    = data.get("cluster", "")
    #         rack       = data.get("rack", "")
    #         node       = data.get("node", "")
    #         event_type = data.get("event_type", "")
    #         severity   = data.get("severity", "")
    #
    #         # 필수 필드 검증
    #         missing = [f for f, v in [
    #             ("cluster", cluster), ("rack", rack), ("node", node),
    #             ("event_type", event_type), ("severity", severity),
    #         ] if not v]
    #         if missing:
    #             print(f"[EventKafkaSubscriber] ⚠️ 필수 필드 누락 {missing}, 메시지 드롭: {list(data.keys())}")
    #             return None
    #
    #         from_state = data.get("from", "?")
    #         to_state   = data.get("to",   "?")
    #         score      = data.get("score", 0.0)
    #
    #         time.sleep(0.001)
    #
    #         return data
    #
    #     except (json.JSONDecodeError, UnicodeDecodeError) as e:
    #         print(f"[EventKafkaSubscriber] 메시지 파싱 실패: {e}")
    #         return None


# ── ClusterRankSubscriber ────────────────────────────────────────────────────

class ClusterRankSubscriber:
    """
    datacenter.metrics.stageab 토픽 전용 구독자 (id=="cluster-rank" 필터).
    NodeStateSubscriber 와 동일한 confluent / kafka-python 이중 백엔드.

    본 구독자는 replay 토픽 전환을 지원하지 않는다 — 프로듀서가 replay
    변형을 publish 하지 않기 때문(2026-04-22-cluster-rank-consumer-poc-design.md §5.4).
    """

    def __init__(
        self,
        bootstrap_servers: list,
        topic: str,
        group_id: str,
        data_queue: queue.Queue,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._queue             = data_queue

        self._thread:    threading.Thread | None = None
        self._running   = False
        self._connected = False

        if _BACKEND is None:
            print("[ClusterRankSubscriber] ❌ Kafka 라이브러리 미설치.")
        else:
            print(f"[ClusterRankSubscriber] 백엔드: {_BACKEND}")

    # ── 제어 ─────────────────────────────────────────────────────────────────

    def start(self):
        if _BACKEND is None:
            return
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop, name="ClusterRankSubscriberThread", daemon=True
        )
        self._thread.start()
        print(f"[ClusterRankSubscriber] 시작 — topic: {self._topic}")

    def stop(self):
        self._running = False
        print("[ClusterRankSubscriber] 정지 요청")

    def is_connected(self) -> bool:
        return self._connected

    # ── 내부: 소비 루프 ───────────────────────────────────────────────────────

    def _consume_loop(self):
        if _BACKEND == "confluent":
            self._consume_loop_confluent()
        elif _BACKEND == "kafka-python":
            self._consume_loop_kafka_python()

    def _consume_loop_confluent(self):
        my_topic = self._topic
        while self._running:
            consumer = None
            try:
                conf = {
                    "bootstrap.servers": ",".join(str(s) for s in self._bootstrap_servers),
                    "group.id":           self._group_id,
                    "auto.offset.reset":  "latest",
                    "enable.auto.commit": True,
                    "session.timeout.ms": 10000,
                }
                consumer = _ConfluentConsumer(conf)
                consumer.subscribe([my_topic])
                self._connected = True
                print(f"[ClusterRankSubscriber] ✅ confluent-kafka 연결 성공 — {self._bootstrap_servers}")

                while self._running and self._topic == my_topic:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())
                    parsed = parse_cluster_rank_message(msg.value())
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[ClusterRankSubscriber] ⚠️ confluent-kafka 오류: {e}")
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._topic != my_topic:
                break
            if self._running:
                print("[ClusterRankSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[ClusterRankSubscriber] 정지됨")

    def _consume_loop_kafka_python(self):
        my_topic = self._topic
        while self._running:
            consumer = None
            try:
                consumer = _KafkaConsumer(
                    my_topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda v: v,
                    request_timeout_ms=10000,
                    session_timeout_ms=10000,
                )
                self._connected = True
                print(f"[ClusterRankSubscriber] ✅ kafka-python 연결 성공 — {self._bootstrap_servers}")

                for msg in consumer:
                    if not self._running:
                        break
                    if self._topic != my_topic:
                        break
                    parsed = parse_cluster_rank_message(msg.value)
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[ClusterRankSubscriber] ⚠️ kafka-python 오류: {e}")
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._topic != my_topic:
                break
            if self._running:
                print("[ClusterRankSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[ClusterRankSubscriber] 정지됨")
