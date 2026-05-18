"""
Kafka Dummy Producer — Datacenter Digital Twin 테스트용
================================================
실제 Kafka 메시지 대신 랜덤 메트릭을 주기적으로 produce 합니다.

사용법:
  python dummy_producer.py                  # 기본 설정으로 실행
  python dummy_producer.py --interval 2     # 2초마다 전송
  python dummy_producer.py --host 192.168.x.x:9092  # Kafka 브로커 지정

요구사항:
  pip install confluent-kafka

메시지 형식 (신 포맷 — loop-producer와 동일):
  {
    "ts":      <Unix Epoch ms>,
    "cluster": "datax",
    "node":    "Box_4U_HDD_1",
    "status":  "HEALTHY",
    "metrics": {
      "cpu":     { "util": 0.75, "cores": 16.0, "load1": 6.2, "load5": 6.8, "load15": 6.5, "eff": 0.68 },
      "mem":     { "util": 0.60, "total_gb": 64.0, "avail_gb": 25.6, "oom_cnt": 0 },
      "net":     { "in_mbps": 120.0, "out_mbps": 80.0, "retrans": 0.5,
                   "nic_err_sum": 0.0, "nic_drop_sum": 0.0, "netstat_err": 0.0, "err_sum": 0.0 },
      "gpu":     { "util": 0.0, "temp": 38.0, "pwr": 24.5,
                   "mem_util": 0.0, "mem_used_gb": 0.0, "total_gb": 24.0 },
      "storage": { "util": 0.27, "read_mbps": 0.0, "write_mbps": 0.17, "io_mbps": 0.17 }
    },
    "debug_ts": <Unix Epoch ms>
  }
"""

import argparse
import json
import random
import time
from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - TOPOLOGY]
# USD 씬에서 실제 탐색된 cluster / rack / box prim 이름을 기입하세요.
# cluster 이름은 소문자 (Kafka 신 포맷 기준: "datax", "twinx").
# rack_id는 None으로 설정하면 extension이 cluster+box만으로 prim을 찾습니다.
# ─────────────────────────────────────────────────────────────────────────────
TOPOLOGY = {
    # cluster(소문자): { rack_id | None: [ box_id, ... ] }
    "datax": {
        "Rack_42U_A3": [
            "Box_1U_Control_2",
            "Box_1U_E1S",
            "Box_2U_Control_1",
            "Box_2U_E3S_1",
            "Box_2U_U2_1",
            "Box_4U_HDD_1",
            "Box_4U_HDD_2",
            "Box_1U_DTN_1",
            "Box_1U_DTN_2",
            "Box_1U_DTN_3",
            "Box_1U_DTN_4",
            "BoX_1U_1G_Switch",
            "Box_1U_100G_Switch",
        ],
    },
    "twinx": {
        "Rack_42U_A4": [
            "Box_2U_ARM_Server",
            "Box_1U_100G_Switch",
            "Box_1U_1G_Switch",
            "Box_2U_SV4000_1",
            "Box_2U_SV4000_2",
            "Box_2U_RM352_1",
            "Box_2U_RM352_2",
            "Box_2U_EdgeBox_1",
            "Box_2U_EdgeBox_2",
            "Box_2U_EdgeBox_3",
            "Box_2U_EdgeBox_4",
            "Box_1U_E300_1",
            "Box_1U_E300_2",
            "Box_1U_E300_3",
            "Box_4U_L40S",
        ],
    },
}

# GPU 메모리 총량 (노드 이름 기반)
# L40S: 48 GB, 나머지: 24 GB (온보드/관리 컨트롤러)
def _gpu_total_gb(box: str) -> float:
    return 48.0 if "L40S" in box else 24.0

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - KAFKA]
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_BROKER       = "localhost:9092"
DEFAULT_TOPIC        = "datacenter.metrics"
DEFAULT_INTERVAL_SEC = 1.0   # 전송 주기 (초)

# 특정 서버를 WARNING/CRITICAL 상태로 강제 (테스트용)
FORCED_CRITICAL = []  # 예: [("datax", "Box_4U_HDD_1")]
FORCED_WARNING  = []  # 예: [("datax", "Box_1U_DTN_1")]


# ─────────────────────────────────────────────────────────────────────────────
# 메트릭 생성 로직
# ─────────────────────────────────────────────────────────────────────────────
class ServerState:
    """서버 하나의 메트릭 상태를 관리합니다 (점진적 변화 시뮬레이션)."""

    def __init__(self, cluster: str, rack: str | None, box: str):
        self.cluster = cluster
        self.rack    = rack
        self.box     = box

        self.cpu_util    = random.uniform(0.80, 0.98)
        self.mem_util    = random.uniform(0.60, 0.90)
        self.gpu_temp    = random.uniform(55.0, 85.0)
        self.gpu_util    = random.uniform(0.0, 0.05) if "L40S" not in box else random.uniform(0.40, 0.80)
        self.gpu_mem_util = self.gpu_util * random.uniform(0.8, 1.2)
        self.gpu_total_gb = _gpu_total_gb(box)

        self._target_cpu_util  = self.cpu_util
        self._target_mem_util  = self.mem_util
        self._target_gpu_temp  = self.gpu_temp

    def step(self) -> dict:
        """한 스텝 진행하여 새 메트릭 dict를 반환합니다."""
        if random.random() < 0.10:
            self._target_cpu_util = random.uniform(0.80, 0.98)
            self._target_mem_util = random.uniform(0.60, 0.90)
            self._target_gpu_temp = random.uniform(55.0, 85.0)

        self.cpu_util  += (self._target_cpu_util - self.cpu_util) * 0.15 + random.uniform(-0.01, 0.01)
        self.mem_util  += (self._target_mem_util - self.mem_util) * 0.10 + random.uniform(-0.005, 0.005)
        self.gpu_temp  += (self._target_gpu_temp - self.gpu_temp) * 0.08 + random.uniform(-0.3, 0.3)

        self.cpu_util  = max(0.0,  min(1.0,   self.cpu_util))
        self.mem_util  = max(0.0,  min(1.0,   self.mem_util))
        self.gpu_temp  = max(28.0, min(110.0, self.gpu_temp))

        cores       = 16.0
        cpu_load5   = round(max(0.0, self.cpu_util * cores * (1 + random.uniform(-0.05, 0.15))), 2)
        cpu_load1   = round(max(0.0, cpu_load5 * (1 + random.uniform(-0.1, 0.2))), 2)
        cpu_load15  = round(max(0.0, cpu_load5 * (1 - random.uniform(0.0, 0.1))), 2)
        mem_total_gb = 64.0
        gpu_mem_util = max(0.0, min(1.0, self.gpu_util * random.uniform(0.9, 1.1)))
        gpu_mem_used_gb = round(gpu_mem_util * self.gpu_total_gb, 3)
        gpu_pwr = 24.0 + self.gpu_util * 300.0 if "L40S" in self.box else round(24.0 + self.gpu_util * 8.0 + random.uniform(-2, 2), 1)

        status = self._calc_status()
        now_ms = int(time.time() * 1000)

        return {
            "ts":      now_ms,
            "cluster": self.cluster,
            "node":    self.box,
            "status":  status,
            "metrics": {
                "cpu": {
                    "util":  round(self.cpu_util, 4),
                    "cores": cores,
                    "load1": cpu_load1,
                    "load5": cpu_load5,
                    "load15": cpu_load15,
                    "eff":   round(max(0.0, self.cpu_util * (1 - random.uniform(0.0, 0.15))), 4),
                },
                "mem": {
                    "util":     round(self.mem_util, 4),
                    "total_gb": mem_total_gb,
                    "avail_gb": round(mem_total_gb * (1.0 - self.mem_util), 1),
                    "oom_cnt":  0,
                },
                "net": {
                    "in_mbps":      round(random.uniform(1.0, 500.0), 2),
                    "out_mbps":     round(random.uniform(1.0, 300.0), 2),
                    "retrans":      round(random.uniform(0.0, 3.0), 1),
                    "nic_err_sum":  0.0,
                    "nic_drop_sum": 0.0,
                    "netstat_err":  0.0,
                    "err_sum":      0.0,
                },
                "gpu": {
                    "util":        round(self.gpu_util, 4),
                    "temp":        round(self.gpu_temp, 1),
                    "pwr":         round(gpu_pwr, 3),
                    "mem_util":    round(gpu_mem_util, 4),
                    "mem_used_gb": gpu_mem_used_gb,
                    "total_gb":    self.gpu_total_gb,
                },
                "storage": {
                    "util":       round(random.uniform(0.05, 0.50), 4),
                    "read_mbps":  round(random.uniform(0.0, 200.0), 3),
                    "write_mbps": round(random.uniform(0.0, 100.0), 3),
                    "io_mbps":    round(random.uniform(0.0, 300.0), 3),
                },
            },
            "debug_ts": int(time.time() * 1000),
        }

    def _calc_status(self) -> str:
        key = (self.cluster, self.box)
        if key in FORCED_CRITICAL:
            return "CRITICAL"
        if key in FORCED_WARNING:
            return "WARNING"
        if self.gpu_temp >= 85 or self.cpu_util >= 0.95:
            return "CRITICAL"
        if self.gpu_temp >= 70 or self.cpu_util >= 0.85:
            return "WARNING"
        return "HEALTHY"


# ─────────────────────────────────────────────────────────────────────────────
# Producer
# ─────────────────────────────────────────────────────────────────────────────
def build_states() -> list[ServerState]:
    states = []
    for cluster, racks in TOPOLOGY.items():
        for rack, boxes in racks.items():
            for box in boxes:
                states.append(ServerState(cluster, rack, box))
    return states


def run(broker: str, topic: str, interval: float):
    try:
        from confluent_kafka import Producer
        backend = "confluent-kafka"
    except ImportError:
        try:
            from kafka import KafkaProducer
            backend = "kafka-python"
        except ImportError:
            print("❌ Kafka 라이브러리 없음. 설치: pip install confluent-kafka")
            return

    print(f"[DummyProducer] 백엔드: {backend}")
    print(f"[DummyProducer] 브로커: {broker}  토픽: {topic}  주기: {interval}s")
    print(f"[DummyProducer] 노드 수: {sum(len(boxes) for racks in TOPOLOGY.values() for boxes in racks.values())}")

    states = build_states()

    if backend == "confluent-kafka":
        _run_confluent(states, broker, topic, interval)
    else:
        _run_kafka_python(states, broker, topic, interval)


def _run_confluent(states: list, broker: str, topic: str, interval: float):
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": broker})
    print("[DummyProducer] ✅ confluent-kafka 연결 (비동기)")

    try:
        while True:
            start = time.time()
            for state in states:
                msg = state.step()
                producer.produce(
                    topic,
                    key=f"{state.cluster}/{state.box}",
                    value=json.dumps(msg).encode("utf-8"),
                )
                _print_status(msg)

            producer.flush()
            elapsed = time.time() - start
            time.sleep(max(0, interval - elapsed))

    except KeyboardInterrupt:
        print("\n[DummyProducer] 정지 (Ctrl+C)")
    finally:
        producer.flush()


def _run_kafka_python(states: list, broker: str, topic: str, interval: float):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[broker],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print("[DummyProducer] ✅ kafka-python 연결")

    try:
        while True:
            start = time.time()
            for state in states:
                msg = state.step()
                producer.send(topic, value=msg,
                              key=f"{state.cluster}/{state.box}".encode())
                _print_status(msg)

            elapsed = time.time() - start
            time.sleep(max(0, interval - elapsed))

    except KeyboardInterrupt:
        print("\n[DummyProducer] 정지 (Ctrl+C)")
    finally:
        producer.close()


def _print_status(msg: dict):
    m      = msg["metrics"]
    status = msg["status"]
    icon   = {"HEALTHY": "🟢", "WARNING": "🟡", "CRITICAL": "🔴"}.get(status, "⚪")
    ts     = datetime.fromtimestamp(msg["ts"] / 1000).strftime("%H:%M:%S")
    cpu    = m["cpu"]
    gpu    = m["gpu"]
    print(
        f"  {icon} [{ts}] {msg['cluster']} / {msg['node']}"
        f"  CPU:{cpu['util']*100:5.1f}%  MEM:{m['mem']['util']*100:5.1f}%"
        f"  GPU_TEMP:{gpu['temp']:5.1f}°C  GPU_PWR:{gpu['pwr']:6.1f}W  {status}"
    )


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Datacenter Kafka Dummy Producer")
    parser.add_argument("--host",     default=DEFAULT_BROKER,       help="Kafka broker (default: localhost:9092)")
    parser.add_argument("--topic",    default=DEFAULT_TOPIC,        help="Kafka topic")
    parser.add_argument("--interval", default=DEFAULT_INTERVAL_SEC, type=float, help="전송 주기 (초)")
    args = parser.parse_args()

    run(broker=args.host, topic=args.topic, interval=args.interval)
