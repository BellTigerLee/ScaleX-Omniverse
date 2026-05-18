"""
Seed Data Generator — 새 메트릭 스키마 버전
────────────────────────────────────────────────────────────────────────────
Kafka 메시지 포맷에 맞춘 중첩 메트릭 구조 (cpu/mem/net/gpu/storage).
Iceberg에 dc.metrics_seed 단일 테이블 생성 (최초 1회, 이미 존재하면 스킵).

노드 유형별 특성 (node 이름으로 자동 분류):
  Switch    : 초고속 네트워크(40~100Gbps), 저CPU, 저온, 소형 스토리지
  DTN       : 고대역 네트워크(5~20Gbps), 고CPU, 대용량 스토리지
  HDD       : 대용량 스토리지(100TB), 고온, 순차 I/O
  NVMe(E1S/E3S/U2): 초고속 I/O(1~3GB/s), 고메모리, 중간 CPU
  Control   : 범용 서버, 균형 잡힌 부하

알람 임계값:
  cpu_util > 0.75 → WARNING    cpu_util > 0.90 → CRITICAL
  sys_temp > 70°C → WARNING    sys_temp > 85°C → CRITICAL
  load5/cores > 1 → CPU 포화

시나리오 (600초 = 10분):
  0~299s   : WARNING 구간      (pf 0 → 0.7)
  300~539s : WARNING+CRITICAL  (pf 0.7 → 1.2)
  540~599s : 점진적 회복        (pf 1.2 → 0)

[수정] 스키마 변경 후 재시딩 방법:
  docker compose down -v && docker compose up -d --build
"""

import os
import time
import random
import uuid
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, FloatType,
)
from pyiceberg.exceptions import NamespaceAlreadyExistsError

# ── 환경변수 ──────────────────────────────────────────────────────────────────
CATALOG_URI  = os.getenv("CATALOG_URI",   "http://nessie:19120/iceberg/")
S3_ENDPOINT  = os.getenv("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS    = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET    = os.getenv("S3_SECRET_KEY", "minioadmin")
WAREHOUSE    = os.getenv("WAREHOUSE",     "s3://warehouse/")

NAMESPACE    = "dc"
SEED_TABLE   = "metrics_seed"
EVENT_TABLE  = "events_seed"
DURATION_SEC = 600          # 10분
CLUSTERS = ["datax", "twinx"]
NODES = {
    "datax": [
        "Box_1U_Control_2", "Box_1U_E1S",     "Box_2U_Control_1",
        "Box_2U_E3S_1",     "Box_2U_U2_1",    "Box_4U_HDD_1",
        "Box_4U_HDD_2",     "Box_1U_DTN_1",   "Box_1U_DTN_2",
        "Box_1U_DTN_3",     "Box_1U_DTN_4",   "BoX_1U_1G_Switch",
        "Box_1U_100G_Switch",
    ],
    "twinx": [
        "Box_2U_ARM_Server", "Box_1U_100G_Switch", "Box_1U_1G_Switch",
        "Box_2U_SV4000_1", "Box_2U_SV4000_2", "Box_2U_RM352_1", "Box_2U_RM352_2",
        "Box_2U_EdgeBox_1", "Box_2U_EdgeBox_2", "Box_2U_EdgeBox_3", "Box_2U_EdgeBox_4",
        "Box_1U_E300_1","Box_1U_E300_2","Box_1U_E300_3",
        "Box_4U_L40S"
    ]
}




# ── Iceberg 스키마 ────────────────────────────────────────────────────────────
# ts: 시드에서는 0~599 초 오프셋, loop-producer가 실제 Unix ms 타임스탬프로 교체
SCHEMA = Schema(
    NestedField(1,  "ts",              LongType(),   required=True),
    NestedField(2,  "cluster",         StringType(), required=True),
    NestedField(3,  "node",            StringType(), required=True),
    NestedField(4,  "status",          StringType()),  # HEALTHY/WARNING/CRITICAL
    # CPU
    NestedField(5,  "cpu_util",           FloatType()),   # usage ratio (0~1)
    NestedField(6,  "cpu_cores",          FloatType()),   # 물리 코어 수
    NestedField(7,  "cpu_load1",          FloatType()),   # 1분 load average
    NestedField(8,  "cpu_load5",          FloatType()),   # 5분 load average
    NestedField(9,  "cpu_load15",         FloatType()),   # 15분 load average
    NestedField(10, "cpu_eff",            FloatType()),   # CPU 효율 ratio (0~1)
    # Memory
    NestedField(11, "mem_util",           FloatType()),   # usage ratio (0~1)
    NestedField(12, "mem_total_gb",       FloatType()),   # 총 메모리 GB
    NestedField(13, "mem_avail_gb",       FloatType()),   # 가용 메모리 GB
    NestedField(14, "mem_oom_cnt",        FloatType()),   # OOM killer 발동 횟수
    # Network
    NestedField(15, "net_in_mbps",        FloatType()),   # 수신 처리량 Mbps
    NestedField(16, "net_out_mbps",       FloatType()),   # 송신 처리량 Mbps
    NestedField(17, "net_retrans",        FloatType()),   # TCP 재전송 /s
    NestedField(18, "net_nic_err_sum",    FloatType()),   # NIC 하드웨어 에러 합산 /s
    NestedField(19, "net_nic_drop_sum",   FloatType()),   # NIC 드롭 패킷 합산 /s
    NestedField(20, "net_netstat_err",    FloatType()),   # 네트워크 스택 에러 /s
    NestedField(21, "net_err_sum",        FloatType()),   # 전체 에러 합산 /s
    # GPU
    NestedField(22, "gpu_util",           FloatType()),   # GPU 사용률 ratio (0~1)
    NestedField(23, "gpu_temp",           FloatType()),   # GPU 온도 °C
    NestedField(24, "gpu_pwr",            FloatType()),   # GPU 전력 W
    NestedField(25, "gpu_mem_util",       FloatType()),   # GPU 메모리 사용률 ratio (0~1)
    NestedField(26, "gpu_mem_used_gb",    FloatType()),   # GPU 메모리 사용량 GB
    NestedField(27, "gpu_total_gb",       FloatType()),   # GPU 메모리 총량 GB
    # Storage
    NestedField(28, "storage_util",       FloatType()),   # 디스크 사용률 ratio (0~1)
    NestedField(29, "storage_read_mbps",  FloatType()),   # 디스크 읽기 Mbps
    NestedField(30, "storage_write_mbps", FloatType()),   # 디스크 쓰기 Mbps
    NestedField(31, "storage_io_mbps",    FloatType()),   # 디스크 I/O 합산 Mbps
)

ARROW_SCHEMA = pa.schema([
    pa.field("ts",                 pa.int64(),  nullable=False),
    pa.field("cluster",            pa.string(), nullable=False),
    pa.field("node",               pa.string(), nullable=False),
    pa.field("status",             pa.string()),
    pa.field("cpu_util",           pa.float32()),
    pa.field("cpu_cores",          pa.float32()),
    pa.field("cpu_load1",          pa.float32()),
    pa.field("cpu_load5",          pa.float32()),
    pa.field("cpu_load15",         pa.float32()),
    pa.field("cpu_eff",            pa.float32()),
    pa.field("mem_util",           pa.float32()),
    pa.field("mem_total_gb",       pa.float32()),
    pa.field("mem_avail_gb",       pa.float32()),
    pa.field("mem_oom_cnt",        pa.float32()),
    pa.field("net_in_mbps",        pa.float32()),
    pa.field("net_out_mbps",       pa.float32()),
    pa.field("net_retrans",        pa.float32()),
    pa.field("net_nic_err_sum",    pa.float32()),
    pa.field("net_nic_drop_sum",   pa.float32()),
    pa.field("net_netstat_err",    pa.float32()),
    pa.field("net_err_sum",        pa.float32()),
    pa.field("gpu_util",           pa.float32()),
    pa.field("gpu_temp",           pa.float32()),
    pa.field("gpu_pwr",            pa.float32()),
    pa.field("gpu_mem_util",       pa.float32()),
    pa.field("gpu_mem_used_gb",    pa.float32()),
    pa.field("gpu_total_gb",       pa.float32()),
    pa.field("storage_util",       pa.float32()),
    pa.field("storage_read_mbps",  pa.float32()),
    pa.field("storage_write_mbps", pa.float32()),
    pa.field("storage_io_mbps",    pa.float32()),
])


# ── Events Seed 스키마 ────────────────────────────────────────────────────────
TOPOLOGY = {
    # event_producer.py TOPOLOGY와 동일하게 유지 (live/replay rack 이름 일치)
    "datax": {
        "Rack_42U_A3": [
            "BoX_1U_1G_Switch", "Box_1U_100G_Switch", "Box_1U_Control_2", "Box_2U_Control_1",
            "Box_4U_HDD_1", "Box_4U_HDD_2", "Box_1U_E1S", "Box_2U_E3S_1", "Box_2U_U2_1",
            "Box_1U_DTN_1", "Box_1U_DTN_2", "Box_1U_DTN_3", "Box_1U_DTN_4",
        ],
    },
    "twinx": {
        "Rack_42U_A4": [
            "Box_2U_ARM_Server", "Box_1U_100G_Switch", "Box_1U_1G_Switch",
            "Box_2U_SV4000_1", "Box_2U_SV4000_2", "Box_2U_RM352_1", "Box_2U_RM352_2",
            "Box_2U_EdgeBox_1", "Box_2U_EdgeBox_2", "Box_2U_EdgeBox_3", "Box_2U_EdgeBox_4",
            "Box_1U_E300_1", "Box_1U_E300_2", "Box_1U_E300_3", "Box_4U_L40S",
        ],
    },
}

EVENT_SCHEMA = Schema(
    NestedField(1,  "ts",          LongType(),   required=True),  # 0~599 초 오프셋
    NestedField(2,  "cluster",     StringType(), required=True),
    NestedField(3,  "rack",        StringType(), required=True),
    NestedField(4,  "node",        StringType(), required=True),
    NestedField(5,  "event_id",    StringType(), required=True),
    NestedField(6,  "event_type",  StringType()),  # HEALTH_TRANSITION|CPU_PRESSURE|...
    NestedField(7,  "category",    StringType()),
    NestedField(8,  "severity",    StringType()),  # INFO|WARNING|CRITICAL
    NestedField(9,  "status",      StringType()),  # OPEN|CLOSED
    NestedField(10, "from_state",  StringType()),
    NestedField(11, "to_state",    StringType()),
    NestedField(12, "score",       FloatType()),
    NestedField(13, "message",     StringType()),
    NestedField(14, "reason_cpu",  FloatType()),   # nullable
    NestedField(15, "reason_mem",  FloatType()),   # nullable
    NestedField(16, "source",      StringType()),
    NestedField(17, "scope",       StringType()),
)

EVENT_ARROW_SCHEMA = pa.schema([
    pa.field("ts",         pa.int64(),  nullable=False),
    pa.field("cluster",    pa.string(), nullable=False),
    pa.field("rack",       pa.string(), nullable=False),
    pa.field("node",       pa.string(), nullable=False),
    pa.field("event_id",   pa.string(), nullable=False),
    pa.field("event_type", pa.string()),
    pa.field("category",   pa.string()),
    pa.field("severity",   pa.string()),
    pa.field("status",     pa.string()),
    pa.field("from_state", pa.string()),
    pa.field("to_state",   pa.string()),
    pa.field("score",      pa.float32()),
    pa.field("message",    pa.string()),
    pa.field("reason_cpu", pa.float32()),
    pa.field("reason_mem", pa.float32()),
    pa.field("source",     pa.string()),
    pa.field("scope",      pa.string()),
])

# 이벤트 타입별 메타
_EVENT_META = {
    "HEALTH_TRANSITION":   ("health",   "flink-health-engine"),
    "CPU_PRESSURE":        ("compute",  "prometheus-alertmanager"),
    "MEMORY_PRESSURE":     ("compute",  "prometheus-alertmanager"),
    "NETWORK_INSTABILITY": ("network",  "network-monitor"),
    "TELEMETRY_STALE":     ("telemetry","telemetry-watchdog"),
}

# phase별 이벤트 패턴: (event_type, severity, from_state, to_state)
_PHASE_EVENTS = {
    0: [("TELEMETRY_STALE",     "INFO",     "HEALTHY",  "HEALTHY")],
    1: [
        ("CPU_PRESSURE",        "WARNING",  "HEALTHY",  "WARNING"),
        ("MEMORY_PRESSURE",     "WARNING",  "HEALTHY",  "WARNING"),
        ("NETWORK_INSTABILITY", "WARNING",  "HEALTHY",  "WARNING"),
        ("HEALTH_TRANSITION",   "WARNING",  "HEALTHY",  "WARNING"),
        ("CPU_PRESSURE",        "CRITICAL", "WARNING",  "CRITICAL"),
        ("HEALTH_TRANSITION",   "CRITICAL", "WARNING",  "CRITICAL"),
    ],
    2: [
        ("HEALTH_TRANSITION",   "WARNING",  "CRITICAL", "WARNING"),
        ("HEALTH_TRANSITION",   "INFO",     "WARNING",  "HEALTHY"),
        ("CPU_PRESSURE",        "INFO",     "CRITICAL", "HEALTHY"),
        ("MEMORY_PRESSURE",     "INFO",     "WARNING",  "HEALTHY"),
    ],
}


def _make_score(severity: str, rng: random.Random) -> float:
    if severity == "CRITICAL": return round(rng.uniform(0.80, 1.00), 2)
    if severity == "WARNING":  return round(rng.uniform(0.55, 0.79), 2)
    return round(rng.uniform(0.10, 0.45), 2)


def _make_reason(event_type: str, severity: str, rng: random.Random):
    """(reason_cpu, reason_mem) — 해당 없으면 None."""
    if event_type in ("CPU_PRESSURE", "HEALTH_TRANSITION"):
        base = 0.80 if severity != "INFO" else 0.30
        cpu = round(rng.uniform(base, min(base + 0.18, 1.0)), 2)
        mem = round(rng.uniform(base * 0.85, min(base * 0.85 + 0.15, 1.0)), 2) if event_type == "HEALTH_TRANSITION" else None
        return cpu, mem
    if event_type == "MEMORY_PRESSURE":
        base = 0.78 if severity != "INFO" else 0.30
        return None, round(rng.uniform(base, min(base + 0.18, 1.0)), 2)
    return None, None


def _make_message(event_type: str, from_state: str, to_state: str, node: str) -> str:
    if event_type == "HEALTH_TRANSITION":
        return f"Node health changed from {from_state} to {to_state}"
    if event_type == "CPU_PRESSURE":
        return f"CPU utilization exceeded threshold on {node}"
    if event_type == "MEMORY_PRESSURE":
        return f"Memory pressure detected on {node}"
    if event_type == "NETWORK_INSTABILITY":
        return f"Network instability: high retransmission rate on {node}"
    return f"Telemetry data stale: metrics collection delayed on {node}"


def build_events_arrow_table() -> pa.Table:
    """
    10초 슬롯마다 이벤트 1개 생성 — 총 60개 (600s / 10s).
    event_producer는 group_by_ts로 슬롯 단위 배치를 구성하므로
    10초마다 1번씩 Kafka에 전송됨.

    severity는 metrics 시나리오와 동기화:
      0~299s  : WARNING          (30개)
      300~539s: WARNING/CRITICAL (24개)
      540~599s: 회복 이벤트       ( 6개)
    """
    rng = random.Random(42)
    rows: dict[str, list] = {k: [] for k in EVENT_ARROW_SCHEMA.names}

    all_nodes = [
        (cluster, rack, node)
        for cluster, racks in TOPOLOGY.items()
        for rack, nodes in racks.items()
        for node in nodes
    ]
    rng.shuffle(all_nodes)
    node_count = len(all_nodes)
    node_idx = 0

    EVENT_INTERVAL = 10  # 슬롯 간격(초) = event_producer 실제 전송 간격

    _WARNING_ONLY = _PHASE_EVENTS[1][:4]  # WARNING만 (CRITICAL 제외)

    for ts in range(0, DURATION_SEC, EVENT_INTERVAL):
        cluster, rack, node = all_nodes[node_idx % node_count]
        node_idx += 1

        if ts < 300:
            pool = _WARNING_ONLY           # 0~5분: WARNING
        elif ts < 540:
            pool = _PHASE_EVENTS[1]        # 5~9분: WARNING + CRITICAL
        else:
            pool = _PHASE_EVENTS[2]        # 9~10분: 회복

        _append_event(rows, ts, cluster, rack, node, rng.choice(pool), rng)

    return pa.table(rows, schema=EVENT_ARROW_SCHEMA)


def _append_event(rows, ts, cluster, rack, node, ev_tuple, rng):
    event_type, severity, from_state, to_state = ev_tuple
    category, source = _EVENT_META[event_type]
    reason_cpu, reason_mem = _make_reason(event_type, severity, rng)
    rows["ts"].append(ts)
    rows["cluster"].append(cluster)
    rows["rack"].append(rack)
    rows["node"].append(node)
    rows["event_id"].append(str(uuid.uuid4()))
    rows["event_type"].append(event_type)
    rows["category"].append(category)
    rows["severity"].append(severity)
    rows["status"].append("OPEN" if severity != "INFO" else "CLOSED")
    rows["from_state"].append(from_state)
    rows["to_state"].append(to_state)
    rows["score"].append(_make_score(severity, rng))
    rows["message"].append(_make_message(event_type, from_state, to_state, node))
    rows["reason_cpu"].append(reason_cpu)
    rows["reason_mem"].append(reason_mem)
    rows["source"].append(source)
    rows["scope"].append("node")


# ── 노드 유형 프로파일 ────────────────────────────────────────────────────────
def get_node_profile(node: str) -> dict:
    """
    node 이름 기반 하드웨어 프로파일 반환.
    net_gbps: (lo, hi) 단위 Gbps → 내부에서 Mbps로 변환
    disk_mbps: (lo, hi) 단위 MB/s
    """
    if "Switch" in node:
        return dict(type="switch",  cores=4,  mem_gb=16,  disk_tb=0.5,  gpu_total_gb=24.0,
                    cpu_base=12,  temp_base=42, power_kw=0.08,
                    net_gbps=(40,   100), disk_mbps=(1,    10))
    if "DTN" in node:
        return dict(type="dtn",     cores=32, mem_gb=128, disk_tb=10,   gpu_total_gb=24.0,
                    cpu_base=45,  temp_base=58, power_kw=0.35,
                    net_gbps=(5,   20),  disk_mbps=(200,  800))
    if "HDD" in node:
        return dict(type="hdd",     cores=16, mem_gb=64,  disk_tb=100,  gpu_total_gb=24.0,
                    cpu_base=30,  temp_base=62, power_kw=0.28,
                    net_gbps=(0.1,  2),  disk_mbps=(80,   250))
    if any(x in node for x in ("E1S", "E3S", "U2")):
        return dict(type="nvme",    cores=32, mem_gb=256, disk_tb=20,   gpu_total_gb=24.0,
                    cpu_base=50,  temp_base=56, power_kw=0.40,
                    net_gbps=(1,   10),  disk_mbps=(1000, 3000))
    # ── twinx 전용 ────────────────────────────────────────────────────────────
    if "L40S" in node:
        return dict(type="gpu",     cores=32, mem_gb=256, disk_tb=4,    gpu_total_gb=48.0,
                    cpu_base=40,  temp_base=60, power_kw=0.60,
                    net_gbps=(1,   10),  disk_mbps=(500,  2000))
    if "ARM_Server" in node:
        return dict(type="arm",     cores=64, mem_gb=256, disk_tb=2,    gpu_total_gb=24.0,
                    cpu_base=35,  temp_base=50, power_kw=0.20,
                    net_gbps=(1,   10),  disk_mbps=(200,  800))
    if any(x in node for x in ("SV4000", "RM352")):
        return dict(type="storage", cores=16, mem_gb=64,  disk_tb=200,  gpu_total_gb=24.0,
                    cpu_base=25,  temp_base=55, power_kw=0.35,
                    net_gbps=(1,   10),  disk_mbps=(200,  1000))
    if "EdgeBox" in node:
        return dict(type="edge",    cores=8,  mem_gb=32,  disk_tb=1,    gpu_total_gb=24.0,
                    cpu_base=30,  temp_base=48, power_kw=0.10,
                    net_gbps=(0.1,  1),  disk_mbps=(50,   200))
    if "E300" in node:
        return dict(type="small",   cores=16, mem_gb=64,  disk_tb=1,    gpu_total_gb=24.0,
                    cpu_base=35,  temp_base=52, power_kw=0.18,
                    net_gbps=(0.5,  5),  disk_mbps=(100,  500))
    # Control / default
    return dict(type="control",     cores=24, mem_gb=128, disk_tb=2,    gpu_total_gb=24.0,
                cpu_base=38,  temp_base=55, power_kw=0.30,
                net_gbps=(0.5,  5),  disk_mbps=(100,  600))


# ── 메트릭 생성 ───────────────────────────────────────────────────────────────
def generate_metrics(t: int, node: str) -> dict:
    """
    재현 가능한 시나리오 메트릭 (node 이름 + 시간 기반 seed).

    Phase factor (10분 시나리오):
      0~199s   → 0.0   (HEALTHY baseline)
      200~399s → 0→1   (escalate)
      400~599s → 1→0   (recover)
    """
    rng = random.Random(sum(ord(c) for c in node) * 1000 + t)
    p   = get_node_profile(node)
    n   = lambda lo, hi: rng.uniform(lo, hi)

    if t < 300:
        # 0~5분: WARNING 구간 — pf 0 → 0.7
        pf = (t / 300.0) * 0.7
    elif t < 540:
        # 5~9분: WARNING+CRITICAL 혼재 — pf 0.7 → 1.2
        pf = 0.7 + ((t - 300) / 240.0) * 0.5
    else:
        # 9~10분: 점진적 회복 — pf 1.2 → 0
        pf = max(0.0, 1.2 * (1.0 - (t - 540) / 60.0))

    # ── CPU ──────────────────────────────────────────────────────────────────
    cpu_pct   = round(min(100.0, max(0.0,
        p["cpu_base"] + pf * (90 - p["cpu_base"]) * 0.85 + n(-5, 5)
    )), 1)
    cores     = float(p["cores"])
    cpu_util  = round(cpu_pct / 100.0, 4)
    cpu_load5 = round(max(0.0, cpu_util * cores * (1 + pf * 0.3) + n(-0.1, 0.2)), 2)
    cpu_load1 = round(max(0.0, cpu_load5 * (1 + pf * 0.15) + n(-0.2, 0.3)), 2)
    cpu_load15 = round(max(0.0, cpu_load5 * (1 - pf * 0.10) + n(-0.1, 0.1)), 2)
    # 효율: iowait 없이 순수 CPU 처리 비율 (부하 증가 시 감소)
    cpu_eff   = round(max(0.0, min(1.0, cpu_util * (1.0 - pf * n(0.05, 0.20)))), 4)

    # ── Memory ───────────────────────────────────────────────────────────────
    mem_pct      = round(min(100.0, max(0.0, 40 + pf * 30 + n(-8, 10))), 1)
    mem_util     = round(mem_pct / 100.0, 4)
    mem_total_gb = float(p["mem_gb"])
    mem_avail_gb = round(mem_total_gb * (1.0 - mem_util), 1)
    mem_oom_cnt  = float(max(0, int(pf * n(-2, 3)))) if pf > 0.8 else 0.0

    # 내부 온도 (GPU temp 계산에 사용)
    sys_temp = round(min(110.0, max(30.0,
        p["temp_base"] + pf * (95 - p["temp_base"]) * 0.7 + n(-3, 4)
    )), 1)

    # ── Network ──────────────────────────────────────────────────────────────
    net_lo       = int(p["net_gbps"][0] * 1000)
    net_hi       = int(p["net_gbps"][1] * 1000)
    net_in_mbps  = round(max(0.0, n(net_lo, net_hi) * (1 + pf * 0.3)), 0)
    net_out_mbps = round(max(0.0, n(net_lo, net_hi) * (1 + pf * 0.25)), 0)
    net_retrans     = round(max(0.0, n(0, 3) + pf * n(2, 15)), 1)
    net_nic_err_sum = round(max(0.0, n(0, 0.5) + pf * n(0, 2)), 1)
    net_nic_drop_sum = round(max(0.0, n(0, 0.3) + pf * n(0, 1.5)), 1)
    net_netstat_err = round(max(0.0, n(0, 0.2) + pf * n(0, 1)), 1)
    # NIC 에러 + 드롭 합산
    net_err_sum  = round(max(0.0, net_nic_err_sum + net_nic_drop_sum + net_netstat_err + pf * n(0, 3)), 1)

    # ── GPU ──────────────────────────────────────────────────────────────────
    gpu_total_gb = p["gpu_total_gb"]
    if p["type"] == "gpu":
        # L40S 등 전용 GPU 서버: 고사용률, 고온, 고전력
        gpu_util        = round(max(0.0, min(1.0, 0.40 + pf * n(0.30, 0.50))), 4)
        gpu_mem_util    = round(max(0.0, min(1.0, 0.35 + pf * n(0.20, 0.40))), 4)
        gpu_temp        = round(max(30.0, 55.0 + pf * 35.0 + n(-3, 5)), 1)
        gpu_pwr         = round(max(80.0, min(350.0, 150.0 + pf * 150.0 + n(-10, 15))), 1)
    else:
        # 온보드/관리 컨트롤러 기준 저전력 (전용 GPU 없는 노드)
        gpu_util        = round(max(0.0, min(1.0, n(0, 0.02) + pf * n(0, 0.03))), 4)
        gpu_mem_util    = round(max(0.0, min(1.0, n(0, 0.01))), 4)
        gpu_temp        = round(max(28.0, sys_temp * 0.62 + pf * 8.0 + n(-2, 3)), 1)
        gpu_pwr         = round(max(10.0, min(55.0, 24.0 + pf * 8.0 + n(-3, 4))), 1)
    gpu_mem_used_gb = round(gpu_mem_util * gpu_total_gb, 3)

    # ── Storage ──────────────────────────────────────────────────────────────
    dlo, dhi          = p["disk_mbps"]
    storage_read_mbps  = round(max(0.0, n(dlo, dhi) * (1 + pf * 0.4)), 0)
    storage_write_mbps = round(max(0.0, n(dlo * 0.4, dhi * 0.6) * (1 + pf * 0.4)), 0)
    storage_io_mbps   = round(storage_read_mbps + storage_write_mbps, 0)
    storage_util      = round(min(1.0, max(0.0, (35 + n(-5, 20) + pf * 10) / 100.0)), 4)

    # ── Status ───────────────────────────────────────────────────────────────
    if sys_temp >= 85 or cpu_pct >= 90:
        status = "CRITICAL"
    elif sys_temp >= 70 or cpu_pct >= 75 or (cpu_load5 / max(1, cores)) > 1.0:
        status = "WARNING"
    else:
        status = "HEALTHY"

    return dict(
        status=status,
        cpu_util=cpu_util,                cpu_cores=cores,
        cpu_load1=cpu_load1,              cpu_load5=cpu_load5,
        cpu_load15=cpu_load15,            cpu_eff=cpu_eff,
        mem_util=mem_util,                mem_total_gb=mem_total_gb,
        mem_avail_gb=mem_avail_gb,        mem_oom_cnt=mem_oom_cnt,
        net_in_mbps=net_in_mbps,          net_out_mbps=net_out_mbps,
        net_retrans=net_retrans,          net_nic_err_sum=net_nic_err_sum,
        net_nic_drop_sum=net_nic_drop_sum, net_netstat_err=net_netstat_err,
        net_err_sum=net_err_sum,
        gpu_util=gpu_util,                gpu_temp=gpu_temp,
        gpu_pwr=gpu_pwr,                  gpu_mem_util=gpu_mem_util,
        gpu_mem_used_gb=gpu_mem_used_gb,  gpu_total_gb=gpu_total_gb,
        storage_util=storage_util,        storage_read_mbps=storage_read_mbps,
        storage_write_mbps=storage_write_mbps, storage_io_mbps=storage_io_mbps,
    )


# ── Arrow 테이블 빌드 ─────────────────────────────────────────────────────────
def build_arrow_table(base_ts: int) -> pa.Table:
    rows: dict[str, list] = {k: [] for k in ARROW_SCHEMA.names}
    for t in range(DURATION_SEC):
        ts = base_ts + t
        for cluster in CLUSTERS:
            for node in NODES[cluster]:
                m = generate_metrics(t, node)
                rows["ts"].append(ts)
                rows["cluster"].append(cluster)
                rows["node"].append(node)
                for k in m:
                    rows[k].append(m[k])
    return pa.table(rows, schema=ARROW_SCHEMA)


# ── 카탈로그 헬퍼 ─────────────────────────────────────────────────────────────
def get_catalog():
    return load_catalog(
        "nessie",
        **{
            "type":                  "rest",
            "uri":                   CATALOG_URI,
            "s3.endpoint":           S3_ENDPOINT,
            "s3.access-key-id":      S3_ACCESS,
            "s3.secret-access-key":  S3_SECRET,
            "s3.path-style-access":  "true",
            "s3.region":             "us-east-1",
            "warehouse":             WAREHOUSE,
        },
    )


def wait_for_catalog(catalog, retries: int = 30):
    for i in range(retries):
        try:
            catalog.list_namespaces()
            print("[seed] Nessie 카탈로그 연결 성공")
            return
        except Exception as e:
            print(f"[seed] 카탈로그 대기 중 ({i+1}/{retries}): {e}")
            time.sleep(5)
    raise RuntimeError("Nessie 카탈로그 연결 실패")


def ensure_namespace(catalog):
    try:
        catalog.create_namespace(NAMESPACE)
        print(f"[seed] namespace '{NAMESPACE}' 생성")
    except NamespaceAlreadyExistsError:
        print(f"[seed] namespace '{NAMESPACE}' 이미 존재")


def table_exists(catalog, full_name: str) -> bool:
    try:
        catalog.load_table(full_name)
        return True
    except Exception:
        return False


def main():
    catalog   = get_catalog()
    wait_for_catalog(catalog)
    ensure_namespace(catalog)

    # ── metrics_seed ──────────────────────────────────────────────────────────
    seed_full = f"{NAMESPACE}.{SEED_TABLE}"

    if table_exists(catalog, seed_full):
        print(f"[seed] '{seed_full}' 이미 존재 — 시딩 스킵")
        print("[seed] ⚠️  스키마 변경 시 'docker compose down -v && docker compose up -d --build' 필요")
    else:
        table      = catalog.create_table(seed_full, schema=SCHEMA)
        base_ts    = 0
        arrow_data = build_arrow_table(base_ts)
        table.append(arrow_data)
        total_nodes = sum(len(v) for v in NODES.values())
        print(
            f"[seed] '{seed_full}' 생성 완료 — "
            f"{len(arrow_data)}행 ({DURATION_SEC}s × {total_nodes}노드)\n"
            f"       클러스터: {', '.join(CLUSTERS)}\n"
            f"       컬럼 수: {len(ARROW_SCHEMA.names)}"
        )

    # ── events_seed ───────────────────────────────────────────────────────────
    event_full = f"{NAMESPACE}.{EVENT_TABLE}"

    if table_exists(catalog, event_full):
        print(f"[seed] '{event_full}' 이미 존재 — 시딩 스킵")
    else:
        etable     = catalog.create_table(event_full, schema=EVENT_SCHEMA)
        arrow_evs  = build_events_arrow_table()
        etable.append(arrow_evs)
        print(
            f"[seed] '{event_full}' 생성 완료 — "
            f"{len(arrow_evs)}행 (이벤트 시나리오 600s)\n"
            f"       컬럼 수: {len(EVENT_ARROW_SCHEMA.names)}"
        )

    print("[seed] 완료")


if __name__ == "__main__":
    main()
