"""
Loop Producer
────────────────────────────────────────────────────────────────────────────
Iceberg의 metrics_seed 테이블을 읽어 Kafka에 루프 전송합니다.
실제 파이프라인에서는 이 역할을 Spark Streaming / Flink가 대체합니다.

동작:
  1. seed-data가 생성한 dc.metrics_seed 테이블 로드 (최대 5분 재시도)
  2. ts 순서대로 각 노드 메시지를 Kafka로 전송 (중첩 메트릭 포맷)
  3. 원본 간격(1초) 유지, 타임스탬프는 현재 시각(Unix ms)으로 교체
  4. 끝까지 전송 후 처음부터 무한 반복

Kafka 메시지 포맷:
  {
    "ts":      <Unix Epoch ms>,
    "cluster": "datax",
    "node":    "Box_4U_HDD_1",
    "status":  "HEALTHY",
    "metrics": {
      "cpu":     { "util": 0.30, "cores": 16.0, "load1": 6.2, "load5": 6.8, "load15": 6.5, "eff": 0.28 },
      "mem":     { "util": 0.55, "total_gb": 64.0, "avail_gb": 28.8, "oom_cnt": 0 },
      "net":     { "in_mbps": 450.0, "out_mbps": 200.0, "retrans": 1.2,
                   "nic_err_sum": 0.0, "nic_drop_sum": 0.0, "netstat_err": 0.0, "err_sum": 0 },
      "gpu":     { "util": 0.0, "temp": 38.5, "pwr": 24.0,
                   "mem_util": 0.0, "mem_used_gb": 0.0, "total_gb": 24.0 },
      "storage": { "util": 0.42, "read_mbps": 180.0, "write_mbps": 140.0, "io_mbps": 320.0 }
    }
  }
"""

import os
import json
import time
from collections import defaultdict

from pyiceberg.catalog import load_catalog
from confluent_kafka import Producer

# ── 환경변수 ──────────────────────────────────────────────────────────────────
CATALOG_URI   = os.getenv("CATALOG_URI",   "http://nessie:19120/iceberg/")
S3_ENDPOINT   = os.getenv("S3_ENDPOINT",   "http://minio:9000")
S3_ACCESS     = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET     = os.getenv("S3_SECRET_KEY", "minioadmin")
WAREHOUSE     = os.getenv("WAREHOUSE",     "s3://warehouse/")
KAFKA_BROKER  = os.getenv("KAFKA_BROKER",  "kafka:29092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",   "datacenter.metrics")
INTERVAL      = float(os.getenv("INTERVAL", "1.0"))

# seed.py의 flat 컬럼명 목록 (ts/cluster/node/status 제외)
METRIC_COLS = [
    "cpu_util", "cpu_cores", "cpu_load1", "cpu_load5", "cpu_load15", "cpu_eff",
    "mem_util", "mem_total_gb", "mem_avail_gb", "mem_oom_cnt",
    "net_in_mbps", "net_out_mbps", "net_retrans",
    "net_nic_err_sum", "net_nic_drop_sum", "net_netstat_err", "net_err_sum",
    "gpu_util", "gpu_temp", "gpu_pwr", "gpu_mem_util", "gpu_mem_used_gb", "gpu_total_gb",
    "storage_util", "storage_read_mbps", "storage_write_mbps", "storage_io_mbps",
]

def get_catalog():
    return load_catalog(
        "nessie",
        **{
            "type":                 "rest",
            "uri":                  CATALOG_URI,
            "s3.endpoint":          S3_ENDPOINT,
            "s3.access-key-id":     S3_ACCESS,
            "s3.secret-access-key": S3_SECRET,
            "s3.path-style-access": "true",
            "s3.region":            "us-east-1",
            "warehouse":            WAREHOUSE,
        },
    )


def load_seed_rows(catalog, retries: int = 30) -> list[dict]:
    """metrics_seed 테이블을 로드 (seed-data 완료까지 재시도)."""
    for i in range(retries):
        try:
            table = catalog.load_table("dc.metrics_seed")
            arrow = table.scan().to_arrow()
            rows  = arrow.to_pydict()
            n     = len(rows["ts"])
            if n == 0:
                raise RuntimeError("metrics_seed 테이블이 비어있음 — seed-data 완료 대기 중")
            result = []
            for j in range(n):
                row = {
                    "ts":      rows["ts"][j],
                    "cluster": rows["cluster"][j],
                    "node":    rows["node"][j],
                    "status":  rows["status"][j],
                }
                for col in METRIC_COLS:
                    row[col] = float(rows[col][j]) if col in rows else 0.0
                result.append(row)
            print(f"[loop-producer] {n}행 로드 완료 ({len(METRIC_COLS) + 4}컬럼)")
            return result
        except Exception as e:
            print(f"[loop-producer] 시드 데이터 대기 중 ({i+1}/{retries}): {e}")
            time.sleep(10)
    raise RuntimeError("metrics_seed 로드 실패 — seed-data 서비스를 확인하세요")


def group_by_ts(rows: list[dict]) -> list[tuple[int, list[dict]]]:
    """같은 ts의 행들을 묶어 (ts, [rows]) 리스트 반환 (ts 정렬)."""
    groups: dict[int, list] = defaultdict(list)
    for row in rows:
        groups[row["ts"]].append(row)
    return sorted(groups.items())


def build_kafka_msg(row: dict, now_ts_ms: int) -> dict:
    """flat seed row → 중첩 Kafka 메시지 변환."""
    return {
        "ts":      now_ts_ms,
        "cluster": row["cluster"],
        "node":    row["node"],
        "status":  row.get("status", "HEALTHY"),
        "metrics": {
            "cpu": {
                "util":  round(row.get("cpu_util",   0.0), 4),
                "cores": row.get("cpu_cores",  0.0),
                "load1": round(row.get("cpu_load1",  0.0), 2),
                "load5": round(row.get("cpu_load5",  0.0), 2),
                "load15": round(row.get("cpu_load15", 0.0), 2),
                "eff":   round(row.get("cpu_eff",    0.0), 4),
            },
            "mem": {
                "util":     round(row.get("mem_util",     0.0), 4),
                "total_gb": row.get("mem_total_gb", 0.0),
                "avail_gb": round(row.get("mem_avail_gb", 0.0), 1),
                "oom_cnt":  int(row.get("mem_oom_cnt", 0)),
            },
            "net": {
                "in_mbps":      row.get("net_in_mbps",       0.0),
                "out_mbps":     row.get("net_out_mbps",      0.0),
                "retrans":      round(row.get("net_retrans",      0.0), 1),
                "nic_err_sum":  round(row.get("net_nic_err_sum",  0.0), 1),
                "nic_drop_sum": round(row.get("net_nic_drop_sum", 0.0), 1),
                "netstat_err":  round(row.get("net_netstat_err",  0.0), 1),
                "err_sum":      round(row.get("net_err_sum",      0.0), 1),
            },
            "gpu": {
                "util":        round(row.get("gpu_util",        0.0), 4),
                "temp":        round(row.get("gpu_temp",        0.0), 1),
                "pwr":         round(row.get("gpu_pwr",         0.0), 1),
                "mem_util":    round(row.get("gpu_mem_util",    0.0), 4),
                "mem_used_gb": round(row.get("gpu_mem_used_gb", 0.0), 3),
                "total_gb":    round(row.get("gpu_total_gb",    0.0), 3),
            },
            "storage": {
                "util":       round(row.get("storage_util",       0.0), 4),
                "read_mbps":  row.get("storage_read_mbps",  0.0),
                "write_mbps": row.get("storage_write_mbps", 0.0),
                "io_mbps":    row.get("storage_io_mbps",    0.0),
            },
        },
        "original_ts": row["ts"],  # 시드 오프셋 (디버그용)
    }


def main():
    catalog  = get_catalog()
    rows     = load_seed_rows(catalog)
    groups   = group_by_ts(rows)

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    boxes_per_step = len(rows) // len(groups) if groups else 0
    print(f"[loop-producer] Kafka 연결: {KAFKA_BROKER}  토픽: {KAFKA_TOPIC}")
    print(f"[loop-producer] {len(groups)}스텝 × {boxes_per_step}노드 루프 시작")

    loop = 0
    while True:
        loop += 1
        print(f"[loop-producer] ── 루프 #{loop} ──")

        prev_orig_ts = groups[0][0]
        for orig_ts, batch in groups:
            now_ms = int(time.time() * 1000)

            for row in batch:
                msg = build_kafka_msg(row, now_ms)
                producer.produce(
                    KAFKA_TOPIC,
                    key=f"{row['cluster']}/{row['node']}",
                    value=json.dumps(msg).encode("utf-8"),
                )

            producer.flush()

            gap = orig_ts - prev_orig_ts
            time.sleep(gap if gap > 0 else INTERVAL)
            prev_orig_ts = orig_ts


if __name__ == "__main__":
    main()
