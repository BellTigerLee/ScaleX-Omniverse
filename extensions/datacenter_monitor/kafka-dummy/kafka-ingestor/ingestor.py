"""
Kafka Ingestor
────────────────────────────────────────────────────────────────────────────
Kafka의 datacenter.metrics 토픽을 소비하여 Iceberg dc.metrics 테이블에
배치로 적재합니다.

Kafka 메시지 포맷 (중첩 구조):
  {
    "ts":      <Unix Epoch ms>,
    "cluster": "datax",
    "node":    "work5",
    "status":  "HEALTHY",          # HEALTHY | WARNING | CRITICAL
    "metrics": {
      "cpu":     { "util": 0.071, "cores": 48.0, "load5": 2.61, "eff": 0.213 },
      "mem":     { "util": 0.089, "total_gb": 251.5, "avail_gb": 229.0, "oom_cnt": 0 },
      "net":     { "in_mbps": 34.5, "out_mbps": 50.8, "retrans": 0.0, "err_sum": 0 },
      "gpu":     { "util": 0.0, "mem_util": 0.0, "temp": 38.0, "pwr": 24.7 },
      "storage": { "util": 0.065, "io_mbps": 2.2 }
    }
  }

실제 파이프라인에서는 Spark Structured Streaming / Flink가 이 역할을 합니다.
"""

import asyncio
import json
import os
import time
from collections import defaultdict

import pyarrow as pa
from aiokafka import AIOKafkaConsumer
from pyiceberg.catalog import load_catalog

# ── 환경변수 ──────────────────────────────────────────────────────────────────
CATALOG_URI        = os.getenv("CATALOG_URI",        "http://nessie:19120/iceberg/")
S3_ENDPOINT        = os.getenv("S3_ENDPOINT",        "http://minio:9000")
S3_ACCESS          = os.getenv("S3_ACCESS_KEY",      "minioadmin")
S3_SECRET          = os.getenv("S3_SECRET_KEY",      "minioadmin")
WAREHOUSE          = os.getenv("WAREHOUSE",           "s3://warehouse/")
KAFKA_BROKER       = os.getenv("KAFKA_BROKER",        "kafka:29092")
KAFKA_TOPIC        = os.getenv("KAFKA_TOPIC",         "datacenter.metrics")
KAFKA_GROUP_ID     = os.getenv("KAFKA_GROUP_ID",      "iceberg-ingestor")
BATCH_INTERVAL_SEC = int(os.getenv("BATCH_INTERVAL_SEC", "10"))

ARROW_SCHEMA = pa.schema([
    pa.field("ts",              pa.int64()),   # Unix Epoch ms
    pa.field("cluster",         pa.string()),
    pa.field("node",            pa.string()),
    pa.field("status",          pa.string()),  # HEALTHY | WARNING | CRITICAL
    # CPU
    pa.field("cpu_util",        pa.float32()), # usage ratio (0~1)
    pa.field("cpu_cores",       pa.float32()),
    pa.field("cpu_load5",       pa.float32()),
    pa.field("cpu_eff",         pa.float32()), # efficiency ratio (0~1)
    # Memory
    pa.field("mem_util",        pa.float32()), # usage ratio (0~1)
    pa.field("mem_total_gb",    pa.float32()),
    pa.field("mem_avail_gb",    pa.float32()),
    pa.field("mem_oom_cnt",     pa.float32()),
    # Network
    pa.field("net_in_mbps",     pa.float32()),
    pa.field("net_out_mbps",    pa.float32()),
    pa.field("net_retrans",     pa.float32()),
    pa.field("net_err_sum",     pa.float32()),
    # GPU
    pa.field("gpu_util",        pa.float32()), # usage ratio (0~1)
    pa.field("gpu_mem_util",    pa.float32()), # memory ratio (0~1)
    pa.field("gpu_temp",        pa.float32()), # °C
    pa.field("gpu_pwr",         pa.float32()), # W
    # Storage
    pa.field("storage_util",    pa.float32()), # usage ratio (0~1)
    pa.field("storage_io_mbps", pa.float32()),
])


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


async def wait_for_table(catalog, full_name: str, retries: int = 30):
    for i in range(retries):
        try:
            table = catalog.load_table(full_name)
            print(f"[ingestor] Iceberg 테이블 '{full_name}' 연결 성공")
            return table
        except Exception as e:
            print(f"[ingestor] 테이블 대기 중 ({i+1}/{retries}): {e}")
            await asyncio.sleep(10)
    raise RuntimeError(f"테이블 '{full_name}' 연결 실패")


async def flush_buffer(table, buf: dict) -> int:
    n = len(buf["ts"])
    if n == 0:
        return 0
    arrow_table = pa.table(dict(buf), schema=ARROW_SCHEMA)
    table.append(arrow_table)
    print(f"[ingestor] {n}행 Iceberg 적재 완료")
    return n


async def main():
    catalog = get_catalog()
    table   = await wait_for_table(catalog, "dc.metrics")

    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    print(f"[ingestor] Kafka 구독 시작: {KAFKA_TOPIC}")

    buf        = defaultdict(list)
    last_flush = time.time()

    try:
        async for msg in consumer:
            data = msg.value
            m    = data.get("metrics", {})
            cpu  = m.get("cpu",     {})
            mem  = m.get("mem",     {})
            net  = m.get("net",     {})
            gpu  = m.get("gpu",     {})
            stor = m.get("storage", {})

            buf["ts"].append(int(data.get("ts", int(time.time() * 1000))))
            buf["cluster"].append(data.get("cluster", ""))
            buf["node"].append(data.get("node", ""))
            buf["status"].append(data.get("status", "HEALTHY"))

            buf["cpu_util"].append(float(cpu.get("util",  0.0)))
            buf["cpu_cores"].append(float(cpu.get("cores", 0.0)))
            buf["cpu_load5"].append(float(cpu.get("load5", 0.0)))
            buf["cpu_eff"].append(float(cpu.get("eff",   0.0)))

            buf["mem_util"].append(float(mem.get("util",     0.0)))
            buf["mem_total_gb"].append(float(mem.get("total_gb", 0.0)))
            buf["mem_avail_gb"].append(float(mem.get("avail_gb", 0.0)))
            buf["mem_oom_cnt"].append(float(mem.get("oom_cnt",  0)))

            buf["net_in_mbps"].append(float(net.get("in_mbps",  0.0)))
            buf["net_out_mbps"].append(float(net.get("out_mbps", 0.0)))
            buf["net_retrans"].append(float(net.get("retrans",  0.0)))
            buf["net_err_sum"].append(float(net.get("err_sum",  0)))

            buf["gpu_util"].append(float(gpu.get("util",     0.0)))
            buf["gpu_mem_util"].append(float(gpu.get("mem_util", 0.0)))
            buf["gpu_temp"].append(float(gpu.get("temp",     0.0)))
            buf["gpu_pwr"].append(float(gpu.get("pwr",      0.0)))

            buf["storage_util"].append(float(stor.get("util",    0.0)))
            buf["storage_io_mbps"].append(float(stor.get("io_mbps", 0.0)))

            if time.time() - last_flush >= BATCH_INTERVAL_SEC:
                await flush_buffer(table, buf)
                buf.clear()
                last_flush = time.time()

    finally:
        # 남은 버퍼 flush
        await flush_buffer(table, buf)
        await consumer.stop()
        print("[ingestor] 종료")


if __name__ == "__main__":
    asyncio.run(main())
