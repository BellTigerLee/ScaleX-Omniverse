"""
Event Loop Producer
────────────────────────────────────────────────────────────────────────────
Iceberg의 node_events 테이블을 읽어 Kafka에 루프 전송합니다.

동작:
  1. dc.node_events 테이블 로드 (없으면 생성, 최대 5분 재시도)
  2. ts 순서대로 각 이벤트 메시지를 Kafka로 전송
  3. 원본 ts 간격 유지, 타임스탬프는 현재 시각(ISO 8601 UTC)으로 교체
  4. 끝까지 전송 후 처음부터 무한 반복

Kafka 메시지 포맷:
  {
    "event_id":   "uuid4",
    "ts":         "2026-03-13T10:15:07Z",
    "cluster":    "datax",
    "rack":       "Rack_42U_A3",
    "node":       "Box_4U_HDD_1",
    "scope":      "node",
    "source":     "flink-health-engine",
    "event_type": "HEALTH_TRANSITION",
    "category":   "health",
    "severity":   "WARNING",
    "status":     "OPEN",
    "from":       "HEALTHY",
    "to":         "WARNING",
    "score":      0.63,
    "message":    "Node health changed from HEALTHY to WARNING",
    "reason":     { "cpu_util": 0.91 }
  }
"""

import json
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, LongType, StringType, DoubleType,
)
from confluent_kafka import Producer

from config import (
    KAFKA_BROKER, KAFKA_TOPIC, INTERVAL,
    CATALOG_URI, ENDPOINT, ACCESSKEY, SECRETKEY, WAREHOUSE,
    ICEBERG_NAMESPACE, ICEBERG_TABLE,
    NESSIE_USER, NESSIE_PASSWORD,
)

TABLE_ID = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE}"

NODE_EVENTS_SCHEMA = Schema(
    NestedField(1,  "ts",          LongType(),   required=True),
    NestedField(2,  "cluster",     StringType(), required=True),
    NestedField(3,  "rack",        StringType(), required=True),
    NestedField(4,  "node",        StringType(), required=True),
    NestedField(5,  "event_id",    StringType(), required=True),
    NestedField(6,  "event_type",  StringType(), required=True),
    NestedField(7,  "category",    StringType(), required=True),
    NestedField(8,  "severity",    StringType(), required=True),
    NestedField(9,  "status",      StringType(), required=True),
    NestedField(10, "from_state",  StringType(), required=True),
    NestedField(11, "to_state",    StringType(), required=True),
    NestedField(12, "score",       DoubleType(), required=True),
    NestedField(13, "message",     StringType(), required=True),
    NestedField(14, "reason_cpu",  DoubleType(), required=False),
    NestedField(15, "reason_mem",  DoubleType(), required=False),
    NestedField(16, "source",      StringType(), required=True),
    NestedField(17, "scope",       StringType(), required=True),
)


def get_catalog():
    import base64
    basic = base64.b64encode(f"{NESSIE_USER}:{NESSIE_PASSWORD}".encode()).decode()
    return load_catalog(
        "bronze_cat",
        **{
            "type":                       "rest",
            "uri":                        CATALOG_URI,
            "warehouse":                  WAREHOUSE,
            "header.Authorization":       f"Basic {basic}",
            "s3.endpoint":                ENDPOINT,
            "s3.access-key-id":           ACCESSKEY,
            "s3.secret-access-key":       SECRETKEY,
            "s3.path-style-access":       "true",
            "s3.region":                  "us-east-1",
        },
    )


def ensure_table(catalog):
    """node_events 테이블이 없으면 생성."""
    try:
        return catalog.load_table(TABLE_ID)
    except Exception:
        pass

    print(f"[event-producer] {TABLE_ID} 테이블 없음 — 생성합니다")
    try:
        catalog.create_namespace(ICEBERG_NAMESPACE)
    except Exception:
        pass  # 이미 존재하면 무시

    table = catalog.create_table(
        identifier=TABLE_ID,
        schema=NODE_EVENTS_SCHEMA,
    )
    print(f"[event-producer] {TABLE_ID} 테이블 생성 완료")
    return table


def load_event_rows(catalog, retries: int = 30) -> list[dict]:
    """node_events 테이블을 로드 (데이터가 생길 때까지 재시도)."""
    for i in range(retries):
        try:
            table = ensure_table(catalog)
            arrow = table.scan().to_arrow()
            rows  = arrow.to_pydict()
            n     = len(rows.get("ts", []))
            if n == 0:
                raise RuntimeError(f"{TABLE_ID} 테이블이 비어있음 — 데이터 적재 대기 중")
            result = []
            for j in range(n):
                result.append({
                    "ts":          rows["ts"][j],
                    "cluster":     rows["cluster"][j],
                    "rack":        rows["rack"][j],
                    "node":        rows["node"][j],
                    "event_id":    rows["event_id"][j],
                    "event_type":  rows["event_type"][j],
                    "category":    rows["category"][j],
                    "severity":    rows["severity"][j],
                    "status":      rows["status"][j],
                    "from_state":  rows["from_state"][j],
                    "to_state":    rows["to_state"][j],
                    "score":       float(rows["score"][j]) if rows["score"][j] is not None else 0.0,
                    "message":     rows["message"][j],
                    "reason_cpu":  rows["reason_cpu"][j],
                    "reason_mem":  rows["reason_mem"][j],
                    "source":      rows["source"][j],
                    "scope":       rows["scope"][j],
                })
            print(f"[event-producer] {n}행 로드 완료")
            return result
        except Exception as e:
            print(f"[event-producer] 데이터 대기 중 ({i+1}/{retries}): {e}")
            time.sleep(10)
    raise RuntimeError(f"{TABLE_ID} 로드 실패 — 테이블에 데이터가 없습니다")


def group_by_ts(rows: list[dict]) -> list[tuple[int, list[dict]]]:
    """같은 ts의 이벤트를 묶어 (ts, [rows]) 리스트 반환 (ts 정렬)."""
    groups: dict[int, list] = defaultdict(list)
    for row in rows:
        groups[row["ts"]].append(row)
    return sorted(groups.items())


def build_kafka_msg(row: dict, now_iso: str) -> dict:
    """seed row → Kafka 이벤트 메시지 변환."""
    reason = {}
    if row["reason_cpu"] is not None:
        reason["cpu_util"] = float(row["reason_cpu"])
    if row["reason_mem"] is not None:
        reason["mem_util"] = float(row["reason_mem"])

    return {
        "event_id":   str(uuid.uuid4()),
        "ts":         now_iso,
        "cluster":    row["cluster"],
        "rack":       row["rack"],
        "node":       row["node"],
        "scope":      row["scope"],
        "source":     row["source"],
        "event_type": row["event_type"],
        "category":   row["category"],
        "severity":   row["severity"],
        "status":     row["status"],
        "from":       row["from_state"],
        "to":         row["to_state"],
        "score":      row["score"],
        "message":    row["message"],
        "reason":     reason,
        "original_ts": row["ts"],
    }


def main():
    catalog = get_catalog()
    rows    = load_event_rows(catalog)
    groups  = group_by_ts(rows)

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    print(f"[event-producer] Kafka 연결: {KAFKA_BROKER}  토픽: {KAFKA_TOPIC}")
    print(f"[event-producer] {len(rows)}개 이벤트 / {len(groups)}개 ts 그룹 루프 시작")

    loop = 0
    while True:
        loop += 1
        print(f"[event-producer] ── 루프 #{loop} ({len(groups)}스텝) ──")

        prev_orig_ts = groups[0][0]
        for orig_ts, batch in groups:
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            for row in batch:
                msg = build_kafka_msg(row, now_iso)
                producer.produce(
                    KAFKA_TOPIC,
                    key=f"{row['cluster']}/{row['rack']}/{row['node']}",
                    value=json.dumps(msg).encode("utf-8"),
                )
                print(f"[event-producer] {msg['event_type']:<22} {msg['severity']:<8} "
                      f"{row['cluster']}/{row['rack']}/{row['node']}")

            producer.flush()

            gap = orig_ts - prev_orig_ts
            time.sleep(gap if gap > 0 else INTERVAL)
            prev_orig_ts = orig_ts


if __name__ == "__main__":
    main()
