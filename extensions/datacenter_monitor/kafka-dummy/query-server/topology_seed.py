"""
topology_seed.py
────────────────────────────────────────────────────────────────────────────
topology.json을 읽어 PostgreSQL에 clusters / boxes / nodes 테이블을 생성하고
초기 데이터를 등록합니다.

실행:
  python topology_seed.py

환경변수:
  TOPOLOGY_JSON  topology.json 경로 (기본값: /app/topology.json)
  PG_HOST        PostgreSQL 호스트 (기본값: localhost)
  PG_PORT        PostgreSQL 포트   (기본값: 5432)
  PG_DB          데이터베이스 이름 (기본값: datacenter)
  PG_USER        사용자            (기본값: postgres)
  PG_PASSWORD    비밀번호          (기본값: postgres)

DDL:
  clusters(id)
  boxes(cluster_id, prim_name, rack_id, has_node)  ← 복합 PK (cluster_id, prim_name)
  nodes(node_id, cluster_id, prim_name)             ← Kafka node_id → prim 1:1 매핑
"""

import json
import os
import sys

import psycopg2
from psycopg2.extras import execute_values

TOPOLOGY_JSON = os.getenv("TOPOLOGY_JSON", "/app/topology.json")

PG_HOST     = os.getenv("PG_HOST",     "cnpg-otel-a-rw.observability.svc.cluster.local")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB",       "nessie_db")
PG_USER     = os.getenv("PG_USER",     "nessie_lover")
PG_PASSWORD = os.getenv("PG_PASSWORD", "nessie_love")

# NESSIE_HOST = os.getenv("NESSIE_HOST", "nessie.observability.svc.cluster.local")
# NESSIE_PORT = int(os.getenv("NESSIE_PORT", "19120"))
# NESSIE_DB       = os.getenv("NESSIE_DB",       "nessie_db")
# NESSIE_USER = os.getenv("NESSIE_USER", "")
# NESSIE_PASSWORD = os.getenv("NESSIE_PASSWORD", "")

DROP_DDL = """
DROP TABLE IF EXISTS nodes;
DROP TABLE IF EXISTS boxes;
DROP TABLE IF EXISTS clusters;
"""

DDL = """
CREATE TABLE IF NOT EXISTS clusters (
    id VARCHAR PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS boxes (
    cluster_id VARCHAR NOT NULL REFERENCES clusters(id),
    prim_name  VARCHAR NOT NULL,
    rack_id    VARCHAR NOT NULL,
    has_node   BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (cluster_id, prim_name)
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id    VARCHAR PRIMARY KEY,
    cluster_id VARCHAR NOT NULL,
    prim_name  VARCHAR NOT NULL,
    FOREIGN KEY (cluster_id, prim_name) REFERENCES boxes(cluster_id, prim_name)
);
"""


def load_topology(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def seed(conn, topology: dict):
    cur = conn.cursor()

    # ── DROP (임시) ───────────────────────────────────────────────────────────
    cur.execute(DROP_DDL)

    # ── DDL ──────────────────────────────────────────────────────────────────
    cur.execute(DDL)

    cluster_rows = []
    box_rows     = []
    node_rows    = []

    for cluster_id, racks in topology.items():
        cluster_rows.append((cluster_id,))

        for rack_id, boxes in racks.items():
            for prim_name, node_ids in boxes.items():
                has_node = len(node_ids) > 0
                box_rows.append((cluster_id, prim_name, rack_id, has_node))

                for node_id in node_ids:
                    node_rows.append((node_id, cluster_id, prim_name))

    # ── clusters ─────────────────────────────────────────────────────────────
    execute_values(
        cur,
        """
        INSERT INTO clusters (id)
        VALUES %s
        ON CONFLICT (id) DO NOTHING
        """,
        cluster_rows,
    )
    print(f"[topology_seed] clusters: {len(cluster_rows)}개 upsert")

    # ── boxes ─────────────────────────────────────────────────────────────────
    execute_values(
        cur,
        """
        INSERT INTO boxes (cluster_id, prim_name, rack_id, has_node)
        VALUES %s
        ON CONFLICT (cluster_id, prim_name) DO UPDATE
            SET rack_id  = EXCLUDED.rack_id,
                has_node = EXCLUDED.has_node
        """,
        box_rows,
    )
    print(f"[topology_seed] boxes: {len(box_rows)}개 upsert")

    # ── nodes ─────────────────────────────────────────────────────────────────
    execute_values(
        cur,
        """
        INSERT INTO nodes (node_id, cluster_id, prim_name)
        VALUES %s
        ON CONFLICT (node_id) DO UPDATE
            SET cluster_id = EXCLUDED.cluster_id,
                prim_name  = EXCLUDED.prim_name
        """,
        node_rows,
    )
    print(f"[topology_seed] nodes: {len(node_rows)}개 upsert")

    conn.commit()
    cur.close()


def main():
    print(f"[topology_seed] topology 파일 로드: {TOPOLOGY_JSON}")
    topology = load_topology(TOPOLOGY_JSON)

    print(f"[topology_seed] PostgreSQL 연결: {PG_HOST}:{PG_PORT}/{PG_DB}")
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )

    try:
        seed(conn, topology)
        print("[topology_seed] 완료")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
