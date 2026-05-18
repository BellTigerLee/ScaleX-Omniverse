"""
topology_db.py
────────────────────────────────────────────────────────────────────────────
PostgreSQL에서 topology를 조회하는 헬퍼.

주요 함수:
  get_topology()          → 전체 cluster/rack/box/node 구조 반환
  get_prim_for_node(node_id) → Kafka node_id → (cluster_id, prim_name)
"""

import asyncio
from functools import partial

import psycopg2
import psycopg2.extras

from topology_seed import PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD


def _get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASSWORD,
    )


def _query(sql: str, params=None) -> list[dict]:
    with _get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]


async def _query_async(sql: str, params=None) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(_query, sql, params))


# ── 공개 API ──────────────────────────────────────────────────────────────────

async def get_topology() -> dict:
    """
    clusters / boxes / nodes 테이블을 JOIN해 전체 topology 구조를 반환합니다.

    반환 형식:
    {
      "clusters": [
        {
          "id": "datax",
          "racks": [
            {
              "id": "Rack_42U_A3",
              "boxes": [
                {
                  "prim_name": "Box_1U_DTN_3",
                  "has_node": true,
                  "nodes": ["datax-dtn-3"]
                },
                {
                  "prim_name": "Box_1U_1G_Switch",
                  "has_node": false,
                  "nodes": []
                }
              ]
            }
          ]
        }
      ]
    }
    """
    rows = await _query_async("""
        SELECT
            b.cluster_id,
            b.rack_id,
            b.prim_name,
            b.has_node,
            COALESCE(
                array_agg(n.node_id) FILTER (WHERE n.node_id IS NOT NULL),
                '{}'
            ) AS node_ids
        FROM boxes b
        LEFT JOIN nodes n
            ON n.cluster_id = b.cluster_id AND n.prim_name = b.prim_name
        GROUP BY b.cluster_id, b.rack_id, b.prim_name, b.has_node
        ORDER BY b.cluster_id, b.rack_id, b.prim_name
    """)

    # cluster → rack → boxes 중첩 구조로 조립
    clusters: dict[str, dict[str, list]] = {}
    for r in rows:
        cid      = r["cluster_id"]
        rid      = r["rack_id"]
        clusters.setdefault(cid, {}).setdefault(rid, []).append({
            "prim_name": r["prim_name"],
            "has_node":  r["has_node"],
            "nodes":     list(r["node_ids"]),
        })

    return {
        "clusters": [
            {
                "id": cid,
                "racks": [
                    {"id": rid, "boxes": boxes}
                    for rid, boxes in racks.items()
                ],
            }
            for cid, racks in clusters.items()
        ]
    }


async def get_prim_for_node(node_id: str) -> tuple[str, str] | None:
    """
    Kafka node_id → (cluster_id, prim_name) 반환.
    매핑이 없으면 None.
    """
    rows = await _query_async(
        "SELECT cluster_id, prim_name FROM nodes WHERE node_id = %s",
        (node_id,),
    )
    if not rows:
        return None
    return rows[0]["cluster_id"], rows[0]["prim_name"]
