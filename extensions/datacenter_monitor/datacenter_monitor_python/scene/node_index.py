"""
Node-index: (cluster, node_name) → prim_name 매핑.

Topology API 응답을 파싱하여 Kafka envelope 의 raw node 이름을
USD 씬 box prim 이름으로 변환한다. prim_name 은 _server_index /
_cluster_box_index 와 키 형식이 일치하므로 기존 인덱스로 prim_path 까지
완주 가능.

응답 스키마 (2026-04-17 현재, 10.31.31.210:3001/topology):
  {
    "clusters": [
      {
        "id": "<cluster>",
        "racks": [
          {
            "id": "<rack>",
            "boxes": [
              {
                "prim_name": "<Box_...>",
                "has_node": <bool>,
                "nodes":    ["<node_name>", ...]
              }
            ]
          }
        ]
      }
    ]
  }

한 prim 에 여러 node 가 속할 수 있으며 (예: Box_1U_Control_2 →
[datax-ctrlpln-2-k8s-cp2, datax-ctrlpln-2-k8s-cp3]) 그 경우 같은
prim_name 에 복수 node 가 매핑된다. Kafka envelope 이 node 이름으로 오므로
node → prim_name 은 1:1, prim_name → node 는 1:N 관계.
"""
from __future__ import annotations

import json
import urllib.request
from typing import Optional


def parse_topology_response(data: dict) -> dict:
    """
    Topology JSON → {(cluster_lower, node_name) → prim_name} dict.

    - cluster 는 소문자로 정규화 (envelope 의 cluster 도 소문자로 들어온다는 가정과 일치,
      resolver 쪽에서도 `.lower()` 로 매칭함).
    - node_name 은 **정규화하지 않는다** — Kafka envelope 의 `node` 필드와 bit-exact 매칭.
    """
    index: dict = {}
    for cluster in data.get("clusters", []):
        cluster_id = (cluster.get("id") or "").lower()
        if not cluster_id:
            continue
        for rack in cluster.get("racks", []):
            for box in rack.get("boxes", []):
                prim_name = box.get("prim_name")
                if not prim_name:
                    continue
                for node_name in box.get("nodes") or []:
                    if not node_name:
                        continue
                    index[(cluster_id, node_name)] = prim_name
    return index


def fetch_topology_index(url: str, timeout: float = 5.0) -> Optional[dict]:
    """
    Topology URL 에서 JSON 을 받아 parse_topology_response 로 index 생성.
    네트워크·JSON·스키마 오류 시 경고 로그 + None 반환 (호출자가 fallback 결정).
    """
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
    except Exception as e:
        print(f"[NodeIndex] topology fetch 실패 ({url}): {e}")
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"[NodeIndex] topology JSON 파싱 실패: {e}")
        return None

    try:
        return parse_topology_response(data)
    except Exception as e:
        print(f"[NodeIndex] topology 스키마 파싱 실패: {e}")
        return None
