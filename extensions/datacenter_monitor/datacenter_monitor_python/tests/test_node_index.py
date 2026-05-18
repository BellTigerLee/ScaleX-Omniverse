"""Unit tests for scene/node_index.py — topology response parser."""

from datacenter_monitor_python.scene.node_index import parse_topology_response


def _sample_response() -> dict:
    """사용자가 공유한 실제 응답 형식의 축약본."""
    return {
        "clusters": [
            {
                "id": "twinx",
                "racks": [
                    {
                        "id": "Rack_42U_A4",
                        "boxes": [
                            {
                                "prim_name": "Box_4U_L40S",
                                "has_node": True,
                                "nodes": ["l40s"],
                            },
                            {
                                "prim_name": "Box_2U_EdgeBox_1",
                                "has_node": True,
                                "nodes": ["edgebox1"],
                            },
                            {
                                "prim_name": "Box_1U_100G_Switch",
                                "has_node": False,
                                "nodes": [],
                            },
                        ],
                    },
                ],
            },
            {
                "id": "datax",
                "racks": [
                    {
                        "id": "Rack_42U_A3",
                        "boxes": [
                            {
                                "prim_name": "Box_1U_Control_2",
                                "has_node": True,
                                "nodes": [
                                    "datax-ctrlpln-2-k8s-cp2",
                                    "datax-ctrlpln-2-k8s-cp3",
                                ],
                            },
                            {
                                "prim_name": "Box_1U_DTN_1",
                                "has_node": True,
                                "nodes": ["datax-dtn-1"],
                            },
                        ],
                    },
                ],
            },
        ],
    }


def test_parse_basic_node_to_prim():
    idx = parse_topology_response(_sample_response())
    assert idx[("twinx", "l40s")] == "Box_4U_L40S"
    assert idx[("twinx", "edgebox1")] == "Box_2U_EdgeBox_1"
    assert idx[("datax", "datax-dtn-1")] == "Box_1U_DTN_1"


def test_parse_multiple_nodes_per_prim():
    idx = parse_topology_response(_sample_response())
    # 같은 prim 에 두 node 가 있어도 둘 다 매핑돼야 함
    assert idx[("datax", "datax-ctrlpln-2-k8s-cp2")] == "Box_1U_Control_2"
    assert idx[("datax", "datax-ctrlpln-2-k8s-cp3")] == "Box_1U_Control_2"


def test_parse_skips_boxes_with_no_nodes():
    idx = parse_topology_response(_sample_response())
    # Box_1U_100G_Switch 는 has_node=False, nodes=[] → 인덱스에 포함되지 않음
    for key in idx.keys():
        assert "100G_Switch" not in idx.get(key, "")


def test_parse_normalizes_cluster_to_lowercase():
    data = {
        "clusters": [
            {
                "id": "TwinX",
                "racks": [
                    {
                        "id": "Rack_X",
                        "boxes": [
                            {"prim_name": "Box_A", "has_node": True, "nodes": ["node1"]},
                        ],
                    },
                ],
            },
        ],
    }
    idx = parse_topology_response(data)
    assert idx == {("twinx", "node1"): "Box_A"}


def test_parse_preserves_node_casing_exact():
    """node 이름은 원문 그대로 유지 — Kafka envelope 과 bit-exact 매칭 보장."""
    data = {
        "clusters": [
            {
                "id": "c",
                "racks": [
                    {
                        "id": "r",
                        "boxes": [
                            {"prim_name": "Box_1", "has_node": True, "nodes": ["MyNode-01"]},
                        ],
                    },
                ],
            },
        ],
    }
    idx = parse_topology_response(data)
    assert idx[("c", "MyNode-01")] == "Box_1"
    assert ("c", "mynode-01") not in idx
    assert ("c", "MYNODE-01") not in idx


def test_parse_empty_response():
    assert parse_topology_response({}) == {}
    assert parse_topology_response({"clusters": []}) == {}


def test_parse_cluster_without_id():
    data = {"clusters": [{"racks": [{"id": "r", "boxes": [
        {"prim_name": "Box_A", "has_node": True, "nodes": ["n"]},
    ]}]}]}
    assert parse_topology_response(data) == {}


def test_parse_box_without_prim_name():
    data = {
        "clusters": [
            {
                "id": "c",
                "racks": [
                    {
                        "id": "r",
                        "boxes": [
                            {"has_node": True, "nodes": ["n"]},  # prim_name 누락
                            {"prim_name": "Box_ok", "has_node": True, "nodes": ["m"]},
                        ],
                    },
                ],
            },
        ],
    }
    idx = parse_topology_response(data)
    assert idx == {("c", "m"): "Box_ok"}


def test_parse_none_and_empty_node_entries_skipped():
    data = {
        "clusters": [
            {
                "id": "c",
                "racks": [
                    {
                        "id": "r",
                        "boxes": [
                            {"prim_name": "Box_A", "has_node": True, "nodes": ["n1", "", None, "n2"]},
                        ],
                    },
                ],
            },
        ],
    }
    idx = parse_topology_response(data)
    assert idx == {
        ("c", "n1"): "Box_A",
        ("c", "n2"): "Box_A",
    }


def test_parse_missing_nodes_key_treated_as_empty():
    data = {
        "clusters": [
            {
                "id": "c",
                "racks": [
                    {
                        "id": "r",
                        "boxes": [
                            {"prim_name": "Box_A", "has_node": False},  # nodes 키 누락
                            {"prim_name": "Box_B", "has_node": True, "nodes": ["n1"]},
                        ],
                    },
                ],
            },
        ],
    }
    idx = parse_topology_response(data)
    assert idx == {("c", "n1"): "Box_B"}
