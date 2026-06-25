"""Unit tests for kafka_subscriber.parse_metric_snapshot."""

import json

import pytest

from kafka_subscriber import parse_metric_snapshot


def _snapshot() -> dict:
    return {
        "schema_version": 1,
        "kind": "node_metrics_snapshot",
        "ts": 1782194209000,
        "cluster": "ecclab",
        "node": "work2",
        "status": "HEALTHY",
        "metrics": {
            "cpu": {"util": 0.3, "cores": 32.0},
            "gpu": {"util": 0.0, "total_gb": 0.0},
        },
        "telemetry": {
            "source": "victoriametrics",
            "rule_interval_sec": 15,
            "scrape_interval_sec": 30,
            "lag_sec": 10.1,
            "missing_after_sec": 120.0,
        },
        "kubernetes": {"ready": True},
        "debug_ts": 1782194209000,
        "future_field": {"kept": True},
    }


def _to_bytes(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")


def test_parse_queryserver_metric_snapshot_preserves_payload():
    payload = _snapshot()
    parsed = parse_metric_snapshot(_to_bytes(payload))

    assert parsed is not None
    assert parsed["schema_version"] == 1
    assert parsed["kind"] == "node_metrics_snapshot"
    assert parsed["cluster"] == "ecclab"
    assert parsed["node"] == "work2"
    assert parsed["metrics"]["cpu"]["util"] == 0.3
    assert parsed["telemetry"]["source"] == "victoriametrics"
    assert parsed["kubernetes"]["ready"] is True
    assert parsed["future_field"] == {"kept": True}


def test_parse_legacy_aliases_adds_canonical_cluster_and_node():
    payload = _snapshot()
    payload.pop("cluster")
    payload.pop("node")
    payload["cluster_id"] = "datax"
    payload["box_id"] = "Box_1"

    parsed = parse_metric_snapshot(_to_bytes(payload))

    assert parsed is not None
    assert parsed["cluster"] == "datax"
    assert parsed["node"] == "Box_1"
    assert parsed["cluster_id"] == "datax"
    assert parsed["box_id"] == "Box_1"


@pytest.mark.parametrize("missing_field", ["cluster", "node", "ts", "metrics"])
def test_parse_missing_required_field_returns_none(missing_field):
    payload = _snapshot()
    del payload[missing_field]
    assert parse_metric_snapshot(_to_bytes(payload)) is None


def test_parse_metrics_not_dict_returns_none():
    payload = _snapshot()
    payload["metrics"] = []
    assert parse_metric_snapshot(_to_bytes(payload)) is None


def test_parse_non_json_returns_none():
    assert parse_metric_snapshot(b"not-json{") is None


def test_parse_bad_utf8_returns_none():
    assert parse_metric_snapshot(b"\xff\xfe\xfa") is None
