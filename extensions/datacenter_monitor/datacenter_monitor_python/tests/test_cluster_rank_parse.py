"""Unit tests for kafka_subscriber.parse_cluster_rank_message — stageab cluster-rank parser."""

import json

import pytest

from kafka_subscriber import parse_cluster_rank_message


def _valid_envelope() -> dict:
    return {
        "id":      "cluster-rank",
        "cluster": "datax",
        "ts":      1776830800669,
        "ranking": [
            {"node": "datax-dtn-1", "rank": 1, "cpu_util": 0.12558619799508416},
            {"node": "datax-hdd-3", "rank": 2, "cpu_util": 0.1233234777396713},
        ],
    }


def _to_bytes(env: dict) -> bytes:
    return json.dumps(env).encode("utf-8")


def test_parse_valid_envelope_returns_dict():
    env = _valid_envelope()
    parsed = parse_cluster_rank_message(_to_bytes(env))
    assert parsed is not None
    for key in ("id", "cluster", "ts", "ranking"):
        assert parsed[key] == env[key]


def test_parse_empty_ranking_allowed():
    """Empty-terminal emit: all nodes stale → ranking=[] must still parse."""
    env = _valid_envelope()
    env["ranking"] = []
    parsed = parse_cluster_rank_message(_to_bytes(env))
    assert parsed is not None
    assert parsed["ranking"] == []


@pytest.mark.parametrize("missing_field", ["id", "cluster", "ts", "ranking"])
def test_parse_missing_required_field_returns_none(missing_field):
    env = _valid_envelope()
    del env[missing_field]
    assert parse_cluster_rank_message(_to_bytes(env)) is None


@pytest.mark.parametrize("wrong_id", ["cluster-severity", "cluster-rank-mem", "", "CLUSTER-RANK"])
def test_parse_mismatched_id_returns_none(wrong_id):
    env = _valid_envelope()
    env["id"] = wrong_id
    assert parse_cluster_rank_message(_to_bytes(env)) is None


def test_parse_non_list_ranking_returns_none():
    env = _valid_envelope()
    env["ranking"] = {"not": "a list"}
    assert parse_cluster_rank_message(_to_bytes(env)) is None


def test_parse_non_json_returns_none():
    assert parse_cluster_rank_message(b"not-json{") is None


def test_parse_bad_utf8_returns_none():
    assert parse_cluster_rank_message(b"\xff\xfe\xfa") is None


def test_parse_non_dict_top_level_returns_none():
    assert parse_cluster_rank_message(json.dumps(["list", "not", "dict"]).encode("utf-8")) is None
