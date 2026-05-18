"""Unit tests for kafka_subscriber.parse_node_state_message — canonical envelope parser."""

import json

import pytest

from kafka_subscriber import parse_node_state_message


def _valid_envelope() -> dict:
    return {
        "kind":            "transition",
        "scope":           "node",
        "cluster":         "datax",
        "node":            "work2",
        "pod":             None,
        "status":          "HEALTHY",
        "reasons":         [],
        "ts":              1744441200123,
        "state_since":     1744441125000,
        "previous_status": "DISCONNECTED",
        "last_seen_at":    1744441125000,
        "gap_sec":         75,
    }


def _to_bytes(env: dict) -> bytes:
    return json.dumps(env).encode("utf-8")


def test_parse_valid_envelope_returns_dict():
    env = _valid_envelope()
    parsed = parse_node_state_message(_to_bytes(env))
    assert parsed is not None
    for key in ("kind", "scope", "cluster", "node", "pod", "status",
                "reasons", "ts", "state_since", "previous_status",
                "last_seen_at", "gap_sec"):
        assert parsed[key] == env[key]


def test_parse_empty_reasons_allowed():
    env = _valid_envelope()
    env["reasons"] = []
    assert parse_node_state_message(_to_bytes(env)) is not None


def test_parse_previous_status_null_allowed():
    env = _valid_envelope()
    env["previous_status"] = None
    assert parse_node_state_message(_to_bytes(env)) is not None


def test_parse_snapshot_kind():
    env = _valid_envelope()
    env["kind"] = "snapshot"
    env["previous_status"] = None
    assert parse_node_state_message(_to_bytes(env)) is not None


@pytest.mark.parametrize("missing_field", [
    "kind", "scope", "cluster", "node", "status",
    "ts", "state_since", "last_seen_at", "gap_sec", "reasons",
])
def test_parse_missing_required_field_returns_none(missing_field):
    env = _valid_envelope()
    del env[missing_field]
    assert parse_node_state_message(_to_bytes(env)) is None


@pytest.mark.parametrize("bad_status", ["OK", "healthy", "", "unknown", "Disconnected"])
def test_parse_invalid_status_enum_returns_none(bad_status):
    env = _valid_envelope()
    env["status"] = bad_status
    assert parse_node_state_message(_to_bytes(env)) is None


@pytest.mark.parametrize("accepted_status", ["HEALTHY", "WARNING", "CRITICAL", "DISCONNECTED", "UNKNOWN"])
def test_parse_accepts_all_canonical_status_values(accepted_status):
    env = _valid_envelope()
    env["status"] = accepted_status
    assert parse_node_state_message(_to_bytes(env)) is not None


def test_parse_non_json_returns_none():
    assert parse_node_state_message(b"not-json{") is None


def test_parse_bad_utf8_returns_none():
    assert parse_node_state_message(b"\xff\xfe\xfa") is None
