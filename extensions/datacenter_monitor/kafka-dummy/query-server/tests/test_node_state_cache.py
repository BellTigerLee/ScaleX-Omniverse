"""NodeStateCache unit tests."""

import pytest

from node_state_cache import NodeStateCache


@pytest.fixture
def cache():
    return NodeStateCache()


@pytest.fixture
def valid_envelope():
    return {
        "kind": "snapshot",
        "scope": "node",
        "cluster": "datax",
        "node": "work3",
        "pod": None,
        "status": "CRITICAL",
        "reasons": ["DiskPressure"],
        "ts": 1777445574991,
        "state_since": 1777431324382,
        "last_seen_at": 1777445549360,
        "gap_sec": 25,
        "previous_status": "HEALTHY",
    }


def _ingest(cache, envelope):
    if cache._validate(envelope):
        key = f"{envelope['cluster']}/{envelope['node']}"
        cache._latest[key] = envelope


def test_validate_accepts_valid_envelope(cache, valid_envelope):
    assert cache._validate(valid_envelope) is True


def test_validate_rejects_non_dict(cache):
    assert cache._validate("not a dict") is False
    assert cache._validate(None) is False
    assert cache._validate(123) is False


def test_validate_rejects_missing_required_field(cache, valid_envelope):
    for field in (
        "kind", "scope", "cluster", "node", "status",
        "ts", "state_since", "last_seen_at", "gap_sec", "reasons",
    ):
        broken = dict(valid_envelope)
        del broken[field]
        assert cache._validate(broken) is False, f"missing {field} should fail"


def test_validate_rejects_unknown_status(cache, valid_envelope):
    bad = dict(valid_envelope, status="ZOMBIE")
    assert cache._validate(bad) is False


def test_validate_rejects_non_list_reasons(cache, valid_envelope):
    bad = dict(valid_envelope, reasons="DiskPressure")
    assert cache._validate(bad) is False


def test_validate_accepts_empty_reasons(cache, valid_envelope):
    ok = dict(valid_envelope, reasons=[])
    assert cache._validate(ok) is True


def test_get_latest_all_empty(cache):
    assert cache.get_latest_all() == []


def test_get_latest_all_returns_one(cache, valid_envelope):
    _ingest(cache, valid_envelope)
    result = cache.get_latest_all()
    assert len(result) == 1
    assert result[0]["node"] == "work3"


def test_cache_overwrites_same_key(cache, valid_envelope):
    _ingest(cache, valid_envelope)

    later = dict(valid_envelope, ts=1777445999999, status="HEALTHY", reasons=[])
    _ingest(cache, later)

    result = cache.get_latest_all()
    assert len(result) == 1
    assert result[0]["status"] == "HEALTHY"
    assert result[0]["ts"] == 1777445999999


def test_cache_keeps_distinct_nodes(cache, valid_envelope):
    _ingest(cache, valid_envelope)
    other = dict(valid_envelope, node="work4")
    _ingest(cache, other)

    result = cache.get_latest_all()
    assert len(result) == 2
    nodes = {e["node"] for e in result}
    assert nodes == {"work3", "work4"}


def test_get_latest_specific(cache, valid_envelope):
    _ingest(cache, valid_envelope)
    assert cache.get_latest("datax", "work3")["node"] == "work3"
    assert cache.get_latest("datax", "absent") is None
