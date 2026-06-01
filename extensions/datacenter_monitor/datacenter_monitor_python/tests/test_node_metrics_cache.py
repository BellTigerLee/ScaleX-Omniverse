"""Unit tests for scene/node_metrics.py — datacenter.metrics prim cache."""

from datacenter_monitor_python.scene.node_metrics import _NodeMetricsMixin


class _CacheHarness(_NodeMetricsMixin):
    def __init__(self, mapping: dict[tuple[str, str], str]):
        self._mapping = mapping
        self._init_node_metrics()

    def _resolve_prim_path(self, cluster: str, node: str):
        return self._mapping.get(((cluster or "").lower(), node))


def _msg(cluster: str, node: str, ts: int, cpu: float = 0.1) -> dict:
    return {
        "ts": ts,
        "cluster": cluster,
        "node": node,
        "status": "HEALTHY",
        "metrics": {"cpu": {"util": cpu}},
        "debug_ts": ts + 1,
    }


def test_cache_one_node_for_one_prim():
    prim_path = "/World/TwinX_Cluster/Rack_A/Box_1"
    cache = _CacheHarness({("twinx", "work1"): prim_path})

    first = _msg("TwinX", "work1", 100)
    cache.cache_node_metrics(first)

    assert cache.get_node_metrics(prim_path) == [first]


def test_cache_multiple_nodes_for_same_prim():
    prim_path = "/World/DataX_Cluster/Rack_A/Box_Control"
    cache = _CacheHarness({
        ("datax", "cp2"): prim_path,
        ("datax", "cp3"): prim_path,
    })

    cp2 = _msg("datax", "cp2", 100)
    cp3 = _msg("datax", "cp3", 101)
    cache.cache_node_metrics(cp2)
    cache.cache_node_metrics(cp3)

    assert cache.get_node_metrics(prim_path) == [cp2, cp3]


def test_cache_upsert_overwrites_same_node():
    prim_path = "/World/DataX_Cluster/Rack_A/Box_DTN"
    cache = _CacheHarness({("datax", "dtn1"): prim_path})

    old = _msg("datax", "dtn1", 100, cpu=0.1)
    latest = _msg("datax", "dtn1", 200, cpu=0.9)
    cache.cache_node_metrics(old)
    cache.cache_node_metrics(latest)

    assert cache.get_node_metrics(prim_path) == [latest]


def test_cache_skips_unresolved_node():
    cache = _CacheHarness({})

    cache.cache_node_metrics(_msg("unknown", "work9", 100))

    assert cache._node_metrics_cache == {}


def test_get_node_metrics_missing_prim_returns_empty_list():
    cache = _CacheHarness({})

    assert cache.get_node_metrics("/World/Missing") == []


def test_cache_ignores_non_dict_and_missing_node():
    cache = _CacheHarness({("datax", "work1"): "/World/Box_1"})

    cache.cache_node_metrics(None)
    cache.cache_node_metrics("not a dict")
    cache.cache_node_metrics({"cluster": "datax"})

    assert cache._node_metrics_cache == {}
