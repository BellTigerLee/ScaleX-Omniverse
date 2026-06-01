"""Prim-keyed cache for latest datacenter.metrics messages."""


class _NodeMetricsMixin:
    """Cache datacenter.metrics snapshots by resolved USD prim path."""

    def _init_node_metrics(self):
        # prim_path -> {node_name -> latest original datacenter.metrics message}
        self._node_metrics_cache: dict[str, dict[str, dict]] = {}

    def cache_node_metrics(self, msg: dict) -> None:
        """Resolve one datacenter.metrics message to a prim and cache it.

        Unresolvable messages are skipped quietly. The topic is compacted/upserted,
        so a later message can populate the cache after topology is ready.
        """
        if not isinstance(msg, dict):
            return

        cluster = msg.get("cluster", "")
        node = msg.get("node", "")
        if not node:
            return

        prim_path = self._resolve_prim_path(cluster, node)
        if prim_path is None:
            return

        self._node_metrics_cache.setdefault(prim_path, {})[node] = msg

    def get_node_metrics(self, prim_path: str) -> list:
        """Return latest original metrics messages for all nodes mapped to prim_path."""
        by_node = self._node_metrics_cache.get(prim_path, {})
        return list(by_node.values())
