"""
_TopologyMixin
USD 씬 계층 자동 탐색 — Cluster / Rack / Box(Node) 구조 인덱싱.

담당:
  - discover_topology()       : SCALE_POD_PATH 기반 탐색
  - _discover_topology_flat() : SCALE_POD_PATH 없을 때 fallback 탐색
  - get_cached_topology()     : 캐시 반환 또는 재탐색
  - normalize_node_x_position(): 모든 노드 X 좌표 정규화
"""

from pxr import Gf, Usd, UsdGeom

from ..global_variables import (
    SCENE_ROOT,
    SCALE_POD_PATH,
    CLUSTER_SUFFIX,
    RACK_PREFIX,
    BOX_PREFIX,
    NODE_X_DEFAULT,
    NODE_X_SPECIAL,
)


class _TopologyMixin:
    """USD 씬 topology 탐색 및 경로 인덱스 관리 Mixin."""

    def _init_topology(self):
        """Topology 관련 인스턴스 속성 초기화. SceneManager.__init__에서 호출."""
        # { "ClusterName": full_cluster_prim_path }
        self._cluster_paths: dict[str, str] = {}
        # { "ClusterName/RackName": full_rack_prim_path }
        self._rack_paths: dict[str, str] = {}
        # { "ClusterName/RackName/BoxName": full_box_prim_path }
        self._server_index: dict[str, str] = {}
        # { "ClusterName/BoxName": [prim_path, ...] }  — rack 없는 조회용 fallback
        self._cluster_box_index: dict[str, list] = {}

    # ──────────────────────────────────────────────────────────────────────
    # 탐색 진입점
    # ──────────────────────────────────────────────────────────────────────

    def discover_topology(self) -> dict:
        """
        USD 씬 계층을 자동 탐색합니다.

        탐색 경로:
          SCALE_POD_PATH/
            {Name}_Cluster/    ← CLUSTER_SUFFIX = "_Cluster"
              Rack_{Name}/     ← RACK_PREFIX = "Rack_"
                Box_{Name}/    ← BOX_PREFIX = "Box_"

        Returns:
          { "clusters": [...], "racks": [...] }
        """
        if not self._stage:
            return {}

        pod_prim = self._stage.GetPrimAtPath(SCALE_POD_PATH)
        if not pod_prim or not pod_prim.IsValid():
            print(f"[SceneManager] SCALE_POD_PATH '{SCALE_POD_PATH}' 없음 — flat 탐색으로 전환")
            return self._discover_topology_flat()

        clusters = []
        all_racks = []

        for cluster_prim in pod_prim.GetChildren():
            cluster_name = cluster_prim.GetName()
            if not cluster_name.endswith(CLUSTER_SUFFIX):
                continue

            cluster_path = str(cluster_prim.GetPath())
            self._cluster_paths[cluster_name] = cluster_path
            racks = []

            for rack_prim in cluster_prim.GetChildren():
                rack_name = rack_prim.GetName()
                if not rack_name.startswith(RACK_PREFIX):
                    continue

                rack_path = str(rack_prim.GetPath())
                self._rack_paths[f"{cluster_name}/{rack_name}"] = rack_path

                # Kafka lowercase alias: "datax/Rack_42U_A3" → prim_path
                _cluster_lower = cluster_name.lower()
                if _cluster_lower.endswith("_cluster"):
                    _cluster_lower = _cluster_lower[:-len(CLUSTER_SUFFIX)]
                _rack_alias = f"{_cluster_lower}/{rack_name}"
                if _rack_alias not in self._rack_paths:
                    self._rack_paths[_rack_alias] = rack_path

                servers = []
                for box_prim in rack_prim.GetChildren():
                    box_name = box_prim.GetName()
                    if not box_name.lower().startswith(BOX_PREFIX.lower()):
                        continue
                    box_path = str(box_prim.GetPath())
                    servers.append({
                        "id":       box_name,
                        "primPath": box_path,
                        "label":    box_name.replace("_", "-"),
                    })
                    self._server_index[f"{cluster_name}/{rack_name}/{box_name}"] = box_path

                    # rack 없는 조회용 fallback 인덱스
                    cb_key = f"{cluster_name}/{box_name}"
                    self._cluster_box_index.setdefault(cb_key, []).append(box_path)

                    # Kafka lowercase 별칭: "DataX_Cluster/Box_xxx" → "datax/Box_xxx"
                    _base = cluster_name.lower()
                    if _base.endswith("_cluster"):
                        _base = _base[:-len(CLUSTER_SUFFIX)]
                    alias_key = f"{_base}/{box_name}"
                    if alias_key not in self._cluster_box_index:
                        self._cluster_box_index.setdefault(alias_key, []).append(box_path)

                    # 머티리얼 캐시 및 glass cube 생성 (_MaterialMixin에 위임)
                    self._cache_node_material(box_path, box_prim)
                    self._create_glass_cube(box_path, box_prim)
                    # self.create_status_cylinders(box_path, box_prim)

                rack_entry = {
                    "id":        rack_name,
                    "clusterId": cluster_name,
                    "primPath":  rack_path,
                    "label":     rack_name.replace("_", "-"),
                    "nodes":     servers,
                }
                racks.append(rack_entry)
                all_racks.append(rack_entry)

            clusters.append({
                "id":       cluster_name,
                "primPath": cluster_path,
                "label":    cluster_name.replace("_", "-"),
                "racks":    racks,
            })

        print(
            f"[SceneManager] topology 탐색 완료: "
            f"{len(clusters)}개 cluster, {len(all_racks)}개 rack"
        )
        result = {"clusters": clusters, "racks": all_racks}
        self._topology_cache = result
        self.normalize_node_x_position()
        return result

    def _discover_topology_flat(self) -> dict:
        """SCALE_POD_PATH가 없을 때 SCENE_ROOT 직접 자식에서 Rack_ 탐색 (fallback)."""
        root_prim = self._stage.GetPrimAtPath(SCENE_ROOT)
        if not root_prim or not root_prim.IsValid():
            return {}

        all_racks = []
        for child in root_prim.GetChildren():
            name = child.GetName()
            if not name.startswith(RACK_PREFIX):
                continue
            rack_path = str(child.GetPath())
            self._rack_paths[name] = rack_path

            servers = []
            for box_prim in child.GetChildren():
                bn = box_prim.GetName()
                if not bn.lower().startswith(BOX_PREFIX.lower()):
                    continue
                bp = str(box_prim.GetPath())
                servers.append({"id": bn, "primPath": bp, "label": bn.replace("_", "-")})
                self._server_index[f"{name}/{bn}"] = bp
                self._cluster_box_index.setdefault(bn, []).append(bp)
                self._cache_node_material(bp, box_prim)
                self._create_glass_cube(bp, box_prim)
                # self.create_status_cylinders(bp, box_prim)

            all_racks.append({
                "id":       name,
                "primPath": rack_path,
                "label":    name.replace("_", "-"),
                "nodes":    servers,
            })

        print(f"[SceneManager] topology (flat) 탐색 완료: {len(all_racks)}개 rack")
        result = {"racks": all_racks}
        self._topology_cache = result
        return result

    def get_cached_topology(self) -> dict:
        """마지막으로 탐색한 topology를 반환합니다. 없으면 재탐색합니다."""
        cache = getattr(self, "_topology_cache", None)
        if cache:
            return cache
        return self.discover_topology()

    # ──────────────────────────────────────────────────────────────────────
    # 노드 X 좌표 정규화
    # ──────────────────────────────────────────────────────────────────────

    def normalize_node_x_position(self):
        """
        모든 노드(Box)의 X 좌표를 정규화합니다.
        - NODE_X_SPECIAL에 정의된 특수 노드 → 지정된 X 위치
        - 나머지 노드 → NODE_X_DEFAULT 값
        Y, Z 좌표는 그대로 유지합니다.
        """
        if not self._stage:
            return

        count_default, count_special = 0, 0
        special_nodes_applied = []

        for node_path in self._server_index.values():
            prim = self._stage.GetPrimAtPath(node_path)
            if not prim or not prim.IsValid():
                continue

            node_name = prim.GetName()
            x_pos     = NODE_X_SPECIAL.get(node_name)
            is_special = x_pos is not None
            if not is_special:
                x_pos = NODE_X_DEFAULT

            xformable   = UsdGeom.Xformable(prim)
            translate_op = None
            for op in xformable.GetOrderedXformOps():
                if op.GetOpType() == UsdGeom.XformOp.TypeTranslate:
                    translate_op = op
                    break
            if translate_op is None:
                translate_op = xformable.AddTranslateOp()

            current = translate_op.Get() or Gf.Vec3d(0, 0, 0)
            translate_op.Set(Gf.Vec3d(x_pos, current[1], current[2]))

            if is_special:
                count_special += 1
                special_nodes_applied.append(f"{node_name}={x_pos}")
            else:
                count_default += 1

        log_msg = f"[SceneManager] 노드 X 좌표 정규화: {count_default}개 → X={NODE_X_DEFAULT}"
        if count_special > 0:
            log_msg += f", {count_special}개 특수 노드: {', '.join(special_nodes_applied)}"
        print(log_msg)
