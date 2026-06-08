"""
_TopologyMixin
USD 씬 계층 자동 탐색 — Cluster / Rack / Box(Node) 구조 인덱싱.

담당:
  - discover_topology()       : SCALE_POD_PATH 기반 탐색
  - _discover_topology_flat() : SCALE_POD_PATH 없을 때 fallback 탐색
  - get_cached_topology()     : 캐시 반환 또는 재탐색
  - normalize_node_x_position(): 필요 시 수동 호출하는 노드 X 좌표 정규화 helper
"""

from pxr import Gf, Usd, UsdGeom

from ..global_variables import (
    SCENE_ROOT,
    SCALE_POD_PATH,
    CLUSTER_SUFFIX,
    BOX_PREFIX,
    NODE_INDEX_URL,
    NODE_X_DEFAULT,
    NODE_X_SPECIAL,
)

_RACK_42U_PREFIX = "Rack_42U_"
_EXPLICIT_RACK_NAMES = {"Rack_Switch", "Rack_Control", "Rack_Storage", "Rack_DTN"}


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
        # Topology API raw 응답 lazy fetch 중복 방지
        self._topology_api_fetch_attempted: bool = False

    # ──────────────────────────────────────────────────────────────────────
    # 탐색 진입점
    # ──────────────────────────────────────────────────────────────────────

    def discover_topology(self) -> dict:
        """
        USD 씬 계층을 자동 탐색합니다.

        탐색 경로:
          SCALE_POD_PATH/
            {Name}_Cluster/    ← CLUSTER_SUFFIX = "_Cluster"
              Rack_42U_* or explicit rack names
                Box_{Name}/    ← BOX_PREFIX = "Box_"

        Returns:
          { "clusters": [...], "racks": [...] }
        """
        if not self._stage:
            return {}

        pod_prim = self._stage.GetPrimAtPath(SCALE_POD_PATH)
        if pod_prim and pod_prim.IsValid():
            result = self._discover_topology_clustered(pod_prim, "pod")
            if result.get("racks"):
                return result
            print(f"[SceneManager] SCALE_POD_PATH '{SCALE_POD_PATH}' 아래 rack 없음 — scene 전체 탐색으로 전환")
        else:
            print(f"[SceneManager] SCALE_POD_PATH '{SCALE_POD_PATH}' 없음 — scene 전체 탐색으로 전환")

        root_prim = self._stage.GetPrimAtPath(SCENE_ROOT)
        if not root_prim or not root_prim.IsValid():
            return {}

        result = self._discover_topology_clustered(root_prim, "recursive")
        if result.get("racks"):
            return result

        result = self._discover_topology_flat(root_prim)
        if result.get("racks"):
            return result

        return self._discover_topology_from_api(root_prim)

    def _discover_topology_clustered(self, root_prim, mode: str) -> dict:
        """root_prim 하위에서 cluster/rack/box 구조를 재귀 탐색합니다."""
        clusters = []
        all_racks = []

        for cluster_prim in self._find_descendants(root_prim, self._is_cluster_prim):
            cluster_entry, rack_entries = self._build_cluster_entry(cluster_prim)
            clusters.append(cluster_entry)
            all_racks.extend(rack_entries)

        print(
            f"[SceneManager] topology ({mode}) 탐색 완료: "
            f"{len(clusters)}개 cluster, {len(all_racks)}개 rack"
        )
        result = {"clusters": clusters, "racks": all_racks}
        self._topology_cache = result
        return result

    def _discover_topology_flat(self, root_prim=None) -> dict:
        """Cluster가 없을 때 SCENE_ROOT 하위에서 Rack_ 구조를 재귀 탐색합니다."""
        if root_prim is None:
            root_prim = self._stage.GetPrimAtPath(SCENE_ROOT)
            if not root_prim or not root_prim.IsValid():
                return {}

        all_racks = []
        for rack_prim in self._find_descendants(root_prim, self._is_rack_prim):
            name = rack_prim.GetName()
            rack_path = str(rack_prim.GetPath())
            self._rack_paths[name] = rack_path

            servers = []
            for box_prim in self._find_descendants(rack_prim, self._is_box_prim):
                bn = box_prim.GetName()
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

    def _build_cluster_entry(self, cluster_prim):
        """Cluster prim 하나를 topology entry와 rack entry 목록으로 변환합니다."""
        cluster_name = cluster_prim.GetName()
        cluster_path = str(cluster_prim.GetPath())
        self._cluster_paths[cluster_name] = cluster_path

        racks = []
        all_racks = []
        for rack_prim in self._find_descendants(cluster_prim, self._is_rack_prim):
            rack_entry = self._build_rack_entry(cluster_name, rack_prim)
            racks.append(rack_entry)
            all_racks.append(rack_entry)

        cluster_entry = {
            "id":       cluster_name,
            "primPath": cluster_path,
            "label":    cluster_name.replace("_", "-"),
            "racks":    racks,
        }
        return cluster_entry, all_racks

    def _build_rack_entry(self, cluster_name: str, rack_prim) -> dict:
        """Rack prim 하나를 topology entry로 변환하고 인덱스를 등록합니다."""
        rack_name = rack_prim.GetName()
        rack_path = str(rack_prim.GetPath())
        self._rack_paths[f"{cluster_name}/{rack_name}"] = rack_path

        # Kafka lowercase alias: "datax/Rack_42U_A3" → prim_path
        cluster_alias = self._cluster_alias(cluster_name)
        rack_alias = f"{cluster_alias}/{rack_name}"
        if rack_alias not in self._rack_paths:
            self._rack_paths[rack_alias] = rack_path

        servers = []
        for box_prim in self._find_descendants(rack_prim, self._is_box_prim):
            box_name = box_prim.GetName()
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
            alias_key = f"{cluster_alias}/{box_name}"
            self._cluster_box_index.setdefault(alias_key, []).append(box_path)

            # 머티리얼 캐시 및 glass cube 생성 (_MaterialMixin에 위임)
            self._cache_node_material(box_path, box_prim)
            self._create_glass_cube(box_path, box_prim)
            # self.create_status_cylinders(box_path, box_prim)

        return {
            "id":        rack_name,
            "clusterId": cluster_name,
            "primPath":  rack_path,
            "label":     rack_name.replace("_", "-"),
            "nodes":     servers,
        }

    def _cluster_alias(self, cluster_name: str) -> str:
        """Kafka cluster id와 맞추기 위한 lowercase cluster 별칭."""
        alias = cluster_name.lower()
        suffix = CLUSTER_SUFFIX.lower()
        if alias.endswith(suffix):
            alias = alias[:-len(suffix)]
        return alias

    def _is_cluster_prim(self, prim) -> bool:
        return prim.GetName().endswith(CLUSTER_SUFFIX)

    def _is_rack_prim(self, prim) -> bool:
        name = prim.GetName()
        # Rack_Mount_* prims are internal rack asset parts, not topology rack containers.
        return name.startswith(_RACK_42U_PREFIX) or name in _EXPLICIT_RACK_NAMES

    def _is_box_prim(self, prim) -> bool:
        return prim.GetName().lower().startswith(BOX_PREFIX.lower())

    def _find_descendants(self, root_prim, predicate):
        """root_prim 하위 prim 중 predicate에 맞는 prim을 depth-first로 반환합니다."""
        matches = []
        stack = list(root_prim.GetChildren())
        while stack:
            prim = stack.pop(0)
            if predicate(prim):
                matches.append(prim)
            stack[0:0] = list(prim.GetChildren())
        return matches

    def _discover_topology_from_api(self, root_prim) -> dict:
        """Topology API 의 prim_name 을 기준으로 stage primPath 를 채우는 최후 fallback."""
        data = self._get_topology_api_data()
        if not data:
            return {}

        name_lookup = self._build_prim_name_lookup(root_prim)
        clusters = []
        all_racks = []
        missing_boxes = []

        for api_cluster in data.get("clusters", []):
            cluster_id = api_cluster.get("id")
            if not cluster_id:
                continue

            cluster_prim = self._first_named_prim(name_lookup, self._cluster_name_candidates(cluster_id))
            cluster_path = str(cluster_prim.GetPath()) if cluster_prim else SCENE_ROOT
            self._cluster_paths[cluster_id] = cluster_path

            racks = []
            for api_rack in api_cluster.get("racks", []):
                rack_id = api_rack.get("id")
                if not rack_id:
                    continue

                rack_prim = self._first_named_prim(name_lookup, [rack_id])
                rack_search_root = rack_prim or cluster_prim or root_prim
                servers = []
                first_box_prim = None

                for api_box in api_rack.get("boxes", []):
                    box_name = api_box.get("prim_name")
                    if not box_name:
                        continue
                    box_prim = self._find_descendant_by_name(rack_search_root, box_name)
                    if box_prim is None and rack_search_root is not root_prim:
                        box_prim = self._find_descendant_by_name(root_prim, box_name)
                    if box_prim is None:
                        missing_boxes.append(f"{cluster_id}/{rack_id}/{box_name}")
                        continue

                    if first_box_prim is None:
                        first_box_prim = box_prim
                    box_path = str(box_prim.GetPath())
                    servers.append({
                        "id":       box_name,
                        "primPath": box_path,
                        "label":    box_name.replace("_", "-"),
                    })
                    self._server_index[f"{cluster_id}/{rack_id}/{box_name}"] = box_path
                    self._cluster_box_index.setdefault(f"{cluster_id.lower()}/{box_name}", []).append(box_path)
                    self._cluster_box_index.setdefault(box_name, []).append(box_path)
                    self._cache_node_material(box_path, box_prim)
                    self._create_glass_cube(box_path, box_prim)

                if not servers and rack_prim is None:
                    continue

                rack_path = self._resolve_api_rack_path(rack_prim, first_box_prim, cluster_path)
                self._rack_paths[f"{cluster_id}/{rack_id}"] = rack_path
                self._rack_paths[f"{cluster_id.lower()}/{rack_id}"] = rack_path
                rack_entry = {
                    "id":        rack_id,
                    "clusterId": cluster_id,
                    "primPath":  rack_path,
                    "label":     rack_id.replace("_", "-"),
                    "nodes":     servers,
                }
                racks.append(rack_entry)
                all_racks.append(rack_entry)

            clusters.append({
                "id":       cluster_id,
                "primPath": cluster_path,
                "label":    cluster_id.replace("_Cluster", "").lower(),
                "racks":    racks,
            })

        if missing_boxes:
            preview = ", ".join(missing_boxes[:8])
            suffix = " ..." if len(missing_boxes) > 8 else ""
            print(f"[SceneManager] topology API prim_name 중 stage 미발견: {preview}{suffix}")

        print(
            f"[SceneManager] topology (api) 탐색 완료: "
            f"{len(clusters)}개 cluster, {len(all_racks)}개 rack"
        )
        result = {"clusters": clusters, "racks": all_racks}
        self._topology_cache = result
        return result

    def _get_topology_api_data(self):
        """SceneManager에 보관된 topology API 응답을 반환하고, 없으면 한 번만 fetch 합니다."""
        data = getattr(self, "_topology_api_data", None)
        if data is not None:
            return data
        if self._topology_api_fetch_attempted or not NODE_INDEX_URL:
            return None

        self._topology_api_fetch_attempted = True
        from .node_index import fetch_topology_response, parse_topology_response
        data = fetch_topology_response(NODE_INDEX_URL)
        if data is None:
            return None
        self._topology_api_data = data
        try:
            self._cluster_node_to_prim = parse_topology_response(data)
        except Exception as e:
            print(f"[SceneManager] topology API node index 파싱 실패: {e}")
        return data

    def _build_prim_name_lookup(self, root_prim) -> dict:
        """stage 하위 prim 을 이름별로 인덱싱합니다."""
        lookup = {}
        for prim in self._find_descendants(root_prim, lambda _prim: True):
            lookup.setdefault(prim.GetName(), []).append(prim)
        return lookup

    def _first_named_prim(self, name_lookup: dict, names: list[str]):
        """candidate 이름 목록에 해당하는 첫 prim 을 대소문자 fallback 포함해 찾습니다."""
        for name in names:
            matches = name_lookup.get(name)
            if matches:
                return matches[0]

        lowered = {name.lower() for name in names}
        for prim_name, matches in name_lookup.items():
            if prim_name.lower() in lowered and matches:
                return matches[0]
        return None

    def _cluster_name_candidates(self, cluster_id: str) -> list[str]:
        """API cluster id(datax/twinx 등)에서 가능한 USD cluster prim 이름 후보 생성."""
        base = str(cluster_id)
        variants = [base, base.lower(), base.upper(), base.capitalize()]
        if base.lower().endswith("x") and len(base) > 1:
            variants.append(base[:-1].capitalize() + "X")

        names = []
        for variant in variants:
            names.append(variant)
            names.append(f"{variant}{CLUSTER_SUFFIX}")
        return list(dict.fromkeys(names))

    def _find_descendant_by_name(self, root_prim, name: str):
        """root_prim 자신 또는 하위에서 이름이 일치하는 첫 prim 을 찾습니다."""
        if root_prim and root_prim.GetName() == name:
            return root_prim
        for prim in self._find_descendants(root_prim, lambda p: p.GetName() == name):
            return prim
        name_lower = name.lower()
        for prim in self._find_descendants(root_prim, lambda p: p.GetName().lower() == name_lower):
            return prim
        return None

    def _resolve_api_rack_path(self, rack_prim, first_box_prim, fallback_path: str) -> str:
        """API fallback 에서 rack primPath 를 최대한 실제 stage 경로로 보정합니다."""
        if rack_prim:
            return str(rack_prim.GetPath())
        if first_box_prim:
            parent = first_box_prim.GetParent()
            if parent and parent.IsValid():
                return str(parent.GetPath())
        return fallback_path

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
