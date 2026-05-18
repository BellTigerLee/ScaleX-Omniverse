"""
_MaterialMixin
노드 머티리얼 캐싱, Overlay Cube 생성, 상태별 색상 업데이트.

담당:
  - _cache_node_material()       : FrontPanel 셰이더 입력 캐시 (레거시 — 현재 미사용)
  - _create_glass_cube()         : BBox 크기에 맞춘 overlay Cube 동적 생성
  - _apply_node_status()         : Overlay Cube 색상 변경 (HEALTHY/WARNING/CRITICAL)
  - _reset_node_color()          : Overlay Cube 색상을 HEALTHY 기본으로 초기화
  - update_node_color_from_kafka(): Kafka 메시지로 노드 상태 색상 업데이트

교차 Mixin 의존:
  - create_alert_decal(), hide_alert_decal() → _AlertMixin
"""

import time

from pxr import Gf, Sdf, Usd, UsdGeom, UsdShade

from ..global_variables import (
    FRONT_PANEL_MATERIAL_KEYWORD,
    SHADER_KEYWORD,
    GLASS_CUBE_ENABLE_EMISSION,
    GLASS_CUBE_HEALTHY_COLOR,
    GLASS_CUBE_WARNING_COLOR,
    GLASS_CUBE_CRITICAL_COLOR,
    GLASS_CUBE_EMISSIVE_WARNING,
    GLASS_CUBE_EMISSIVE_CRITICAL,
    GLASS_CUBE_DISCONNECTED_COLOR,
    BOX_PREFIX,
)

_OVERLAY_BLACK = Gf.Vec3f(0.0, 0.0, 0.0)
_OVERLAY_DEFAULT_COLOR = Gf.Vec3f(*GLASS_CUBE_HEALTHY_COLOR)
_OVERLAY_HEALTH_BLINK_SEC = 1.0


class _MaterialMixin:
    """노드 머티리얼 및 overlay cube 상태 시각화 Mixin."""

    def _init_material(self):
        """머티리얼 관련 인스턴스 속성 초기화. SceneManager.__init__에서 호출."""
        # FrontPanel 셰이더 캐시 (레거시 — 현재 glass cube로 교체됨)
        # { node_path: {"enable": Input, "color": Input} }
        self._node_material_cache: dict[str, dict] = {}
        # Overlay Cube 셰이더 입력/가시성 캐시
        # { node_path: {"diffuse": Input, "emissive": Input, "imageable": Imageable} }
        self._glass_cube_cache: dict[str, dict] = {}
        # Cylinder 인디케이터 캐시 (미사용)
        # self._node_cylinders: dict[str, list] = {}

    def _init_node_state(self):
        """Node-state 캐시 초기화. SceneManager.__init__ 에서 호출."""
        # prim_path → status (5-enum 중 하나). 메시지 수신 전 노드는 키가 없음.
        self._node_status: dict[str, str] = {}
        # (cluster_lower, node_name) → prim_name — REST topology 에서 로드.
        # 비어있으면 _resolve_prim_path 가 BOX_ prefix 휴리스틱 fallback 을 사용.
        self._cluster_node_to_prim: dict = {}
        # prim_path → HEALTHY 수신 후 visible 로 유지할 시작 시각.
        # tick_pulse 가 짧은 visible 구간 뒤 다시 invisible 로 내린다.
        self._node_pulse_start: dict[str, float] = {}
        # Dev fake mapping: 미등록 node 를 topology prim 에 stable-hash 로 고정 배정.
        # 한번 배정된 노드는 extension 수명 동안 유지 (시각 안정성).
        self._dev_fake_mapping_enabled: bool = False
        self._dev_fake_assigned: dict[str, tuple] = {}   # node_name → (cluster_lower, prim_name)

    def set_dev_fake_mapping(self, enabled: bool) -> None:
        """Dev mode: topology 에 없는 node 이름을 topology prim 에 stable-hash 로 매핑."""
        self._dev_fake_mapping_enabled = enabled
        if enabled:
            print("[SceneManager] ⚠️  DEV_FAKE_NODE_MAPPING=true — 미등록 node 는 임의 prim 에 매핑됨")

    def _dev_fake_resolve(self, node: str):
        """
        미등록 node 를 topology 의 (cluster, prim_name) 쌍에 1:1 고정 배정.

        배정 규칙:
          - hash(node) 로 출발 인덱스 계산, 이미 다른 node 에 배정된 prim 이면
            다음 인덱스로 선형 탐색 (linear probing).
          - 따라서 어떤 두 node 도 같은 prim 을 공유하지 않는다 (1 node = 1 prim).
          - 캐시에 한 번 들어가면 extension 수명 동안 유지 → sticky.
          - topology prim 수를 초과하는 node 가 들어오면 더는 배정 못 하고 None 반환.
        """
        cached = self._dev_fake_assigned.get(node)
        if cached is not None:
            target_cluster, prim_name = cached
        else:
            candidates = sorted({(c, p) for (c, _), p in self._cluster_node_to_prim.items()})
            if not candidates:
                return None

            used = set(self._dev_fake_assigned.values())
            if len(used) >= len(candidates):
                print(
                    f"[SceneManager] DEV_FAKE_MAPPING: node={node} 배정 실패 — "
                    f"topology prim {len(candidates)}개 모두 사용 중"
                )
                return None

            n = len(candidates)
            start = hash(node) % n
            target = None
            for offset in range(n):
                pair = candidates[(start + offset) % n]
                if pair not in used:
                    target = pair
                    break
            if target is None:
                return None   # should not happen given the length check above

            target_cluster, prim_name = target
            self._dev_fake_assigned[node] = target
            print(
                f"[SceneManager] DEV_FAKE_MAPPING: node={node} → "
                f"{target_cluster}/{prim_name}  "
                f"(1:1 배정, {len(self._dev_fake_assigned)}/{n})"
            )

        paths = self._cluster_box_index.get(f"{target_cluster}/{prim_name}", []) \
             or self._cluster_box_index.get(prim_name, [])
        return paths[0] if paths else None

    def load_node_index_from_url(self, url: str) -> int:
        """
        URL 에서 토폴로지 JSON 을 받아 (cluster, node) → prim_name 인덱스를 채운다.
        실패 시 기존 인덱스를 유지. 로드된 매핑 개수를 반환.
        """
        from .node_index import fetch_topology_index
        index = fetch_topology_index(url)
        if index is None:
            print(f"[SceneManager] node index 로드 실패 — fallback 휴리스틱으로 동작 ({url})")
            return 0
        self._cluster_node_to_prim = index
        print(f"[SceneManager] node index 로드 완료: {len(index)}개 (cluster, node)→prim 매핑")
        return len(index)

    def _resolve_prim_path(self, cluster: str, node: str):
        """
        canonical envelope 의 (cluster, node) → USD prim path.

        Resolution 순서:
          1. Node index (REST topology) 가 로드돼 있으면 (cluster_lower, node) → prim_name 을
             얻고, _cluster_box_index[f"{cluster_lower}/{prim_name}"] 로 prim_path 획득.
          2. index 미로드 / 매칭 실패 시 BOX_ prefix 휴리스틱 fallback:
             candidate = [node, f"{BOX_PREFIX}{node}"] 을 세 단계(cluster-qualified /
             unqualified / server_index suffix) 로 순차 조회.
        """
        if not node:
            return None
        cluster_key = (cluster or "").lower() or None

        # 1. Node index 우선 (정확 매칭)
        if cluster_key and self._cluster_node_to_prim:
            prim_name = self._cluster_node_to_prim.get((cluster_key, node))
            if prim_name:
                paths = self._cluster_box_index.get(f"{cluster_key}/{prim_name}", [])
                if paths:
                    return paths[0]
                paths = self._cluster_box_index.get(prim_name, [])
                if paths:
                    return paths[0]
                # index 는 알고 있지만 USD 씬에 prim 이 없음 — 휴리스틱으로 떨어뜨리지 않고
                # 명확한 진단 로그 후 None (씬/토폴로지 불일치 증상).
                print(
                    f"[SceneManager] node-index hit 했으나 prim 미존재: "
                    f"cluster={cluster_key} node={node} prim_name={prim_name}"
                )
                return None

        # 2. Fallback 휴리스틱 (BOX_ prefix 보완)
        candidates = [node, f"{BOX_PREFIX}{node}"] if not node.startswith(BOX_PREFIX) else [node]

        if cluster_key:
            for cand in candidates:
                paths = self._cluster_box_index.get(f"{cluster_key}/{cand}", [])
                if paths:
                    return paths[0]

        for cand in candidates:
            paths = self._cluster_box_index.get(cand, [])
            if paths:
                return paths[0]

        for cand in candidates:
            suffix = f"/{cand}"
            for key, path in self._server_index.items():
                if key.endswith(suffix):
                    return path

        # 최후 수단: dev fake mapping (opt-in, topology 의 임의 prim 에 고정 배정)
        if self._dev_fake_mapping_enabled:
            return self._dev_fake_resolve(node)
        return None

    # ──────────────────────────────────────────────────────────────────────
    # FrontPanel 머티리얼 캐시 (레거시 — glass cube로 교체됨)
    # ──────────────────────────────────────────────────────────────────────

    def _cache_node_material(self, node_path: str, node_prim):
        """
        node의 FrontPanel 머티리얼 셰이더 입력을 미리 캐시합니다.

        [수정 포인트 - MATERIAL STRUCTURE]
        씬의 머티리얼 구조(Looks 폴더 위치, 셰이더 이름 등)에 맞게 수정하세요.
        """
        looks_path = node_prim.GetPath().AppendChild("Looks")
        looks_prim = self._stage.GetPrimAtPath(looks_path)
        if not looks_prim or not looks_prim.IsValid():
            return

        for mat_child in looks_prim.GetChildren():
            if FRONT_PANEL_MATERIAL_KEYWORD not in mat_child.GetName().lower():
                continue
            for shader_child in mat_child.GetChildren():
                if SHADER_KEYWORD not in shader_child.GetName().lower():
                    continue
                shader    = UsdShade.Shader(shader_child)
                enable_in = shader.CreateInput("enable_emission", Sdf.ValueTypeNames.Bool)
                color_in  = shader.CreateInput("emissive_color",  Sdf.ValueTypeNames.Color3f)
                self._node_material_cache[node_path] = {
                    "enable": enable_in,
                    "color":  color_in,
                }
                print(f"[SceneManager] 머티리얼 캐시: {node_path}")
                return

    # ──────────────────────────────────────────────────────────────────────
    # Overlay Cube 생성
    # ──────────────────────────────────────────────────────────────────────

    def _create_glass_cube(self, node_path: str, node_prim):
        """
        node_prim의 BBox 크기에 맞춘 overlay cube를 생성합니다.

        생성 경로: {node_path}/GlassOverlay/GlassCube
        생성된 cube에는 UsdPreviewSurface material을 할당합니다.
        """
        if not self._stage:
            return

        scope_path = f"{node_path}/GlassOverlay"
        cube_path  = f"{scope_path}/GlassCube"

        # 기존 GlassOverlay 는 material 입력 이름이 다를 수 있어 재사용하지 않는다.
        existing = self._stage.GetPrimAtPath(cube_path)
        if existing and existing.IsValid():
            self._stage.RemovePrim(scope_path)

        # ── BBox 크기 계산 (로컬) ──
        bbox_cache  = UsdGeom.BBoxCache(Usd.TimeCode.Default(), ["default", "render"])
        local_bound = bbox_cache.ComputeLocalBound(node_prim)
        rng         = local_bound.GetRange()
        bbox_min, bbox_max = rng.GetMin(), rng.GetMax()

        sx = float(bbox_max[0] - bbox_min[0]) + 0.1
        sy = float(bbox_max[1] - bbox_min[1]) + 0.1
        sz = float(bbox_max[2] - bbox_min[2]) + 0.1

        if max(sx, sy, sz) < 1e-3:
            print(f"[SceneManager] overlay cube 스킵 (BBox 너무 작음): {node_path}")
            return

        cx = float(bbox_min[0] + bbox_max[0]) * 0.5
        cy = float(bbox_min[1] + bbox_max[1]) * 0.5
        cz = float(bbox_min[2] + bbox_max[2]) * 0.5

        # ── Scope 및 Cube 생성 ──
        UsdGeom.Scope.Define(self._stage, scope_path)
        cube = UsdGeom.Cube.Define(self._stage, cube_path)
        cube.GetSizeAttr().Set(1.0)
        xformable = UsdGeom.Xformable(cube)
        xformable.AddTranslateOp().Set(Gf.Vec3d(cx, cy, cz))
        scale_op = xformable.AddScaleOp()
        scale = Gf.Vec3f(sx, sy, sz)
        scale_op.Set(scale)

        mat_path    = f"{cube_path}/OverlayMaterial"
        shader_path = f"{mat_path}/OverlayShader"
        mat    = UsdShade.Material.Define(self._stage, mat_path)
        shader = UsdShade.Shader.Define(self._stage, shader_path)
        shader.CreateIdAttr("UsdPreviewSurface")
        diffuse_in  = shader.CreateInput("diffuseColor",  Sdf.ValueTypeNames.Color3f)
        emissive_in = shader.CreateInput("emissiveColor", Sdf.ValueTypeNames.Color3f)
        opacity_in  = shader.CreateInput("opacity",       Sdf.ValueTypeNames.Float)
        shader.CreateInput("opacityThreshold", Sdf.ValueTypeNames.Float).Set(0.0)
        shader.CreateInput("ior", Sdf.ValueTypeNames.Float).Set(1.0)
        shader.CreateInput("roughness", Sdf.ValueTypeNames.Float).Set(0.65)
        shader.CreateInput("metallic",  Sdf.ValueTypeNames.Float).Set(0.0)
        diffuse_in.Set(_OVERLAY_DEFAULT_COLOR)
        emissive_in.Set(Gf.Vec3f(0.0, 0.0, 0.0))
        opacity_in.Set(0.0)
        mat.CreateSurfaceOutput().ConnectToSource(shader.ConnectableAPI(), "surface")
        UsdShade.MaterialBindingAPI.Apply(cube.GetPrim()).Bind(mat)

        imageable = UsdGeom.Imageable(cube.GetPrim())
        imageable.MakeInvisible()

        self._glass_cube_cache[node_path] = {
            "diffuse": diffuse_in,
            "emissive": emissive_in,
            "opacity": opacity_in,
            "imageable": imageable,
            "scale_op": scale_op,
            "scale": scale,
        }
        print(f"[SceneManager] material cube 생성: {cube_path} ({sx:.2f}×{sy:.2f}×{sz:.2f})")

    def _show_glass_cube(self, handles: dict) -> None:
        """Overlay cube를 visible + opacity=1로 보이게 한다."""
        diffuse = handles.get("diffuse")
        if diffuse:
            diffuse.Set(_OVERLAY_DEFAULT_COLOR)
        emissive = handles.get("emissive")
        if emissive:
            emissive.Set(_OVERLAY_BLACK)
        scale_op = handles.get("scale_op")
        scale = handles.get("scale")
        if scale_op and scale is not None:
            scale_op.Set(scale)
        opacity = handles.get("opacity")
        if opacity:
            opacity.Set(1.0)
        handles["imageable"].MakeVisible()

    def _hide_glass_cube(self, handles: dict) -> None:
        """Overlay cube를 invisible + opacity=0으로 숨긴다."""
        emissive = handles.get("emissive")
        if emissive:
            emissive.Set(_OVERLAY_BLACK)
        opacity = handles.get("opacity")
        if opacity:
            opacity.Set(0.0)
        handles["imageable"].MakeInvisible()

    # ──────────────────────────────────────────────────────────────────────
    # Overlay Cube 색상 업데이트
    # ──────────────────────────────────────────────────────────────────────

    def _apply_node_status(self, node_path: str, status: str):
        """
        status="HEALTHY"  → overlay cube HEALTHY 색상 (녹색, emissive OFF)
        status="WARNING"  → overlay cube WARNING 색상 (주황, 약한 emissive)
        status="CRITICAL" → overlay cube CRITICAL 색상 (빨강, 강한 emissive)
        """
        handles = self._glass_cube_cache.get(node_path)
        if not handles:
            prim = self._stage.GetPrimAtPath(node_path)
            if prim and prim.IsValid():
                self._create_glass_cube(node_path, prim)
            handles = self._glass_cube_cache.get(node_path)

        if not handles:
            print(f"[SceneManager]   ❌ overlay cube 캐시 없음: {node_path}")
            return

        diffuse = handles.get("diffuse")
        emissive = handles.get("emissive")
        if not diffuse or not emissive:
            self._show_glass_cube(handles)
            return

        # print(f"[SceneManager]   ✅ overlay cube 색상: {node_path.split('/')[-1]} status={status}")
        if status == "WARNING":
            diffuse.Set(Gf.Vec3f(*GLASS_CUBE_WARNING_COLOR))
            if GLASS_CUBE_ENABLE_EMISSION:
                emissive.Set(Gf.Vec3f(
                    GLASS_CUBE_WARNING_COLOR[0] * GLASS_CUBE_EMISSIVE_WARNING,
                    GLASS_CUBE_WARNING_COLOR[1] * GLASS_CUBE_EMISSIVE_WARNING,
                    GLASS_CUBE_WARNING_COLOR[2] * GLASS_CUBE_EMISSIVE_WARNING,
                ))
            else:
                emissive.Set(Gf.Vec3f(0.0, 0.0, 0.0))
        elif status == "CRITICAL":
            diffuse.Set(Gf.Vec3f(*GLASS_CUBE_CRITICAL_COLOR))
            if GLASS_CUBE_ENABLE_EMISSION:
                emissive.Set(Gf.Vec3f(
                    GLASS_CUBE_CRITICAL_COLOR[0] * GLASS_CUBE_EMISSIVE_CRITICAL,
                    GLASS_CUBE_CRITICAL_COLOR[1] * GLASS_CUBE_EMISSIVE_CRITICAL,
                    GLASS_CUBE_CRITICAL_COLOR[2] * GLASS_CUBE_EMISSIVE_CRITICAL,
                ))
            else:
                emissive.Set(Gf.Vec3f(0.0, 0.0, 0.0))
        else:  # HEALTHY or unknown
            diffuse.Set(Gf.Vec3f(*GLASS_CUBE_HEALTHY_COLOR))
            emissive.Set(Gf.Vec3f(0.0, 0.0, 0.0))

        # self.update_node_cylinder_status(node_path, status)

    def _reset_node_color(self, node_path: str):
        """node 색상을 기본 상태(HEALTHY)로 초기화합니다."""
        self._apply_node_status(node_path, "HEALTHY")

    def _apply_diffuse_for_status(self, node_path: str, status: str) -> None:
        """
        status 변화 시 1회만 호출되어 diffuse 색만 교체.
        emissive 는 tick_pulse 가 매 프레임 관리하므로 여기서 건드리지 않는다.
        UNKNOWN / WARNING / CRITICAL 은 no-op (Phase 1 범위 밖).
        """
        handles = self._glass_cube_cache.get(node_path)
        if not handles:
            prim = self._stage.GetPrimAtPath(node_path)
            if prim and prim.IsValid():
                self._create_glass_cube(node_path, prim)
            handles = self._glass_cube_cache.get(node_path)
        if not handles:
            return

        diffuse = handles.get("diffuse")
        if not diffuse:
            self._hide_glass_cube(handles)
            return

        if status == "HEALTHY":
            diffuse.Set(Gf.Vec3f(*GLASS_CUBE_HEALTHY_COLOR))
        elif status == "DISCONNECTED":
            diffuse.Set(Gf.Vec3f(*GLASS_CUBE_DISCONNECTED_COLOR))
        # 그 외 status 는 diffuse 건드리지 않음

    def apply_node_state(self, envelope: dict) -> None:
        """
        canonical node-state envelope 1개를 소비.
        _server_index / _cluster_box_index 에 없는 노드는 경고 로그 + 드롭.

        HEALTHY 수신 시 overlay cube 를 잠깐 visible 로 올리고,
        tick_pulse() 에서 다시 invisible 로 내린다. 그 외 status 는 항상 invisible.
        이 경로에서는 색상 pulse/emissive 를 쓰지 않는다.
        """
        if not self._stage:
            return

        cluster = envelope.get("cluster", "")
        node    = envelope.get("node", "")
        status  = envelope.get("status", "")

        prim_path = self._resolve_prim_path(cluster, node)
        if prim_path is None:
            print(
                f"[SceneManager] ⚠️ node-state 수신했으나 미등록 노드: "
                f"cluster={cluster} node={node} status={status} (드롭)"
            )
            return

        prev = self._node_status.get(prim_path)
        self._node_status[prim_path] = status

        if prev != status:
            ps = envelope.get("previous_status")
            print(
                f"[SceneManager] node-state: {cluster}/{node} "
                f"{prev} → {status} (envelope previous_status={ps})"
            )

        handles = self._glass_cube_cache.get(prim_path)
        if not handles:
            prim = self._stage.GetPrimAtPath(prim_path)
            if prim and prim.IsValid():
                self._create_glass_cube(prim_path, prim)
            handles = self._glass_cube_cache.get(prim_path)

        self._node_pulse_start.pop(prim_path, None)
        if not handles:
            return

        if status == "HEALTHY":
            self._show_glass_cube(handles)
            self._node_pulse_start[prim_path] = time.monotonic()
        else:
            self._node_pulse_start.pop(prim_path, None)
            self._hide_glass_cube(handles)

    def tick_pulse(self, now_sec: float) -> None:
        """
        HEALTHY 수신으로 잠깐 visible 된 overlay cube 를 다시 invisible 로 내린다.
        이 경로에서는 색상 pulse/emissive 를 쓰지 않는다.
        """
        expired: list[str] = []
        for node_path, start_sec in self._node_pulse_start.items():
            if now_sec - start_sec < _OVERLAY_HEALTH_BLINK_SEC:
                continue

            handles = self._glass_cube_cache.get(node_path)
            if handles:
                self._hide_glass_cube(handles)
            expired.append(node_path)

        for node_path in expired:
            self._node_pulse_start.pop(node_path, None)

        # Baseline policy: active HEALTHY blink 외에는 overlay cube 가 남아 있지 않아야 한다.
        for node_path, handles in self._glass_cube_cache.items():
            if node_path not in self._node_pulse_start:
                self._hide_glass_cube(handles)

    # ──────────────────────────────────────────────────────────────────────
    # Kafka 기반 색상 업데이트
    # ──────────────────────────────────────────────────────────────────────

    def update_node_color_from_kafka(self, kafka_msg: dict, view_state: dict):
        """
        Kafka 메시지의 status 필드에 따라 glass cube 색상을 업데이트합니다.

        HEALTHY  → 기본 청색
        WARNING  → 주황 (약한 emissive)
        CRITICAL → 빨강 (강한 emissive)

        구 포맷(error bool)은 CRITICAL/HEALTHY로 변환하여 처리합니다.
        """
        if not self._stage:
            return

        cluster_id = (kafka_msg.get("cluster") or kafka_msg.get("cluster_id") or "").lower() or None
        rack_id    = kafka_msg.get("rack_id")
        server_id  = (
            kafka_msg.get("node") or kafka_msg.get("box_id")
            or kafka_msg.get("server_id") or kafka_msg.get("node_id")
        )
        _status = (
            kafka_msg.get("status", "")
            or ("CRITICAL" if kafka_msg.get("error") else "HEALTHY")
        )

        if not cluster_id and not rack_id:
            return

        # print(
        #     f"[SceneManager] color_from_kafka: cluster={cluster_id} node={server_id} "
        #     f"status={_status} stage={view_state['stage']}"
        # )

        # rack prim path 조회
        rack_path = None
        if rack_id:
            rack_path = (
                self._rack_paths.get(f"{cluster_id}/{rack_id}") if cluster_id
                else next(
                    (p for k, p in self._rack_paths.items() if k.endswith(f"/{rack_id}")),
                    None,
                )
            )

        # node prim paths 조회
        def _find_node_paths() -> list:
            if not server_id:
                return []
            if rack_id and cluster_id:
                p = self._server_index.get(f"{cluster_id}/{rack_id}/{server_id}")
                return [p] if p else []
            if cluster_id:
                return self._cluster_box_index.get(f"{cluster_id}/{server_id}", [])
            return self._cluster_box_index.get(server_id, [])

        # Alert Decal (Stage C/D)
        if rack_path and view_state["stage"] in ("C", "D"):
            if _status in ("WARNING", "CRITICAL"):
                self.create_alert_decal(rack_path)
            else:
                self.hide_alert_decal(rack_path)

        # legacy: status 적용 경로 — superseded by SceneManager.apply_node_state
        #         via node-state.events → tick_pulse (2026-04-17).
        #         dashboard 토픽의 status 는 Flink placeholder 이므로 더 이상 색상 결정에 쓰지 않는다.
        # if server_id:
        #     for node_path in _find_node_paths():
        #         if view_state["stage"] == "D":
        #             self._reset_node_color(node_path=node_path)
        #             continue
        #         self._apply_node_status(node_path, _status)

    # ──────────────────────────────────────────────────────────────────────
    # 상태 Cylinder 인디케이터 (미사용 — 필요 시 주석 해제)
    # ──────────────────────────────────────────────────────────────────────

    # Cylinder 배치 상수 (노드 로컬 좌표)
    _CYL_X      =  -0.24107
    _CYL_Y      =  -0.4377
    _CYL_Z_BASE =   3.60359
    _CYL_Z_STEP =   0.7
    _CYL_RADIUS =   0.08
    _CYL_HEIGHT =   0.25
    _CYL_ROT_X  =   0.0
    _CYL_ROT_Y  =  90.0
    _CYL_ROT_Z  =   0.0

    _CYL_COLORS_ACTIVE = [
        (0.0,    500.0,  0.0),   # HEALTHY  : 초록
        (500.0,  250.0,  0.0),   # WARNING  : 주황
        (3000.0, 0.0,    0.0),   # CRITICAL : 빨강
    ]
    _CYL_COLOR_DIM   = (0.05, 0.05, 0.05)
    _CYL_STATUS_LIST = ["HEALTHY", "WARNING", "CRITICAL"]

    def create_status_cylinders(self, node_path: str, node_prim):
        """(미사용) node_prim 아래 신호등식 Cylinder 3개를 생성합니다."""
        if not self._stage:
            return
        scope_path = f"{node_path}/StatusIndicators"
        if self._stage.GetPrimAtPath(scope_path).IsValid():
            return

        from pxr import Sdf as _Sdf
        UsdGeom.Scope.Define(self._stage, scope_path)
        handles = []
        _names  = ["Healthy", "Warning", "Critical"]
        for i, status_name in enumerate(self._CYL_STATUS_LIST):
            cyl_path = f"{scope_path}/_State_{_names[i]}"
            z_pos    = self._CYL_Z_BASE + i * self._CYL_Z_STEP
            cyl = UsdGeom.Cylinder.Define(self._stage, cyl_path)
            cyl.GetRadiusAttr().Set(self._CYL_RADIUS)
            cyl.GetHeightAttr().Set(self._CYL_HEIGHT)
            xform = UsdGeom.Xformable(cyl)
            xform.AddTranslateOp().Set(Gf.Vec3d(self._CYL_X, self._CYL_Y, z_pos))
            xform.AddRotateXOp().Set(self._CYL_ROT_X)
            xform.AddRotateYOp().Set(self._CYL_ROT_Y)
            xform.AddRotateZOp().Set(self._CYL_ROT_Z)
            mat_path    = f"{cyl_path}/Mat"
            shader_path = f"{mat_path}/Shader"
            mat    = UsdShade.Material.Define(self._stage, mat_path)
            shader = UsdShade.Shader.Define(self._stage, shader_path)
            shader.CreateIdAttr("UsdPreviewSurface")
            shader.CreateInput("roughness", _Sdf.ValueTypeNames.Float).Set(0.5)
            shader.CreateInput("metallic",  _Sdf.ValueTypeNames.Float).Set(0.0)
            enable_in = shader.CreateInput("enable_emission", _Sdf.ValueTypeNames.Bool)
            color_in  = shader.CreateInput("emissive_color",  _Sdf.ValueTypeNames.Color3f)
            mat.CreateSurfaceOutput().ConnectToSource(shader.ConnectableAPI(), "surface")
            UsdShade.MaterialBindingAPI.Apply(cyl.GetPrim()).Bind(mat)
            enable_in.Set(False)
            color_in.Set(Gf.Vec3f(*self._CYL_COLOR_DIM))
            handles.append({"enable": enable_in, "color": color_in})
        self._node_cylinders[node_path] = handles

    def update_node_cylinder_status(self, node_path: str, status: str):
        """(미사용) Cylinder 인디케이터를 status에 맞게 업데이트합니다."""
        handles    = getattr(self, "_node_cylinders", {}).get(node_path)
        if not handles:
            return
        idx_map    = {"HEALTHY": 0, "WARNING": 1, "CRITICAL": 2}
        active_idx = idx_map.get((status or "").upper(), 0)
        for i, h in enumerate(handles):
            if i == active_idx:
                h["enable"].Set(True)
                h["color"].Set(Gf.Vec3f(*self._CYL_COLORS_ACTIVE[i]))
            else:
                h["enable"].Set(False)
                h["color"].Set(Gf.Vec3f(*self._CYL_COLOR_DIM))
