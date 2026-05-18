"""
_NodeVisibilityMixin
Cluster/Rack/Node 가시성 제어 및 Stage 전환 (A↔B↔C↔D).

담당:
  - cluster_focus()              : Stage A → B (다른 Cluster 숨김)
  - rack_focus()                 : Stage B → C (다른 Rack 숨김 + 카메라 줌인 + rack node 캐시)
  - node_inspect()               : Stage C → D (Node pop-forward + 형제 노드 dim)
  - node_deselect()              : Stage D → C (pop 복원 + 형제 노드 opacity 복원 + Rack 카메라 복귀)
  - rack_deselect_to_cluster()   : Stage C → B (색상/decal 초기화 + overview 복귀)
  - _cache_rack_nodes()          : Stage C 진입 시 rack node path · MDL opacity 핸들 캐시
  - _set_rack_siblings_opacity() : 선택 노드를 제외한 rack + 형제 노드 dim 일괄 적용
  - _restore_rack_nodes_opacity(): rack + 모든 노드 opacity를 캐시된 원래 값으로 복원
  - _get_looks_shader_input()    : Looks/ 아래 재질 이름 키워드로 셰이더 Input 탐색 헬퍼
  - _set_other_clusters_visibility()
  - _set_other_racks_visibility()

교차 Mixin 의존:
  - _compute_rack_cam(), _start_camera_animation() → _CameraControllerMixin
  - _reset_node_color()                            → _MaterialMixin
  - hide_alert_decal()                             → _AlertMixin

MDL opacity 파라미터:
  - Rack   : Darker_Chassis_Metal  → opacity_val (float, dim=NODE_DIM_CHASSIS_OPACITY)
  - Node   : Custom_Chassis_Metal  → opacity_val (float, dim=NODE_DIM_CHASSIS_OPACITY)
  - Node   : FrontPanelMaterial    → opacity_mode (int,   dim=NODE_DIM_FRONTPANEL_MODE)
  복원값은 rack_focus 시 원래 값을 읽어 캐시에 보관.
"""

import omni.usd
from pxr import Gf, Sdf, UsdGeom, UsdShade

from ..global_variables import (
    CAMERA_OVERVIEW_POSITION,
    CAMERA_OVERVIEW_TARGET,
    FRONT_PANEL_MATERIAL_KEYWORD,
    NODE_DIM_CHASSIS_OPACITY,
    NODE_DIM_FRONTPANEL_MODE,
    NODE_POP_DISTANCE,
)


class _NodeVisibilityMixin:
    """Cluster/Rack/Node 가시성 및 Stage 전환 Mixin."""

    def _init_node_visibility(self):
        """Node visibility 관련 인스턴스 속성 초기화. SceneManager.__init__에서 호출."""
        # { full_server_prim_path: Gf.Vec3d(original) }  — pop-forward 복원용
        self._node_original_translate: dict[str, Gf.Vec3d] = {}

        # Stage C 캐시 ── rack_focus 시 한 번만 구축, node_inspect/deselect에서 재사용
        # 현재 포커스된 rack 내 node prim path 목록
        self._rack_node_paths_cache: list[str] = []

        # { node_path: {"chassis": (UsdShade.Input, orig_val),
        #               "frontpanel": (UsdShade.Input, orig_val) | None} }
        self._node_dim_handles: dict[str, dict] = {}

        # Rack Darker_Chassis_Metal opacity_val 핸들 + 원래 값
        # (UsdShade.Input, orig_float) | None
        self._rack_chassis_handle: tuple | None = None

        # rack 내 모든 node의 GlassCube UsdGeom.Imageable 캐시
        # C→D 시 MakeInvisible, D→C 시 MakeVisible
        self._rack_glass_imageable_cache: list = []

        # 투명화 모드 플래그 (True = node_inspect 시 sibling dim 적용)
        self._transparency_enabled: bool = True
        # 현재 Stage D의 선택 노드 path (None이면 Stage D 아님)
        self._current_inspected_node: str | None = None

    # ──────────────────────────────────────────────────────────────────────
    # Stage A → B : Cluster 포커스
    # ──────────────────────────────────────────────────────────────────────

    def cluster_focus(self, cluster_prim_path: str, hide_others: bool = True):
        """
        선택된 cluster를 포커스하고 다른 cluster를 숨깁니다.
        카메라는 현재 위치 유지 (Stage B: overview 시점에서 cluster 확인).
        """
        if not self._stage:
            return
        if hide_others:
            self._set_other_clusters_visibility(cluster_prim_path, visible=False)
        omni.usd.get_context().get_selection().clear_selected_prim_paths()
        print(f"[SceneManager] cluster_focus: {cluster_prim_path}")

    def _set_other_clusters_visibility(self, selected_cluster_path: str, visible: bool):
        """선택된 cluster를 제외한 나머지의 visibility를 설정합니다."""
        for cluster_path in self._cluster_paths.values():
            prim = self._stage.GetPrimAtPath(cluster_path)
            if not prim or not prim.IsValid():
                continue
            if cluster_path == selected_cluster_path:
                UsdGeom.Imageable(prim).MakeVisible()
            else:
                if visible:
                    UsdGeom.Imageable(prim).MakeVisible()
                else:
                    UsdGeom.Imageable(prim).MakeInvisible()

    # ──────────────────────────────────────────────────────────────────────
    # Stage B → C : Rack 포커스
    # ──────────────────────────────────────────────────────────────────────

    def rack_focus(self, rack_prim_path: str, hide_others: bool = True):
        """
        선택된 rack에 카메라를 줌인하고 나머지 rack을 숨깁니다.
        smoothstep 애니메이션으로 카메라 이동.
        Stage C 진입 시 rack 내 node path · MDL opacity 핸들을 캐시합니다.
        """
        if not self._stage:
            return
        if hide_others:
            self._set_other_racks_visibility(rack_prim_path, visible=False)

        # Stage C 캐시 구축 ── 이후 node_inspect / node_deselect 에서 재사용
        self._cache_rack_nodes(rack_prim_path)

        cam_pos, cam_target = self._compute_rack_cam(rack_prim_path)
        self._start_camera_animation(cam_pos, cam_target)
        omni.usd.get_context().get_selection().clear_selected_prim_paths()
        print(f"[SceneManager] rack_focus: {rack_prim_path} → cam {cam_pos}")

    def _set_other_racks_visibility(self, selected_rack_path: str, visible: bool):
        """선택된 rack을 제외한 나머지 rack의 visibility를 설정합니다."""
        for rack_id, rack_path in self._rack_paths.items():
            prim = self._stage.GetPrimAtPath(rack_path)
            if not prim or not prim.IsValid():
                continue
            if rack_path == selected_rack_path:
                UsdGeom.Imageable(prim).MakeVisible()
            else:
                if visible:
                    UsdGeom.Imageable(prim).MakeVisible()
                else:
                    UsdGeom.Imageable(prim).MakeInvisible()

    # ──────────────────────────────────────────────────────────────────────
    # MDL 셰이더 Input 탐색 헬퍼
    # ──────────────────────────────────────────────────────────────────────

    def _get_looks_shader_input(
        self,
        prim_path: str,
        mat_keyword: str,
        input_name: str,
        sdf_type: Sdf.ValueTypeName,
    ):
        """
        {prim_path}/Looks/ 자식 Material 중 이름에 mat_keyword(대소문자 무시)가 포함된
        Material의 자식 Shader에서 input_name Input을 반환합니다.

        MDL 파라미터가 USD 레이어에 아직 authored 되지 않은 경우
        (GetInput() valid: False / GetInputs() 빈 리스트) CreateInput()으로
        직접 authoring해 Set()이 동작하도록 합니다.

        Args:
            prim_path   : 탐색 기준 prim의 USD path 문자열
            mat_keyword : Material 이름 필터 키워드 (ex. "chassis", "frontpanel")
            input_name  : 셰이더 Input 이름 (ex. "opacity_val", "opacity_mode")
            sdf_type    : Input 타입 (ex. Sdf.ValueTypeNames.Float / .Int)
        """
        looks_prim = self._stage.GetPrimAtPath(f"{prim_path}/Looks")
        if not looks_prim or not looks_prim.IsValid():
            return None

        keyword_lower = mat_keyword.lower()
        for mat_child in looks_prim.GetChildren():
            if keyword_lower not in mat_child.GetName().lower():
                continue
            for shader_child in mat_child.GetChildren():
                shader = UsdShade.Shader(shader_child)

                # GetInput은 이미 authored된 경우에만 valid를 반환
                inp = shader.GetInput(input_name)
                if inp:
                    return inp

                # USD 레이어에 한 번도 Set된 적 없는 MDL 파라미터 →
                # CreateInput으로 직접 authoring해야 Set()이 동작함
                inp = shader.CreateInput(input_name, sdf_type)
                if inp:
                    return inp
        return None

    # ──────────────────────────────────────────────────────────────────────
    # Stage C 캐시 구축
    # ──────────────────────────────────────────────────────────────────────

    def _cache_rack_nodes(self, rack_prim_path: str):
        """
        Stage C 진입(rack_focus) 시 한 번 호출.
        rack prim과 rack 내 모든 node의 MDL opacity 셰이더 Input 핸들과
        현재(원래) 값을 캐시합니다.

        이후 node_inspect(C→D) / node_deselect(D→C)에서는 USD path 재탐색 없이
        캐시된 핸들에 .Set()만 호출하므로 매우 빠릅니다.
        """
        # 기존 캐시 전부 초기화
        self._rack_node_paths_cache       = []
        self._node_dim_handles            = {}
        self._rack_chassis_handle         = None
        self._rack_glass_imageable_cache  = []

        # rack_prim_path → rack_key 역방향 조회
        # _rack_paths: { "ClusterName/RackName": full_rack_prim_path }
        rack_key = next(
            (k for k, v in self._rack_paths.items() if v == rack_prim_path), None
        )
        if rack_key is None:
            print(f"[SceneManager] _cache_rack_nodes: rack_key 없음 → {rack_prim_path}")
            return

        # ── Rack 자체 Darker_Chassis_Metal opacity_val 핸들 캐시 ──
        rack_inp = self._get_looks_shader_input(
            rack_prim_path, "chassis", "opacity_val", Sdf.ValueTypeNames.Float
        )
        if rack_inp:
            orig = rack_inp.Get()
            self._rack_chassis_handle = (rack_inp, orig if orig is not None else 1.0)
        else:
            print(f"[SceneManager] _cache_rack_nodes: rack chassis 핸들 없음 → {rack_prim_path}")

        # ── rack 내 모든 node path 수집 ──
        prefix     = rack_key + "/"
        node_paths = [
            path for key, path in self._server_index.items()
            if key.startswith(prefix)
        ]
        self._rack_node_paths_cache = node_paths

        # ── 각 node의 Custom_Chassis_Metal / FrontPanel 핸들 캐시 ──
        cached = 0
        for node_path in node_paths:
            # Custom_Chassis_Metal → opacity_val
            chassis_inp = self._get_looks_shader_input(
                node_path, "chassis", "opacity_val", Sdf.ValueTypeNames.Float
            )
            if chassis_inp is None:
                # 핸들을 찾지 못한 노드는 dim 대상에서 제외
                continue
            chassis_orig = chassis_inp.Get()

            # FrontPanelMaterial → opacity_constant  (FRONT_PANEL_MATERIAL_KEYWORD 재사용)
            fp_inp = self._get_looks_shader_input(
                node_path, FRONT_PANEL_MATERIAL_KEYWORD, "opacity_constant",
                Sdf.ValueTypeNames.Float,
            )
            fp_pair = None
            if fp_inp:
                fp_orig = fp_inp.Get()
                fp_pair = (fp_inp, fp_orig if fp_orig is not None else 1.0)

            # FrontPanelMaterial → enable_opacity_texture (bool)
            fp_enable_inp = self._get_looks_shader_input(
                node_path, FRONT_PANEL_MATERIAL_KEYWORD, "enable_opacity_texture",
                Sdf.ValueTypeNames.Bool,
            )
            fp_enable_pair = None
            if fp_enable_inp:
                fp_enable_orig = fp_enable_inp.Get()
                fp_enable_pair = (
                    fp_enable_inp,
                    fp_enable_orig if fp_enable_orig is not None else True,
                )

            self._node_dim_handles[node_path] = {
                "chassis":    (chassis_inp, chassis_orig if chassis_orig is not None else 1.0),
                "frontpanel": fp_pair,
                "fp_enable":  fp_enable_pair,
            }
            cached += 1

            # GlassCube Imageable 캐시 — 경로: {node_path}/GlassOverlay/GlassCube
            glass_prim = self._stage.GetPrimAtPath(f"{node_path}/GlassOverlay/GlassCube")
            if glass_prim and glass_prim.IsValid():
                self._rack_glass_imageable_cache.append(UsdGeom.Imageable(glass_prim))

        print(
            f"[SceneManager] _cache_rack_nodes: {rack_key} — "
            f"rack={'OK' if self._rack_chassis_handle else 'MISS'}, "
            f"nodes={cached}/{len(node_paths)}, "
            f"glass={len(self._rack_glass_imageable_cache)}"
        )

    # ──────────────────────────────────────────────────────────────────────
    # Stage C → D : Rack + sibling dim / D → C : opacity 복원
    # ──────────────────────────────────────────────────────────────────────

    def _set_rack_siblings_opacity(self, selected_node_path: str):
        """
        선택된 노드를 제외한 Rack prim과 rack 내 모든 sibling 노드를 dim 처리하고,
        모든 노드의 GlassCube를 invisible로 설정합니다.

        Rack   : Darker_Chassis_Metal  opacity_val  → NODE_DIM_CHASSIS_OPACITY
        Node   : Custom_Chassis_Metal  opacity_val  → NODE_DIM_CHASSIS_OPACITY
        Node   : FrontPanelMaterial    opacity_mode → NODE_DIM_FRONTPANEL_MODE
        Node   : GlassOverlay/GlassCube             → MakeInvisible (전체)
        """
        # Rack dim
        if self._rack_chassis_handle:
            self._rack_chassis_handle[0].Set(NODE_DIM_CHASSIS_OPACITY)

        # Sibling node dim (선택 노드 제외)
        for node_path, handles in self._node_dim_handles.items():
            if node_path == selected_node_path:
                continue
            handles["chassis"][0].Set(NODE_DIM_CHASSIS_OPACITY)
            if handles["frontpanel"]:
                handles["frontpanel"][0].Set(NODE_DIM_FRONTPANEL_MODE)
            if handles["fp_enable"]:
                handles["fp_enable"][0].Set(False)

        # 모든 node GlassCube invisible (선택 노드 포함 — Stage D 에서는 불필요한 오버레이 제거)
        for node_path in self._rack_node_paths_cache:
            handles = self._glass_cube_cache.get(node_path)
            if handles:
                self._hide_glass_cube(handles)

        dim_count = sum(
            1 for p in self._node_dim_handles if p != selected_node_path
        )
        print(
            f"[SceneManager] _set_rack_siblings_opacity: "
            f"rack + {dim_count} nodes → dim, "
            f"{len(self._rack_glass_imageable_cache)} GlassCubes → invisible"
        )

    def _restore_rack_nodes_opacity(self):
        """
        Rack prim과 rack 내 모든 노드의 opacity를 캐시된 원래 값으로 복원하고,
        모든 노드의 GlassCube는 baseline 상태(invisible)로 유지합니다.
        캐시가 비어있으면 no-op.
        """
        # Rack 복원
        if self._rack_chassis_handle:
            self._rack_chassis_handle[0].Set(self._rack_chassis_handle[1])

        # 모든 node 복원
        for handles in self._node_dim_handles.values():
            handles["chassis"][0].Set(handles["chassis"][1])
            if handles["frontpanel"]:
                handles["frontpanel"][0].Set(handles["frontpanel"][1])
            if handles["fp_enable"]:
                handles["fp_enable"][0].Set(handles["fp_enable"][1])

        # 모든 node GlassCube baseline 복원: node-state HEALTHY blink 때만 잠깐 visible.
        for node_path in self._rack_node_paths_cache:
            handles = self._glass_cube_cache.get(node_path)
            if handles:
                self._hide_glass_cube(handles)

        if self._rack_chassis_handle or self._node_dim_handles:
            print(
                f"[SceneManager] _restore_rack_nodes_opacity: "
                f"rack + {len(self._node_dim_handles)} nodes → 원래 값 복원, "
                f"{len(self._rack_glass_imageable_cache)} GlassCubes → invisible"
            )

    # ──────────────────────────────────────────────────────────────────────
    # 투명화 모드 토글 (React "set_transparency_mode" 메시지 수신 시 호출)
    # ──────────────────────────────────────────────────────────────────────

    def set_transparency_mode(self, enabled: bool):
        """
        투명화(dim) 모드를 설정하고, 현재 Stage D라면 즉시 씬에 반영합니다.
        - enabled=True  → _set_rack_siblings_opacity() 즉시 적용
        - enabled=False → _restore_rack_nodes_opacity() 즉시 적용
        Stage D가 아니면(_current_inspected_node=None) 플래그만 업데이트하고 씬 변경 없음.
        """
        self._transparency_enabled = enabled
        print(f"[SceneManager] set_transparency_mode: enabled={enabled}, "
              f"node={self._current_inspected_node}")

        if self._current_inspected_node is None:
            return

        if enabled:
            self._set_rack_siblings_opacity(self._current_inspected_node)
        else:
            self._restore_rack_nodes_opacity()

    # ──────────────────────────────────────────────────────────────────────
    # Stage C → D : Node 인스펙션 (pop-forward)
    # ──────────────────────────────────────────────────────────────────────

    def node_inspect(self, node_prim_path: str, pop_distance: float = NODE_POP_DISTANCE):
        """
        node를 -X 방향으로 pop_distance 만큼 이동하고,
        Rack + 선택 노드를 제외한 모든 sibling 노드를 dim 처리합니다.

        D→C→D 재선택 시에도 dim이 정확히 갱신됩니다.
        (node_inspect 진입 시 항상 이전 dim을 먼저 복원한 뒤 새로 적용)

        [수정 포인트 - POP DIRECTION]
        카메라가 -X 쪽에서 접근하므로 -X 방향이 앞쪽입니다.
        방향을 바꾸려면 pop_offset 벡터를 수정하세요.
        """
        if not self._stage:
            return

        # ① 이전 dim 상태를 무조건 먼저 복원
        #    (D→C→D 재선택 / deselect 없이 연속 inspect 시 누적 방지)
        self._restore_rack_nodes_opacity()

        # ② 이미 튀어나온 다른 노드가 있으면 먼저 원위치로 복원 (중첩 방지)
        for other_path in list(self._node_original_translate.keys()):
            if other_path == node_prim_path:
                continue
            other_prim = self._stage.GetPrimAtPath(other_path)
            if other_prim and other_prim.IsValid():
                for op in UsdGeom.Xformable(other_prim).GetOrderedXformOps():
                    if op.GetOpType() == UsdGeom.XformOp.TypeTranslate:
                        op.Set(self._node_original_translate[other_path])
                        break
            del self._node_original_translate[other_path]

        # ③ 선택 노드 pop-forward
        prim = self._stage.GetPrimAtPath(node_prim_path)
        if not prim or not prim.IsValid():
            return

        xformable    = UsdGeom.Xformable(prim)
        translate_op = None
        for op in xformable.GetOrderedXformOps():
            if op.GetOpType() == UsdGeom.XformOp.TypeTranslate:
                translate_op = op
                break
        if translate_op is None:
            translate_op = xformable.AddTranslateOp()

        current = translate_op.Get() or Gf.Vec3d(0, 0, 0)
        if node_prim_path not in self._node_original_translate:
            self._node_original_translate[node_prim_path] = Gf.Vec3d(current)

        pop_offset = Gf.Vec3d(-pop_distance, 0, 0)
        translate_op.Set(Gf.Vec3d(current) + pop_offset)

        # ④ 선택 노드 기록 + 투명화 플래그에 따라 dim 조건부 적용
        self._current_inspected_node = node_prim_path
        if self._transparency_enabled:
            self._set_rack_siblings_opacity(node_prim_path)

        # ⑤ 카메라 이동
        cam_pos, cam_target = self._compute_rack_cam(node_prim_path)
        self._start_camera_animation(cam_pos, cam_target)

        omni.usd.get_context().get_selection().clear_selected_prim_paths()
        print(f"[SceneManager] node_inspect: {node_prim_path} → cam {cam_pos}")

    # ──────────────────────────────────────────────────────────────────────
    # Stage D → C : Node Deselect
    # ──────────────────────────────────────────────────────────────────────

    def node_deselect(self, node_prim_path: str, return_rack_path: str):
        """node를 원위치로 되돌리고 rack 뷰로 복귀합니다.
        dim 처리된 Rack + sibling 노드의 opacity를 원래 값으로 복원합니다.
        """
        if not self._stage:
            return

        # pop-forward 복원
        if node_prim_path in self._node_original_translate:
            prim = self._stage.GetPrimAtPath(node_prim_path)
            if prim and prim.IsValid():
                for op in UsdGeom.Xformable(prim).GetOrderedXformOps():
                    if op.GetOpType() == UsdGeom.XformOp.TypeTranslate:
                        op.Set(self._node_original_translate[node_prim_path])
                        break
            del self._node_original_translate[node_prim_path]

        # dim 복원
        self._restore_rack_nodes_opacity()
        self._current_inspected_node = None

        self._reset_node_color(node_prim_path)

        cam_pos, cam_target = self._compute_rack_cam(return_rack_path)
        self._start_camera_animation(cam_pos, cam_target)
        omni.usd.get_context().get_selection().clear_selected_prim_paths()
        print(f"[SceneManager] node_deselect → rack: {return_rack_path}")

    # ──────────────────────────────────────────────────────────────────────
    # Stage C → B : Rack Deselect
    # ──────────────────────────────────────────────────────────────────────

    def rack_deselect_to_cluster(self, rack_prim_path: str, cluster_prim_path: str):
        """Rack 뷰(Stage C)에서 Cluster 뷰(Stage B)로 복귀합니다.
        rack visibility는 복원하지 않음 — scene_reset(→ Stage A)에서만 복원합니다.
        """
        if not self._stage:
            return

        # dim이 걸려있을 경우 복원 후 Stage C 캐시 비움
        self._restore_rack_nodes_opacity()
        self._rack_node_paths_cache      = []
        self._node_dim_handles           = {}
        self._rack_chassis_handle        = None
        self._rack_glass_imageable_cache = []

        # 모든 node 색상 초기화 (Stage C에서 적용된 색상 제거)
        for node_path in list(self._node_material_cache.keys()):
            self._reset_node_color(node_path)

        # 모든 Alert Decal 숨김
        for rack_path in self._rack_paths.values():
            self.hide_alert_decal(rack_path)

        ov_pos, ov_target = (
            self._cam_overview if self._cam_overview
            else (CAMERA_OVERVIEW_POSITION, CAMERA_OVERVIEW_TARGET)
        )
        self._start_camera_animation(ov_pos, ov_target)
        omni.usd.get_context().get_selection().clear_selected_prim_paths()
        print(f"[SceneManager] rack_deselect_to_cluster → cluster: {cluster_prim_path}")
