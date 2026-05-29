"""
SceneManager — Thin Facade
USD 씬 조작 전담 클래스. 반드시 Kit 메인 스레드에서 호출해야 합니다.

각 도메인은 scene/ 패키지의 Mixin으로 분리되어 있습니다.

  scene/camera.py          → _CameraControllerMixin  (카메라 위치·애니메이션)
  scene/topology.py        → _TopologyMixin           (USD 씬 계층 탐색·인덱싱)
  scene/node_visibility.py → _NodeVisibilityMixin     (Cluster/Rack/Node 가시성·Stage 전환)
  scene/material.py        → _MaterialMixin           (Glass Cube 생성·상태 색상 업데이트)
  scene/node_metrics.py    → _NodeMetricsMixin        (prim 별 최신 datacenter.metrics 캐시)
  scene/alert.py           → _AlertMixin              (Rack 경고 마커)
  scene/event_alert.py     → _EventAlertMixin         (Kafka 이벤트 ImagePanel 패널)

공개 API (외부 호출처: extension.py, message_handler.py):
  initialize()                    — Stage 열릴 때 초기화
  cleanup()                       — Stage 닫힐 때 정리
  discover_topology()             — USD 계층 탐색 (_TopologyMixin)
  get_cached_topology()           — 캐시 반환 (_TopologyMixin)
  cluster_focus()                 — Stage A → B (_NodeVisibilityMixin)
  rack_focus()                    — Stage B → C (_NodeVisibilityMixin)
  node_inspect()                  — Stage C → D (_NodeVisibilityMixin)
  node_deselect()                 — Stage D → C (_NodeVisibilityMixin)
  rack_deselect_to_cluster()      — Stage C → B (_NodeVisibilityMixin)
  scene_reset()                   — 어디서든 → Stage A (여기서 오케스트레이션)
  cache_node_metrics()            — datacenter.metrics 최신값 prim 별 캐시 (_NodeMetricsMixin)
  get_node_metrics()              — node_inspect 시 prim 의 최신 metrics 반환 (_NodeMetricsMixin)
  update_node_color_from_kafka()  — Kafka 상태 색상 업데이트 (_MaterialMixin)
  tick_camera_animation()         — 매 프레임 카메라 애니메이션 진행 (_CameraControllerMixin)
  create_alert_decal()            — Rack 경고 마커 생성 (_AlertMixin)
  hide_alert_decal()              — Rack 경고 마커 숨김 (_AlertMixin)
  show_event_panel()              — Kafka 이벤트 ImagePanel 생성 (_EventAlertMixin)
  tick_event_panels()             — 매 프레임 만료 패널 제거 (_EventAlertMixin)
"""

import omni.usd
from pxr import UsdGeom, Usd

from .global_variables import (
    CAMERA_OVERVIEW_POSITION,
    CAMERA_OVERVIEW_TARGET,
)
from .scene.camera         import _CameraControllerMixin
from .scene.topology       import _TopologyMixin
from .scene.node_visibility import _NodeVisibilityMixin
from .scene.material       import _MaterialMixin
from .scene.node_metrics   import _NodeMetricsMixin
from .scene.alert          import _AlertMixin
from .scene.event_alert    import _EventAlertMixin


class SceneManager(
    _CameraControllerMixin,
    _TopologyMixin,
    _NodeVisibilityMixin,
    _MaterialMixin,
    _NodeMetricsMixin,
    _AlertMixin,
    _EventAlertMixin,
):
    """
    SceneManager — Mixin 조합 Facade.
    모든 공개 메서드는 각 Mixin에 구현되어 있으며,
    __init__ / initialize / cleanup / scene_reset 만 여기서 관리합니다.
    """

    def __init__(self):
        self._stage: Usd.Stage = None

        # 각 Mixin 속성 초기화
        self._init_topology()         # _cluster_paths, _rack_paths, _server_index, _cluster_box_index
        self._init_material()         # _node_material_cache
        self._init_node_state()       # _node_status
        self._init_node_metrics()     # _node_metrics_cache
        self._init_node_visibility()  # _node_original_translate
        self._init_camera()           # _cam_anim, _cam_current, _cam_overview
        self._init_event_alert()      # _active_panels

    # ──────────────────────────────────────────────────────────────────────
    # 초기화 / 정리
    # ──────────────────────────────────────────────────────────────────────

    def initialize(self):
        """Stage 열릴 때 호출. Stage 참조 획득 및 overview 카메라 위치 저장."""
        self._stage = omni.usd.get_context().get_stage()
        if not self._stage:
            print("[SceneManager] stage 없음")
            return

        self._cluster_paths.clear()
        self._rack_paths.clear()
        self._server_index.clear()
        self._cluster_box_index.clear()
        self._topology_cache = None
        self._node_material_cache.clear()
        self._glass_cube_cache.clear()
        self._node_metrics_cache.clear()
        self._node_original_translate.clear()

        pos, target = self._read_cam_pos_target()
        if pos is not None:
            self._cam_overview = (pos, target)
            self._cam_current  = (pos, target)
        else:
            self._cam_overview = (CAMERA_OVERVIEW_POSITION, CAMERA_OVERVIEW_TARGET)
            self._cam_current  = (CAMERA_OVERVIEW_POSITION, CAMERA_OVERVIEW_TARGET)

        print("[SceneManager] 초기화 완료 — overview 카메라 저장:", self._cam_overview[0])

    def cleanup(self):
        """Stage 닫힐 때 호출. 모든 캐시와 상태 초기화."""
        self._clear_all_event_panels()
        self._stage = None
        self._cluster_paths.clear()
        self._rack_paths.clear()
        self._server_index.clear()
        self._cluster_box_index.clear()
        self._node_material_cache.clear()
        self._glass_cube_cache.clear()
        self._node_metrics_cache.clear()
        self._node_original_translate.clear()
        # Stage C 캐시 (_NodeVisibilityMixin)
        self._rack_node_paths_cache      = []
        self._node_dim_handles           = {}
        self._rack_chassis_handle        = None
        self._rack_glass_imageable_cache = []
        self._current_inspected_node = None
        self._cam_anim    = None
        self._cam_current  = None
        self._cam_overview = None

    # ──────────────────────────────────────────────────────────────────────
    # Scene Reset — 어느 Stage에서든 → Stage A
    # ──────────────────────────────────────────────────────────────────────

    def scene_reset(self):
        """모든 cluster/rack visibility 복원, pop-forward 복원, 색상 초기화, 카메라 원위치."""
        if not self._stage:
            return

        # 모든 cluster visibility 복원
        for cluster_path in self._cluster_paths.values():
            prim = self._stage.GetPrimAtPath(cluster_path)
            if prim and prim.IsValid():
                UsdGeom.Imageable(prim).MakeVisible()

        # 모든 rack visibility 복원
        for rack_path in self._rack_paths.values():
            prim = self._stage.GetPrimAtPath(rack_path)
            if prim and prim.IsValid():
                UsdGeom.Imageable(prim).MakeVisible()

        # pop-forward된 node 전부 복원
        for node_path, original_pos in list(self._node_original_translate.items()):
            prim = self._stage.GetPrimAtPath(node_path)
            if prim and prim.IsValid():
                for op in UsdGeom.Xformable(prim).GetOrderedXformOps():
                    if op.GetOpType() == UsdGeom.XformOp.TypeTranslate:
                        op.Set(original_pos)
                        break
        self._node_original_translate.clear()

        # dim 처리된 Rack + sibling 노드 opacity 복원 후 Stage C 캐시 비움
        self._restore_rack_nodes_opacity()
        self._rack_node_paths_cache      = []
        self._node_dim_handles           = {}
        self._rack_chassis_handle        = None
        self._rack_glass_imageable_cache = []
        self._current_inspected_node     = None

        # 모든 node 색상 초기화 (glass cube → HEALTHY)
        for node_path in list(self._node_material_cache.keys()):
            self._reset_node_color(node_path)

        # 모든 Alert Decal 숨김
        for rack_path in self._rack_paths.values():
            self.hide_alert_decal(rack_path)

        # 모든 EventPanel prim 즉시 제거
        self._clear_all_event_panels()

        omni.usd.get_context().get_selection().clear_selected_prim_paths()

        ov_pos, ov_target = (
            self._cam_overview if self._cam_overview
            else (CAMERA_OVERVIEW_POSITION, CAMERA_OVERVIEW_TARGET)
        )
        self._start_camera_animation(ov_pos, ov_target)
        print("[SceneManager] scene_reset 완료")
