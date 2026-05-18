"""
_CameraControllerMixin
카메라 위치/타겟 읽기·쓰기, BBox 기반 Rack 카메라 계산, smoothstep 애니메이션.

사용 방법:
  SceneManager가 이 Mixin을 상속하고 __init__ 에서 _init_camera()를 호출해야 합니다.
  공유 상태(_stage, _cam_overview 등)는 SceneManager.__init__의 self를 통해 접근합니다.
"""

import omni.kit.viewport.utility as vp_utils
from pxr import Gf, Usd, UsdGeom

from ..global_variables import (
    CAMERA_OVERVIEW_POSITION,
    CAMERA_OVERVIEW_TARGET,
    CAMERA_RACK_LOOK_OFFSET,
    CAMERA_RACK_DISTANCE_FACTOR,
    CAMERA_ANIM_FRAMES,
)

# ─────────────────────────────────────────────────────────────────────────────
# 모듈 레벨 보간 헬퍼
# ─────────────────────────────────────────────────────────────────────────────

def _smoothstep(t: float) -> float:
    """Ease in-out 보간 (0 → 1)."""
    t = max(0.0, min(1.0, t))
    return t * t * (3.0 - 2.0 * t)


def _lerp3(a, b, t: float):
    """3-tuple 선형 보간."""
    return (
        a[0] + (b[0] - a[0]) * t,
        a[1] + (b[1] - a[1]) * t,
        a[2] + (b[2] - a[2]) * t,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mixin
# ─────────────────────────────────────────────────────────────────────────────

class _CameraControllerMixin:
    """카메라 위치 제어 및 smoothstep 애니메이션 담당 Mixin."""

    def _init_camera(self):
        """카메라 관련 인스턴스 속성 초기화. SceneManager.__init__에서 호출."""
        self._cam_anim    = None   # 진행 중인 애니메이션 파라미터 dict
        self._cam_current = None   # 최근 적용한 (pos, target) — 보간 시작점
        self._cam_overview = None  # Stage 초기화 시 저장한 overview 카메라 위치/타겟

    # ──────────────────────────────────────────────────────────────────────
    # 카메라 즉시 이동
    # ──────────────────────────────────────────────────────────────────────

    def _set_cam_pos_target(self, pos, target):
        """
        카메라를 pos 위치에서 target 방향으로 즉시 이동합니다.
        rotateXYZ 계산 불필요 — position + look-at 방식.
        """
        try:
            from omni.kit.viewport.utility.camera_state import ViewportCameraState
            vp = vp_utils.get_active_viewport()
            if not vp or not self._stage:
                return
            cam_state = ViewportCameraState(vp.camera_path, vp)
            cam_state.set_position_world(Gf.Vec3d(*pos),    True)
            cam_state.set_target_world(Gf.Vec3d(*target), True)
            self._cam_current = (tuple(pos), tuple(target))
        except Exception as e:
            print(f"[SceneManager] 카메라 설정 실패: {e}")

    def _read_cam_pos_target(self):
        """ViewportCameraState에서 현재 position / target 읽기. 실패 시 (None, None)."""
        try:
            from omni.kit.viewport.utility.camera_state import ViewportCameraState
            vp = vp_utils.get_active_viewport()
            if not vp or not self._stage:
                return None, None
            cam_state = ViewportCameraState(vp.camera_path, vp)
            pos    = cam_state.position_world
            target = cam_state.target_world
            if pos is None or target is None:
                return None, None
            return (
                (float(pos[0]),    float(pos[1]),    float(pos[2])),
                (float(target[0]), float(target[1]), float(target[2])),
            )
        except Exception:
            return None, None

    # ──────────────────────────────────────────────────────────────────────
    # Rack BBox 기반 카메라 위치 계산
    # ──────────────────────────────────────────────────────────────────────

    def _compute_rack_cam(self, rack_prim_path: str):
        """
        Rack의 world BBox 중심 + 정규화된 CAMERA_RACK_LOOK_OFFSET 방향으로
        (cam_pos, cam_target) 을 반환합니다.

        거리 = rack BBox 최장변 × CAMERA_RACK_DISTANCE_FACTOR
        오프셋 방향만 사용하므로 절대값이 아닌 상대적 방향 벡터로 동작합니다.
        """
        prim = self._stage.GetPrimAtPath(rack_prim_path)
        if not prim or not prim.IsValid():
            ov = self._cam_overview
            return (
                ov[0] if ov else CAMERA_OVERVIEW_POSITION,
                ov[1] if ov else CAMERA_OVERVIEW_TARGET,
            )

        bbox_cache  = UsdGeom.BBoxCache(Usd.TimeCode.Default(), ["default", "render"])
        world_bound = bbox_cache.ComputeWorldBound(prim)
        c           = world_bound.ComputeCentroid()
        center      = (float(c[0]), float(c[1]), float(c[2]))

        # BBox 최장변 계산
        rng     = world_bound.GetRange()
        bbox_sz = rng.GetMax() - rng.GetMin()
        size    = max(abs(float(bbox_sz[0])), abs(float(bbox_sz[1])), abs(float(bbox_sz[2])))
        if size < 1e-6:
            size = 100.0  # 안전 fallback

        # CAMERA_RACK_LOOK_OFFSET 정규화 → 방향 벡터
        dir_vec = Gf.Vec3d(*CAMERA_RACK_LOOK_OFFSET)
        length  = dir_vec.GetLength()
        if length < 1e-9:
            dir_vec = Gf.Vec3d(-1.0, 0.0, 0.0)
        else:
            dir_vec /= length

        dist    = size * CAMERA_RACK_DISTANCE_FACTOR
        cam_pos = (
            center[0] + float(dir_vec[0]) * dist,
            center[1] + float(dir_vec[1]) * dist,
            center[2] + float(dir_vec[2]) * dist,
        )
        return cam_pos, center

    # ──────────────────────────────────────────────────────────────────────
    # smoothstep 애니메이션
    # ──────────────────────────────────────────────────────────────────────

    def _start_camera_animation(self, end_pos, end_target):
        """현재 카메라 위치/타겟 → end 위치/타겟으로 smoothstep 애니메이션 시작."""
        if self._cam_current:
            start_pos, start_target = self._cam_current
        else:
            start_pos, start_target = self._read_cam_pos_target()
            if start_pos is None:
                if self._cam_overview:
                    start_pos, start_target = self._cam_overview
                else:
                    start_pos, start_target = CAMERA_OVERVIEW_POSITION, CAMERA_OVERVIEW_TARGET
            self._cam_current = (start_pos, start_target)

        self._cam_anim = {
            "start_pos":    start_pos,
            "start_target": start_target,
            "end_pos":      tuple(end_pos),
            "end_target":   tuple(end_target),
            "frame":        0,
            "total_frames": CAMERA_ANIM_FRAMES,
        }

    def tick_camera_animation(self):
        """매 프레임 호출 — 카메라 애니메이션 1 스텝 진행. extension._on_update 에서 호출."""
        if not self._cam_anim:
            return

        anim  = self._cam_anim
        frame = anim["frame"]
        total = anim["total_frames"]

        if frame >= total:
            self._set_cam_pos_target(anim["end_pos"], anim["end_target"])
            self._cam_anim = None
            return

        t      = _smoothstep(frame / (total - 1) if total > 1 else 1.0)
        pos    = _lerp3(anim["start_pos"],    anim["end_pos"],    t)
        target = _lerp3(anim["start_target"], anim["end_target"], t)
        self._set_cam_pos_target(pos, target)
        anim["frame"] += 1
