"""
_AlertMixin
Rack 위에 경고 마커(구체) 생성·표시·숨김.

담당:
  - create_alert_decal() : Rack 위 빨간 발광 구체 생성 또는 재표시
  - hide_alert_decal()   : 마커를 invisible 처리 (삭제 아님)

[수정 포인트 - DECAL STYLE]
현재는 단순 Sphere를 사용합니다.
텍스처 decal이 필요하면 UsdGeom.Mesh + projected UV 방식으로 교체하세요.
"""

from pxr import Gf, Sdf, Usd, UsdGeom, UsdShade

from ..global_variables import (
    ALERT_MARKER_RADIUS,
    ALERT_MARKER_Z_OFFSET,
    EMISSIVE_CRITICAL_INTENSITY,
)


class _AlertMixin:
    """Rack 경고 마커(Alert Decal) Mixin."""

    # ──────────────────────────────────────────────────────────────────────
    # 생성 / 표시
    # ──────────────────────────────────────────────────────────────────────

    def create_alert_decal(self, rack_prim_path: str):
        """
        rack 위에 빨간 발광 구체(경고 마커)를 생성/표시합니다.
        이미 존재하면 visibility만 켭니다.
        """
        if not self._stage:
            return

        decal_path = f"{rack_prim_path}/AlertMarker"
        existing   = self._stage.GetPrimAtPath(decal_path)

        if existing and existing.IsValid():
            UsdGeom.Imageable(existing).MakeVisible()
            return

        rack_prim = self._stage.GetPrimAtPath(rack_prim_path)
        if not rack_prim or not rack_prim.IsValid():
            return

        bbox_cache  = UsdGeom.BBoxCache(Usd.TimeCode.Default(), ["default", "render"])
        world_bound = bbox_cache.ComputeWorldBound(rack_prim)
        center      = world_bound.ComputeCentroid()
        top_z       = world_bound.GetRange().GetMax()[2]

        marker_pos = Gf.Vec3d(center[0], center[1], top_z + ALERT_MARKER_Z_OFFSET)

        sphere = UsdGeom.Sphere.Define(self._stage, decal_path)
        sphere.GetRadiusAttr().Set(ALERT_MARKER_RADIUS)
        UsdGeom.Xformable(sphere).AddTranslateOp().Set(marker_pos)

        mat_path    = f"{decal_path}/AlertMaterial"
        shader_path = f"{mat_path}/AlertShader"
        mat    = UsdShade.Material.Define(self._stage, mat_path)
        shader = UsdShade.Shader.Define(self._stage, shader_path)
        shader.CreateIdAttr("UsdPreviewSurface")
        # [수정 포인트] HDR 강도로 Bloom 효과 활성화
        shader.CreateInput("emissiveColor", Sdf.ValueTypeNames.Color3f).Set(
            Gf.Vec3f(EMISSIVE_CRITICAL_INTENSITY, 0.0, 0.0)
        )
        shader.CreateInput("roughness", Sdf.ValueTypeNames.Float).Set(0.8)
        mat.CreateSurfaceOutput().ConnectToSource(shader.ConnectableAPI(), "surface")
        UsdShade.MaterialBindingAPI.Apply(sphere.GetPrim()).Bind(mat)

        print(f"[SceneManager] Alert Decal 생성: {decal_path}")

    # ──────────────────────────────────────────────────────────────────────
    # 숨김
    # ──────────────────────────────────────────────────────────────────────

    def hide_alert_decal(self, rack_prim_path: str):
        """경고 마커를 숨깁니다 (삭제하지 않고 invisible 처리)."""
        if not self._stage:
            return
        decal_path = f"{rack_prim_path}/AlertMarker"
        prim       = self._stage.GetPrimAtPath(decal_path)
        if prim and prim.IsValid():
            UsdGeom.Imageable(prim).MakeInvisible()
