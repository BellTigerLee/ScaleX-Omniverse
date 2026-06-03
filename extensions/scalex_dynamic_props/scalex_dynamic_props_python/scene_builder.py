"""
scalex_dynamic_props - SceneBuilder

원본 USD 를 오염시키지 않고 "세션 레이어(의 익명 서브레이어)" 에만 author 하는
모든 USD 로직. extension.py 가 스테이지당 1회만 rebuild() 를 호출한다.

핵심 아이디어:
  - stage.GetSessionLayer() 아래에 익명 오버레이 레이어를 끼우고,
    Usd.EditContext 로 그 레이어를 edit target 으로 잡은 채 모든 prim 을 author 한다.
  - 세션 레이어(및 그 서브레이어)는 root .usd 저장 대상이 아니므로 "Save" 해도 원본 불변.
  - rebuild() 자체는 오버레이를 Clear() 하고 다시 그리므로 명시 호출 시 멱등(idempotent)하다.
    [수정] 반복 ASSETS_LOADED 에 의한 불필요한 Clear+재author 는 extension.py 가 차단한다.

USD Viewer 기반(Isaac Sim / omni.physx 의존성 없음).
"""

from pxr import Gf, Sdf, Usd, UsdGeom, UsdShade

from .global_variables import (
    EXTENSION_TITLE,
    OVERLAY_TAG,
    MDL_DARKER,
    MDL_DARKER_SUBID,
    DYNAMIC_ROOT,
    LOOKS_SCOPE,
    POD_BASE,
    FLOOR_FRONT_REL,
    FLOOR_FRONT_MESH_NAME,
    UNDER_PANEL_NAME,
    UNDER_PANEL_TRANSLATE,
    CLUSTER_PANELS,
    WORLD_LOOKS_SCOPE,
    VIEW_PLANE_PATH,
    VIEW_PLANE_POINTS,
    VIEW_PLANE_FACE_VERTEX_COUNTS,
    VIEW_PLANE_FACE_VERTEX_INDICES,
    VIEW_PLANE_NORMALS,
    VIEW_PLANE_ST,
    VIEW_PLANE_EXTENT,
    VIEW_PLANE_TRANSLATE,
    VIEW_PLANE_ROTATE_XYZ,
    VIEW_PLANE_SCALE,
    VIEW_PLANE_MAT_PATH,
    VIEW_PLANE_MAT_DISPLAY_NAME,
    VIEW_PLANE_TEXTURE,
    HIDDEN_PRIM_PATHS,
)


def _log(msg: str):
    print(f"[{EXTENSION_TITLE}] {msg}")


def hex_to_srgb(hex_str: str) -> Gf.Vec3f:
    """'#9cc2e5' → Gf.Vec3f(0.6118, 0.7608, 0.8980). sRGB 값 그대로(0~255 → /255)."""
    h = hex_str.lstrip("#")
    r = int(h[0:2], 16) / 255.0
    g = int(h[2:4], 16) / 255.0
    b = int(h[4:6], 16) / 255.0
    return Gf.Vec3f(r, g, b)


class SceneBuilder:
    """세션 레이어 오버레이에만 under-panel + ScaleX_POD_view plane 을 author 하는 빌더."""

    def __init__(self, stage: Usd.Stage):
        self._stage = stage

    # ── 오버레이 레이어 (오염 방지의 핵심) ────────────────────────────────────
    def _get_or_create_overlay(self) -> Sdf.Layer:
        """세션 레이어 아래의 OVERLAY_TAG 익명 레이어를 찾거나 새로 만들어 반환.

        이미 있으면 재사용(멱등) → 익스텐션 reload/스테이지 재오픈 시 중복 레이어가 쌓이지 않음.
        """
        session = self._stage.GetSessionLayer()
        for ident in session.subLayerPaths:
            if OVERLAY_TAG in ident:
                lyr = Sdf.Layer.Find(ident)
                if lyr is not None:
                    return lyr
        overlay = Sdf.Layer.CreateAnonymous(OVERLAY_TAG)
        # 최상위(인덱스 0)에 끼워 동일 경로 override 가 우선 적용되게 한다.
        session.subLayerPaths.insert(0, overlay.identifier)
        _log(f"세션 오버레이 레이어 생성: {overlay.identifier}")
        return overlay

    def rebuild(self):
        """오버레이를 비우고 under-panel + view plane 을 다시 author. 멱등."""
        overlay = self._get_or_create_overlay()
        overlay.Clear()  # 이전 내용 제거 → 멱등

        with Usd.EditContext(self._stage, Usd.EditTarget(overlay)):
            # 동적 prim 을 모아두는 루트 Scope.
            UsdGeom.Scope.Define(self._stage, DYNAMIC_ROOT)
            UsdGeom.Scope.Define(self._stage, LOOKS_SCOPE)
            # view plane 머티리얼이 들어갈 /World/Looks Scope (없으면 생성).
            UsdGeom.Scope.Define(self._stage, WORLD_LOOKS_SCOPE)
            n_panels = self._build_floor_under_panels()
            n_planes = self._build_view_plane()
            n_hidden = self._hide_prims()
        _log(
            f"rebuild 완료 — under-panel {n_panels}개, view plane {n_planes}개, "
            f"숨김 {n_hidden}개"
        )

    def teardown(self):
        """오버레이 내용 제거 + 세션 레이어에서 분리."""
        session = self._stage.GetSessionLayer()
        for ident in list(session.subLayerPaths):
            if OVERLAY_TAG in ident:
                lyr = Sdf.Layer.Find(ident)
                if lyr is not None:
                    lyr.Clear()
                session.subLayerPaths.remove(ident)
                _log(f"세션 오버레이 레이어 분리: {ident}")

    # ── 머티리얼 헬퍼 ────────────────────────────────────────────────────────
    def _make_color_material(self, mat_path: str, base_color: Gf.Vec3f) -> UsdShade.Material:
        """Darker_Chassis_Metal MDL 머티리얼 생성. base_color 만 클러스터 색으로."""
        mat = UsdShade.Material.Define(self._stage, mat_path)
        shader = UsdShade.Shader.Define(self._stage, mat_path + "/Shader")
        shader.SetSourceAsset(Sdf.AssetPath(str(MDL_DARKER)), "mdl")
        shader.SetSourceAssetSubIdentifier(MDL_DARKER_SUBID, "mdl")
        shader.CreateInput("base_color", Sdf.ValueTypeNames.Color3f).Set(base_color)
        out = shader.CreateOutput("out", Sdf.ValueTypeNames.Token)
        mat.CreateSurfaceOutput("mdl").ConnectToSource(out)
        mat.CreateDisplacementOutput("mdl").ConnectToSource(out)
        mat.CreateVolumeOutput("mdl").ConnectToSource(out)
        return mat

    def _make_omnipbr_material(self, mat_path: str, diffuse_png: str) -> UsdShade.Material:
        """OmniPBR(Albedo 만) 머티리얼 생성. diffuse_texture = png 절대경로. emissive 없음."""
        mat = UsdShade.Material.Define(self._stage, mat_path)
        shader = UsdShade.Shader.Define(self._stage, mat_path + "/Shader")
        # OmniPBR.mdl 은 Kit MDL 검색 경로의 코어 자산 → 베어 네임으로 해석.
        shader.SetSourceAsset(Sdf.AssetPath("OmniPBR.mdl"), "mdl")
        shader.SetSourceAssetSubIdentifier("OmniPBR", "mdl")
        shader.CreateInput("diffuse_texture", Sdf.ValueTypeNames.Asset).Set(
            Sdf.AssetPath(diffuse_png)
        )
        out = shader.CreateOutput("out", Sdf.ValueTypeNames.Token)
        # 저장본과 동일 배선: surface/displacement/volume 모두 Shader.out 으로.
        mat.CreateSurfaceOutput("mdl").ConnectToSource(out)
        mat.CreateDisplacementOutput("mdl").ConnectToSource(out)
        mat.CreateVolumeOutput("mdl").ConnectToSource(out)
        return mat

    # ── 기능 ② FloorPanel under-panel ────────────────────────────────────────
    def _build_floor_under_panels(self) -> int:
        count = 0
        for entry in CLUSTER_PANELS:
            cluster = entry["cluster"]
            base_color = hex_to_srgb(entry["hex"])
            # 클러스터당 머티리얼 1개 → 그 클러스터의 모든 under-panel 이 공유.
            mat_path = f"{LOOKS_SCOPE}/{cluster.split('_')[0]}_Color_Chassis_Metal"
            try:
                mat = self._make_color_material(mat_path, base_color)
            except Exception as exc:
                _log(f"[경고] 색상 머티리얼 생성 실패 {cluster}: {exc}")
                continue

            for rack in entry["racks"]:
                front_path = f"{POD_BASE}/{cluster}/{rack}/{FLOOR_FRONT_REL}"
                try:
                    if self._add_under_panel(front_path, mat):
                        count += 1
                except Exception as exc:
                    _log(f"[경고] under-panel 생성 실패 {cluster}/{rack}: {exc}")
        return count

    def _add_under_panel(self, front_path: str, material: UsdShade.Material) -> bool:
        """FloorPanel_Bottom_Front 아래에 under-panel mesh 를 만들고 색상 MDL 바인딩.

        소스 mesh 의 지오메트리를 런타임에 그대로 복사한다(하드코딩 금지).
        """
        front_prim = self._stage.GetPrimAtPath(front_path)
        if not front_prim or not front_prim.IsValid():
            _log(f"[경고] FloorPanel_Bottom_Front 없음, 건너뜀: {front_path}")
            return False

        # 소스 mesh = FloorPanel_Bottom_Front 의 동명 mesh 자식.
        src_path = f"{front_path}/{FLOOR_FRONT_MESH_NAME}"
        src_prim = self._stage.GetPrimAtPath(src_path)
        if not src_prim or not src_prim.IsValid() or not src_prim.IsA(UsdGeom.Mesh):
            _log(f"[경고] 소스 mesh 없음, 건너뜀: {src_path}")
            return False

        under_path = f"{front_path}/{UNDER_PANEL_NAME}"
        dst = UsdGeom.Mesh.Define(self._stage, under_path)
        self._copy_mesh_geom(UsdGeom.Mesh(src_prim), dst)

        # Transform — xformOpOrder = [xformOp:transform], identity + translate (0,0,-5)
        # A3 처럼 under-panel 이 이미 있으면 xformOp:transform 이 존재하므로 order 를 비우고 override.
        xform = UsdGeom.Xformable(dst.GetPrim())
        xform.ClearXformOpOrder()
        mtx = Gf.Matrix4d(1.0)
        mtx.SetTranslateOnly(Gf.Vec3d(*UNDER_PANEL_TRANSLATE))
        xform.AddTransformOp().Set(mtx)

        UsdShade.MaterialBindingAPI.Apply(dst.GetPrim()).Bind(material)
        return True

    @staticmethod
    def _copy_mesh_geom(src: UsdGeom.Mesh, dst: UsdGeom.Mesh):
        """points / topology / normals(+interp) / extent / subdiv / primvars 복사."""
        pts = src.GetPointsAttr().Get()
        if pts is not None:
            dst.CreatePointsAttr(pts)
        fvc = src.GetFaceVertexCountsAttr().Get()
        if fvc is not None:
            dst.CreateFaceVertexCountsAttr(fvc)
        fvi = src.GetFaceVertexIndicesAttr().Get()
        if fvi is not None:
            dst.CreateFaceVertexIndicesAttr(fvi)

        normals = src.GetNormalsAttr().Get()
        if normals is not None:
            dst.CreateNormalsAttr(normals)
            dst.SetNormalsInterpolation(src.GetNormalsInterpolation())

        extent = src.GetExtentAttr().Get()
        if extent is not None:
            dst.CreateExtentAttr(extent)

        subdiv = src.GetSubdivisionSchemeAttr().Get()
        if subdiv is not None:
            dst.CreateSubdivisionSchemeAttr(subdiv)

        # primvars(st 등) 를 interpolation/indices 까지 그대로 복사.
        src_pv = UsdGeom.PrimvarsAPI(src.GetPrim())
        dst_pv = UsdGeom.PrimvarsAPI(dst.GetPrim())
        for pv in src_pv.GetPrimvars():
            new_pv = dst_pv.CreatePrimvar(
                pv.GetPrimvarName(), pv.GetTypeName(), pv.GetInterpolation()
            )
            val = pv.Get()
            if val is not None:
                new_pv.Set(val)
            if pv.IsIndexed():
                idx = pv.GetIndices()
                if idx is not None:
                    new_pv.SetIndices(idx)

    # ── 기능 ③ ScaleX_POD_view plane ──────────────────────────────────────────
    def _build_view_plane(self) -> int:
        """/World/ScaleX_POD_view 에 100×100 quad plane 을 만들고 OmniPBR(Albedo) 바인딩.

        머티리얼은 /World/Looks 아래에 생성한다. 텍스처가 없으면 건너뛴다.
        """
        tex = VIEW_PLANE_TEXTURE
        if not tex.exists():
            _log(f"[경고] view plane 텍스처 없음, 건너뜀: {tex}")
            return 0
        try:
            mesh = UsdGeom.Mesh.Define(self._stage, VIEW_PLANE_PATH)

            # 메쉬 지오메트리
            mesh.CreatePointsAttr([Gf.Vec3f(*p) for p in VIEW_PLANE_POINTS])
            mesh.CreateFaceVertexCountsAttr(VIEW_PLANE_FACE_VERTEX_COUNTS)
            mesh.CreateFaceVertexIndicesAttr(VIEW_PLANE_FACE_VERTEX_INDICES)
            mesh.CreateExtentAttr([Gf.Vec3f(*v) for v in VIEW_PLANE_EXTENT])
            mesh.CreateSubdivisionSchemeAttr(UsdGeom.Tokens.none)  # 텍스처 평면 왜곡 방지

            # normals (faceVarying)
            mesh.CreateNormalsAttr([Gf.Vec3f(*n) for n in VIEW_PLANE_NORMALS])
            mesh.SetNormalsInterpolation(UsdGeom.Tokens.faceVarying)

            # primvars:st (faceVarying texCoord2f)
            pv_api = UsdGeom.PrimvarsAPI(mesh.GetPrim())
            st = pv_api.CreatePrimvar(
                "st", Sdf.ValueTypeNames.TexCoord2fArray, UsdGeom.Tokens.faceVarying
            )
            st.Set([Gf.Vec2f(*uv) for uv in VIEW_PLANE_ST])

            # Transform — xformOpOrder = [translate, rotateXYZ, scale].
            # 원본 stage 에 동명 prim 이 있을 수 있으므로 order 를 먼저 비운 뒤 다시 쌓는다.
            xform = UsdGeom.Xformable(mesh.GetPrim())
            xform.ClearXformOpOrder()
            xform.AddTranslateOp().Set(Gf.Vec3d(*VIEW_PLANE_TRANSLATE))
            xform.AddRotateXYZOp().Set(Gf.Vec3f(*VIEW_PLANE_ROTATE_XYZ))
            xform.AddScaleOp().Set(Gf.Vec3f(*VIEW_PLANE_SCALE))

            # OmniPBR 머티리얼 (Albedo 만) → /World/Looks 아래 생성 후 바인딩.
            mat = self._make_omnipbr_material(VIEW_PLANE_MAT_PATH, str(tex))
            # USD prim 이름엔 하이픈을 못 쓰므로 요청 원문은 displayName 으로 보존.
            mat_prim = mat.GetPrim()
            try:
                mat_prim.SetDisplayName(VIEW_PLANE_MAT_DISPLAY_NAME)
            except Exception:
                mat_prim.SetMetadata("displayName", VIEW_PLANE_MAT_DISPLAY_NAME)

            UsdShade.MaterialBindingAPI.Apply(mesh.GetPrim()).Bind(mat)
            _log(f"view plane 생성: {VIEW_PLANE_PATH} (mat={VIEW_PLANE_MAT_PATH})")
            return 1
        except Exception as exc:  # 실패해도 나머지 author 는 계속
            _log(f"[경고] view plane 생성 실패: {exc}")
            return 0

    # ── 기능 ④ 시작 시 prim 숨김 ───────────────────────────────────────────────
    def _hide_prims(self) -> int:
        """HIDDEN_PRIM_PATHS 의 각 prim 을 visibility=invisible 로 override.

        오버레이 레이어에만 author 하므로 원본 USD 는 불변. visibility 는 자식까지
        상속되므로 랙 하나를 숨기면 그 아래 노드/메쉬가 모두 사라진다.
        """
        count = 0
        for path in HIDDEN_PRIM_PATHS:
            prim = self._stage.GetPrimAtPath(path)
            if not prim or not prim.IsValid():
                _log(f"[경고] 숨길 prim 없음, 건너뜀: {path}")
                continue
            try:
                UsdGeom.Imageable(prim).CreateVisibilityAttr().Set(UsdGeom.Tokens.invisible)
                count += 1
                _log(f"prim 숨김: {path}")
            except Exception as exc:
                _log(f"[경고] prim 숨김 실패 {path}: {exc}")
        return count
