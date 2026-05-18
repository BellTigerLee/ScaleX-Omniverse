"""
Omniverse Script Editor - UV Transform + Animation + Blue Emissive for /World/Cylinder
stripe.jpg (흑백) 텍스처를 Cylinder 옆면에 적용.
흰 줄 → 파란색 emissive 발광 (전기 흐르는 효과)
검은 줄 → 어두운 base color

Shader Graph:
  UsdPrimvarReader_float2 (st)
       ↓
  UsdTransform2d  ← scale / rotation / translation  ← 매 프레임 갱신
       ├──→ UsdUVTexture (diffuse)   → diffuseColor  (어두운 베이스)
       └──→ UsdUVTexture (emissive)  → emissiveColor (파란 발광)
                [scale = EMISSIVE_COLOR * EMISSIVE_INTENSITY]
  → UsdPreviewSurface
  → UsdGeom.Subset("sides") bind  ← 옆면만

애니메이션 제어:
  - ANIM_SPEED, ANIM_DIRECTION
  - stop_animation()
"""

from pxr import Gf, Sdf, UsdGeom, UsdShade
import omni.usd
import omni.kit.app

# ── 파라미터 (자유롭게 수정) ──────────────────────────────────────────────────
TEXTURE_PATH  = "/home/netai-sys/workspace/dev-tools/extensions/datacenter_monitor/assets/ScaleX_POD_Project/materials/textures/stripe_super_wide3.png"
CYLINDER_PATH = "/World/Cylinder"
MATERIAL_PATH = "/World/Looks/CylinderMat"

UV_SCALE    = Gf.Vec2f(2.0, 1.0)
UV_ROTATION = 90.0                   # 90° → 가로 줄무늬

# 애니메이션 파라미터
ANIM_SPEED     = 0.5
ANIM_DIRECTION = Gf.Vec2f(1.0, 0.0)

# Emissive 파라미터
EMISSIVE_COLOR     = Gf.Vec3f(0.0, 0.5, 1.0)   # 파란색 (R, G, B)
EMISSIVE_INTENSITY = 1.0                         # 클수록 더 밝게 발광
                                                 # Bloom 효과와 함께 사용 시 3~5 권장
                                                 # HDR/Bloom 없을 경우 1~2

# cap 판별 임계값 (face normal Y 성분)
CAP_NORMAL_Y_THRESHOLD = 0.9
# ─────────────────────────────────────────────────────────────────────────────


# ── 내부 상태 ─────────────────────────────────────────────────────────────────
_translation_input = None
_offset            = Gf.Vec2f(0.0, 0.0)
_subscription      = None
# ─────────────────────────────────────────────────────────────────────────────


def _get_or_create_shader(stage, path, shader_id):
    shader = UsdShade.Shader.Get(stage, path)
    if not shader:
        shader = UsdShade.Shader.Define(stage, path)
        shader.CreateIdAttr(shader_id)
    return shader


def _classify_side_faces(mesh_prim):
    mesh        = UsdGeom.Mesh(mesh_prim)
    points      = mesh.GetPointsAttr().Get()
    face_counts = mesh.GetFaceVertexCountsAttr().Get()
    face_verts  = mesh.GetFaceVertexIndicesAttr().Get()

    if not points or not face_counts or not face_verts:
        print("[WARN] mesh attribute를 읽을 수 없습니다. 전체 face에 적용합니다.")
        return list(range(len(face_counts))) if face_counts else []

    side_faces = []
    vi = 0

    for fi, count in enumerate(face_counts):
        if count < 3:
            vi += count
            continue

        p0 = points[face_verts[vi]]
        p1 = points[face_verts[vi + 1]]
        p2 = points[face_verts[vi + 2]]

        e1x = p1[0]-p0[0];  e1y = p1[1]-p0[1];  e1z = p1[2]-p0[2]
        e2x = p2[0]-p0[0];  e2y = p2[1]-p0[1];  e2z = p2[2]-p0[2]

        nx = e1y*e2z - e1z*e2y
        ny = e1z*e2x - e1x*e2z
        nz = e1x*e2y - e1y*e2x

        length = (nx*nx + ny*ny + nz*nz) ** 0.5
        if length > 1e-6:
            if abs(ny / length) < CAP_NORMAL_Y_THRESHOLD:
                side_faces.append(fi)
        else:
            side_faces.append(fi)

        vi += count

    return side_faces


def apply_uv_texture():
    stage = omni.usd.get_context().get_stage()

    cylinder_prim = stage.GetPrimAtPath(CYLINDER_PATH)
    if not cylinder_prim.IsValid():
        print(f"[ERROR] Prim not found: {CYLINDER_PATH}")
        return None

    side_faces = _classify_side_faces(cylinder_prim)
    print(f"[INFO] 옆면 face {len(side_faces)}개 검출")

    # ── Material ────────────────────────────────────────────────────────────
    material = UsdShade.Material.Define(stage, MATERIAL_PATH)

    # UsdPreviewSurface
    surface = _get_or_create_shader(stage, MATERIAL_PATH + "/PBR", "UsdPreviewSurface")
    surface.CreateInput("roughness", Sdf.ValueTypeNames.Float).Set(0.3)
    surface.CreateInput("metallic",  Sdf.ValueTypeNames.Float).Set(0.0)
    # 베이스 색상을 어둡게 — 발광 대비 극대화
    surface.CreateInput("diffuseColor", Sdf.ValueTypeNames.Color3f).Set(
        Gf.Vec3f(0.02, 0.02, 0.02)
    )
    material.CreateSurfaceOutput().ConnectToSource(surface.ConnectableAPI(), "surface")

    # ── UsdTransform2d (공유) ───────────────────────────────────────────────
    uv_xform = _get_or_create_shader(stage, MATERIAL_PATH + "/UVTransform", "UsdTransform2d")
    uv_xform.CreateInput("scale",    Sdf.ValueTypeNames.Float2).Set(UV_SCALE)
    uv_xform.CreateInput("rotation", Sdf.ValueTypeNames.Float).Set(UV_ROTATION)
    translation_input = uv_xform.CreateInput("translation", Sdf.ValueTypeNames.Float2)
    translation_input.Set(Gf.Vec2f(0.0, 0.0))
    uv_xform.CreateOutput("result", Sdf.ValueTypeNames.Float2)

    # ── UsdPrimvarReader_float2 ─────────────────────────────────────────────
    uv_reader = _get_or_create_shader(stage, MATERIAL_PATH + "/UVReader", "UsdPrimvarReader_float2")
    uv_reader.CreateInput("varname",  Sdf.ValueTypeNames.Token).Set("st")
    uv_reader.CreateInput("fallback", Sdf.ValueTypeNames.Float2).Set(Gf.Vec2f(0.0, 0.0))
    uv_reader.CreateOutput("result",  Sdf.ValueTypeNames.Float2)
    uv_xform.CreateInput("in", Sdf.ValueTypeNames.Float2).ConnectToSource(
        uv_reader.ConnectableAPI(), "result"
    )

    # ── Emissive Texture ────────────────────────────────────────────────────
    # UsdUVTexture의 scale = EMISSIVE_COLOR * EMISSIVE_INTENSITY
    # 흰 픽셀(1,1,1) * scale → 파란색으로 변환  |  검은 픽셀(0,0,0) → 그대로 0
    ec = EMISSIVE_COLOR
    ei = EMISSIVE_INTENSITY
    emit_tex = _get_or_create_shader(stage, MATERIAL_PATH + "/EmissiveTex", "UsdUVTexture")
    emit_tex.CreateInput("file",  Sdf.ValueTypeNames.Asset).Set(TEXTURE_PATH)
    emit_tex.CreateInput("wrapS", Sdf.ValueTypeNames.Token).Set("repeat")
    emit_tex.CreateInput("wrapT", Sdf.ValueTypeNames.Token).Set("repeat")
    # 흑백 반전: 흰(1)*(-ei) + ei = 0  /  검(0)*(-ei) + ei = ei → 파란 발광
    # emit_tex.CreateInput("scale", Sdf.ValueTypeNames.Float4).Set(
    #     Gf.Vec4f(-ec[0] * ei, -ec[1] * ei, -ec[2] * ei, 1.0)
    # )
    emit_tex.CreateInput("bias",  Sdf.ValueTypeNames.Float4).Set(
        Gf.Vec4f(ec[0] * ei,  ec[1] * ei,  ec[2] * ei,  1.0)
    )
    emit_tex.CreateInput("st", Sdf.ValueTypeNames.Float2).ConnectToSource(
        uv_xform.ConnectableAPI(), "result"
    )
    emit_tex.CreateOutput("rgb", Sdf.ValueTypeNames.Float3)

    # emissiveColor ← EmissiveTex.rgb
    surface.CreateInput("emissiveColor", Sdf.ValueTypeNames.Color3f).ConnectToSource(
        emit_tex.ConnectableAPI(), "rgb"
    )

    # ── GeomSubset (옆면만 바인딩) ──────────────────────────────────────────
    subset = UsdGeom.Subset.Define(stage, CYLINDER_PATH + "/sides")
    subset.CreateElementTypeAttr("face")
    subset.CreateFamilyNameAttr("materialBind")
    subset.CreateIndicesAttr(side_faces)
    UsdShade.MaterialBindingAPI.Apply(subset.GetPrim()).Bind(material)

    print(f"[OK] Blue emissive material → '{CYLINDER_PATH}/sides'")
    print(f"     emissive color={EMISSIVE_COLOR}  intensity={EMISSIVE_INTENSITY}")
    print(f"     scale={UV_SCALE}  rotation={UV_ROTATION}°")
    print(f"     anim speed={ANIM_SPEED}  direction={ANIM_DIRECTION}")
    return translation_input


def start_animation(translation_input):
    global _offset, _subscription

    dx, dy = ANIM_DIRECTION[0], ANIM_DIRECTION[1]
    length = (dx*dx + dy*dy) ** 0.5
    if length < 1e-6:
        print("[WARN] ANIM_DIRECTION이 영벡터입니다. 애니메이션 없음.")
        return
    nx, ny = dx / length, dy / length

    def on_update(event):
        global _offset
        dt   = event.payload.get("dt", 1.0 / 60.0)
        step = ANIM_SPEED * dt
        _offset = Gf.Vec2f(
            (_offset[0] + nx * step) % 1.0,
            (_offset[1] + ny * step) % 1.0,
        )
        translation_input.Set(_offset)

    _subscription = omni.kit.app.get_app().get_update_event_stream().create_subscription_to_pop(
        on_update, name="cylinder_uv_anim"
    )
    print("[OK] UV 애니메이션 시작. 중단 → stop_animation()")


def stop_animation():
    global _subscription
    if _subscription is not None:
        _subscription = None
        print("[OK] UV 애니메이션 중단")
    else:
        print("[INFO] 실행 중인 애니메이션 없음")


# ── 진입점 ────────────────────────────────────────────────────────────────────
_translation_input = apply_uv_texture()
if _translation_input is not None:
    start_animation(_translation_input)
