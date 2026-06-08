"""
scalex_dynamic_props - 상수/데이터 테이블

이 파일은 "튜닝 면(tuning surface)" 역할을 한다. 모든 prim 경로, transform 값,
색상, MDL 파일명을 여기 한 곳에 모아두고 scene_builder.py 가 소비한다.

모든 수치는 저장본 ScaleX_Twin.usd 를 그대로 읽어 추출한 실제 값이다.
USD 자산의 명명/배치가 바뀌면 (다른 곳이 아니라) 이 파일을 고치면 된다.

datacenter_monitor 와 동일한 규칙:
  - USD Viewer 기반(Isaac Sim / omni.physx 의존성 없음)
  - 한국어 주석 + [수정] 마커
  - print("[ScaleX Dynamic Props] ...") 로깅
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# 0. 로깅 / 식별
# ─────────────────────────────────────────────────────────────────────────────
EXTENSION_TITLE = "ScaleX Dynamic Props"

# [수정] 동적 prop author on/off 환경변수. 기본값은 켜짐.
DYNAMIC_PROPS_ENABLED_ENV = "SCALEX_DYNAMIC_PROPS_ENABLED"
_DISABLED_VALUES = {"0", "false", "off", "no"}


def is_dynamic_props_enabled() -> bool:
    """SCALEX_DYNAMIC_PROPS_ENABLED 값을 읽어 동적 prop 적용 여부를 반환."""
    value = os.getenv(DYNAMIC_PROPS_ENABLED_ENV)
    if value is None:
        return True
    return value.strip().lower() not in _DISABLED_VALUES

# 세션 레이어에 끼워 넣을 익명 오버레이 레이어의 태그.
# 원본 .usd 오염을 막기 위해 이 태그가 들어간 익명 레이어에만 author 한다.
OVERLAY_TAG = "scalex_dynamic_props"

# ─────────────────────────────────────────────────────────────────────────────
# 1. 에셋 경로 resolve
#    MDL 은 복제하지 않고 datacenter_monitor 의 assets 를 경로로 참조한다.
#    EXT_ROOT = .../extensions/scalex_dynamic_props
#    DC_ASSETS = .../extensions/datacenter_monitor/assets/ScaleX_POD_Project
# ─────────────────────────────────────────────────────────────────────────────
EXT_ROOT = Path(__file__).resolve().parent.parent
DC_ASSETS = EXT_ROOT.parent / "datacenter_monitor" / "assets" / "ScaleX_POD_Project"
TEX_DIR = DC_ASSETS / "materials" / "textures"
MDL_DARKER = DC_ASSETS / "materials" / "Darker_Chassis_Metal.mdl"

# 동적 prim/머티리얼을 모아두는 루트 (세션 레이어 안에서만 존재).
DYNAMIC_ROOT = "/World/ScaleX_Dynamic"
LOOKS_SCOPE = DYNAMIC_ROOT + "/Looks"

# ─────────────────────────────────────────────────────────────────────────────
# 2. 기능 ② — 클러스터별 색상 FloorPanel under-panel
# ─────────────────────────────────────────────────────────────────────────────

# POD 베이스 경로: <POD_BASE>/<Cluster>/<Rack>
POD_BASE = "/World/SCENT_Multi_POD_Module/ScaleX_POD"

# 랙 내부에서 FloorPanel_Bottom_Front(Xform) 까지의 상대 경로 (모든 대상 랙 공통).
#   .../<Rack>/Rack_42U/Body/Core/Rack_Core/FloorPanel_Bottom_Front
FLOOR_FRONT_REL = "Rack_42U/Body/Core/Rack_Core/FloorPanel_Bottom_Front"
# FloorPanel_Bottom_Front 의 소스 mesh 자식 이름(= 자기 자신과 동명의 mesh).
FLOOR_FRONT_MESH_NAME = "FloorPanel_Bottom_Front"
# 새로 만들 under-panel 이름.
UNDER_PANEL_NAME = "FloorPanel_Bottom_Under_Front"
# under-panel 을 부모(FloorPanel_Bottom_Front) 로컬에서 Y 로 내리는 양 (저장본과 동일).
UNDER_PANEL_TRANSLATE = (0.0, 0.0, -5.0)

# MDL (저장본과 동일 형식): Darker_Chassis_Metal, base_color 만 클러스터 색으로.
MDL_DARKER_SUBID = "Darker_Chassis_Metal"

# 2.1 대상 클러스터 / 랙 / 색상.
#     color: sRGB 값 그대로(0~255 → /255). linear 변환하지 않는다.
#     [수정] 색을 바꾸거나 대상 랙을 추가/삭제하려면 이 리스트만 고치면 된다.
#     색을 주지 않은 EdgeX/Anonymous 클러스터는 대상이 아니다(실제 씬에 EdgeX_Cluster 없음).
CLUSTER_PANELS = [
    {"cluster": "DataX_Cluster",   "racks": ["Rack_42U_A3"],                "hex": "#9cc2e5"},
    {"cluster": "TwinX_Cluster",   "racks": ["Rack_42U_A4"],                "hex": "#a8d08c"},
    {"cluster": "MobileX_Cluster", "racks": ["Rack_42U_A1"],                "hex": "#ffd766"},
    {"cluster": "AutoX_Cluster",   "racks": ["Rack_42U_A5", "Rack_42U_A6"], "hex": "#bfbfbf"},
]

# ─────────────────────────────────────────────────────────────────────────────
# 3. 기능 ③ — ScaleX_POD_view plane (앱 시작 시 1회 생성)
#    100×100 quad(XZ 평면, normal +Y) 에 OmniPBR(Albedo) 머티리얼을 바인딩한다.
#    [수정] 위치/회전/스케일/텍스처/머티리얼명을 바꾸려면 이 블록만 고치면 된다.
# ─────────────────────────────────────────────────────────────────────────────

# 3.1 plane prim 경로/이름.
VIEW_PLANE_PATH = "/World/ScaleX_POD_view"

# 3.2 공통 quad 메쉬 (100×100, XZ 평면, normal +Y).
VIEW_PLANE_POINTS = [
    (-50.0, 0.0, -50.0),
    (50.0, 0.0, -50.0),
    (-50.0, 0.0, 50.0),
    (50.0, 0.0, 50.0),
]
VIEW_PLANE_FACE_VERTEX_COUNTS = [4]
VIEW_PLANE_FACE_VERTEX_INDICES = [0, 2, 3, 1]
VIEW_PLANE_NORMALS = [(0.0, 1.0, 0.0)] * 4            # faceVarying
VIEW_PLANE_ST = [(0.0, 1.0), (0.0, 0.0), (1.0, 0.0), (1.0, 1.0)]  # faceVarying texCoord2f
VIEW_PLANE_EXTENT = [(-50.0, 0.0, -50.0), (50.0, 0.0, 50.0)]

# 3.3 Transform — xformOpOrder = [translate, rotateXYZ, scale].
VIEW_PLANE_TRANSLATE = (230.99456, 152.02296, -412.37006)
VIEW_PLANE_ROTATE_XYZ = (0.0, 270.0, 90.0)
VIEW_PLANE_SCALE = (1.6, 1.0, 0.9)

# 3.4 OmniPBR 머티리얼 — /World/Looks 아래 생성, Albedo(diffuse_texture) 만 지정.
#     USD prim 이름엔 하이픈을 못 쓰므로 prim 이름은 ScaleX_POd_STER 로 만들고,
#     요청 원문(ScaleX-POd-STER) 은 prim displayName 으로 보존한다.
WORLD_LOOKS_SCOPE = "/World/Looks"
VIEW_PLANE_MAT_NAME = "ScaleX_POd_STER"
VIEW_PLANE_MAT_DISPLAY_NAME = "ScaleX-POd-STER"
VIEW_PLANE_MAT_PATH = WORLD_LOOKS_SCOPE + "/" + VIEW_PLANE_MAT_NAME
VIEW_PLANE_TEXTURE = TEX_DIR / "ScaleX-POD_view_1.png"

# ─────────────────────────────────────────────────────────────────────────────
# 4. 기능 ④ — 앱 시작 시 숨길 prim (visibility = invisible)
#    세션 오버레이에만 override 하므로 원본 USD 는 그대로다. teardown(OFF/종료) 시
#    오버레이가 분리되어 다시 보인다. visibility 는 자식까지 상속된다.
#    [수정] 숨길 prim 을 추가/삭제하려면 이 리스트만 고치면 된다.
# ─────────────────────────────────────────────────────────────────────────────
HIDDEN_PRIM_PATHS = [
    "/World/SCENT_Multi_POD_Module/ScaleX_POD/Anonymous_Cluster/Rack_42U_A0",
]
