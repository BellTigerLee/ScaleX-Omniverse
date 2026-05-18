from pathlib import Path

EXTENSION_TITLE       = "Datacenter Monitor"
EXTENSION_DESCRIPTION = "Kafka-driven datacenter digital twin with WebRTC interaction"
EXTENSION_ROOT        = Path(__file__).resolve().parent.parent

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - USD SCENE PATH]
# 실제 USD 파일 경로로 변경하세요.
# None 이면 이미 열려있는 stage를 그대로 사용합니다.
# ─────────────────────────────────────────────────────────────────────────────
# ↓ 실제 USD 파일 경로로 변경하세요. None이면 자동 로드를 건너뜁니다.
MAIN_STAGE_USD_PATH = EXTENSION_ROOT / "assets" / "ScaleX_POD_Project" / "ScaleX_Twin.usd"

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - USD HIERARCHY]
# 실제 USD 씬 계층 구조:
#   /World/
#     SCENT_Multi_POD_Module/
#       ScaleX_POD/
#         {Name}_Cluster/    ← CLUSTER_SUFFIX = "_Cluster"
#           Rack_{Name}/     ← RACK_PREFIX = "Rack_"
#             Server_{Name}/ ← SERVER_PREFIX = "Server_"
#
# SCALE_POD_PATH : rack Cluster들의 부모 prim 경로
# ─────────────────────────────────────────────────────────────────────────────
SCENE_ROOT     = "/World"   # 씬 최상위 (visibility 복구 등에 사용)
SCALE_POD_PATH = "/World/SCENT_Multi_POD_Module/ScaleX_POD"

# prim 이름 판별 키워드 (대소문자 정확히 일치)
# 계층: {Name}_Cluster / Rack_{Name} / Box_{Name} / Server_{Name}
CLUSTER_SUFFIX = "_Cluster"   # {Name}_Cluster
RACK_PREFIX    = "Rack_"      # Rack_{Name}
BOX_PREFIX     = "Box_"       # Box_{Name}  — Rack 안의 중간 격납 단위
SERVER_PREFIX  = "Server_"    # Server_{Name}
# Storage_, Switch_ 등 다른 이름의 prim은 SERVER_PREFIX 미매칭으로 자동 스킵됩니다.

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - KAFKA]
# Kafka bootstrap 주소는 config/env.<profile> 파일에서 로드됩니다.
# 프로파일 선택: DC_PROFILE 환경변수 또는 config/active 심볼릭 링크.
# 자세한 설정법은 extensions/datacenter_monitor/README.md 참조.
# kafka-python 설치 필요:
#   <omniverse_python> -m pip install kafka-python
# ─────────────────────────────────────────────────────────────────────────────
from .config_loader import load_profile

_profile = load_profile()
KAFKA_BOOTSTRAP_SERVERS = [f"{_profile['CLUSTER_HOST']}:{_profile['KAFKA_NODEPORT']}"]
KAFKA_TOPIC_LIVE        = "datacenter.metrics"      # 실시간 메트릭 토픽
KAFKA_TOPIC_REPLAY      = "datacenter.metrics.replay"  # Replay 전용 토픽 (Query Server가 produce)
KAFKA_TOPIC_EVENT      = "datacenter.metrics.event"  # event전용 토픽 (Query Server가 produce)
KAFKA_TOPIC_NODE_STATE  = "datacenter.metrics.node-state.events"  # canonical node-state 토픽 (Flink가 produce)
KAFKA_TOPIC_REPLAY_EVENT = "datacenter.metrics.replay.event"  # Replay 전용 event 토픽
KAFKA_TOPIC_CLUSTER_RANK = "datacenter.metrics.stageab"  # C축 cluster CPU rank 토픽 (Flink가 produce)
KAFKA_GROUP_ID          = "omniverse-datacenter-monitor"

# Topology API — canonical (cluster, node) → prim_name 매핑 source.
# 프로파일의 TOPOLOGY_URL 키가 없으면 None (이 경우 _resolve_prim_path 의 휴리스틱 fallback 만 사용).
NODE_INDEX_URL = _profile.get("TOPOLOGY_URL") or None

# Dev 전용: 로컬에서 Flink 가 "work2..work8" 처럼 topology 와 무관한 node 이름을 보낼 때
# 정상 resolution 실패 시 topology 의 아무 prim 에나 stable-hash 로 1:1 고정 배정한다.
# PoC 환경에서는 반드시 false (기본값).
_dev_flag = _profile.get("DEV_FAKE_NODE_MAPPING", "") or ""
DEV_FAKE_NODE_MAPPING = _dev_flag.lower() in ("true", "1", "yes", "on")

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - KAFKA MESSAGE FORMAT]
# Kafka 메시지 형식이 아래와 다르면 kafka_subscriber.py의 파싱 로직을 수정하세요.
#
# 메시지 형식 (JSON):
# {
#   "ts"      : 1775454754796,
#   "cluster" : "datax",              ← USD prim 이름과 매칭 (대소문자 무관)
#   "node"    : "work5",              ← USD prim 이름과 매칭
#   "status"  : "HEALTHY",            ← "HEALTHY" | "WARNING" | "CRITICAL"
#   "metrics" : {
#     "cpu"    : { "util": 0.035, "cores": 12.0, "load1": 0.38,
#                  "load5": 0.41, "load15": 0.43, "eff": 0.053 },
#     "mem"    : { "util": 0.201, "total_gb": 16.62,
#                  "avail_gb": 13.29, "oom_cnt": 0 },
#     "net"    : { "retrans": 0.0, "in_mbps": 2.62, "out_mbps": 2.89,
#                  "nic_err_sum": 0.0, "nic_drop_sum": 0.0,
#                  "netstat_err": 0.0, "err_sum": 0.0 },
#     "gpu"    : { "util": 0.0, "temp": 38.0, "pwr": 24.868,
#                  "mem_util": 0.0, "mem_used_gb": 0.0, "total_gb": 23.525 },
#     "storage": { "util": 0.274, "read_mbps": 0.0,
#                  "write_mbps": 0.171, "io_mbps": 0.171 }
#   },
#   "debug_ts": 1775454757851
# }
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - COLOR THRESHOLDS]
# 온도 기반 색상 임계값 (°C). 필요에 따라 다른 지표(CPU, 전력 등)로 교체 가능.
# ─────────────────────────────────────────────────────────────────────────────
TEMP_NORMAL   = 70.0   # < 이 값: 정상 (색상 변환 없음)
TEMP_WARNING  = 85.0   # < 이 값: 경고 (노란색 발광)
# >= TEMP_WARNING: 위험 (빨간색 HDR 발광 + Alert Decal)

# 발광 강도 (HDR — Bloom 효과를 위해 1.0 이상)
EMISSIVE_WARNING_INTENSITY  = 500.0
EMISSIVE_CRITICAL_INTENSITY = 3000.0

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - CAMERA]
#
# ▶ 값을 얻는 방법 (Omniverse Script Editor에서 실행):
#
#   from omni.kit.viewport.utility.camera_state import ViewportCameraState
#   import omni.kit.viewport.utility as vp_utils
#   vp    = vp_utils.get_active_viewport()
#   state = ViewportCameraState(vp.camera_path, vp)
#   pos   = state.position_world
#   tgt   = state.target_world
#   print(f"POSITION = ({pos[0]:.1f}, {pos[1]:.1f}, {pos[2]:.1f})")
#   print(f"TARGET   = ({tgt[0]:.1f}, {tgt[1]:.1f}, {tgt[2]:.1f})")
#
# ▶ Rack 카메라는 각 rack의 BBox 중심(world)에 오프셋을 더해 자동 계산됩니다.
#   오프셋 = "rack 중심에서 카메라 방향으로 얼마나 이동하냐"
#   예) rack 정면이 -X 방향이면 CAMERA_RACK_LOOK_OFFSET = (-200, 50, 0)
#   좌표계(Y-up/Z-up)나 Reference 여부와 무관하게 동작합니다.
# ─────────────────────────────────────────────────────────────────────────────
NODE_POP_DISTANCE      = 100.0   # node pop-forward 이동 거리 (미터)

# Stage A — 전체씬 개요 카메라 (scene_reset 시 복귀 위치)
# Script Editor로 원하는 시점에서 읽은 값을 여기에 입력하세요.
CAMERA_OVERVIEW_POSITION = (-1388.0,  638.0,  700.0)   # 카메라 위치
CAMERA_OVERVIEW_TARGET   = (    0.0,    0.0,    0.0)   # 카메라가 바라보는 점 (씬 중심)

# Stage C — Rack 포커스 카메라 (BBox 자동 계산)
# CAMERA_RACK_LOOK_OFFSET : 방향 벡터 (크기는 무시됨, 단위벡터로 정규화 후 사용)
#   rack 정면이 -X 방향이면 (-1, 0, 0), -Z 방향이면 (0, 0, -1) 등
#   Y 성분은 카메라 높이 조절에 사용됩니다 (양수 = 위에서 내려다봄)
CAMERA_RACK_LOOK_OFFSET      = (-1.0,  0.2,  0.0)   # 방향 벡터 (정규화 후 사용)
# CAMERA_RACK_DISTANCE_FACTOR : 카메라 거리 = rack bbox 최장변 × 이 값
#   값이 클수록 더 멀리, 작을수록 더 가까이
CAMERA_RACK_DISTANCE_FACTOR  =  6.0                  # bbox 최장변 배수

# 카메라 줌인 애니메이션 프레임 수 (60 fps 기준 약 1.5 초)
CAMERA_ANIM_FRAMES           =   90

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - ALERT DECAL]
# 경고 표시의 크기 (미터 단위)
# ─────────────────────────────────────────────────────────────────────────────
ALERT_MARKER_RADIUS    = 0.15
ALERT_MARKER_Z_OFFSET  = 0.5   # rack 상단에서 얼마나 위에 표시할지

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - EVENT ALERT IMAGE PANEL]
# Kafka 이벤트(WARNING/CRITICAL) 발생 시 rack 위에 표시할 ImagePanel USD
# ─────────────────────────────────────────────────────────────────────────────
IMAGE_PANEL_USD_PATH     = str(
    EXTENSION_ROOT / "assets" / "ScaleX_POD_Project"
    / "subcomponents" / "Others" / "ImagePanel.usd"
)
EVENT_PANEL_LIFETIME_SEC = 5.0   # 패널이 화면에 표시되는 시간 (초)

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - MATERIAL DISCOVERY]
# Node의 Front Panel 머티리얼을 찾을 때 사용하는 키워드
# 씬의 머티리얼 이름 규칙에 맞게 수정하세요.
# ─────────────────────────────────────────────────────────────────────────────
FRONT_PANEL_MATERIAL_KEYWORD = "frontpanel"  # 소문자 검색 (대소문자 무시)
SHADER_KEYWORD               = "shader"

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - NODE X POSITION NORMALIZATION]
# 노드별 X 좌표 정규화 설정
# ─────────────────────────────────────────────────────────────────────────────
NODE_X_DEFAULT = 91.63  # 기본 X 위치 (모든 노드 기본값)
NODE_X_SPECIAL = {
    "Box_3U_DGX_A100_1" : 94.01,   # DGX_A100 노드만 다른 X 위치 (예시: 85.0)
    "Box_3U_DGX_A100_2" : 94.01,   # DGX_A100 노드만 다른 X 위치 (예시: 85.0)
    "Box_3U_DGX_A100_3" : 94.01,   # DGX_A100 노드만 다른 X 위치 (예시: 85.0)
    "Box_6U_DGX_A100"   : 94.01,   # DGX_A100 노드만 다른 X 위치 (예시: 85.0)
    # 추가로 특수한 위치를 가져야 할 노드들을 여기에 추가
    # "노드이름": X값,
}

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - NODE INSPECT DIM]
# Stage C→D 진입 시 선택 노드 제외한 Rack·sibling 노드에 적용할 dim 값.
# MDL 파라미터에 직접 기록되므로 MDL 파일과 맞춰야 합니다.
# ─────────────────────────────────────────────────────────────────────────────
NODE_DIM_CHASSIS_OPACITY     = 0.02   # Custom/Darker_Chassis_Metal opacity_val — dim 값
NODE_DIM_FRONTPANEL_MODE     = 0.0     # FrontPanelMaterial opacity_mode         — dim 값

# ─────────────────────────────────────────────────────────────────────────────
# [수정 포인트 - OVERLAY CUBE]
# 노드 크기에 맞춘 transient overlay cube (UsdPreviewSurface) material 설정.
# Glass/OmniPBR material은 denoiser 잔상이 생길 수 있어 사용하지 않는다.
# ─────────────────────────────────────────────────────────────────────────────
GLASS_CUBE_ENABLE_EMISSION   = False
GLASS_CUBE_OPACITY_CONSTANT  = 0
GLASS_CUBE_HEALTHY_COLOR      = (0.05, 1.5, 0.05)  # HEALTHY (녹색, 튜닝 대상)
GLASS_CUBE_DISCONNECTED_COLOR = (0.35, 0.35, 0.35)  # DISCONNECTED (어두운 회색)
GLASS_CUBE_WARNING_COLOR     = (1.0, 0.5,  0.0) # WARNING 상태 (주황)
GLASS_CUBE_CRITICAL_COLOR    = (1.0, 0.0,  0.0) # CRITICAL 상태 (빨강)
GLASS_CUBE_EMISSIVE_HEALTHY  = 0.0001              # HEALTHY emissive 강도 (꺼짐)
GLASS_CUBE_EMISSIVE_WARNING  = 100.0            # WARNING emissive 강도
GLASS_CUBE_EMISSIVE_CRITICAL = 500.0            # CRITICAL emissive 강도

# ── Node-state pulse 파라미터 (one-shot rise + exp tail on message receipt) ─
# Kafka 메시지 수신 시마다 한 번의 pulse 를 재생한다.
# rise: sin quarter, fall: exponential decay.
# `*_EMISSIVE_COLOR` 는 diffuse 색과 분리된 튜닝용 emissive tint.
NODE_PULSE_HEALTHY_MIN          = 0.0
NODE_PULSE_HEALTHY_MAX          = 2.0
NODE_PULSE_HEALTHY_PERIOD_SEC   = 1.3
NODE_PULSE_HEALTHY_EMISSIVE_COLOR = GLASS_CUBE_HEALTHY_COLOR
NODE_PULSE_HEALTHY_TAIL_SEC       = 0.4
NODE_PULSE_HEALTHY_TAIL_TAU       = 0.9

NODE_PULSE_DISCONNECTED_MIN         = 0.0
NODE_PULSE_DISCONNECTED_MAX         = 1.5
NODE_PULSE_DISCONNECTED_PERIOD_SEC  = NODE_PULSE_HEALTHY_PERIOD_SEC * 2   # HEALTHY × 2
NODE_PULSE_DISCONNECTED_EMISSIVE_COLOR = GLASS_CUBE_DISCONNECTED_COLOR
NODE_PULSE_DISCONNECTED_TAIL_SEC       = 1.6
NODE_PULSE_DISCONNECTED_TAIL_TAU       = 0.6
