"""
_NetLineAnimMixin
각 Rack의 NetLine_in / NetLine_out Cylinder에 UV 펄스 애니메이션 머티리얼을 적용하고,
Kafka net.in_mbps / net.out_mbps 값에 따라 실시간으로 애니메이션 속도를 조절한다.

Shader Graph (Rack마다 독립 머티리얼):
  UsdPrimvarReader_float2 (st)
       ↓
  UsdTransform2d  ← translation 매 프레임 갱신
       └──→ UsdUVTexture (emissive)  → emissiveColor (파란 발광)
  → UsdPreviewSurface
  → cylinder mesh prim 전체 바인딩

속도 매핑:
  speed = NET_LINE_SPEED_MIN + (mbps / NET_LINE_MBPS_SCALE) * (NET_LINE_SPEED_MAX - NET_LINE_SPEED_MIN)
  clamp([NET_LINE_SPEED_MIN, NET_LINE_SPEED_MAX])
"""

import time

from pxr import Gf, Sdf, UsdShade

from ..global_variables import (
    NET_LINE_INNER_PATH,
    NET_LINE_IN_NAME,
    NET_LINE_OUT_NAME,
    NET_LINE_TEXTURE_PATH,
    NET_LINE_UV_SCALE,
    NET_LINE_UV_ROTATION,
    NET_LINE_EMISSIVE_COLOR,
    NET_LINE_EMISSIVE_INTENSITY,
    NET_LINE_SPEED_MIN,
    NET_LINE_SPEED_MAX,
    NET_LINE_MBPS_SCALE,
)

# NetLine_in: +X 방향 (유입), NetLine_out: -X 방향 (유출)
_DIRECTIONS = {
    NET_LINE_IN_NAME:  Gf.Vec2f(1.0,  0.0),
    NET_LINE_OUT_NAME: Gf.Vec2f(-1.0, 0.0),
}


class _NetLineAnimMixin:
    """Rack별 NetLine UV 애니메이션 관리 Mixin."""

    def _init_net_line_anim(self):
        """SceneManager.__init__ 에서 호출."""
        # rack_prim_path → {
        #   "in":  {"translation_input": UsdShade.Input, "offset": Gf.Vec2f, "speed": float},
        #   "out": {"translation_input": UsdShade.Input, "offset": Gf.Vec2f, "speed": float},
        # }
        self._net_line_states: dict = {}
        self._net_line_last_tick: float = time.time()
        # 메시 데이터 미준비로 setup 실패한 rack 경로 — tick마다 retry
        self._pending_racks: set = set()

    # ──────────────────────────────────────────────────────────────────────
    # 초기화 — topology 탐색 시 Rack마다 호출
    # ──────────────────────────────────────────────────────────────────────

    def setup_rack_net_lines(self, rack_prim_path: str) -> None:
        """
        rack_prim_path 하위의 NetLine_in / NetLine_out 에 머티리얼을 적용하고
        애니메이션 상태를 초기화한다.

        rack_prim_path 예시:
          /World/SCENT_Multi_POD_Module/ScaleX_POD/DataX_Cluster/Rack_42U_A3
        NetLine 경로:
          {rack_prim_path}/Rack_42U/Body/Lines/NetLine_in
          {rack_prim_path}/Rack_42U/Body/Lines/NetLine_out
        """
        if not self._stage:
            return

        lines_base = rack_prim_path + NET_LINE_INNER_PATH
        mat_base   = _rack_mat_base(rack_prim_path)

        state = {}
        for line_name, direction in _DIRECTIONS.items():
            cylinder_path = f"{lines_base}/{line_name}"
            material_path = f"{mat_base}_{line_name}"
            print(f"Material Path: {material_path}, cylinder_path: {cylinder_path}")

            t_input = self._apply_net_line_material(cylinder_path, material_path)
            if t_input is None:
                continue

            # 이미 state가 있으면 translation_input만 교체하고 offset/speed 유지
            prev = (self._net_line_states.get(rack_prim_path) or {}).get(line_name)
            state[line_name] = {
                "translation_input": t_input,
                "offset":    prev["offset"]    if prev else Gf.Vec2f(0.0, 0.0),
                "speed":     prev["speed"]     if prev else NET_LINE_SPEED_MIN,
                "direction": direction,
            }

        if state:
            self._net_line_states[rack_prim_path] = state

        # 일부라도 실패하면 pending에 추가해 retry; 전부 성공하면 제거
        if len(state) < len(_DIRECTIONS):
            self._pending_racks.add(rack_prim_path)
        else:
            self._pending_racks.discard(rack_prim_path)
            print(f"[NetLineAnim] setup OK → {rack_prim_path}  lines={list(state.keys())}")
            # ── TEMP TEST: DataX NetLine_in만 강제 속도 ──────────────
            if "DataX" in rack_prim_path and NET_LINE_IN_NAME in state:
                state[NET_LINE_IN_NAME]["speed"] = 1.0   # 원하는 속도값
            # ─────────────────────────────────────────────────────────



    # ──────────────────────────────────────────────────────────────────────
    # 속도 갱신 — Kafka 메시지로부터
    # ──────────────────────────────────────────────────────────────────────

    def update_rack_net_speed_from_kafka(self, kafka_msg: dict) -> None:
        """
        Kafka 메시지에서 rack 경로를 찾아 in/out 속도를 갱신한다.
        cluster/rack_id → _rack_paths 조회 → update_rack_net_speed() 위임.
        """
        metrics = kafka_msg.get("metrics") or {}
        net     = metrics.get("net") or {}
        in_mbps  = net.get("in_mbps",  0.0)
        out_mbps = net.get("out_mbps", 0.0)

        # if in_mbps == 0.0 and out_mbps == 0.0:
        #     return  # net 메트릭 없음 — skip

        # cluster 필드 (여러 형식 허용)
        cluster = (
            kafka_msg.get("cluster") or
            kafka_msg.get("cluster_id") or ""
        )
        # rack 필드
        rack_id = (
            kafka_msg.get("rack_id") or
            kafka_msg.get("rack") or ""
        )
        if not cluster or not rack_id:
            return

        # _rack_paths: 원본 키와 lowercase alias 모두 등록돼 있음
        rack_path = (
            self._rack_paths.get(f"{cluster}/{rack_id}") or
            self._rack_paths.get(f"{cluster.lower()}/{rack_id}") or
            self._rack_paths.get(rack_id)
        )
        if not rack_path:
            return

        self.update_rack_net_speed(rack_path, in_mbps, out_mbps)

    def update_rack_net_speed(self, rack_path: str, in_mbps: float, out_mbps: float) -> None:
        """rack_path 의 NetLine_in / NetLine_out 속도를 직접 갱신한다."""
        state = self._net_line_states.get(rack_path)
        if not state:
            return

        if NET_LINE_IN_NAME in state:
            state[NET_LINE_IN_NAME]["speed"]  = _mbps_to_speed(in_mbps)
        if NET_LINE_OUT_NAME in state:
            state[NET_LINE_OUT_NAME]["speed"] = _mbps_to_speed(out_mbps)

    # ──────────────────────────────────────────────────────────────────────
    # 매 프레임 tick — extension._on_update() 에서 호출
    # ──────────────────────────────────────────────────────────────────────

    def tick_net_line_anim(self) -> None:
        """모든 Rack NetLine의 UV offset을 한 프레임 진행한다."""
        # pending rack retry — 메시 데이터가 준비될 때까지 매 프레임 시도
        if self._pending_racks and self._stage:
            for rack_path in list(self._pending_racks):
                self.setup_rack_net_lines(rack_path)

        if not self._net_line_states:
            return

        now = time.time()
        dt  = min(now - self._net_line_last_tick, 0.1)  # 최대 100ms cap
        self._net_line_last_tick = now

        for rack_state in self._net_line_states.values():
            for line_state in rack_state.values():
                t_input   = line_state["translation_input"]
                direction = line_state["direction"]
                speed     = line_state["speed"]
                offset    = line_state["offset"]

                new_offset = Gf.Vec2f(
                    (offset[0] + direction[0] * speed * dt) % 1.0,
                    (offset[1] + direction[1] * speed * dt) % 1.0,
                )
                line_state["offset"] = new_offset
                t_input.Set(new_offset)

    def reinitialize_net_lines(self) -> None:
        """
        ASSETS_LOADED 등 외부 이벤트로부터 호출 가능한 수동 재초기화.
        tick의 pending retry와 병행 동작한다.
        """
        if not self._rack_paths:
            return
        unique_paths = set(self._rack_paths.values())
        for rack_path in unique_paths:
            self.setup_rack_net_lines(rack_path)
        print(f"[NetLineAnim] reinitialize 요청 — {len(unique_paths)}개 rack")

    # ──────────────────────────────────────────────────────────────────────
    # 정리
    # ──────────────────────────────────────────────────────────────────────

    def cleanup_net_line_anim(self) -> None:
        """Stage 닫힐 때 상태 초기화."""
        self._net_line_states.clear()
        self._pending_racks.clear()
        self._net_line_last_tick = time.time()

    # ──────────────────────────────────────────────────────────────────────
    # 내부 — 머티리얼 생성
    # ──────────────────────────────────────────────────────────────────────

    def _apply_net_line_material(self, cylinder_path: str, material_path: str):
        """
        cylinder_path Cylinder 전체에 UV 애니메이션 머티리얼을 적용한다.
        translation_input (UsdShade.Input) 을 반환. 실패 시 None.

        ※ GeomSubset 방식을 쓰지 않는 이유:
          familyName="materialBind" subset이 존재하면 렌더러가 mesh 레벨
          원본 binding을 완전히 무시한다. subset이 비어있거나 face 분류가
          틀리면 cylinder 전체가 invisible해지는 버그가 발생한다.
          NetLine cylinder는 얇고 길어 cap이 사실상 보이지 않으므로
          전체 mesh에 직접 바인딩해도 시각적으로 동일하다.
        """
        stage = self._stage
        cylinder_prim = stage.GetPrimAtPath(cylinder_path)
        if not cylinder_prim or not cylinder_prim.IsValid():
            return None

        # ── Material ──────────────────────────────────────────────────────
        material = UsdShade.Material.Define(stage, material_path)

        surface = _get_or_create_shader(stage, material_path + "/PBR", "UsdPreviewSurface")
        surface.CreateInput("roughness",   Sdf.ValueTypeNames.Float).Set(0.3)
        surface.CreateInput("metallic",    Sdf.ValueTypeNames.Float).Set(0.0)
        surface.CreateInput("diffuseColor", Sdf.ValueTypeNames.Color3f).Set(
            Gf.Vec3f(0.1, 0.1, 0.1)
        )
        material.CreateSurfaceOutput().ConnectToSource(surface.ConnectableAPI(), "surface")

        # ── UsdTransform2d ────────────────────────────────────────────────
        uv_xform = _get_or_create_shader(stage, material_path + "/UVTransform", "UsdTransform2d")
        uv_xform.CreateInput("scale",    Sdf.ValueTypeNames.Float2).Set(
            Gf.Vec2f(*NET_LINE_UV_SCALE)
        )
        uv_xform.CreateInput("rotation", Sdf.ValueTypeNames.Float).Set(NET_LINE_UV_ROTATION)
        translation_input = uv_xform.CreateInput("translation", Sdf.ValueTypeNames.Float2)
        translation_input.Set(Gf.Vec2f(0.0, 0.0))
        uv_xform.CreateOutput("result", Sdf.ValueTypeNames.Float2)

        # ── UsdPrimvarReader_float2 ───────────────────────────────────────
        uv_reader = _get_or_create_shader(
            stage, material_path + "/UVReader", "UsdPrimvarReader_float2"
        )
        uv_reader.CreateInput("varname",  Sdf.ValueTypeNames.Token).Set("st")
        uv_reader.CreateInput("fallback", Sdf.ValueTypeNames.Float2).Set(Gf.Vec2f(0.0, 0.0))
        uv_reader.CreateOutput("result",  Sdf.ValueTypeNames.Float2)
        uv_xform.CreateInput("in", Sdf.ValueTypeNames.Float2).ConnectToSource(
            uv_reader.ConnectableAPI(), "result"
        )

        # ── Emissive Texture ──────────────────────────────────────────────
        ec = NET_LINE_EMISSIVE_COLOR
        ei = NET_LINE_EMISSIVE_INTENSITY
        emit_tex = _get_or_create_shader(stage, material_path + "/EmissiveTex", "UsdUVTexture")
        emit_tex.CreateInput("file",  Sdf.ValueTypeNames.Asset).Set(NET_LINE_TEXTURE_PATH)
        emit_tex.CreateInput("wrapS", Sdf.ValueTypeNames.Token).Set("repeat")
        emit_tex.CreateInput("wrapT", Sdf.ValueTypeNames.Token).Set("repeat")
        emit_tex.CreateInput("scale", Sdf.ValueTypeNames.Float4).Set(
            Gf.Vec4f(-1.0, -1.0, -1.0, 1.0)
        )
        emit_tex.CreateInput("bias",  Sdf.ValueTypeNames.Float4).Set(
            Gf.Vec4f(0.0, ec[1] * ei, ec[1] * ei, 1.0)
        )
        emit_tex.CreateInput("st", Sdf.ValueTypeNames.Float2).ConnectToSource(
            uv_xform.ConnectableAPI(), "result"
        )
        emit_tex.CreateOutput("rgb", Sdf.ValueTypeNames.Float3)

        surface.CreateInput("emissiveColor", Sdf.ValueTypeNames.Color3f).ConnectToSource(
            emit_tex.ConnectableAPI(), "rgb"
        )

        # ── Cylinder 전체에 직접 바인딩 ───────────────────────────────────
        UsdShade.MaterialBindingAPI.Apply(cylinder_prim).Bind(material)

        return translation_input


# ── 모듈 레벨 헬퍼 함수 ────────────────────────────────────────────────────────

def _rack_mat_base(rack_prim_path: str) -> str:
    """
    rack_prim_path 하위 Rack_42U/Looks 에 Material 경로 접두사를 생성한다.
    예) .../DataX_Cluster/Rack_42U_A3 → .../Rack_42U_A3/Rack_42U/Looks/NL
    """
    return f"{rack_prim_path}/Rack_42U/Looks/NL"


def _mbps_to_speed(mbps: float) -> float:
    """mbps 값을 UV 애니메이션 속도(units/sec)로 변환한다."""
    raw = NET_LINE_SPEED_MIN + (mbps / NET_LINE_MBPS_SCALE) * (
        NET_LINE_SPEED_MAX - NET_LINE_SPEED_MIN
    )
    return max(NET_LINE_SPEED_MIN, min(NET_LINE_SPEED_MAX, raw))


def _get_or_create_shader(stage, path, shader_id):
    shader = UsdShade.Shader.Get(stage, path)
    if not shader:
        shader = UsdShade.Shader.Define(stage, path)
        shader.CreateIdAttr(shader_id)
    return shader
