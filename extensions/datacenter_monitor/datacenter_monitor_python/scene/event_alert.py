"""
_EventAlertMixin
Kafka WARNING/CRITICAL 이벤트 발생 시 rack 또는 node 위에 ImagePanel.usd를 동적으로
생성하고 EVENT_PANEL_LIFETIME_SEC 후 자동 제거한다.

패널 키잉:
  Stage A/B : rack_prim_path  → rack당 1개
  Stage C   : node_prim_path  → 노드당 1개

타이머 리셋:
  같은 키에 새 이벤트 도착 시 기존 prim 재활용, 만료 시간만 갱신.

포지셔닝 (X축 -90도 회전 후 — Local Y↔Z 교환):
  Stage A/B : (rack_x_center, rack_y_center, rack_z_max)   ← 랙 상단 (Z가 높이축)
  Stage C   : (rack_x_min,   rack_y_min,    node_z_center) ← 노드 정면 왼쪽 (Y가 깊이축)

회전:
  Z축 -90도 회전 → 패널 법선(+X)을 -Y 방향(Rack 정면)으로 정렬.

담당:
  - _init_event_alert()       : 상태 초기화 (SceneManager.__init__에서 호출)
  - show_event_panel()        : ImagePanel prim 생성 또는 타이머 리셋
  - tick_event_panels()       : 매 프레임 만료 prim 제거
  - _clear_all_event_panels() : 모든 패널 즉시 제거 (scene_reset/cleanup에서 호출)

스레드 제약:
  모든 메서드는 Kit 메인 스레드 전용.
  extension.py의 _on_update()에서만 호출한다.
"""

import os
import re
import time

from pxr import Gf, Usd, UsdGeom

from ..global_variables import (
    IMAGE_PANEL_USD_PATH,
    EVENT_PANEL_LIFETIME_SEC,
)


class _EventAlertMixin:
    """Rack/Node 위 ImagePanel 경고 패널 생성·타이머·제거 Mixin."""

    # ──────────────────────────────────────────────────────────────────────
    # 초기화
    # ──────────────────────────────────────────────────────────────────────

    def _init_event_alert(self):
        """상태 초기화. SceneManager.__init__에서 호출."""
        # { key_prim_path: (panel_prim_path, expire_timestamp) }
        # key: Stage A/B → rack_prim_path, Stage C → node_prim_path
        self._active_panels: dict[str, tuple[str, float]] = {}

    # ──────────────────────────────────────────────────────────────────────
    # 패널 생성 / 타이머 리셋
    # ──────────────────────────────────────────────────────────────────────

    def show_event_panel(
        self,
        cluster:    str,
        rack:       str,
        node:       str,
        event_id:   str,
        view_stage: dict,
    ):
        """
        Stage에 따라 rack 또는 node 위에 ImagePanel.usd를 생성하거나
        이미 존재하면 타이머만 리셋한다.

        Args:
            cluster:    Kafka 이벤트 cluster 필드 (소문자, 예: "datax")
            rack:       Kafka 이벤트 rack 필드   (예: "Rack_42U_A3")
            node:       Kafka 이벤트 node 필드   (예: "Box_4U_HDD_1")
            event_id:   Kafka 이벤트 event_id
            view_stage: {"stage": "A"|"B"|"C"|"D"}
        """
        if not self._stage:
            return

        stage_code = view_stage.get("stage", "A")
        print("Stage Code is :", stage_code)

        # ── Stage 결정: A/B → rack, C → node ─────────────────────────────
        if stage_code in ("A", "B"):
            key_path, pos = self._resolve_rack_position(cluster, rack)
        elif stage_code == "C":  # Stage C
            key_path, pos = self._resolve_node_position(cluster, rack, node)

        if key_path is None or pos is None:
            return

        expire_at = time.time() + EVENT_PANEL_LIFETIME_SEC

        # ── 이미 존재하는 패널 → 타이머만 리셋 ──────────────────────────
        if key_path in self._active_panels:
            panel_path, _ = self._active_panels[key_path]
            self._active_panels[key_path] = (panel_path, expire_at)
            print(f"[EventAlert] 타이머 리셋: {panel_path}")
            return

        # ── 신규 패널 생성 ────────────────────────────────────────────────
        if not os.path.isfile(IMAGE_PANEL_USD_PATH):
            print(f"[EventAlert] ImagePanel.usd 없음: {IMAGE_PANEL_USD_PATH}")
            return

        safe_id = re.sub(r"[^A-Za-z0-9_]", "_", event_id) or "panel"
        if safe_id[0].isdigit():
            safe_id = "_" + safe_id
        panel_path = f"{key_path}/EventPanel_{safe_id}"

        prim = self._stage.DefinePrim(panel_path, "Xform")
        prim.GetReferences().AddReference(assetPath=IMAGE_PANEL_USD_PATH)

        # ImagePanel Z 크기의 절반만큼 위로 올려 패널 하단이 기준점에 위치하도록 함
        # (X축 -90도 회전 후 높이축이 Y→Z로 변경됨)
        panel_bbox = UsdGeom.BBoxCache(
            Usd.TimeCode.Default(), ["default", "render"]
        ).ComputeWorldBound(prim).GetRange()
        panel_half_y = (
            (panel_bbox.GetMax()[1] - panel_bbox.GetMin()[1]) / 2.0
            if not panel_bbox.IsEmpty() else 0.0
        )

        xformable = UsdGeom.Xformable(prim)

        translate_op = xformable.AddTranslateOp()
        translate_op.Set(Gf.Vec3d(pos[0], pos[1] + panel_half_y, pos[2]))

        if stage_code == "C":
            # 노드(Box_*)가 X축 -90° 회전 → 패널 생성 시 -90° 상속돼 시계방향 90° 기울어짐
            # +90° X 회전으로 보정해 느낌표가 정방향으로 보이도록 함
            # xformable.AddRotateYOp().Set(90.0)
            translate_op.Set(Gf.Vec3d(pos[0] - 78, pos[1] - panel_half_y, pos[2]))
            xformable.AddRotateXOp().Set(90.0)

        xformable.AddRotateYOp().Set(90.0)   # Rack 정면 방향 정렬
        self._active_panels[key_path] = (panel_path, expire_at)
        print(f"[EventAlert] 패널 생성: {panel_path}  pos={pos}  +half_y={panel_half_y:.2f}")

    # ──────────────────────────────────────────────────────────────────────
    # 위치 계산 헬퍼
    # ──────────────────────────────────────────────────────────────────────

    def _resolve_rack_position(
        self, cluster: str, rack: str
    ) -> tuple[str | None, tuple | None]:
        """
        Stage A/B: rack prim 경로 + (x_center, y_center, z_max) 반환.
        X축 -90도 회전 후 높이축이 Z, 깊이축이 Y로 변경됨.
        실패 시 (None, None).
        """
        rack_key = f"{cluster.lower()}/{rack}"
        rack_prim_path = self._rack_paths.get(rack_key)
        if not rack_prim_path:
            print(f"[EventAlert] rack 없음: {rack_key}")
            return None, None

        rack_prim = self._stage.GetPrimAtPath(rack_prim_path)
        if not rack_prim or not rack_prim.IsValid():
            return None, None

        bbox = UsdGeom.BBoxCache(
            Usd.TimeCode.Default(), ["default", "render"]
        ).ComputeWorldBound(rack_prim).GetRange()
        if bbox.IsEmpty():
            print(f"[EventAlert] rack bbox 비어있음: {rack_prim_path}")
            return None, None

        x_center = (bbox.GetMin()[0] + bbox.GetMax()[0]) / 2.0
        y_max    =  bbox.GetMax()[1]
        z_center = (bbox.GetMin()[2] + bbox.GetMax()[2]) / 2.0

        return rack_prim_path, (x_center, y_max, z_center)

    def _resolve_node_position(
        self, cluster: str, rack: str, node: str
    ) -> tuple[str | None, tuple | None]:
        """
        Stage C: node prim 경로 + (rack_x_min, rack_y_min, node_z_center) 반환.
        X축 -90도 회전 후 축 변환:
          - 높이축: Y → Z  (node_z_center 사용)
          - 정면축: Z → Y  (rack_y_min = 정면, was rack_z_min)
          - 왼쪽 : x_min   (노드 정면 기준 왼쪽)
        실패 시 (None, None).
        """
        # node prim path: _cluster_box_index에 lowercase alias 존재
        node_paths = self._cluster_box_index.get(f"{cluster.lower()}/{node}", [])
        if not node_paths:
            print(f"[EventAlert] node 없음: {cluster}/{node}")
            return None, None
        node_prim_path = node_paths[0]

        node_prim = self._stage.GetPrimAtPath(node_prim_path)
        if not node_prim or not node_prim.IsValid():
            return None, None

        bbox_cache = UsdGeom.BBoxCache(Usd.TimeCode.Default(), ["default", "render"])

        # node bbox → z_center (높이 중심, 회전 후 Z가 높이축)
        node_bbox = bbox_cache.ComputeWorldBound(node_prim).GetRange()
        if node_bbox.IsEmpty():
            print(f"[EventAlert] node bbox 비어있음: {node_prim_path}")
            return None, None
        node_z_center = (node_bbox.GetMin()[2] + node_bbox.GetMax()[2]) / 2.0

        # rack bbox → x_min(왼쪽), y_min(정면, 회전 후 Y가 깊이축)
        rack_key = f"{cluster.lower()}/{rack}"
        rack_prim_path = self._rack_paths.get(rack_key)
        rack_prim = self._stage.GetPrimAtPath(rack_prim_path) if rack_prim_path else None
        if rack_prim and rack_prim.IsValid():
            rack_bbox = bbox_cache.ComputeWorldBound(rack_prim).GetRange()
            if not rack_bbox.IsEmpty():
                x_min = rack_bbox.GetMin()[0]  # 랙 왼쪽
                y_min = rack_bbox.GetMin()[1]  # 랙 정면 (회전 후 Y가 깊이축, 최솟값 = 정면)
            else:
                x_min = node_bbox.GetMin()[0]
                y_min = node_bbox.GetMin()[1]
        else:
            # rack 정보 없으면 node bbox 사용 fallback
            x_min = node_bbox.GetMin()[0]
            y_min = node_bbox.GetMin()[1]

        return node_prim_path, (x_min, y_min, node_z_center)

    # ──────────────────────────────────────────────────────────────────────
    # 매 프레임 타이머 체크
    # ──────────────────────────────────────────────────────────────────────

    def tick_event_panels(self):
        """
        만료된 ImagePanel prim을 제거한다. Kit 메인 스레드(_on_update)에서 매 프레임 호출.
        """
        if not self._stage:
            return
        if not hasattr(self, "_active_panels"):
            return
        now = time.time()
        expired = [
            key for key, (_, expire_at) in list(self._active_panels.items())
            if now >= expire_at
        ]
        for key in expired:
            panel_path, _ = self._active_panels.pop(key)
            prim = self._stage.GetPrimAtPath(panel_path)
            if prim and prim.IsValid():
                self._stage.RemovePrim(panel_path)
                print(f"[EventAlert] 패널 만료 제거: {panel_path}")

    # ──────────────────────────────────────────────────────────────────────
    # 전체 패널 즉시 제거
    # ──────────────────────────────────────────────────────────────────────

    def _clear_all_event_panels(self):
        """
        모든 활성 ImagePanel prim을 즉시 제거한다.
        scene_reset() 및 cleanup()에서 호출한다.
        """
        if not hasattr(self, "_active_panels"):
            return
        if self._stage:
            for key, (panel_path, _) in list(self._active_panels.items()):
                prim = self._stage.GetPrimAtPath(panel_path)
                if prim and prim.IsValid():
                    self._stage.RemovePrim(panel_path)
        self._active_panels.clear()
