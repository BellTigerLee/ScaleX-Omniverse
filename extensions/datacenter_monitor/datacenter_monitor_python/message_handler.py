"""
MessageHandler
React ↔ Kit WebRTC 양방향 메시지 처리.

4단계 View 계층:
  Stage A: 전체씬 (Cluster 목록)
  Stage B: Cluster 포커스 (선택된 Cluster, 다른 Cluster 숨김)
  Stage C: Rack 포커스 (선택된 Rack, 다른 Rack 숨김)
  Stage D: Node 인스펙션 (선택된 Server pop-forward)

수신 메시지 종류 (React → Kit):
  { type: "cluster_focus",            prim: "/World/.../DataX_Cluster",  hide_others: true }
  { type: "rack_focus",               prim: "/World/.../Rack_A",          hide_others: true }
  { type: "node_inspect",             prim: "/World/.../Server_01" }
  { type: "node_deselect",            return_rack: "/World/.../Rack_A" }
  { type: "rack_deselect_to_cluster", return_cluster: "/World/.../DataX_Cluster" }
  { type: "scene_reset" }
  { type: "get_topology" }
  { type: "replay_start" }
  { type: "replay_stop" }
  { type: "set_transparency_mode", enabled: true/false }

통신 흐름:
  React → Kit:
    AppStreamer.sendMessage({ event_type: "datacenter_monitor", payload: {...} })
    → omni.kit.livestream.messaging 확장이 validate 후
      queue_event("datacenter_monitor", {type:..., sender_id:...}) 발생
    → carb.eventdispatcher 로 수신 (_on_incoming_raw)

  Kit → React:
    omni.kit.app.queue_event("omni.kit.livestream.send_message", {"message": json_str})
    → WebRTC 스트리밍 레이어가 React의 onCustomEvent 로 전달
"""

import json

import carb
import carb.eventdispatcher
import omni.kit.app
import omni.usd

from .scene_manager import SceneManager

# omni.kit.livestream.messaging 확장의 extension.toml 에서 확인된 이벤트 이름
_SEND_MESSAGE_EVENT    = "omni.kit.livestream.send_message"
_RECEIVE_EVENT_TYPE    = "datacenter_monitor"   # messaging 확장이 dispatch하는 이벤트 이름


class MessageHandler:
    """
    React(WebRTC client) ↔ Omniverse Kit(Extension) 간 메시지 라우터.
    """

    def __init__(
        self,
        scene_manager: SceneManager,
        view_stage_ref: dict,
        on_view_stage_change,
        on_replay_start=None,
        on_replay_stop=None,
    ):
        self._scene_manager        = scene_manager
        self._view_stage           = view_stage_ref
        self._on_view_stage_change = on_view_stage_change
        self._on_replay_start      = on_replay_start
        self._on_replay_stop       = on_replay_stop
        self._incoming_sub         = None

    # ─────────────────────────────────────────────────────────────────────
    # 수신 핸들러 등록 / 해제
    # ─────────────────────────────────────────────────────────────────────

    def register_incoming_handler(self):
        """
        React → Kit 메시지 수신 핸들러 등록.

        omni.kit.livestream.messaging 확장은 React 메시지를 수신하면
        carb.eventdispatcher 로 event_name = "datacenter_monitor" 이벤트를 발생시킵니다.
        여기서 그 이벤트를 구독합니다.
        """
        ed = carb.eventdispatcher.get_eventdispatcher()
        self._incoming_sub = ed.observe_event(
            event_name=_RECEIVE_EVENT_TYPE,
            on_event=self._on_incoming_raw,
        )
        print("[MessageHandler] React → Kit 수신 등록 완료 (event: datacenter_monitor)")

    def unregister(self):
        if self._incoming_sub:
            self._incoming_sub.reset()
            self._incoming_sub = None
        print("[MessageHandler] 수신 핸들러 해제")

    # ─────────────────────────────────────────────────────────────────────
    # 수신 처리
    # ─────────────────────────────────────────────────────────────────────

    def _on_incoming_raw(self, event):
        """
        carb.eventdispatcher 이벤트를 받아 파싱합니다.

        messaging 확장이 React payload dict에 sender_id를 추가한 후
        queue_event("datacenter_monitor", payload_dict) 로 dispatch합니다.
        즉 event.payload = { "type": "rack_focus", "prim": "...", "sender_id": 123 }
        """
        try:
            payload = dict(event.payload) if event.payload else {}
        except Exception as e:
            print(f"[MessageHandler] payload 변환 실패: {e}")
            return

        payload.pop("sender_id", None)

        if not payload.get("type"):
            print(f"[MessageHandler] type 없는 payload: {payload}")
            return

        self._on_incoming_message(json.dumps(payload))

    def _on_incoming_message(self, data: str):
        """JSON 문자열을 파싱하여 적절한 SceneManager 메서드를 호출합니다."""
        try:
            msg = json.loads(data)
        except json.JSONDecodeError as e:
            print(f"[MessageHandler] JSON 파싱 실패: {e} | raw: {data[:100]}")
            return

        msg_type = msg.get("type")
        print(f"[MessageHandler] ← React: {msg_type}")

        if msg_type == "cluster_focus":
            # Stage A → B
            prim_path   = msg.get("prim", "")
            hide_others = msg.get("hide_others", True)
            if prim_path:
                self._scene_manager.cluster_focus(prim_path, hide_others)
                cluster_id = prim_path.split("/")[-1]
                self._on_view_stage_change({
                    "stage":     "B",
                    "clusterId": cluster_id,
                    "primPath":  prim_path,
                })

        elif msg_type == "rack_focus":
            # Stage B → C
            print("msg 내용: ", msg)
            prim_path   = msg.get("prim", "")
            hide_others = msg.get("hide_others", True)
            if prim_path:
                self._scene_manager.rack_focus(prim_path, hide_others)
                parts      = prim_path.split("/")
                rack_id    = parts[-1]
                cluster_id = next((p for p in reversed(parts) if p.endswith("_Cluster")), "")
                print("Prim path있음")
                print("Rack ID: ", rack_id, "Cluster ID: ", cluster_id, "prim_path: ", prim_path)
                self._on_view_stage_change({
                    "stage":     "C",
                    "clusterId": cluster_id,
                    "rackId":    rack_id,
                    "primPath":  prim_path,
                })
            print("Rack Focus...!")

        elif msg_type == "node_inspect":
            # Stage C → D
            prim_path = msg.get("prim", "")
            if prim_path:
                self._scene_manager.node_inspect(prim_path)
                parts      = prim_path.split("/")
                node_id    = parts[-1]
                rack_id    = parts[-2] if len(parts) >= 2 else ""
                cluster_id = next((p for p in reversed(parts) if p.endswith("_Cluster")), "")
                self._on_view_stage_change({
                    "stage":     "D",
                    "clusterId": cluster_id,
                    "rackId":    rack_id,
                    "nodeId":    node_id,
                    "primPath":  prim_path,
                })

        elif msg_type == "node_deselect":
            # Stage D → C
            return_rack = msg.get("return_rack", "")
            current     = self._view_stage
            node_path   = current.get("primPath", "")
            if node_path and return_rack:
                self._scene_manager.node_deselect(node_path, return_rack)
                parts      = return_rack.split("/")
                rack_id    = parts[-1]
                cluster_id = next((p for p in reversed(parts) if p.endswith("_Cluster")), "")
                self._on_view_stage_change({
                    "stage":     "C",
                    "clusterId": cluster_id,
                    "rackId":    rack_id,
                    "primPath":  return_rack,
                })

        elif msg_type == "rack_deselect_to_cluster":
            # Stage C → B
            return_cluster = msg.get("return_cluster", "")
            current        = self._view_stage
            rack_path      = current.get("primPath", "")
            if return_cluster:
                self._scene_manager.rack_deselect_to_cluster(rack_path, return_cluster)
                cluster_id = return_cluster.split("/")[-1]
                self._on_view_stage_change({
                    "stage":     "B",
                    "clusterId": cluster_id,
                    "primPath":  return_cluster,
                })

        elif msg_type == "scene_reset":
            # 어느 단계에서든 → Stage A
            self._scene_manager.scene_reset()
            self._on_view_stage_change({"stage": "A"})
            self.send_selection_changed("None", [])

        elif msg_type == "get_topology":
            topology = self._scene_manager.get_cached_topology()
            if topology:
                self.send_scene_manifest(topology)
            else:
                print("[MessageHandler] get_topology: stage 아직 미준비")

        elif msg_type == "replay_start":
            if self._on_replay_start:
                self._on_replay_start()

        elif msg_type == "replay_stop":
            if self._on_replay_stop:
                self._on_replay_stop()

        elif msg_type == "set_transparency_mode":
            enabled = msg.get("enabled", True)
            self._scene_manager.set_transparency_mode(bool(enabled))

        else:
            print(f"[MessageHandler] 알 수 없는 메시지 타입: {msg_type}")

    # ─────────────────────────────────────────────────────────────────────
    # 송신: Kit → React
    # ─────────────────────────────────────────────────────────────────────

    def _send_to_client(self, payload_dict: dict):
        """
        Kit → React로 커스텀 이벤트를 전송합니다.

        omni.kit.livestream.messaging 의 send_message_event (extension.toml):
          exts."omni.kit.livestream.messaging".send_message_event = "omni.kit.livestream.send_message"
        WebRTC 스트리밍 레이어가 이 이벤트의 "message" 필드를
        React의 onCustomEvent 로 JSON 파싱 후 전달합니다.
        """
        data_str = json.dumps(payload_dict)
        try:
            omni.kit.app.queue_event(_SEND_MESSAGE_EVENT, {"message": data_str})
            print(f"[MessageHandler] → 클라이언트 전송: {data_str[:80]}")
        except Exception as e:
            print(f"[MessageHandler] 전송 실패: {e} | {data_str[:60]}")

    def send_selection_changed(self, active_prim: str, selected_prims: list):
        self._send_to_client({
            "event_type": "selection_changed",
            "payload": {
                "active":   active_prim,
                "selected": selected_prims,
            },
        })

    def send_scene_manifest(self, topology: dict):
        """씬 topology를 React로 전송합니다."""
        self._send_to_client({
            "event_type": "scene_manifest",
            "payload": topology,
        })
        print(f"[MessageHandler] scene_manifest 전송: {len(topology.get('racks', []))}개 rack")

    def send_cluster_rank(self, payload: dict):
        """stageab cluster-rank 페이로드를 React 로 포워딩 (console.log 용)."""
        self._send_to_client({
            "event_type": "cluster_rank",
            "payload":    payload,
        })

    def send_alert_notification(self, rack_id: str, node_id: str, status: str, metrics: dict):
        self._send_to_client({
            "event_type": "alert_notification",
            "payload": {
                "rack_id": rack_id,
                "node_id": node_id,
                "status":  status,
                "metrics": metrics,
            },
        })
