"""
datacenter_monitor - Main Extension
USD Viewer 기반. Isaac Sim / 물리엔진 의존성 없음.

역할:
  - Kit 앱 시작 시 Kafka 구독 스레드 시작
  - React ↔ Kit WebRTC 메시지 연결
  - Kit 매 프레임마다 Kafka 큐를 drain하여 SceneManager로 전달
  - Stage 이벤트(열기/닫기) 처리
"""

import queue
import gc
import time

import carb
import omni.ext
import omni.kit.app
import omni.usd
from omni.usd import StageEventType

from .global_variables import (
    EXTENSION_TITLE,
    MAIN_STAGE_USD_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_LIVE,
    KAFKA_TOPIC_REPLAY,
    KAFKA_TOPIC_EVENT,
    KAFKA_TOPIC_REPLAY_EVENT,
    KAFKA_TOPIC_NODE_STATE,
    KAFKA_TOPIC_CLUSTER_RANK,
    KAFKA_GROUP_ID,
    NODE_INDEX_URL,
    DEV_FAKE_NODE_MAPPING,
)
from .kafka_subscriber import KafkaSubscriber, NodeStateSubscriber, ClusterRankSubscriber
from .scene_manager import SceneManager
from .message_handler import MessageHandler


class Extension(omni.ext.IExt):
    """
    USD Viewer용 Extension 메인 클래스.
    Isaac Sim 템플릿과 달리:
      - omni.physx 없음
      - isaacsim.* 없음
      - timeline play/stop 없음
      - 매 프레임 업데이트는 omni.kit.app update 이벤트로 처리
    """

    def on_startup(self, ext_id: str):
        self._ext_id = ext_id
        print(f"[{EXTENSION_TITLE}] on_startup")

        # ── 공유 Kafka 데이터 큐 ────────────────────────────────────────────
        # Kafka 스레드(producer) → Kit 메인 스레드(consumer) 단방향 큐
        # USD 조작은 반드시 메인 스레드에서만 해야 하므로 큐를 통해 전달
        self._kafka_queue: queue.Queue = queue.Queue(maxsize=500)

        # ── 현재 View Stage 상태 ────────────────────────────────────────────
        # "A": 전체씬(Cluster 목록)
        # "B": Cluster 포커스
        # "C": Rack 포커스
        # "D": Node 인스펙션
        self._view_stage: dict = {"stage": "A"}

        # ── SceneManager: USD 씬 조작 담당 ──────────────────────────────────
        self._scene_manager = SceneManager()
        self._stage_ready_delay_frames = -1
        self._stage_ready_attempts_remaining = 0

        # ── Topology API 에서 (cluster, node) → prim_name 인덱스 로드 ─────
        # URL 미설정 시 fallback 휴리스틱(BOX_ prefix) 으로 동작. 실패해도 구독은 계속.
        if NODE_INDEX_URL:
            self._scene_manager.load_node_index_from_url(NODE_INDEX_URL)
        else:
            print(
                f"[{EXTENSION_TITLE}] NODE_INDEX_URL 미설정 — fallback 휴리스틱으로 동작. "
                f"config/env.<profile> 에 TOPOLOGY_URL 을 추가하면 정확 매칭 활성화."
            )

        # Dev 전용 — 미등록 node 를 topology 의 임의 prim 에 stable-hash 로 매핑
        self._scene_manager.set_dev_fake_mapping(DEV_FAKE_NODE_MAPPING)

        # ── MessageHandler: React ↔ Kit WebRTC 메시지 담당 ─────────────────
        self._message_handler = MessageHandler(
            scene_manager=self._scene_manager,
            view_stage_ref=self._view_stage,
            on_view_stage_change=self._on_view_stage_change,
            on_replay_start=self._on_replay_start,
            on_replay_stop=self._on_replay_stop,
        )

        # ── Kafka 구독 스레드 시작 (metrics) ────────────────────────────────
        self._kafka_subscriber = KafkaSubscriber(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_LIVE,
            group_id=KAFKA_GROUP_ID,
            data_queue=self._kafka_queue,
        )
        self._kafka_subscriber.start()

        # ── Node-state 구독자 (canonical envelope, 2026-04-17-... spec) ────
        self._event_queue: queue.Queue = queue.Queue(maxsize=100)
        self._event_subscriber = NodeStateSubscriber(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_NODE_STATE,
            group_id=KAFKA_GROUP_ID + "-node-state",
            data_queue=self._event_queue,
        )
        self._event_subscriber.start()

        # ── Cluster-rank 구독자 (stageab, id=="cluster-rank") ──────────────
        self._cluster_rank_queue: queue.Queue = queue.Queue(maxsize=100)
        self._cluster_rank_subscriber = ClusterRankSubscriber(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_CLUSTER_RANK,
            group_id=KAFKA_GROUP_ID + "-cluster-rank",
            data_queue=self._cluster_rank_queue,
        )
        self._cluster_rank_subscriber.start()

        # ── USD Stage 이벤트 구독 ───────────────────────────────────────────
        self._usd_context = omni.usd.get_context()
        stage_events = self._usd_context.get_stage_event_stream()
        self._stage_event_sub = stage_events.create_subscription_to_pop(
            self._on_stage_event, name=f"{EXTENSION_TITLE}_stage"
        )

        # ── Kit 매 프레임 업데이트 구독 ─────────────────────────────────────
        # 여기서 Kafka 큐를 drain하고 SceneManager를 업데이트합니다.
        app = omni.kit.app.get_app()
        self._update_sub = app.get_update_event_stream().create_subscription_to_pop(
            self._on_update, name=f"{EXTENSION_TITLE}_update"
        )

        # ── WebRTC 메시지 수신 등록 ─────────────────────────────────────────
        self._message_handler.register_incoming_handler()

        # ── USD 자동 로드 ───────────────────────────────────────────────────
        # MAIN_STAGE_USD_PATH가 설정되어 있으면 Extension 시작 시 자동으로 열립니다.
        # open_stage() 완료 후 OPENED stage 이벤트 → _on_stage_event → _on_stage_ready
        if MAIN_STAGE_USD_PATH:
            from pathlib import Path
            path = Path(MAIN_STAGE_USD_PATH)
            if path.exists():
                print(f"[{EXTENSION_TITLE}] USD 자동 로드: {path}")
                self._usd_context.open_stage(str(path))
                # OPENED 이벤트가 비동기로 발생하므로 여기서 _on_stage_ready를 직접 호출하지 않습니다.
            else:
                print(f"[{EXTENSION_TITLE}] ❌ USD 파일 없음: {path}")
                print(f"[{EXTENSION_TITLE}]    global_variables.py의 MAIN_STAGE_USD_PATH를 확인하세요.")
        elif self._usd_context.get_stage():
            # USD 경로 미설정이지만 이미 열려있는 stage가 있으면 그걸 사용
            self._schedule_stage_ready(delay_frames=1)

    def on_shutdown(self):
        print(f"[{EXTENSION_TITLE}] on_shutdown")

        self._kafka_subscriber.stop()
        self._event_subscriber.stop()
        self._cluster_rank_subscriber.stop()
        self._message_handler.unregister()

        self._stage_event_sub = None
        self._update_sub = None
        self._scene_manager.cleanup()
        gc.collect()

    # ── Kit 매 프레임 업데이트 ────────────────────────────────────────────
    def _on_update(self, event):
        """
        Kit 메인 스레드에서 매 프레임 호출됩니다.
        Kafka 큐를 drain하여 SceneManager에 전달합니다.
        USD 조작은 반드시 여기(메인 스레드)에서 해야 합니다.
        """
        # 카메라 애니메이션 1 스텝 진행
        self._process_stage_ready_retry()
        self._scene_manager.tick_camera_animation()
        self._scene_manager.tick_event_panels()

        # metrics 큐에서 최대 20개씩 처리 (프레임 드랍 방지)
        processed = 0
        while not self._kafka_queue.empty() and processed < 20:
            try:
                kafka_msg = self._kafka_queue.get_nowait()
                current_stage = self._view_stage["stage"]
                # print(
                #     f"[Extension] 큐 처리 stage={current_stage} "
                #     f"cluster={kafka_msg.get('cluster') or kafka_msg.get('cluster_id')} "
                #     f"node={kafka_msg.get('node') or kafka_msg.get('box_id','-')} "
                #     f"status={kafka_msg.get('status') or kafka_msg.get('metrics',{}).get('status','?')}"
                # )
                # legacy: superseded by node-state.events → SceneManager.apply_node_state (2026-04-17).
                # dashboard 토픽의 status 필드는 placeholder 이므로 색상 결정 경로에서 제외.
                # self._scene_manager.update_node_color_from_kafka(
                #     kafka_msg, self._view_stage
                # )
                processed += 1
            except queue.Empty:
                break

        # node-state envelope 큐에서 최대 10개씩 처리 (canonical schema)
        ev_processed = 0
        while not self._event_queue.empty() and ev_processed < 10:
            try:
                env = self._event_queue.get_nowait()
                self._scene_manager.apply_node_state(env)
                ev_processed += 1
            except queue.Empty:
                break

        # cluster-rank 큐 drain → React 로 포워딩 (최대 10개/frame)
        rank_processed = 0
        while not self._cluster_rank_queue.empty() and rank_processed < 10:
            try:
                env = self._cluster_rank_queue.get_nowait()
                self._message_handler.send_cluster_rank(env)
                rank_processed += 1
            except queue.Empty:
                break

        # legacy: HEALTH_TRANSITION severity → show_event_panel 경로.
        #         canonical envelope 에는 severity 필드가 없으므로 비활성화.
        #         Phase 2 에서 reasons-driven 알림 UI 를 별도 spec 으로 재설계 예정.
        # while not self._event_queue.empty() and ev_processed < 10:
        #     try:
        #         ev = self._event_queue.get_nowait()
        #         severity = ev.get("severity", "")
        #         if severity in ("WARNING", "CRITICAL"):
        #             self._scene_manager.show_event_panel(
        #                 cluster=ev.get("cluster", ""),
        #                 rack=ev.get("rack", ""),
        #                 node=ev.get("node", ""),
        #                 event_id=ev.get("event_id", ""),
        #                 view_stage=self._view_stage,
        #             )
        #         ev_processed += 1
        #     except queue.Empty:
        #         break

        # node-state emissive breathing pulse (매 프레임)
        self._scene_manager.tick_pulse(time.monotonic())

    # ── Stage 이벤트 ─────────────────────────────────────────────────────
    def _on_stage_event(self, event):
        if event.type == int(StageEventType.OPENED):
            print(f"[{EXTENSION_TITLE}] Stage opened")
            self._schedule_stage_ready(delay_frames=30)

        elif event.type == int(StageEventType.CLOSED):
            print(f"[{EXTENSION_TITLE}] Stage closed")
            self._stage_ready_delay_frames = -1
            self._stage_ready_attempts_remaining = 0
            self._scene_manager.cleanup()
            self._view_stage["stage"] = "A"

        elif event.type == int(StageEventType.SELECTION_CHANGED):
            self._on_selection_changed()

    def _on_selection_changed(self):
        """
        뷰포트에서 prim을 클릭했을 때 호출됩니다.
        선택된 prim path를 React에 전달하여 Stage 전환을 트리거합니다.

        내부에서 clear_selected_prim_paths() 호출 시에도 이 이벤트가 발생하지만,
        그 경우 selected가 비어있으므로 아무것도 전송하지 않습니다.
        """
        selected = self._usd_context.get_selection().get_selected_prim_paths()
        if not selected:
            return  # clear_selected_prim_paths() 내부 호출로 인한 이벤트 무시

        active = selected[0]
        print(f"[{EXTENSION_TITLE}] 뷰포트 클릭: {active}")
        self._message_handler.send_selection_changed(active, list(selected))

    def _schedule_stage_ready(self, delay_frames: int):
        """Stage/reference 로딩이 끝날 시간을 주고 topology 초기화를 지연 실행합니다."""
        self._stage_ready_delay_frames = max(0, delay_frames)
        self._stage_ready_attempts_remaining = 20
        print(
            f"[{EXTENSION_TITLE}] Stage ready 예약: "
            f"{self._stage_ready_delay_frames} frames 후 시도"
        )

    def _process_stage_ready_retry(self):
        """초기 stage 로딩 타이밍 경쟁을 피하기 위해 cube 생성까지 재시도합니다."""
        if self._stage_ready_attempts_remaining <= 0:
            return
        if self._stage_ready_delay_frames > 0:
            self._stage_ready_delay_frames -= 1
            return

        if self._on_stage_ready():
            self._stage_ready_attempts_remaining = 0
            self._stage_ready_delay_frames = -1
            return

        self._stage_ready_attempts_remaining -= 1
        if self._stage_ready_attempts_remaining <= 0:
            print(f"[{EXTENSION_TITLE}] Stage ready 실패: cube 생성 안 됨")
            self._stage_ready_delay_frames = -1
            return
        self._stage_ready_delay_frames = 15
        print(
            f"[{EXTENSION_TITLE}] Stage ready 재시도 예약: "
            f"남은 시도 {self._stage_ready_attempts_remaining}"
        )

    def _on_stage_ready(self) -> bool:
        """Stage가 열렸을 때 씬 초기화 및 topology를 React에 전송합니다."""
        self._scene_manager.initialize()

        # [수정 포인트 - SCENE MANIFEST]
        # 씬에서 rack/node 구조를 자동으로 발견하여 React에 전송합니다.
        # React의 MOCK_TOPOLOGY를 이 데이터로 교체할 수 있습니다.
        topology = self._scene_manager.discover_topology()
        cube_count = len(getattr(self._scene_manager, "_glass_cube_cache", {}))
        if topology and cube_count:
            self._message_handler.send_scene_manifest(topology)
            print(f"[{EXTENSION_TITLE}] Stage ready 완료: cube={cube_count}")
            return True
        print(f"[{EXTENSION_TITLE}] Stage ready 대기: topology={bool(topology)} cube={cube_count}")
        return False

    # ── View Stage 변경 콜백 ─────────────────────────────────────────────
    def _on_view_stage_change(self, new_stage: dict):
        """MessageHandler가 stage 전환을 결정했을 때 호출됩니다."""
        self._view_stage.update(new_stage)
        print(f"[{EXTENSION_TITLE}] View stage → {self._view_stage}")

    # ── Replay 토픽 전환 콜백 ─────────────────────────────────────────────
    def _on_replay_start(self):
        """React가 replay_start를 전송하면 Kafka 구독 토픽을 replay 토픽으로 전환합니다."""
        print(f"[{EXTENSION_TITLE}] Replay 시작 → Kafka 토픽 전환: {KAFKA_TOPIC_REPLAY} / {KAFKA_TOPIC_REPLAY_EVENT}")
        self._clear_kafka_queue()  # live 잔여 메시지 제거 후 전환
        self._clear_event_queue()
        self._kafka_subscriber.switch_topic(KAFKA_TOPIC_REPLAY)
        self._event_subscriber.switch_topic(KAFKA_TOPIC_REPLAY_EVENT)

    def _on_replay_stop(self):
        """React가 replay_stop을 전송하면 Kafka 구독 토픽을 live 토픽으로 복원합니다."""
        print(f"[{EXTENSION_TITLE}] Replay 종료 → Kafka 토픽 복원: {KAFKA_TOPIC_LIVE}")
        self._clear_kafka_queue()  # replay 잔여 메시지 제거 후 복원
        self._clear_event_queue()
        self._kafka_subscriber.switch_topic(KAFKA_TOPIC_LIVE)
        self._event_subscriber.switch_topic(KAFKA_TOPIC_EVENT)

    def _clear_kafka_queue(self):
        """큐의 잔여 메시지를 모두 제거합니다 (topic 전환 시 데이터 혼입 방지)."""
        import queue as _queue
        cleared = 0
        while True:
            try:
                self._kafka_queue.get_nowait()
                cleared += 1
            except _queue.Empty:
                break
        if cleared:
            print(f"[{EXTENSION_TITLE}] 큐 초기화: {cleared}개 잔여 메시지 제거")

    def _clear_event_queue(self):
        """이벤트 큐의 잔여 메시지를 모두 제거합니다 (topic 전환 시 데이터 혼입 방지)."""
        import queue as _queue
        cleared = 0
        while True:
            try:
                self._event_queue.get_nowait()
                cleared += 1
            except _queue.Empty:
                break
        if cleared:
            print(f"[{EXTENSION_TITLE}] event 큐 초기화: {cleared}개 잔여 메시지 제거")
