"""
scalex_dynamic_props - Main Extension
USD Viewer 기반. Isaac Sim / 물리엔진 의존성 없음.

역할:
  - Kit 앱 시작 시 omni.usd 스테이지 이벤트 구독.
  - 스테이지의 references/payload 로드 완료(ASSETS_LOADED) 시점에 SceneBuilder.rebuild() 1회 호출
    → 클러스터별 색상 under-panel + ScaleX_POD_view plane 을 "세션 레이어"에만 동적 생성.
  - 원본 .usd 는 절대 수정하지 않는다(오염 방지). datacenter_monitor 가 여는 스테이지 위에서 동작.

datacenter_monitor 의 Extension 과 동일한 USD Viewer 규약:
  - omni.physx 없음 / isaacsim.* 없음 / timeline play·stop 없음
  - USD 조작은 반드시 Kit 메인 스레드에서만 (스테이지 이벤트 콜백은 메인 스레드)
"""

import traceback

import omni.ext
import omni.usd
from omni.usd import StageEventType

from .global_variables import EXTENSION_TITLE, DYNAMIC_PROPS_ENABLED_ENV, is_dynamic_props_enabled
from .scene_builder import SceneBuilder


def _log(msg: str):
    print(f"[{EXTENSION_TITLE}] {msg}")


class Extension(omni.ext.IExt):
    """세션 레이어에 under-panel / view plane 을 동적 생성하는 USD Viewer용 Extension."""

    def on_startup(self, ext_id: str):
        self._ext_id = ext_id
        self._builder: SceneBuilder | None = None
        # [수정] 텍스처/MDL 로드가 다시 ASSETS_LOADED 를 fire 해도 같은 스테이지는 1회만 빌드한다.
        self._built_stage_key: str | None = None
        self._skip_logged_stage_key: str | None = None
        self._disabled_logged_stage_key: str | None = None
        _log("on_startup")

        ctx = omni.usd.get_context()
        # 스테이지 열기/에셋 로드/닫기 이벤트 구독 (콜백은 메인 스레드에서 호출됨).
        self._stage_sub = ctx.get_stage_event_stream().create_subscription_to_pop(
            self._on_stage_event, name="scalex_dynamic_props.stage_event"
        )

        # 이미 스테이지가 떠 있으면(익스텐션을 나중에 켠 경우) 즉시 1회 시도.
        stage = ctx.get_stage()
        if stage is not None:
            self._try_build("startup")

    def on_shutdown(self):
        _log("on_shutdown")
        # 생성한 prim 제거 + 세션 오버레이 레이어 분리.
        if self._builder is not None:
            try:
                self._builder.teardown()
            except Exception as exc:
                _log(f"[경고] teardown 실패: {exc}")
            self._builder = None
        self._reset_stage_guard("shutdown", clear_builder=False)
        # 구독 해제.
        self._stage_sub = None

    # ── 스테이지 이벤트 ───────────────────────────────────────────────────────
    def _on_stage_event(self, event):
        if event.type == int(StageEventType.OPENED):
            # [수정] 새 스테이지는 다시 1회 빌드할 수 있게 가드 리셋.
            self._reset_stage_guard("opened")
        elif event.type == int(StageEventType.ASSETS_LOADED):
            # references/payload 로드 완료 → 소스 mesh 읽기가 안전한 시점.
            self._try_build("assets_loaded")
        elif event.type in (int(StageEventType.CLOSING), int(StageEventType.CLOSED)):
            # 스테이지가 닫히면 이전 빌더 참조를 버린다(레이어는 스테이지와 함께 사라짐).
            self._reset_stage_guard("closing_or_closed")

    def _stage_key(self, stage) -> str:
        """[수정] 현재 스테이지 인스턴스와 root layer 를 함께 구분하는 키."""
        root = stage.GetRootLayer()
        root_ident = root.identifier if root is not None else "<anonymous>"
        return f"{id(stage)}:{root_ident}"

    def _reset_stage_guard(self, reason: str, clear_builder: bool = True):
        self._built_stage_key = None
        self._skip_logged_stage_key = None
        self._disabled_logged_stage_key = None
        if clear_builder:
            self._builder = None
        _log(f"스테이지 빌드 가드 리셋 (trigger={reason})")

    def _try_build(self, reason: str):
        stage = omni.usd.get_context().get_stage()
        if stage is None:
            return
        stage_key = self._stage_key(stage)
        if not is_dynamic_props_enabled():
            # [수정] 비활성화 시 아무것도 author 하지 않고 기존 세션 오버레이를 제거한다.
            self._teardown_stage_overlay(stage, stage_key, reason)
            self._built_stage_key = None
            self._skip_logged_stage_key = None
            return
        self._disabled_logged_stage_key = None
        if self._built_stage_key == stage_key:
            if self._skip_logged_stage_key != stage_key:
                _log(f"[수정] build 건너뜀: 이미 빌드된 스테이지 (trigger={reason})")
                self._skip_logged_stage_key = stage_key
            return
        try:
            # [수정] 스테이지마다 1회만 빌드하여 ASSETS_LOADED 재진입으로 인한 Clear+재author churn 방지.
            if self._builder is None or self._built_stage_key != stage_key:
                self._builder = SceneBuilder(stage)
            self._builder.rebuild()
            self._built_stage_key = stage_key
            self._skip_logged_stage_key = None
            _log(f"build 성공 (trigger={reason})")
        except Exception as exc:
            self._built_stage_key = None
            _log(f"[오류] build 실패 (trigger={reason}): {exc}")
            traceback.print_exc()

    def _teardown_stage_overlay(self, stage, stage_key: str, reason: str):
        try:
            # [수정] OFF 전환은 항상 현재 스테이지의 세션 오버레이를 대상으로 정리한다.
            SceneBuilder(stage).teardown()
            self._builder = None
            if self._disabled_logged_stage_key != stage_key:
                _log(
                    f"[수정] {DYNAMIC_PROPS_ENABLED_ENV}=OFF: 세션 오버레이 정리 완료 "
                    f"(trigger={reason})"
                )
                self._disabled_logged_stage_key = stage_key
        except Exception as exc:
            _log(f"[경고] 비활성화 teardown 실패 (trigger={reason}): {exc}")
