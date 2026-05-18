"""
Node-state pulse 시각 매핑 (pure, USD 비의존).

**One-shot 모델:** Kafka 메시지 수신 시마다 호출자는 `elapsed_sec = 0` 에서 재시작해
tick 마다 compute_emissive(status, elapsed) 를 호출한다. 함수는 rise 구간에서는
sin quarter (0 → peak), tail 구간에서는 exponential decay (peak → ~0) RGB 를
반환하고, 총 pulse 길이 이후에는 None 을 반환해 "pulse 종료" 를 통지한다.

본 모듈의 모든 함수는 순수 함수로 pytest 단위 테스트 대상.
"""

from __future__ import annotations

import math
from typing import NamedTuple, Optional, Tuple

from ..global_variables import (
    NODE_PULSE_DISCONNECTED_EMISSIVE_COLOR,
    NODE_PULSE_DISCONNECTED_MAX,
    NODE_PULSE_DISCONNECTED_MIN,
    NODE_PULSE_DISCONNECTED_PERIOD_SEC,
    NODE_PULSE_DISCONNECTED_TAIL_SEC,
    NODE_PULSE_DISCONNECTED_TAIL_TAU,
    NODE_PULSE_HEALTHY_EMISSIVE_COLOR,
    NODE_PULSE_HEALTHY_MAX,
    NODE_PULSE_HEALTHY_MIN,
    NODE_PULSE_HEALTHY_PERIOD_SEC,
    NODE_PULSE_HEALTHY_TAIL_SEC,
    NODE_PULSE_HEALTHY_TAIL_TAU,
)


class PulseParams(NamedTuple):
    emissive_color: Tuple[float, float, float]
    i_min:          float
    i_max:          float
    period:         float
    tail_sec:       float
    tail_tau:       float


def pulse_params(status) -> Optional[PulseParams]:
    """status → PulseParams 매핑. no-op 상태(UNKNOWN/WARNING/CRITICAL/미지값) 는 None."""
    if status == "HEALTHY":
        return PulseParams(
            emissive_color=NODE_PULSE_HEALTHY_EMISSIVE_COLOR,
            i_min=NODE_PULSE_HEALTHY_MIN,
            i_max=NODE_PULSE_HEALTHY_MAX,
            period=NODE_PULSE_HEALTHY_PERIOD_SEC,
            tail_sec=NODE_PULSE_HEALTHY_TAIL_SEC,
            tail_tau=NODE_PULSE_HEALTHY_TAIL_TAU,
        )
    if status == "DISCONNECTED":
        return PulseParams(
            emissive_color=NODE_PULSE_DISCONNECTED_EMISSIVE_COLOR,
            i_min=NODE_PULSE_DISCONNECTED_MIN,
            i_max=NODE_PULSE_DISCONNECTED_MAX,
            period=NODE_PULSE_DISCONNECTED_PERIOD_SEC,
            tail_sec=NODE_PULSE_DISCONNECTED_TAIL_SEC,
            tail_tau=NODE_PULSE_DISCONNECTED_TAIL_TAU,
        )
    return None


def compute_emissive(status, elapsed_sec: float) -> Optional[Tuple[float, float, float]]:
    """
    status + elapsed_sec → emissive RGB 튜플.

    반환값:
      - `None` 이면 "pulse 정지" 시그널. 사유:
          * status 가 no-op (UNKNOWN/WARNING/CRITICAL/미지값)
          * elapsed_sec < 0 (호출자 버그 방어)
          * period <= 0 또는 pulse 총 길이 초과
          * tail decay 가드 발동
        → 호출자는 emissive 를 0 으로 set 하고 start 타이머를 폐기한다.
      - 튜플이면 현재 envelope 위치의 RGB. 호출자는 emissive Input 에 Set.

    rise:
      T_rise = period / 2
      intensity = i_min + (i_max - i_min) * sin(π/2 * elapsed / T_rise)

    tail:
      dt = elapsed - T_rise
      intensity = i_min + (i_max - i_min) * exp(-dt / tail_tau)
    """
    p = pulse_params(status)
    if p is None:
        return None
    if elapsed_sec < 0.0 or p.period <= 0.0:
        return None
    rise_sec = p.period / 2.0
    total_sec = rise_sec + p.tail_sec

    if elapsed_sec <= rise_sec:
        phase = math.sin((math.pi / 2.0) * elapsed_sec / rise_sec)
        intensity = p.i_min + (p.i_max - p.i_min) * phase
    elif elapsed_sec <= total_sec:
        if p.tail_tau <= 1e-9:
            return None
        dt = elapsed_sec - rise_sec
        intensity = p.i_min + (p.i_max - p.i_min) * math.exp(-dt / p.tail_tau)
    else:
        return None

    r, g, b = p.emissive_color
    return (r * intensity, g * intensity, b * intensity)
