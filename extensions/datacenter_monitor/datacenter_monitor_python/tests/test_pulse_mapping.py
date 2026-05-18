"""Unit tests for scene/node_state_pulse.py — pulse_params & compute_emissive."""

import math

import pytest

from datacenter_monitor_python import global_variables as gv
from datacenter_monitor_python.scene import node_state_pulse as pulse_module
from datacenter_monitor_python.scene.node_state_pulse import (
    PulseParams,
    compute_emissive,
    pulse_params,
)
from datacenter_monitor_python.global_variables import (
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


def _intensity_from_rgb(rgb, color):
    for ch_val, ch_color in zip(rgb, color):
        if abs(ch_color) > 1e-12:
            return ch_val / ch_color
    raise AssertionError("expected at least one non-zero color channel")


def test_pulse_params_healthy():
    p = pulse_params("HEALTHY")
    assert isinstance(p, PulseParams)
    assert p.emissive_color == NODE_PULSE_HEALTHY_EMISSIVE_COLOR
    assert p.i_min == NODE_PULSE_HEALTHY_MIN
    assert p.i_max == NODE_PULSE_HEALTHY_MAX
    assert p.period == NODE_PULSE_HEALTHY_PERIOD_SEC
    assert p.tail_sec == NODE_PULSE_HEALTHY_TAIL_SEC
    assert p.tail_tau == NODE_PULSE_HEALTHY_TAIL_TAU


def test_pulse_params_disconnected():
    p = pulse_params("DISCONNECTED")
    assert isinstance(p, PulseParams)
    assert p.emissive_color == NODE_PULSE_DISCONNECTED_EMISSIVE_COLOR
    assert p.i_min == NODE_PULSE_DISCONNECTED_MIN
    assert p.i_max == NODE_PULSE_DISCONNECTED_MAX
    assert p.period == NODE_PULSE_DISCONNECTED_PERIOD_SEC
    assert p.tail_sec == NODE_PULSE_DISCONNECTED_TAIL_SEC
    assert p.tail_tau == NODE_PULSE_DISCONNECTED_TAIL_TAU


@pytest.mark.parametrize("status", ["UNKNOWN", "WARNING", "CRITICAL", "", "garbage", None])
def test_pulse_params_none_for_noop_statuses(status):
    assert pulse_params(status) is None


def test_compute_emissive_none_for_noop_status():
    assert compute_emissive("UNKNOWN", elapsed_sec=0.0) is None
    assert compute_emissive("WARNING", elapsed_sec=0.5) is None


def test_compute_emissive_healthy_at_elapsed_zero_is_i_min():
    """rise 시작점: intensity = i_min."""
    rgb = compute_emissive("HEALTHY", elapsed_sec=0.0)
    assert rgb is not None
    expected_intensity = NODE_PULSE_HEALTHY_MIN
    for ch_val, ch_color in zip(rgb, NODE_PULSE_HEALTHY_EMISSIVE_COLOR):
        assert math.isclose(ch_val, ch_color * expected_intensity, abs_tol=1e-9)


def test_compute_emissive_healthy_at_peak_is_i_max():
    """rise 종점: elapsed = period/2 → intensity = i_max."""
    t_peak = NODE_PULSE_HEALTHY_PERIOD_SEC / 2.0
    rgb = compute_emissive("HEALTHY", elapsed_sec=t_peak)
    assert rgb is not None
    expected_intensity = NODE_PULSE_HEALTHY_MAX
    for ch_val, ch_color in zip(rgb, NODE_PULSE_HEALTHY_EMISSIVE_COLOR):
        assert math.isclose(ch_val, ch_color * expected_intensity, rel_tol=1e-9, abs_tol=1e-9)


def test_compute_emissive_at_total_duration_end_is_near_zero():
    p = pulse_params("HEALTHY")
    assert p is not None
    t_end = p.period / 2.0 + p.tail_sec
    rgb = compute_emissive("HEALTHY", elapsed_sec=t_end)
    assert rgb is not None
    expected_intensity = p.i_min + (p.i_max - p.i_min) * math.exp(-p.tail_sec / p.tail_tau)
    assert math.isclose(
        _intensity_from_rgb(rgb, p.emissive_color),
        expected_intensity,
        rel_tol=1e-9,
        abs_tol=1e-9,
    )


def test_compute_emissive_returns_none_after_period():
    """elapsed > rise + tail_sec → None (pulse 종료)."""
    past = NODE_PULSE_HEALTHY_PERIOD_SEC / 2.0 + NODE_PULSE_HEALTHY_TAIL_SEC + 0.01
    assert compute_emissive("HEALTHY", elapsed_sec=past) is None


def test_compute_emissive_returns_none_for_negative_elapsed():
    """elapsed < 0 방어 — None."""
    assert compute_emissive("HEALTHY", elapsed_sec=-0.1) is None
    assert compute_emissive("DISCONNECTED", elapsed_sec=-1.0) is None


def test_compute_emissive_disconnected_intensity_within_bounds():
    """elapsed ∈ [0, rise + tail] 전 구간에서 intensity ∈ [i_min, i_max]."""
    color = NODE_PULSE_DISCONNECTED_EMISSIVE_COLOR
    i_min = NODE_PULSE_DISCONNECTED_MIN
    i_max = NODE_PULSE_DISCONNECTED_MAX
    period = NODE_PULSE_DISCONNECTED_PERIOD_SEC
    rise = period / 2.0
    total = rise + NODE_PULSE_DISCONNECTED_TAIL_SEC
    samples = [0.0, 0.25 * rise, 0.5 * rise, rise, rise + 0.1, rise + 0.5, total]
    for t in samples:
        rgb = compute_emissive("DISCONNECTED", elapsed_sec=t)
        assert rgb is not None
        for ch_val, ch_color in zip(rgb, color):
            if ch_color > 0:
                intensity = ch_val / ch_color
                assert i_min - 1e-9 <= intensity <= i_max + 1e-9


def test_compute_emissive_rise_is_sin_quarter():
    p = pulse_params("HEALTHY")
    assert p is not None
    rise = p.period / 2.0
    for t in (0.0, rise * 0.25, rise * 0.5, rise * 0.75, rise):
        rgb = compute_emissive("HEALTHY", elapsed_sec=t)
        assert rgb is not None
        expected = p.i_min + (p.i_max - p.i_min) * math.sin((math.pi / 2.0) * t / rise)
        assert math.isclose(
            _intensity_from_rgb(rgb, p.emissive_color),
            expected,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )


def test_compute_emissive_tail_is_exp_decay():
    p = pulse_params("HEALTHY")
    assert p is not None
    rise = p.period / 2.0
    for dt in (0.1 * p.tail_sec, 0.5 * p.tail_sec, 0.9 * p.tail_sec):
        rgb = compute_emissive("HEALTHY", elapsed_sec=rise + dt)
        assert rgb is not None
        expected = p.i_min + (p.i_max - p.i_min) * math.exp(-dt / p.tail_tau)
        assert math.isclose(
            _intensity_from_rgb(rgb, p.emissive_color),
            expected,
            rel_tol=1e-9,
            abs_tol=1e-9,
        )


def test_compute_emissive_tail_monotonic_decrease():
    p = pulse_params("HEALTHY")
    assert p is not None
    rise = p.period / 2.0
    samples = []
    for step in range(10):
        dt = p.tail_sec * step / 9.0
        rgb = compute_emissive("HEALTHY", elapsed_sec=rise + dt)
        assert rgb is not None
        samples.append(_intensity_from_rgb(rgb, p.emissive_color))
    assert all(left > right for left, right in zip(samples, samples[1:]))


def test_compute_emissive_uses_emissive_color_not_diffuse(monkeypatch):
    custom_emissive = (0.1, 0.2, 0.3)
    custom_diffuse = (0.9, 0.9, 0.9)
    monkeypatch.setattr(pulse_module, "NODE_PULSE_HEALTHY_EMISSIVE_COLOR", custom_emissive)
    monkeypatch.setattr(gv, "GLASS_CUBE_HEALTHY_COLOR", custom_diffuse)

    t_peak = pulse_module.NODE_PULSE_HEALTHY_PERIOD_SEC / 2.0
    rgb = pulse_module.compute_emissive("HEALTHY", elapsed_sec=t_peak)
    assert rgb is not None
    for actual, color in zip(rgb, custom_emissive):
        assert math.isclose(actual, color * pulse_module.NODE_PULSE_HEALTHY_MAX, rel_tol=1e-9, abs_tol=1e-9)
