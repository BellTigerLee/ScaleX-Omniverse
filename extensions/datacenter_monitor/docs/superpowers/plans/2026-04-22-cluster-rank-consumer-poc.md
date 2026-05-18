# Cluster Rank Consumer POC — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire an end-to-end passthrough for Flink's `datacenter.metrics.stageab` topic — Omniverse Extension subscribes, forwards `id=="cluster-rank"` messages to React via the existing WebRTC channel, React logs each message to the browser console.

**Architecture:** Clone the existing `NodeStateSubscriber` pattern for a new `ClusterRankSubscriber`. Forward each message through the existing `MessageHandler._send_to_client` → `omni.kit.livestream.send_message` → React `handleCustomEvent` pipe with a new `event_type: "cluster_rank"`. No shared state, no UI render, no Redis, no replay.

**Tech Stack:** Python 3 (Omniverse Kit runtime), `confluent-kafka` (primary) + `kafka-python` (fallback), pytest (unit), React 18 (`react-dev-tools`), `omni.kit.livestream.messaging` (WebRTC message bus).

**Spec:** `extensions/datacenter_monitor/docs/superpowers/specs/2026-04-22-cluster-rank-consumer-poc-design.md`

---

## File Structure

| Action | Path | Responsibility |
|---|---|---|
| Modify | `extensions/datacenter_monitor/datacenter_monitor_python/global_variables.py` | Add `KAFKA_TOPIC_CLUSTER_RANK` constant |
| Modify | `extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py` | Add `parse_cluster_rank_message` + `ClusterRankSubscriber` class |
| Create | `extensions/datacenter_monitor/datacenter_monitor_python/tests/test_cluster_rank_parse.py` | Unit tests for the parser |
| Modify | `extensions/datacenter_monitor/datacenter_monitor_python/message_handler.py` | Add `send_cluster_rank` method |
| Modify | `extensions/datacenter_monitor/datacenter_monitor_python/extension.py` | Wire subscriber lifecycle + per-frame drain |
| Modify | `react-dev-tools/src/OmniverseViewer.js` | Add `cluster_rank` branch in `handleCustomEvent` |

Each file has one responsibility. Parser is the only unit-testable piece; the subscriber loop (Kafka I/O + thread) and the Kit wiring (carb / omni.kit.app) are integration surfaces that existing `NodeStateSubscriber` / `MessageHandler` already exercise — no new unit tests there.

Suggested bundling for subagent dispatch (per project feedback: group related tasks):
- **Bundle A** — Tasks 1 + 2 + 3 (parser TDD + subscriber class); all within `kafka_subscriber.py` and tests.
- **Bundle B** — Tasks 4 + 5 (forwarding method + extension wiring); the Python plumbing.
- **Bundle C** — Task 6 (React). Independent.
- **Manual** — Task 7 (user verifies on lab1).

---

## Task 1: Add Kafka topic constant

**Files:**
- Modify: `extensions/datacenter_monitor/datacenter_monitor_python/global_variables.py:50-55`

- [ ] **Step 1: Add constant after `KAFKA_TOPIC_REPLAY_EVENT`**

Open `extensions/datacenter_monitor/datacenter_monitor_python/global_variables.py`. After line 54 (`KAFKA_TOPIC_REPLAY_EVENT = ...`), before line 55 (`KAFKA_GROUP_ID = ...`), insert:

```python
KAFKA_TOPIC_CLUSTER_RANK = "datacenter.metrics.stageab"  # C축 cluster CPU rank 토픽 (Flink가 produce)
```

The file should now have 6 `KAFKA_TOPIC_*` lines followed by `KAFKA_GROUP_ID`.

- [ ] **Step 2: Commit**

```bash
git add extensions/datacenter_monitor/datacenter_monitor_python/global_variables.py
git commit -m "$(cat <<'EOF'
feat(extensions/datacenter_monitor): add KAFKA_TOPIC_CLUSTER_RANK constant

stageab 토픽 상수 추가 — Flink C축 cluster CPU rank 프로듀서 출력.
EOF
)"
```

---

## Task 2: Parser — write failing tests, then implement

**Files:**
- Create: `extensions/datacenter_monitor/datacenter_monitor_python/tests/test_cluster_rank_parse.py`
- Modify: `extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py` (add around line 63, after `parse_node_state_message` block)

- [ ] **Step 1: Write the failing test file**

Create `extensions/datacenter_monitor/datacenter_monitor_python/tests/test_cluster_rank_parse.py` with this exact content:

```python
"""Unit tests for kafka_subscriber.parse_cluster_rank_message — stageab cluster-rank parser."""

import json

import pytest

from kafka_subscriber import parse_cluster_rank_message


def _valid_envelope() -> dict:
    return {
        "id":      "cluster-rank",
        "cluster": "datax",
        "ts":      1776830800669,
        "ranking": [
            {"node": "datax-dtn-1", "rank": 1, "cpu_util": 0.12558619799508416},
            {"node": "datax-hdd-3", "rank": 2, "cpu_util": 0.1233234777396713},
        ],
    }


def _to_bytes(env: dict) -> bytes:
    return json.dumps(env).encode("utf-8")


def test_parse_valid_envelope_returns_dict():
    env = _valid_envelope()
    parsed = parse_cluster_rank_message(_to_bytes(env))
    assert parsed is not None
    for key in ("id", "cluster", "ts", "ranking"):
        assert parsed[key] == env[key]


def test_parse_empty_ranking_allowed():
    """Empty-terminal emit: all nodes stale → ranking=[] must still parse."""
    env = _valid_envelope()
    env["ranking"] = []
    parsed = parse_cluster_rank_message(_to_bytes(env))
    assert parsed is not None
    assert parsed["ranking"] == []


@pytest.mark.parametrize("missing_field", ["id", "cluster", "ts", "ranking"])
def test_parse_missing_required_field_returns_none(missing_field):
    env = _valid_envelope()
    del env[missing_field]
    assert parse_cluster_rank_message(_to_bytes(env)) is None


@pytest.mark.parametrize("wrong_id", ["cluster-severity", "cluster-rank-mem", "", "CLUSTER-RANK"])
def test_parse_mismatched_id_returns_none(wrong_id):
    env = _valid_envelope()
    env["id"] = wrong_id
    assert parse_cluster_rank_message(_to_bytes(env)) is None


def test_parse_non_list_ranking_returns_none():
    env = _valid_envelope()
    env["ranking"] = {"not": "a list"}
    assert parse_cluster_rank_message(_to_bytes(env)) is None


def test_parse_non_json_returns_none():
    assert parse_cluster_rank_message(b"not-json{") is None


def test_parse_bad_utf8_returns_none():
    assert parse_cluster_rank_message(b"\xff\xfe\xfa") is None


def test_parse_non_dict_top_level_returns_none():
    assert parse_cluster_rank_message(json.dumps(["list", "not", "dict"]).encode("utf-8")) is None
```

- [ ] **Step 2: Run tests — verify they fail with ImportError**

```bash
cd extensions/datacenter_monitor && pytest datacenter_monitor_python/tests/test_cluster_rank_parse.py -v
```

Expected: collection error / ImportError — `parse_cluster_rank_message` is not yet defined in `kafka_subscriber`.

- [ ] **Step 3: Implement the parser in `kafka_subscriber.py`**

Open `extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py`. After line 62 (the closing `return data` of `parse_node_state_message`), before the library-detection block (`# ── 라이브러리 감지 ─` around line 65), insert:

```python
# ── Cluster rank (stageab) 페이로드 파서 ──────────────────────────────
# 2026-04-22-cluster-rank-consumer-poc-design.md §5.2 참고.
# 토픽은 향후 다른 id (cluster-severity 등) 도 수용하므로 id=="cluster-rank" 로 필터.

_CLUSTER_RANK_REQUIRED_FIELDS = ("id", "cluster", "ts", "ranking")


def parse_cluster_rank_message(raw: bytes):
    """
    Flink stageab cluster-rank 페이로드 (UTF-8 JSON bytes) → dict 또는 None.

    검증:
      - JSON / UTF-8 디코딩 성공
      - top-level 이 dict
      - 필수 필드(_CLUSTER_RANK_REQUIRED_FIELDS) 전부 존재
      - id == "cluster-rank"   (그 외 aggregate 는 조용히 드롭)
      - ranking 이 list
    실패 시 필요하면 경고 로그 후 None 반환.
    """
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[ClusterRankParser] JSON/UTF-8 디코드 실패: {e}")
        return None

    if not isinstance(data, dict):
        print(f"[ClusterRankParser] 최상위가 dict 아님: {type(data).__name__}")
        return None

    missing = [f for f in _CLUSTER_RANK_REQUIRED_FIELDS if f not in data]
    if missing:
        print(f"[ClusterRankParser] 필수 필드 누락 {missing}, 드롭: {list(data.keys())}")
        return None

    if data["id"] != "cluster-rank":
        # 같은 토픽의 다른 aggregate — 정상 경로, 로그 없이 드롭.
        return None

    if not isinstance(data["ranking"], list):
        print(f"[ClusterRankParser] ranking 이 list 아님: {type(data['ranking']).__name__}")
        return None

    return data
```

- [ ] **Step 4: Run tests — verify they pass**

```bash
cd extensions/datacenter_monitor && pytest datacenter_monitor_python/tests/test_cluster_rank_parse.py -v
```

Expected: all tests PASS (happy path + empty ranking + 4 missing fields + 4 wrong-id + non-list ranking + non-JSON + bad UTF-8 + non-dict top-level). Count: 1 + 1 + 4 + 4 + 1 + 1 + 1 + 1 = 14 parametrized runs.

- [ ] **Step 5: Run the full extension test suite — verify no regressions**

```bash
cd extensions/datacenter_monitor && pytest -q
```

Expected: existing tests (node-state parse + pulse mapping) still pass alongside the new cluster-rank tests.

- [ ] **Step 6: Commit**

```bash
git add extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py extensions/datacenter_monitor/datacenter_monitor_python/tests/test_cluster_rank_parse.py
git commit -m "$(cat <<'EOF'
feat(extensions/datacenter_monitor): add parse_cluster_rank_message + tests

stageab 페이로드 파서 — JSON / 필수 필드 / id=="cluster-rank" 필터 /
ranking list 검증. 14 개 pytest 파라메트릭 케이스 통과.
EOF
)"
```

---

## Task 3: Add `ClusterRankSubscriber` class

**Files:**
- Modify: `extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py` (append at end of file, after line 586)

This class is a mechanical clone of `NodeStateSubscriber` (lines 365-549). Do NOT try to factor out a shared base class — matches the existing `KafkaSubscriber` / `NodeStateSubscriber` siblings that also duplicate the loop. YAGNI: refactor after a third subscriber lands.

- [ ] **Step 1: Append the class to `kafka_subscriber.py`**

Open `extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py`. Append to the end of the file (after the legacy-comment block closing at line 586):

```python


# ── ClusterRankSubscriber ────────────────────────────────────────────────────

class ClusterRankSubscriber:
    """
    datacenter.metrics.stageab 토픽 전용 구독자 (id=="cluster-rank" 필터).
    NodeStateSubscriber 와 동일한 confluent / kafka-python 이중 백엔드.

    본 구독자는 replay 토픽 전환을 지원하지 않는다 — 프로듀서가 replay
    변형을 publish 하지 않기 때문(2026-04-22-cluster-rank-consumer-poc-design.md §5.4).
    """

    def __init__(
        self,
        bootstrap_servers: list,
        topic: str,
        group_id: str,
        data_queue: queue.Queue,
    ):
        self._bootstrap_servers = bootstrap_servers
        self._topic             = topic
        self._group_id          = group_id
        self._queue             = data_queue

        self._thread:    threading.Thread | None = None
        self._running   = False
        self._connected = False

        if _BACKEND is None:
            print("[ClusterRankSubscriber] ❌ Kafka 라이브러리 미설치.")
        else:
            print(f"[ClusterRankSubscriber] 백엔드: {_BACKEND}")

    # ── 제어 ─────────────────────────────────────────────────────────────────

    def start(self):
        if _BACKEND is None:
            return
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(
            target=self._consume_loop, name="ClusterRankSubscriberThread", daemon=True
        )
        self._thread.start()
        print(f"[ClusterRankSubscriber] 시작 — topic: {self._topic}")

    def stop(self):
        self._running = False
        print("[ClusterRankSubscriber] 정지 요청")

    def is_connected(self) -> bool:
        return self._connected

    # ── 내부: 소비 루프 ───────────────────────────────────────────────────────

    def _consume_loop(self):
        if _BACKEND == "confluent":
            self._consume_loop_confluent()
        elif _BACKEND == "kafka-python":
            self._consume_loop_kafka_python()

    def _consume_loop_confluent(self):
        my_topic = self._topic
        while self._running:
            consumer = None
            try:
                conf = {
                    "bootstrap.servers": ",".join(str(s) for s in self._bootstrap_servers),
                    "group.id":           self._group_id,
                    "auto.offset.reset":  "latest",
                    "enable.auto.commit": True,
                    "session.timeout.ms": 10000,
                }
                consumer = _ConfluentConsumer(conf)
                consumer.subscribe([my_topic])
                self._connected = True
                print(f"[ClusterRankSubscriber] ✅ confluent-kafka 연결 성공 — {self._bootstrap_servers}")

                while self._running and self._topic == my_topic:
                    msg = consumer.poll(timeout=1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        raise KafkaException(msg.error())
                    parsed = parse_cluster_rank_message(msg.value())
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[ClusterRankSubscriber] ⚠️ confluent-kafka 오류: {e}")
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._topic != my_topic:
                break
            if self._running:
                print("[ClusterRankSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[ClusterRankSubscriber] 정지됨")

    def _consume_loop_kafka_python(self):
        my_topic = self._topic
        while self._running:
            consumer = None
            try:
                consumer = _KafkaConsumer(
                    my_topic,
                    bootstrap_servers=self._bootstrap_servers,
                    group_id=self._group_id,
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    consumer_timeout_ms=1000,
                    value_deserializer=lambda v: v,
                    request_timeout_ms=10000,
                    session_timeout_ms=10000,
                )
                self._connected = True
                print(f"[ClusterRankSubscriber] ✅ kafka-python 연결 성공 — {self._bootstrap_servers}")

                for msg in consumer:
                    if not self._running:
                        break
                    if self._topic != my_topic:
                        break
                    parsed = parse_cluster_rank_message(msg.value)
                    if parsed is not None:
                        if self._queue.full():
                            try:
                                self._queue.get_nowait()
                            except queue.Empty:
                                pass
                        self._queue.put_nowait(parsed)
                        time.sleep(0.001)

            except Exception as e:
                self._connected = False
                print(f"[ClusterRankSubscriber] ⚠️ kafka-python 오류: {e}")
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass

            if self._topic != my_topic:
                break
            if self._running:
                print("[ClusterRankSubscriber] 5초 후 재연결 시도...")
                time.sleep(5)

        self._connected = False
        print("[ClusterRankSubscriber] 정지됨")
```

- [ ] **Step 2: Run extension test suite — verify no import regressions**

```bash
cd extensions/datacenter_monitor && pytest -q
```

Expected: all existing + new parser tests still pass. The subscriber class loads (conftest.py stubs `carb` / Kit deps) even though its Kafka I/O isn't exercised by unit tests.

- [ ] **Step 3: Commit**

```bash
git add extensions/datacenter_monitor/datacenter_monitor_python/kafka_subscriber.py
git commit -m "$(cat <<'EOF'
feat(extensions/datacenter_monitor): add ClusterRankSubscriber class

stageab 토픽 전용 구독자 — NodeStateSubscriber 패턴 클론.
confluent / kafka-python 이중 백엔드, 5초 재연결, queue overflow 시
oldest 드롭. Replay 토픽 전환 미지원 (프로듀서 replay 변형 없음).
EOF
)"
```

---

## Task 4: Add `MessageHandler.send_cluster_rank`

**Files:**
- Modify: `extensions/datacenter_monitor/datacenter_monitor_python/message_handler.py` (insert after line 275, before `send_alert_notification` at line 277)

- [ ] **Step 1: Add the method**

Open `extensions/datacenter_monitor/datacenter_monitor_python/message_handler.py`. After the closing `print(...)` of `send_scene_manifest` (line 275), before `def send_alert_notification(self, ...)` (line 277), insert:

```python
    def send_cluster_rank(self, payload: dict):
        """stageab cluster-rank 페이로드를 React 로 포워딩 (console.log 용)."""
        self._send_to_client({
            "event_type": "cluster_rank",
            "payload":    payload,
        })
```

- [ ] **Step 2: Commit**

```bash
git add extensions/datacenter_monitor/datacenter_monitor_python/message_handler.py
git commit -m "$(cat <<'EOF'
feat(extensions/datacenter_monitor): add MessageHandler.send_cluster_rank

Kit→React 포워딩 메서드 — event_type="cluster_rank" 로 payload 전달.
EOF
)"
```

---

## Task 5: Wire subscriber into `Extension` lifecycle

**Files:**
- Modify: `extensions/datacenter_monitor/datacenter_monitor_python/extension.py`

Three edits: import, startup, shutdown, per-frame drain. Exact locations:

- [ ] **Step 1: Update import on line 35**

Find line 35 (`from .kafka_subscriber import KafkaSubscriber, NodeStateSubscriber`). Replace with:

```python
from .kafka_subscriber import KafkaSubscriber, NodeStateSubscriber, ClusterRankSubscriber
```

- [ ] **Step 2: Update import from `.global_variables` on lines 22-34**

Find the import block starting at line 22:

```python
from .global_variables import (
    EXTENSION_TITLE,
    MAIN_STAGE_USD_PATH,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_LIVE,
    KAFKA_TOPIC_REPLAY,
    KAFKA_TOPIC_EVENT,
    KAFKA_TOPIC_REPLAY_EVENT,
    KAFKA_TOPIC_NODE_STATE,
    KAFKA_GROUP_ID,
    NODE_INDEX_URL,
    DEV_FAKE_NODE_MAPPING,
)
```

Add `KAFKA_TOPIC_CLUSTER_RANK,` between `KAFKA_TOPIC_NODE_STATE,` and `KAFKA_GROUP_ID,`:

```python
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
```

- [ ] **Step 3: Start the subscriber in `on_startup`**

In `on_startup` (starting at line 50), find the node-state subscriber start block ending at line 108 (`self._event_subscriber.start()`). Immediately after that line, insert:

```python

        # ── Cluster-rank 구독자 (stageab, id=="cluster-rank") ──────────────
        self._cluster_rank_queue: queue.Queue = queue.Queue(maxsize=100)
        self._cluster_rank_subscriber = ClusterRankSubscriber(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            topic=KAFKA_TOPIC_CLUSTER_RANK,
            group_id=KAFKA_GROUP_ID + "-cluster-rank",
            data_queue=self._cluster_rank_queue,
        )
        self._cluster_rank_subscriber.start()
```

- [ ] **Step 4: Stop the subscriber in `on_shutdown`**

In `on_shutdown` (starting at line 144), find line 148 (`self._event_subscriber.stop()`). Immediately after that line, insert:

```python
        self._cluster_rank_subscriber.stop()
```

- [ ] **Step 5: Drain + forward per frame in `_on_update`**

In `_on_update` (starting at line 157), find the end of the node-state envelope drain block (the `except queue.Empty: break` at around line 196 — the loop that calls `self._scene_manager.apply_node_state(env)`). Immediately after the inner `break` (and before the legacy HEALTH_TRANSITION comment block at line 198), insert:

```python

        # cluster-rank 큐 drain → React 로 포워딩 (최대 10개/frame)
        rank_processed = 0
        while not self._cluster_rank_queue.empty() and rank_processed < 10:
            try:
                env = self._cluster_rank_queue.get_nowait()
                self._message_handler.send_cluster_rank(env)
                rank_processed += 1
            except queue.Empty:
                break
```

- [ ] **Step 6: Syntax check — import the module**

```bash
cd extensions/datacenter_monitor && python -c "import sys; sys.path.insert(0, '.'); import ast; ast.parse(open('datacenter_monitor_python/extension.py').read()); print('OK')"
```

Expected: `OK`. (Full runtime import requires Omniverse, so we only parse here.)

- [ ] **Step 7: Run extension test suite — verify no regressions**

```bash
cd extensions/datacenter_monitor && pytest -q
```

Expected: all tests still pass.

- [ ] **Step 8: Commit**

```bash
git add extensions/datacenter_monitor/datacenter_monitor_python/extension.py
git commit -m "$(cat <<'EOF'
feat(extensions/datacenter_monitor): wire ClusterRankSubscriber into Extension

on_startup/on_shutdown 에 구독자 start/stop 추가, _on_update 에 cluster-rank
큐 drain → message_handler.send_cluster_rank 포워딩 블록 추가 (최대
10 개/frame). Replay 토픽 전환은 생략 — 프로듀서가 replay 변형 없음.
EOF
)"
```

---

## Task 6: React — log incoming `cluster_rank` events

**Files:**
- Modify: `react-dev-tools/src/OmniverseViewer.js` (insert after line 696, after the `scene_manifest` branch)

- [ ] **Step 1: Add the branch in `handleCustomEvent`**

Open `react-dev-tools/src/OmniverseViewer.js`. Find line 693 — the `scene_manifest` branch:

```javascript
        if (eventType === "scene_manifest" && innerData?.clusters) {
          console.info("🗺️ scene_manifest 수신 → primPath 병합:", innerData.clusters.length, "clusters");
          setTopology(prev => mergeTopologyPrimPaths(prev, innerData));
        }
```

After the closing brace of that `if` block (line 696), insert:

```javascript

        if (eventType === "cluster_rank") {
          console.log("[cluster_rank]", innerData);
        }
```

No state updates, no props change, no re-render. Pure side-effect log.

- [ ] **Step 2: Verify the file still parses (optional — ESLint)**

```bash
cd react-dev-tools && npx eslint src/OmniverseViewer.js 2>&1 | tail -20
```

Expected: no new errors introduced by the added block. Pre-existing warnings are OK.

- [ ] **Step 3: Commit**

```bash
git add react-dev-tools/src/OmniverseViewer.js
git commit -m "$(cat <<'EOF'
feat(react-dev-tools): log cluster_rank events to console

handleCustomEvent 에 event_type=="cluster_rank" 분기 추가 — 수신 payload 를
브라우저 콘솔에 그대로 출력. 상태 변경 없음 (POC console.log 만).
EOF
)"
```

---

## Task 7: Manual E2E verification on lab1

This task is **user-driven** — requires running Omniverse Kit against a live lab1 Kafka cluster with the producer already publishing. Claude cannot execute it; report the checklist result to the user and wait for confirmation.

- [ ] **Step 1: Confirm producer is publishing**

On lab1, use kafbat UI (or `kcat`) to verify `datacenter.metrics.stageab` is receiving messages every 30 s per cluster. Per producer spec §I1-I4 (`flink/docs/superpowers/specs/2026-04-22-cluster-cpu-rank-producer-design.md`).

- [ ] **Step 2: Launch Omniverse Kit with the extension**

Use the existing launch path (`start_datacenter.sh` or equivalent). Watch the Kit log for:

```
[ClusterRankSubscriber] 백엔드: confluent
[ClusterRankSubscriber] 시작 — topic: datacenter.metrics.stageab
[ClusterRankSubscriber] ✅ confluent-kafka 연결 성공 — [...]
```

- [ ] **Step 3: Launch React dev server + open browser console**

```bash
cd react-dev-tools && npm start
```

Open DevTools → Console.

- [ ] **Step 4: Connect WebRTC streaming**

Click **시작** in the status bar to establish the WebRTC session (Kit ↔ React). Within ~30 s of the first post-connect producer emit, the console should show:

```
[cluster_rank] {id: "cluster-rank", cluster: "datax", ts: 1776830800669, ranking: Array(N)}
```

- [ ] **Step 5: (Optional) Reproduce empty-terminal emit**

If you can force every node in a cluster to be stale (stop Flink input), the first post-stale emit should appear with `ranking: []`, then the topic goes quiet until fresh samples arrive.

- [ ] **Step 6: Report back**

Report PASS / FAIL with console log snippet. On PASS, this POC is done — follow-up work per spec §9 (visual channel, Redis, replay, id expansion) is scoped for later sessions.

---

## Self-Review Notes

- **Spec coverage**: §1 goal (Tasks 1-6), §3 input contract (Task 2 parser enforces it), §4 arch (Tasks 3-6 implement each stage), §5.1 const (Task 1), §5.2 parser+subscriber (Tasks 2-3), §5.3 forwarding (Task 4), §5.4 wiring (Task 5), §6 React (Task 6), §7 errors (covered by parser tests + copy of existing subscriber's retry loop), §8.1 unit tests (Task 2), §8.2 manual E2E (Task 7), §2 non-goals intentionally absent from plan (no visual channel, no Redis, no replay, no id expansion). ✓
- **Placeholder scan**: no TBD / "handle later" / "similar to Task N" — every task has full code. ✓
- **Type/name consistency**: `parse_cluster_rank_message` (Task 2) → used by `ClusterRankSubscriber` (Task 3). `ClusterRankSubscriber` (Task 3) → imported + instantiated in `extension.py` (Task 5). `send_cluster_rank` (Task 4) → called in Task 5. `event_type="cluster_rank"` (Task 4) → matched in Task 6. `KAFKA_TOPIC_CLUSTER_RANK` (Task 1) → imported + passed in Task 5. All aligned. ✓
