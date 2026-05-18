"""
Query Server — FastAPI
────────────────────────────────────────────────────────────────────────────
React 대시보드 백엔드. Live 메트릭, 히스토리 조회, Replay 제어를 담당합니다.

모든 히스토리/Replay 데이터는 dc.metrics_seed 단일 테이블 기반입니다.
event_ts 는 0~599 초 오프셋입니다.

엔드포인트:
  GET  /turn-credentials
  GET  /health
  GET  /topology
  GET  /metrics/latest                                     ← LiveCache (Trino 불필요)
  GET  /metrics/{box_id}/history?from_offset=&to_offset=  ← Trino (metrics_seed)
  POST /replay/start   { from_ts, to_ts, speed }
  POST /replay/pause
  POST /replay/resume
  POST /replay/stop
  GET  /replay/status
  WS   /ws/replay      ← Replay 데이터 실시간 push

Swagger UI: http://localhost:8000/docs
"""

import asyncio
import os
import time
import httpx
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from config import TRINO_HOST
from live_cache import LiveCache, EventCache
from node_state_cache import NodeStateCache
from replay_engine import ReplayEngine
from topology_seed import main as seed_topology
from topology_db import get_topology
from trino_client import (
    get_box_history,
    get_data_range,
    get_event_history,
    get_event_replay_rows,
    get_replay_rows,
    trino_ready,
)

# ── 앱 초기화 ─────────────────────────────────────────────────────────────────

live_cache    = LiveCache()
event_cache   = EventCache()
node_state_cache = NodeStateCache()
replay_engine = ReplayEngine()
node_state_enabled = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global node_state_enabled
    # 시작
    try:
        seed_topology()
    except Exception as e:
        print(f"[main] ⚠️  topology seed 실패 (계속 진행): {e}")
    await live_cache.start()
    await event_cache.start()
    try:
        await node_state_cache.start()
        node_state_enabled = True
    except Exception as e:
        node_state_enabled = False
        print(f"[main] ⚠️  NodeStateCache 시작 실패 (계속 진행): {type(e).__name__}: {e}")
        try:
            await node_state_cache.stop()
        except Exception as stop_err:
            print(f"[main] ⚠️  NodeStateCache 정리 실패: {type(stop_err).__name__}: {stop_err}")
    print(f"[main] Trino({TRINO_HOST}) 연결 대기...")
    ok = await trino_ready()
    if ok:
        print("[main] Trino 연결 완료")
    else:
        print("[main] ⚠️  Trino 연결 실패 — 히스토리/Replay 기능 비활성화")
    yield
    # 종료
    await live_cache.stop()
    await event_cache.stop()
    if node_state_enabled:
        await node_state_cache.stop()
    await replay_engine.stop()


app = FastAPI(
    title="Datacenter Query Server",
    description="Datacenter Digital Twin 대시보드 백엔드",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Cloudflare TURN Credentials ──────────────────────────────────────────────

_CF_TURN_KEY_ID = os.environ.get("CF_TURN_KEY_ID", "")
_CF_API_TOKEN   = os.environ.get("CF_API_TOKEN", "")
_CF_TTL         = 86400          # Cloudflare TURN credentials TTL (초)
_CACHE_TTL      = int(_CF_TTL * 0.8)  # 재발급 기준: TTL의 80% (69120초 ≈ 19.2시간)

_turn_cache: dict = {"iceServers": None, "expires_at": 0.0}
_turn_cache_lock = asyncio.Lock()


@app.get("/turn-credentials", tags=["webrtc"])
async def turn_credentials():
    """
    Cloudflare TURN ICE server credentials를 반환합니다.
    서버 메모리에 캐싱하며 TTL의 80% 경과 시 자동 재발급합니다.
    """
    now = time.time()
    # 빠른 경로: 락 없이 캐시 확인
    if _turn_cache["iceServers"] and now < _turn_cache["expires_at"]:
        return {"iceServers": _turn_cache["iceServers"]}

    async with _turn_cache_lock:
        # 락 획득 후 재확인 (다른 요청이 이미 갱신했을 수 있음)
        now = time.time()
        if _turn_cache["iceServers"] and now < _turn_cache["expires_at"]:
            return {"iceServers": _turn_cache["iceServers"]}

        if not _CF_TURN_KEY_ID or not _CF_API_TOKEN:
            raise HTTPException(
                status_code=503,
                detail="Cloudflare TURN 환경변수(CF_TURN_KEY_ID, CF_API_TOKEN)가 설정되지 않았습니다."
            )

        url = (
            f"https://rtc.live.cloudflare.com/v1/turn/keys"
            f"/{_CF_TURN_KEY_ID}/credentials/generate-ice-servers"
        )
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                url,
                headers={
                    "Authorization": f"Bearer {_CF_API_TOKEN}",
                    "Content-Type": "application/json",
                },
                json={"ttl": _CF_TTL},
            )

        if resp.status_code >= 300:
            raise HTTPException(
                status_code=503,
                detail=f"Cloudflare TURN API 호출 실패: {resp.status_code} {resp.text[:200]}"
            )

        data = resp.json()
        ice_servers = data.get("iceServers", [])

        _turn_cache["iceServers"] = ice_servers
        _turn_cache["expires_at"] = time.time() + _CACHE_TTL

        return {"iceServers": ice_servers}


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/health", tags=["system"])
async def health():
    return {
        "status":    "ok",
        "timestamp": int(time.time()),
        "live_boxes": len(live_cache.get_latest_all()),
        "node_state_count": len(node_state_cache.get_latest_all()),
        "node_state_enabled": node_state_enabled,
    }


# ── Topology ──────────────────────────────────────────────────────────────────

@app.get("/topology", tags=["topology"])
async def get_topo():
    """
    알려진 cluster/rack/box/node 목록 반환 (PostgreSQL topology 테이블 기준).
    """
    return await get_topology()


# ── Live Metrics ──────────────────────────────────────────────────────────────

@app.get("/metrics/latest", tags=["metrics"])
async def metrics_latest():
    """
    모든 박스의 최신 상태를 반환합니다.
    Kafka LiveCache에서 직접 서빙 (Trino 미사용, 저지연).
    """
    return live_cache.get_latest_all()


@app.get("/metrics/range", tags=["metrics"])
async def metrics_range():
    """
    사용 가능한 데이터 타임스탬프 범위를 반환합니다.
    프런트엔드 Replay 날짜/시간 피커의 min/max 기준값으로 사용됩니다.

    반환 형식:
      { from_ts, to_ts, from_iso, to_iso, demo_epoch, note }
    """
    return await get_data_range()


@app.get("/metrics/{box_id}/history", tags=["metrics"])
async def metrics_history(
    box_id:      str,
    from_offset: int = Query(..., ge=0, le=599, description="시작 오프셋 (초, 0~299)"),
    to_offset:   int = Query(..., ge=0, le=599, description="종료 오프셋 (초, 0~299)"),
):
    """지정 박스의 시계열 히스토리를 Trino(metrics_seed)에서 조회합니다."""
    if from_offset >= to_offset:
        raise HTTPException(status_code=400, detail="from_offset must be < to_offset")
    return await get_box_history(box_id, from_offset, to_offset)


# ── Node State ────────────────────────────────────────────────────────────────

@app.get("/node-state/latest", tags=["node-state"])
async def node_state_latest():
    """
    datacenter.metrics.node-state.events 토픽의 (cluster, node) 별 최신 envelope 반환.
    NodeStateCache 시작 실패 또는 미수신 상태에서는 [].
    """
    return node_state_cache.get_latest_all()


# ── Events ────────────────────────────────────────────────────────────────────

@app.get("/events/latest", tags=["events"])
async def events_latest(n: int = Query(50, ge=1, le=200, description="반환할 최근 이벤트 수")):
    """
    최근 이벤트 목록을 반환합니다 (EventCache 직접 서빙).
    node 파라미터를 지정하면 해당 노드의 이벤트만 반환합니다.
    """
    return event_cache.get_recent(n)


@app.get("/events/latest/{node_id}", tags=["events"])
async def events_by_node(node_id: str):
    """특정 노드의 최근 이벤트 반환 (EventCache)."""
    return event_cache.get_by_node(node_id)


@app.get("/events/history", tags=["events"])
async def events_history(
    from_offset: int = Query(..., ge=0, le=599, description="시작 오프셋 (초, 0~599)"),
    to_offset:   int = Query(..., ge=0, le=599, description="종료 오프셋 (초, 0~599)"),
):
    """지정 구간의 이벤트를 Trino(events_seed)에서 조회합니다."""
    if from_offset >= to_offset:
        raise HTTPException(status_code=400, detail="from_offset must be < to_offset")
    return await get_event_history(from_offset, to_offset)


# ── Replay ────────────────────────────────────────────────────────────────────

class ReplayStartRequest(BaseModel):
    from_ts: int         # Unix timestamp (demo: DEMO_EPOCH + 오프셋, 예: 1735689600)
    to_ts:   int         # Unix timestamp (demo: DEMO_EPOCH + 오프셋, 예: 1735689660)
    speed:   float = 1.0


@app.post("/replay/start", tags=["replay"])
async def replay_start(req: ReplayStartRequest):
    """
    Replay를 시작합니다.

    from_ts ~ to_ts 구간의 dc.metrics_seed 데이터를 재생합니다.
    재생 데이터는 WebSocket(/ws/replay)과 Kafka replay 토픽으로 동시 전송됩니다.

    Demo 타임스탬프 예시 (DEMO_EPOCH = 1735689600 = 2025-01-01 00:00:00 UTC):
      from_ts=1735689600, to_ts=1735689660  → 처음 1분 (정상 구간)
      from_ts=1735689720, to_ts=1735689840  → 2~4분 (경보 상승 구간)
      from_ts=1735689840, to_ts=1735689900  → 4~5분 (회복 구간)

    [수정] 프로덕션 전환 시: trino_client.get_replay_rows()의 _ts_to_offset() 제거
    """
    if req.from_ts >= req.to_ts:
        raise HTTPException(status_code=400, detail="from_ts must be < to_ts")

    rows = await get_replay_rows(req.from_ts, req.to_ts)
    if not rows:
        raise HTTPException(status_code=404, detail="해당 구간에 데이터가 없습니다")

    event_rows = await get_event_replay_rows(req.from_ts, req.to_ts)

    await replay_engine.start(rows, req.from_ts, req.to_ts, req.speed, event_rows)
    return {"status": "started", "rows": len(rows), "event_rows": len(event_rows)}


@app.post("/replay/pause", tags=["replay"])
async def replay_pause():
    await replay_engine.pause()
    return {"status": "paused"}


@app.post("/replay/resume", tags=["replay"])
async def replay_resume():
    await replay_engine.resume()
    return {"status": "playing"}


@app.post("/replay/stop", tags=["replay"])
async def replay_stop():
    await replay_engine.stop()
    return {"status": "stopped"}


@app.get("/replay/status", tags=["replay"])
async def replay_status():
    return replay_engine.status()


# ── WebSocket: Replay 실시간 push ─────────────────────────────────────────────

@app.websocket("/ws/replay")
async def ws_replay(websocket: WebSocket):
    """
    Replay 중 메시지를 실시간으로 수신합니다.
    연결 후 /replay/start를 호출하면 재생 데이터가 push됩니다.

    메시지 형식:
      {
        "ts":        <Unix Epoch ms>,
        "cluster":   "datax",
        "node":      "Box_4U_HDD_1",
        "status":    "HEALTHY",
        "metrics": {
          "cpu":     { "util": 0.30, "cores": 16.0, "load1": 6.2, "load5": 6.8, "load15": 6.5, "eff": 0.28 },
          "mem":     { "util": 0.55, "total_gb": 64.0, "avail_gb": 28.8, "oom_cnt": 0 },
          "net":     { "in_mbps": 450.0, "out_mbps": 200.0, "retrans": 1.2,
                       "nic_err_sum": 0.0, "nic_drop_sum": 0.0, "netstat_err": 0.0, "err_sum": 0 },
          "gpu":     { "util": 0.0, "temp": 38.5, "pwr": 24.0,
                       "mem_util": 0.0, "mem_used_gb": 0.0, "total_gb": 24.0 },
          "storage": { "util": 0.42, "read_mbps": 180.0, "write_mbps": 140.0, "io_mbps": 320.0 }
        },
        "original_ts": 1706000000,   ← 원본 타임스탬프 (차트 X축 기준)
        "playback_ts": 1706003600    ← 현재 서버 시각 (동기화 기준)
      }
    """
    await websocket.accept()
    replay_engine.add_ws(websocket)
    try:
        while True:
            await websocket.receive_text()   # keep-alive ping 수신
    except WebSocketDisconnect:
        replay_engine.remove_ws(websocket)
