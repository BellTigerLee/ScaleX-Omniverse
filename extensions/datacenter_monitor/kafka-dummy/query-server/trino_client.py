"""
Trino Client
────────────────────────────────────────────────────────────────────────────
히스토리 조회 및 Replay용 Trino 쿼리 헬퍼.
모든 쿼리는 dc.metrics_seed 단일 테이블을 사용합니다.

타임스탬프 전략:
  데모:  ts = 0~599 (DEMO_EPOCH 기준 초 오프셋)
         입력 Unix timestamp → (ts - DEMO_EPOCH)로 오프셋 변환 후 쿼리

  프로덕션 전환:
         ts = 실제 Unix timestamp
         아래 _ts_to_offset() 변환 로직을 제거하고 직접 비교

DEMO_EPOCH = 1735689600 = 2025-01-01 00:00:00 UTC
  00:00:00 ~ 00:04:59 (0~299s)  : 경보 구간 (WARNING 상승)
  00:05:00 ~ 00:08:59 (300~539s): 위기 구간 (WARNING + CRITICAL 혼재)
  00:09:00 ~ 00:09:59 (540~599s): 회복 구간 (점진적 회복)
"""

import asyncio
import time
from functools import partial

from sqlalchemy import create_engine, text
from config import TRINO_HOST, TRINO_PORT, TRINO_USER, TRINO_CATALOG, TRINO_SCHEMA

# ── 데모 기준 타임스탬프 ────────────────────────────────────────────────────────
# 2025-01-01 00:00:00 UTC
# [수정] 프로덕션 전환 시: DEMO_EPOCH 변수와 _ts_to_offset() 함수 제거
#         get_replay_rows(), get_box_history() 의 파라미터를 직접 event_ts와 비교
DEMO_EPOCH = 1735689600


def _ts_to_offset(unix_ts: int) -> int:
    """
    Unix timestamp → seed 데이터 오프셋 (0~599) 변환.
    [수정] 프로덕션 전환 시 이 함수 전체 제거.
    """
    offset = unix_ts - DEMO_EPOCH
    return max(0, min(599, offset))


_ENGINE = None


def _get_engine():
    global _ENGINE
    if _ENGINE is None:
        url = (
            f"trino://{TRINO_USER}@{TRINO_HOST}:{TRINO_PORT}"
            f"/{TRINO_CATALOG}/{TRINO_SCHEMA}"
        )
        _ENGINE = create_engine(url, connect_args={"http_scheme": "http"})
    return _ENGINE


def _run_query(sql: str, params: dict | None = None) -> list[dict]:
    with _get_engine().connect() as conn:
        result = conn.execute(text(sql), params or {})
        cols   = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


async def query_async(sql: str, params: dict | None = None) -> list[dict]:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, partial(_run_query, sql, params))


# ── 도메인 쿼리 ───────────────────────────────────────────────────────────────

async def get_data_range() -> dict:
    """
    사용 가능한 데이터 타임스탬프 범위를 반환합니다.
    프런트엔드 Replay 날짜/시간 피커의 min/max 기준값으로 사용됩니다.

    [수정] 프로덕션 전환 시 아래 주석 해제, 정적 반환 제거:
        sql = "SELECT MIN(event_ts) as min_ts, MAX(event_ts) as max_ts FROM metrics_seed"
        rows = await query_async(sql)
        return {"from_ts": rows[0]["min_ts"], "to_ts": rows[0]["max_ts"]}
    """
    return {
        "from_ts":    DEMO_EPOCH,            # 2025-01-01 00:00:00 UTC
        "to_ts":      DEMO_EPOCH + 599,      # 2025-01-01 00:09:59 UTC
        "from_iso":   "2025-01-01T00:00",    # datetime-local 입력 기본값 (UTC)
        "to_iso":     "2025-01-01T00:10",
        "demo_epoch": DEMO_EPOCH,
        "note": "Demo seed data: 10분 시나리오 (0-599s 오프셋 기반)",
    }


async def get_box_history(box_id: str, from_offset: int, to_offset: int) -> list[dict]:
    """
    박스별 시계열 조회 (metrics_seed 오프셋 기반).
    from_offset / to_offset : 0~599 (초)

    [수정] 프로덕션: 파라미터명을 from_ts/to_ts (Unix timestamp)로 변경,
           쿼리 조건에서 오프셋 변환 제거
    """
    sql = """
        SELECT ts         AS event_ts,
               cluster    AS cluster_id,
               node       AS box_id,
               status,
               cpu_util, cpu_cores, cpu_load5, cpu_eff,
               mem_util, mem_total_gb, mem_avail_gb, mem_oom_cnt,
               net_in_mbps, net_out_mbps, net_retrans, net_err_sum,
               gpu_util, gpu_mem_util, gpu_temp, gpu_pwr,
               storage_util, storage_io_mbps
        FROM   metrics_seed
        WHERE  node = :box_id
          AND  ts   BETWEEN :from_ts AND :to_ts
        ORDER  BY ts
    """
    return await query_async(sql, {"box_id": box_id,
                                   "from_ts": from_offset,
                                   "to_ts":   to_offset})


async def get_replay_rows(from_ts: int, to_ts: int) -> list[dict]:
    """
    Replay용: from_ts ~ to_ts 구간의 전체 박스 데이터.

    데모:  from_ts, to_ts = Unix timestamp → _ts_to_offset()으로 변환
    프로덕션: from_ts, to_ts = Unix timestamp → event_ts와 직접 비교
              아래 _ts_to_offset() 호출 2개를 제거하면 됩니다.
    """
    from_offset = _ts_to_offset(from_ts)
    to_offset   = _ts_to_offset(to_ts)

    # 최소 1초 구간 보장
    if to_offset <= from_offset:
        to_offset = min(599, from_offset + 1)

    sql = """
        SELECT ts         AS event_ts,
               cluster    AS cluster_id,
               node       AS box_id,
               status,
               cpu_util, cpu_cores, cpu_load5, cpu_eff,
               mem_util, mem_total_gb, mem_avail_gb, mem_oom_cnt,
               net_in_mbps, net_out_mbps, net_retrans, net_err_sum,
               gpu_util, gpu_mem_util, gpu_temp, gpu_pwr,
               storage_util, storage_io_mbps
        FROM   metrics_seed
        WHERE  ts BETWEEN :from_offset AND :to_offset
        ORDER  BY ts, node
    """
    return await query_async(sql, {"from_offset": from_offset,
                                   "to_offset":   to_offset})


async def get_event_history(from_offset: int, to_offset: int) -> list[dict]:
    """
    이벤트 히스토리 조회 (events_seed 오프셋 기반).
    from_offset / to_offset : 0~599 (초)
    """
    sql = """
        SELECT ts          AS event_ts,
               cluster,
               rack,
               node,
               event_id,
               event_type,
               category,
               severity,
               status,
               from_state,
               to_state,
               score,
               message,
               reason_cpu,
               reason_mem,
               source,
               scope
        FROM   events_seed
        WHERE  ts BETWEEN :from_ts AND :to_ts
        ORDER  BY ts, node
    """
    return await query_async(sql, {"from_ts": from_offset, "to_ts": to_offset})


async def get_event_replay_rows(from_ts: int, to_ts: int) -> list[dict]:
    """
    Replay용: from_ts ~ to_ts 구간의 이벤트 데이터.
    Demo: from_ts, to_ts = Unix timestamp → _ts_to_offset()으로 변환.
    """
    from_offset = _ts_to_offset(from_ts)
    to_offset   = _ts_to_offset(to_ts)
    if to_offset <= from_offset:
        to_offset = min(599, from_offset + 1)
    return await get_event_history(from_offset, to_offset)


async def get_topology() -> list[dict]:
    """알려진 cluster/box 목록 반환."""
    sql = """
        SELECT DISTINCT cluster AS cluster_id, node AS box_id
        FROM   metrics_seed
        ORDER  BY cluster_id, box_id
    """
    return await query_async(sql)


async def trino_ready(retries: int = 20) -> bool:
    for i in range(retries):
        try:
            await query_async("SELECT 1")
            return True
        except Exception as e:
            print(f"[trino_client] Trino 대기 중 ({i+1}/{retries}): {e}")
            time.sleep(5)
    return False
