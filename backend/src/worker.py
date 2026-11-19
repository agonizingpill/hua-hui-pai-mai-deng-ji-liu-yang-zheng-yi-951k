import os, time, json, logging, threading
from pathlib import Path

import redis

from .db import Session
from .grade_queue import (QUEUE_KEY, SWEEP_INTERVAL_SECONDS, HEARTBEAT_TIMEOUT_SECONDS,
                          parse_message, start_job, compute, finish_job, mark_failed,
                          recover_stuck, now_iso)
from .models import VaseSample

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
q = redis.Redis.from_url(os.environ['REDIS_URL'], decode_responses=True)

# 心跳间隔取超时的 1/3，保证健康计算在恢复器判定死亡前刷新多次。
HEARTBEAT_INTERVAL = max(2, HEARTBEAT_TIMEOUT_SECONDS // 3)


class Heartbeat(threading.Thread):
    """计算期间持续刷新 heartbeat_at，让恢复器知道这一单仍在正常计算。

    只做独立短事务更新，结束即停；worker 进程硬崩溃时线程一并消失，
    心跳停止，恢复器随后按超时把该单补算。
    """

    def __init__(self, sample_id, generation):
        super().__init__(daemon=True)
        self.sample_id, self.generation = sample_id, generation
        self.stop_event = threading.Event()

    def run(self):
        while not self.stop_event.wait(HEARTBEAT_INTERVAL):
            try:
                with Session() as s:
                    x = s.get(VaseSample, self.sample_id)
                    if x is None or x.state != 'running' \
                            or x.compute_generation != self.generation:
                        return
                    x.heartbeat_at = now_iso()
                    s.commit()
            except Exception:
                logging.exception('heartbeat update failed sample=%s', self.sample_id)


def handle(raw):
    parsed = parse_message(raw)
    if parsed is None:
        logging.warning('drop malformed grade message: %r', raw)
        return
    sample_id, generation = parsed

    with Session() as s:
        sample = start_job(s, sample_id, generation)
    if sample is None:
        return

    hb = Heartbeat(sample_id, generation)
    hb.start()
    try:
        try:
            with Session() as s:
                result = compute(sample, s)
        except Exception as ex:
            logging.exception('grade failed sample=%s', sample_id)
            with Session() as s:
                mark_failed(s, sample_id, generation, ex)
            return
        # 计算结束先停心跳并等待退出，避免与 finish_job 的行锁相争。
        hb.stop_event.set()
        hb.join(timeout=HEARTBEAT_INTERVAL + 2)
        try:
            with Session() as s:
                finish_job(s, q, sample_id, generation, result)
        except Exception as ex:
            logging.exception('grade finish failed sample=%s', sample_id)
            with Session() as s:
                mark_failed(s, sample_id, generation, ex)
    finally:
        hb.stop_event.set()


def run():
    # worker 独立于后端启动，也要保证可靠估级流水线字段存在（幂等）。
    try:
        from .seed_service import ensure_columns
        ensure_columns()
    except Exception:
        logging.exception('worker ensure columns failed')
    last_sweep = 0.0
    while True:
        Path('/tmp/floral-heartbeat').touch()
        try:
            # 恢复器：周期性把丢消息的 queued 和硬崩溃的 running 自动接上。
            if time.monotonic() - last_sweep >= SWEEP_INTERVAL_SECONDS:
                with Session() as s:
                    n = recover_stuck(s, q)
                if n:
                    logging.info('sweep requeued %d stuck sample(s)', n)
                last_sweep = time.monotonic()

            item = q.blpop(QUEUE_KEY, timeout=2)
            if not item:
                continue
            handle(item[1])
        except Exception:
            logging.exception('worker loop error')
            time.sleep(1)


if __name__ == '__main__':
    run()
