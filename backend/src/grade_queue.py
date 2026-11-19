"""可靠估级流水线。

设计要点（对应留样送估中断恢复需求）：
- 队列消息带 compute_generation，与 DB 留样的当前代际一起校验；
  重试会 bump 代际，旧 worker 晚回来时代际对不上，结果作废，不覆盖已补好的估值。
- worker 处理前先锁行并复查状态：只有 queued 且代际匹配才计算，
  正在正常计算（running）的单子一律跳过，做到重试不重复出估级。
- recover_stuck() 周期扫描：queued 超过阈值（消息可能已丢）或 running 心跳超时
  （worker 硬崩溃）的留样锁行后补送，入队即 bump 代际并清错误；
  活跃 running 行被其它 worker 锁住时跳过，照常等待。
"""
import os, json, logging
from datetime import datetime, timedelta

from sqlalchemy import select

from .common import lock
from .models import VaseSample, Consignment, Audit
from .grading import predict
from .artifacts import write_artifact, artifact_matches

QUEUE_KEY = 'floral-grade'
# queued 多久没被领走视为消息丢失（Redis flush、rpush 后宕机等）。
QUEUE_STALE_SECONDS = int(os.environ.get('GRADE_QUEUE_STALE_SECONDS', '60'))
# running 心跳多久没刷新视为 worker 硬崩溃。
HEARTBEAT_TIMEOUT_SECONDS = int(os.environ.get('GRADE_HEARTBEAT_TIMEOUT', '90'))
SWEEP_INTERVAL_SECONDS = int(os.environ.get('GRADE_SWEEP_INTERVAL', '5'))


def now_iso():
    return datetime.now().isoformat(timespec='seconds')


def _parse(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_message(raw):
    try:
        msg = json.loads(raw)
        return int(msg['sample_id']), int(msg['generation'])
    except (ValueError, TypeError, KeyError):
        return None


def enqueue(s, q, sample, actor=None):
    """把留样送入估级队列。仅 recorded/failed 可由接口主动送估；
    恢复器对卡住的 queued/running 也走同一套 bump+入队逻辑。

    只改 DB 状态并 flush，随后 rpush，最后 commit；rpush 抛错时调用方可整体回滚。
    """
    if sample.state not in ('recorded', 'failed', 'queued', 'running'):
        raise ValueError('当前状态不允许送估')
    sample.compute_generation = (sample.compute_generation or 1) + 1
    sample.state = 'queued'
    sample.error = None
    sample.enqueued_at = now_iso()
    sample.heartbeat_at = None
    if actor is not None:
        s.add(Audit(actor=getattr(actor, 'email', 'worker'),
                    action=f'送估/补算 {sample.code} 代际{sample.compute_generation}'))
    s.flush()
    # 消息体带代际；即使旧消息还在队列里，worker 也会按代际丢弃。
    q.rpush(QUEUE_KEY, json.dumps({'sample_id': sample.id,
                                   'generation': sample.compute_generation}))
    s.commit()
    return sample


def recover_stuck(s, q):
    """扫描并补送中断的留样，返回补送数量。

    - queued 且 enqueued_at 超过 QUEUE_STALE_SECONDS：消息丢失，补送；
    - running 且 heartbeat_at 超过 HEARTBEAT_TIMEOUT_SECONDS：worker 崩溃，补算；
    - 仍在正常计算（心跳新鲜、行锁被占）的单子不动，照常等。
    """
    recovered = 0
    stale_before = datetime.now() - timedelta(seconds=QUEUE_STALE_SECONDS)
    dead_before = datetime.now() - timedelta(seconds=HEARTBEAT_TIMEOUT_SECONDS)
    rows = s.scalars(select(VaseSample).where(VaseSample.state.in_(('queued', 'running')))
                     .order_by(VaseSample.id)).all()
    for row in rows:
        needs = False
        if row.state == 'queued':
            enqueued = _parse(row.enqueued_at)
            needs = enqueued is None or enqueued < stale_before
        else:
            heartbeat = _parse(row.heartbeat_at) or _parse(row.enqueued_at)
            needs = heartbeat is None or heartbeat < dead_before
        if not needs:
            continue
        # skip_locked：活跃 worker 持有的行锁拿不到就跳过，不打扰正常计算。
        x = s.scalar(select(VaseSample).where(VaseSample.id == row.id)
                     .with_for_update(skip_locked=True)
                     .execution_options(populate_existing=True))
        if x is None or x.state not in ('queued', 'running'):
            continue
        # 锁行后复查时间条件，避免和刚刷新心跳的 worker 打架。
        if x.state == 'queued':
            enqueued = _parse(x.enqueued_at)
            fresh = enqueued is not None and enqueued >= stale_before
        else:
            heartbeat = _parse(x.heartbeat_at) or _parse(x.enqueued_at)
            fresh = heartbeat is not None and heartbeat >= dead_before
        if fresh:
            continue
        logging.warning('recover stuck grading sample=%s state=%s gen=%s',
                        x.id, x.state, x.compute_generation)
        enqueue(s, q, x)
        recovered += 1
    return recovered


def start_job(s, sample_id, generation):
    """领单：锁行复查状态与代际。有效返回留样，否则返回 None（消息丢弃）。

    幂等/防旧：状态必须是 queued 且代际匹配，
    保证重试不重复出估级、晚回来的旧计算不覆盖新结果。
    """
    sample = lock(s, VaseSample, sample_id)
    if sample.state != 'queued' or sample.compute_generation != generation:
        logging.info('skip stale grade message sample=%s gen=%s state=%s current_gen=%s',
                     sample_id, generation, sample.state, sample.compute_generation)
        return None
    sample.state = 'running'
    sample.heartbeat_at = now_iso()
    s.commit()
    return sample


def compute(sample, s):
    """真实 sklearn 估级（耗时步骤，期间由 worker 心跳线程保活）。"""
    cons = s.get(Consignment, sample.consignment_id)
    return predict(sample, cons)


def finish_job(s, q, sample_id, generation, result):
    """计算完成：代际复查 → 原子归档 → DB 落库，两边必须是同一次计算。"""
    # 计算期间可能已被恢复器 bump 代际补算：再锁一次复查，旧结果绝不落库。
    sample = lock(s, VaseSample, sample_id)
    if sample.state != 'running' or sample.compute_generation != generation:
        logging.warning('discard late computation sample=%s gen=%s state=%s gen_now=%s',
                        sample_id, generation, sample.state, sample.compute_generation)
        return False
    # 先原子写归档，再写 DB；归档失败整体判 failed，不会出现有估值无归档。
    write_artifact(sample, result)
    sample = lock(s, VaseSample, sample_id)
    if sample.state != 'running' or sample.compute_generation != generation:
        logging.warning('discard late computation after artifact sample=%s gen=%s',
                        sample_id, generation)
        return False
    sample.prediction = result
    sample.state = 'ready'
    sample.error = None
    s.commit()
    # 落库后再确认归档与当前代际一致；不一致则判失败并补算，不放进可核级状态。
    if not artifact_matches(sample):
        sample = lock(s, VaseSample, sample_id)
        sample.state = 'failed'
        sample.error = '估值归档与本次计算代际不一致，已安排补算'
        s.commit()
        sample = lock(s, VaseSample, sample_id)
        enqueue(s, q, sample)
    return True


def mark_failed(s, sample_id, generation, error):
    """计算异常：仅当代际仍属本次尝试时记 failed，避免误改补算后的新状态。"""
    x = s.scalar(select(VaseSample).where(VaseSample.id == sample_id)
                 .with_for_update(skip_locked=True)
                 .execution_options(populate_existing=True))
    if x is not None and x.state == 'running' and x.compute_generation == generation:
        x.state = 'failed'
        x.error = str(error)[:450]
        s.commit()
        return True
    return False
