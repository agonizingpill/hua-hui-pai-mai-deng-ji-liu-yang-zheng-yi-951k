import os, json, tempfile
from pathlib import Path

ARTIFACT_DIR = Path(os.environ.get('ARTIFACT_DIR', '/artifacts'))


def artifact_path(sample_id):
    """估值归档文件路径：每支留样一份 grade-ID.json。"""
    return ARTIFACT_DIR / f'grade-{sample_id}.json'


def write_artifact(sample, result):
    """原子落盘：先写临时文件再 rename，读到的归档永远是完整 JSON。

    归档与 DB 估值共用同一代际 token，保证两边来自同一次计算。
    """
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    payload = dict(result)
    payload['sample_id'] = sample.id
    payload['compute_generation'] = sample.compute_generation
    path = artifact_path(sample.id)
    fd, tmp = tempfile.mkstemp(prefix=f'.grade-{sample.id}-', suffix='.tmp', dir=ARTIFACT_DIR)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def read_artifact(sample_id):
    path = artifact_path(sample_id)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except (ValueError, OSError):
        return None


def artifact_matches(sample):
    """归档是否属于该留样当前这一代计算。

    晚回来的旧计算即使写了文件，其 compute_generation 也对不上，
    不会被认作当前有效归档。
    """
    doc = read_artifact(sample.id)
    if not doc or doc.get('sample_id') != sample.id:
        return False
    return doc.get('compute_generation') == (sample.compute_generation or 1)
