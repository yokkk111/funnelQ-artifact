import json
import time
from pathlib import Path

def read_external_memory(path):
    if path is None:return 0
    r=json.loads(Path(path).read_text())
    age=time.time()-r['time_unix']
    if not 0 <= age <= 5 or type(r['rss_bytes']) is not int or r['rss_bytes']<=0:
        raise RuntimeError('missing/stale external database memory sample')
    return r['rss_bytes']
