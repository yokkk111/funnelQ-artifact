import argparse
import json
import signal
import subprocess
import sys
import time
from pathlib import Path
from resident_query import phase

ap = argparse.ArgumentParser()
ap.add_argument('--out', type=Path, required=True)
a = ap.parse_args()
a.out.mkdir(parents=True, exist_ok=False)

signal.signal(signal.SIGTERM, signal.SIG_IGN)
p = subprocess.Popen([sys.executable, '-c', 'import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(60)'])
(a.out/'child.json').write_text(json.dumps({'pid':p.pid}))
phase(a.out, 'native-hang-test', .2)
time.sleep(60)
