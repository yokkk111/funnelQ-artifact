import json
import subprocess
import sys
from pathlib import Path
import run as protocol

HERE=Path(__file__).resolve().parent
out=HERE/'adaptive_compact'
results=[]
for label,w,path,q in protocol.base.prior.workloads():
    qpath=out/f'{label}_w{w}_query.json'
    qpath.write_bytes(protocol.js(q))
    result=out/f'{label}_w{w}_isolated.json'
    mode='same_tx' if w==0 else 'episode'
    completed=subprocess.run([sys.executable,str(HERE/'verify_packet.py'),
                              '--manifest',str(HERE/'compact_indexes'/f'{mode}_manifest.json'),
                              '--query',str(qpath),'--packet',str(out/f'{label}_w{w}.packet'),
                              '--output',str(result)],capture_output=True,text=True)
    if completed.returncode:
        raise RuntimeError(completed.stderr)
    row=json.loads(result.read_text());row.update(label=label,window=w)
    results.append(row)
    (out/'isolated_clients.json').write_text(json.dumps(results,indent=2),encoding='utf-8')
    print(label,w,round(row['client_total_s'],3),'s',round(row['peak_client_process_rss_bytes']/2**20,1),'MiB',flush=True)
