import subprocess
import sys
import time
from pathlib import Path
from protocol import *
from trust import authenticate
from business import validate,finish

def run(c,out):
    q=parse(Path(c['query']).read_bytes());validate(q)
    m=parse(Path(c['manifest']).read_bytes());pin=parse(Path(c['pin']).read_bytes())
    need(c['method']==m['method'],'client method')
    buffers={k:Path(p).read_bytes() for k,p in c['trust_files'].items()}
    chain=authenticate(m,buffers,pin,c['rpc'])
    dictionary=parse(buffers['dictionary'])
    need(len(dictionary['transactions'])==m['transactions'],'transaction universe')

    for i in range(m['shards']):
        if c['method']=='mbft':(out/f'{i}_roots.json').write_bytes(buffers[f'{i}_roots'])
        if c['method']=='vchain':
            d=out/f'trust_{i}';d.mkdir()
            for role,file in [('headers','headers.bin'),('pk','pk.bin')]:
                (d/file).write_bytes(buffers[f'{i}_{role}'])
    (out/'authenticated_context.json').write_bytes(js(chain))
    blob=Path(c['packet']).read_bytes();parts=unpack(blob)
    need(len(parts)==m['shards']+1,'proof shard coverage')
    t=time.perf_counter();native=[];stages={}
    if c['method']=='funnelq':rows,stages=verify(parse(parts[0]),parse(buffers['ads']),q)
    else:
        rows=[]
        for i,proof in enumerate(parts[:-1]):
            p=out/f'proof_{i}.bin';p.write_bytes(proof)
            dest=out/f'rows_{i}.jsonl';trusted=out/f'{i}_roots.json' if c['method']=='mbft' else out/f'trust_{i}'
            command=[c['binary'],'verify',str(p),str(trusted),TYPES,str(dest)]
            r=subprocess.run(command,capture_output=True)
            (out/f'native_{i}.stderr').write_bytes(r.stderr)
            need(r.returncode==0,'native rejection: '+r.stderr.decode(errors='replace')[-1500:])
            native.append(parse(r.stdout))
            with dest.open() as f:
                for line in f:
                    row=parse(line);need(row[2] in (0,1,2),'unexpected atomic type');rows.append(row)
    auth_seconds=time.perf_counter()-t;t=time.perf_counter()
    groups=decode(rows,dictionary)
    answers=sorted(tx for tx,rs in groups.items() if finish(rs,q))

    claimed=parse(parts[-1]);need(isinstance(claimed,list) and all(isinstance(x,str) for x in claimed),'answer schema')
    need(claimed==sorted(set(claimed)),'answer order/duplicates')
    need(answers==claimed,'result set mismatch')
    semantic_seconds=time.perf_counter()-t
    decision=time.monotonic_ns()
    return dict(accepted=True,decision_ns=decision,method=c['method'],chain=chain,
                authenticated_rows=len(rows),opened_transactions=len(groups),stages=stages,
                answer_count=len(answers),answer_sha256=sha(js(answers)),answers=answers,
                native=native,auth_seconds=auth_seconds,semantic_seconds=semantic_seconds,
                response_bytes=len(blob),shared_trust_bytes=sum(len(b) for b in buffers.values())+len(js(m)),
                source_database_used=False,previous_verification_reused=False)

if __name__=='__main__':
    out=Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=False)
    try:result=run(parse(Path(sys.argv[1]).read_bytes()),out)
    except Exception as e:
        (out/'decision.json').write_bytes(js(dict(accepted=False,error=repr(e))));raise
    (out/'decision.json').write_bytes(js(result))
