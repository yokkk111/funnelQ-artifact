import importlib.util,subprocess,sys,time
from pathlib import Path
from trust import *

def run(c,out):
    e=parse(Path(c['manifest']).read_bytes());pin=parse(Path(c['pin']).read_bytes())
    require(e['method']==c['method'],'client method mismatch')
    buffers={k:Path(p).read_bytes() for k,p in c['trust_files'].items()}
    auth=authenticate(e,buffers,pin,Rpc(c['rpc']))
    names={'shared_bundle':'bundle.json','config':'config.json','public_key':'public_key.bin','trusted_headers':'trusted_headers.bin'}
    snapshots={}
    for role,b in buffers.items():
        p=out/names.get(role,role+'.json');p.write_bytes(b);p.chmod(0o400);snapshots[role]=str(p)
    del buffers
    (out/'kernel_started.json').write_bytes(canonical(dict(chain_bound=True)))
    begin=time.perf_counter_ns()
    if c['method']=='funnelq':
        p=subprocess.run([c['binary'],'scc-ablation-client',snapshots['shared_bundle'],c['query'],c['packet'],str(out/'native.json'),'count'],capture_output=True)
        (out/'native.stderr').write_bytes(p.stderr);require(p.returncode==0,'FunnelQ reject: '+p.stderr.decode(errors='replace')[-1500:])
        native=parse((out/'native.json').read_bytes());require(native['accepted'],'FunnelQ reject')
        answer=dict(count=native['answers']);response_bytes=Path(c['packet']).stat().st_size
    elif c['method']=='mbft':
        schedule=[]
        for i,proof in enumerate(c['proofs'],1):
            role=f'shard_{i:02}';entry=e['files'][i-1];require(entry['role']==role,'shard mapping')
            schedule.append(dict(id=i,proof=proof,trusted=snapshots[role],trusted_sha256=entry['sha256']))
        require(len(schedule)==20,'shard coverage')
        sched=out/'schedule.json';sched.write_bytes(canonical(schedule))
        desc=out/'mbft.json';desc.write_bytes(canonical(dict(exe=c['binary'],claim=c['claim'],query=c['query'],schedule=str(sched))))
        module=importlib.util.spec_from_file_location('mbft_native_client',c['legacy_client'])
        m=importlib.util.module_from_spec(module);module.loader.exec_module(m)
        m.main(desc,out)
        native=parse((out/'decision.json').read_bytes());require(native['accepted'],'MBFT reject')
        answer=native['answer'];response_bytes=native['wire']['response_bytes']
    else:
        require(c['method']=='vchain','unknown method')
        p=subprocess.run([c['binary'],'client',str(out),c['query'],c['packet'],str(out/'native.json'),c.get('native_mode','clean')],capture_output=True)
        (out/'native.stderr').write_bytes(p.stderr);require(p.returncode==0,'vChain reject: '+p.stderr.decode(errors='replace')[-1500:])
        native=parse((out/'native.json').read_bytes());require(native['accepted'],'vChain reject')

        bindings=sorted(native.pop('bindings'));require(len(bindings)==native['answers'],'binding count')
        answer=dict(count=native['answers'],sha256=sha(canonical(bindings)))
        response_bytes=Path(c['packet']).stat().st_size
    native_ms=(time.perf_counter_ns()-begin)/1e6
    decision=time.monotonic_ns()
    return dict(accepted=True,decision_ns=decision,chain=auth,native_boundary_ms=native_ms,
                answer=answer,native=native,response_bytes=response_bytes)

if __name__=='__main__':
    c=parse(Path(sys.argv[1]).read_bytes());out=Path(sys.argv[2]);out.mkdir(parents=True,exist_ok=False)
    try:r=run(c,out)
    except Exception as ex:
        (out/'decision.json').write_bytes(canonical(dict(accepted=False,error=str(ex))));raise
    (out/'decision.json').write_bytes(canonical(r))
