import argparse, importlib.util, json, subprocess, sys, time
from pathlib import Path
from binding import Rpc,authenticate,parse,canonical,require

def run(config,dest):
    envelope=parse(Path(config['envelope']).read_bytes())
    pin=parse(Path(config['pin']).read_bytes())                                        
    buffers={role:Path(p).read_bytes() for role,p in config['manifests'].items()}
    auth=authenticate(envelope,buffers,pin,Rpc(config['rpc']))

    snapshots={}
    for role,data in buffers.items():
        p=dest/(role+'.json');p.write_bytes(data);p.chmod(0o400);snapshots[role]=str(p)
    (dest/'kernel_started.json').write_bytes(canonical({'chain_bound':True}))
    begin=time.perf_counter_ns()
    if config['dataset']=='defi':
        sys.path.insert(0,config['legacy_dir'])
        spec=importlib.util.spec_from_file_location('legacy_client',Path(config['legacy_dir'])/'client.py')
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        c=dict(config['legacy']);c['trusted']=snapshots[config['role']]
        result=m.verify('funnelq',config['case'],c,dest)

        require(result['accepted'],'native reject')
    else:
        output=dest/'native.json'
        cmd=[config['binary'],'scc-ablation-client',snapshots['shared_bundle'],config['query'],config['packet'],str(output),'count']
        p=subprocess.run(cmd,capture_output=True,timeout=600)
        (dest/'native.stderr').write_bytes(p.stderr)
        require(p.returncode==0,'native reject: '+p.stderr.decode(errors='replace')[-2000:])
        result=parse(output.read_bytes());require(result['accepted'],'native reject')
    native_ms=(time.perf_counter_ns()-begin)/1e6
    decision=time.monotonic_ns()
    return dict(accepted=True,decision_ns=decision,chain=auth,native_boundary_ms=native_ms,native=result)

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('config',type=Path);ap.add_argument('out',type=Path);a=ap.parse_args()
    a.out.mkdir(mode=0o700,parents=True,exist_ok=False)
    try:
        r=run(parse(a.config.read_bytes()),a.out)
    except Exception as e:
        (a.out/'decision.json').write_bytes(canonical(dict(accepted=False,error=str(e))))
        raise
    (a.out/'decision.json').write_bytes(canonical(r))
