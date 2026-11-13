import argparse, importlib.util, json, os, subprocess, sys, time
from pathlib import Path
from contract import digest,answer_id,packet_bytes,response

def require(x,why):
    if not x:raise ValueError(why)

def verify(method,case,config,out):
    if not __debug__:raise RuntimeError('assertions must be enabled for legacy native verifiers')
    if method=='funnelq':
        source=Path(config['code'])
        sys.path.insert(0,str(source.parent))
        spec=importlib.util.spec_from_file_location('joint_protocol',source)
        m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
        q=json.loads(Path(config['query']).read_bytes())
        meta=Path(config['trusted']);trusted=json.loads(meta.read_bytes())
        packet=Path(config['packet']);blob=packet.read_bytes()
        m.ADAPTIVE=blob[:8]==b'DJADS002';require(m.ADAPTIVE,'not the adaptive compact packet')
        m.MAGIC=b'DJADS002'
        db=m.duckdb.connect();db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
        blocks,domains,answers,_=m.authenticate(blob,trusted,q)
        m.tables(db,blocks,domains,q)
        count=int(db.execute(m.base.sql(q,True,True)).fetchone()[0])
        m.sound_unique(blocks,answers,q)
        require(count==len(answers),'count seal')
        decision=time.monotonic_ns()
        db.close()
        wire=response(*packet_bytes(blob));require(wire['response_bytes']==len(blob),'byte partition')
        trusted_bytes=meta.stat().st_size
    else:
        import numpy as np
        sys.path.insert(0,config['engine'])
        from evaluate import validate,request_tokens,exact,normalized,row_token
        from row_codec import loadrows
        cfg=json.loads(Path(config['config']).read_bytes())
        q=json.loads(Path(cfg['query']).read_bytes());validate(q);types=request_tokens(q)
        meta=Path(cfg['trusted_manifest']);manifest=json.loads(meta.read_bytes())
        proofs=[Path(p) for p in cfg['proofs']]
        require(cfg['method']==manifest['method']==method and len(proofs)==len(manifest['shards'])>0,'method/shard mismatch')
        require(len({p.resolve() for p in proofs})==len(proofs),'duplicate shard proof')
        require(cfg['exe']==config['exe'],'unexpected native binary')
        decoded=[];trusted_files={meta}
        for i,(proof,s) in enumerate(zip(proofs,manifest['shards'])):
            for p,h in s['trusted_hashes'].items():
                require(digest(p)==h,'trusted shard mismatch');trusted_files.add(Path(p))
            dest=out/f'rows_{i}.bin'
            r=subprocess.run([cfg['exe'],'verify',str(proof),s['trusted'],types,str(dest)],capture_output=True)
            require(r.returncode==0,r.stderr[-2000:])
            decoded.append(dest)
        rows=loadrows(decoded)
        require(set(row_token(r) for r in np.unique(rows[['kind','sign']]))<=set(types.split(',')),'invalid category')
        answers=exact(rows,q,{})
        claims=Path(cfg['claims']);claimed=np.load(claims,allow_pickle=False)
        require(claimed.dtype==answers.dtype and len(np.unique(claimed))==len(claimed),'invalid claim schema/duplicates')
        require(np.array_equal(normalized(claimed),answers),'claim mismatch')
        decision=time.monotonic_ns()
        for p in decoded:p.unlink()
        wire=response(sum(p.stat().st_size for p in proofs),claims.stat().st_size)
        trusted_bytes=sum(p.stat().st_size for p in trusted_files)

    return {'accepted':True,'decision_ns':decision,'answer':answer_id(answers),'wire':wire,
            'trusted_state_bytes':trusted_bytes,'query_bytes':Path(config['query']).stat().st_size}

if __name__=='__main__':
    ap=argparse.ArgumentParser();ap.add_argument('manifest',type=Path);ap.add_argument('out',type=Path)
    args=ap.parse_args()

    c=json.loads(args.manifest.read_bytes())
    result=verify(c['method'],c['case'],c,args.out)
    (args.out/'decision.json').write_text(json.dumps(result))
