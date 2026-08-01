#!/usr/bin/env python3
from __future__ import annotations
import json, shutil, struct, subprocess, sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
VAL=ROOT/'validation'

def run(cmd):
    print('+',' '.join(map(str,cmd)),flush=True)
    p=subprocess.run([str(x) for x in cmd],cwd=str(ROOT),text=True)
    if p.returncode:
        raise SystemExit(p.returncode)

def tamper_payload(proof:Path,out:Path):
    b=bytearray(proof.read_bytes()); off=7
    def u32():
        nonlocal off
        x=struct.unpack_from('<I',b,off)[0];off+=4;return x
    def u64():
        nonlocal off
        x=struct.unpack_from('<Q',b,off)[0];off+=8;return x
    def skipstr():
        nonlocal off
        n=u32();s=off;off+=n;return s,n
    off+=8
    for _ in range(u32()):
        for _ in range(u32()):skipstr()
    found=False
    for _ in range(u32()):
        off+=12
        for _ in range(u64()):
            flag=b[off];off+=1+4+4
            dlen=u32();off+=dlen;off+=4+4+8
            start,n=skipstr()
            if flag==1 and n and not found:
                payload=bytes(b[start:start+n]);marker=b'"event_id":"';k=payload.find(marker)
                if k>=0:
                    pos=start+k+len(marker);b[pos]=ord('X') if b[pos]!=ord('X') else ord('Y');found=True
    if not found:raise RuntimeError('no opened payload in proof')
    out.write_bytes(b)

def main():
    adapt=ROOT/'systems'/'nft'/'mbft'
    build=adapt/'build'
    run(['cmake','-S',adapt/'mbft','-B',build,'-DCMAKE_BUILD_TYPE=Release'])
    run(['cmake','--build',build,'--config','Release','--target','mbft_nft_adapt','-j','2'])
    exe=build/('mbft_nft_adapt.exe' if sys.platform.startswith('win') else 'mbft_nft_adapt')
    if not exe.exists() and (build/'Release'/'mbft_nft_adapt.exe').exists():exe=build/'Release'/'mbft_nft_adapt.exe'

    mbft_out=VAL/'smoke_recheck_mbft';shutil.rmtree(mbft_out,ignore_errors=True)
    run([sys.executable,adapt/'run_mbft_nft_query_verify.py','--events',VAL/'synthetic_nft_events.csv','--outdir',mbft_out,'--exe',exe])
    report=json.loads((mbft_out/'mbft_nft_query_verify_report.json').read_text())
    assert report['all_pass'] and len(report['rows'])==5

    t=VAL/'smoke_recheck_tamper';shutil.rmtree(t,ignore_errors=True);t.mkdir()
    proof=mbft_out/'atomic'/'proofs'/'nfttrade.mbfhint.proof';tampered=t/'tampered.proof';tamper_payload(proof,tampered)
    p=subprocess.run([str(exe),'verify','--proof',str(tampered),'--commitments',str(mbft_out/'atomic'/'commitments.json'),'--out',str(t/'out.jsonl'),'--metrics',str(t/'metrics.json'),'--event-type','nfttrade'])
    assert p.returncode!=0

    fq_out=VAL/'smoke_recheck_funnelq';shutil.rmtree(fq_out,ignore_errors=True)
    run([sys.executable,ROOT/'experiments'/'run_funnelq_query_verify.py',
         '--query-stores-dir',VAL/'funnelq_synthetic_query_stores',
         '--verification-sidecar-db',VAL/'funnelq_synthetic_stores'/'hvceq'/'nft_hvceq_episode_sidecar.db',
         '--manifest-dir',VAL/'funnelq_synthetic_manifest',
         '--chain-epoch-json',VAL/'funnelq_synthetic_chain_readback.json',
         '--out-dir',fq_out,'--queries','L3_RETURN,L6_NEG_RETURN_SUPPORT,H5_23',
         '--payload-audit-mode','full','--replay-engine','python','--hvceq-exact-engine','python'])
    report=json.loads((fq_out/'funnelq_query_verify_report.json').read_text())
    assert report['all_pass'] and len(report['rows'])==3
    print('ALL SMOKE TESTS PASSED')

if __name__=='__main__':main()
