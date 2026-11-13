import datetime,json,sys,zlib
from pathlib import Path
from codec import *
from trust import canonical,file_sha,parse

BASE=Path('/suite');DEF=BASE/'results/defi_verification_ten_v1'
NFT=BASE/'results/nft_mbft_verification_ten_v1'
VC=BASE/'verification_baselines/results/nft_vchain/repeat_01'
ABL=BASE/'results/nft_ablation_v1/scc'

def main(out):
    out.mkdir(parents=True,exist_ok=False);rows=[];frozen={}
    def track(p):
        p=Path(p);s=str(p)
        if s not in frozen:frozen[s]=dict(bytes=p.stat().st_size,sha256=file_sha(p))
        return p
    def one(dataset,method,case,parts,native,removed):
        r=dict(dataset=dataset,method=method,case=case,native_response_bytes=native,
               native_zlib_layers_removed=removed,**measure(parts));rows.append(r)
        (out/'rows.json').write_bytes(canonical(rows))
        (out/'status.json').write_bytes(canonical(dict(state='running',completed=len(rows),planned=75,active=f'{dataset}/{method}/{case}')))
        print(dataset,method,case,native,r['zlib6_response_bytes'],flush=True)
    for desc in sorted((DEF/'descriptors').glob('*.json')):
        c=parse(track(desc).read_bytes());method=c['method'];case=c['case']
        if method=='funnelq':
            p=track(c['packet']);one('defi',method,case,[defi_funnelq(p.read_bytes())],p.stat().st_size,'directory and every adaptive event/answer column')
        else:
            config=parse(track(c['config']).read_bytes());parts=[track(p) for p in config['proofs']]+[track(config['claims'])]
            one('defi',method,case,parts,sum(size(p) for p in parts),'none: native JSON/bincode and NPY are not zlib compressed')
    schedule=parse(track(NFT/'trusted_schedule.json').read_bytes());proofs=[track(s['proof']) for s in schedule]
    for q in range(1,6):
        case=f'Q{q}';p=track(ABL/f'q{q}_w1800_shared/rep1.packet')
        one('nft','funnelq',case,[nft_funnelq(p.read_bytes())],p.stat().st_size,'event columns and answer payload')
        c=parse(track(NFT/(case+'.json')).read_bytes());claim=track(c['claim']);parts=proofs+[claim]
        one('nft','mbft',case,parts,sum(size(p) for p in parts),'none: native proof files and CSV')
        p=track(VC/case/'response.zlib');one('nft','vchain',case,[inflate(p.read_bytes())],p.stat().st_size,'whole bincode response')
    require(len(rows)==75,'case coverage')
    for p,h in frozen.items():require(file_sha(p)==h['sha256'],'original file changed')
    (out/'frozen_inputs.json').write_bytes(canonical(frozen))
    (out/'contract.json').write_bytes(canonical(dict(version='normalized-native-zlib6-size-control/v1',
        codec='zlib level 6, DEFLATED, wbits 15, memLevel 8, DEFAULT_STRATEGY; one stream per full response; no dictionary',
        zlib_runtime=zlib.ZLIB_RUNTIME_VERSION,framing='VCSIZE01 + uint32 part count + uint64 length per part; no names/timestamps',
        scope='all native proof and answer bytes including opened events; trusted epoch inputs/query request/RPC are separate',
        normalization='remove existing zlib layers; retain native JSON/bincode/column shuffle/delta coding; no inner zlib remains',
        caution='size-only control, not native wire format; not a common serialization experiment; cannot attribute all differences to ADS structure',
        repetitions='deterministic byte count once per frozen configuration; streaming compress/decompress SHA256 roundtrip',
        mutation='no original proof, answer, manifest, database or index changed')))
    (out/'status.json').write_bytes(canonical(dict(state='passed',completed=75,roundtrips=75,original_inputs_unchanged=True)))

if __name__=='__main__':main(Path(sys.argv[1]))
