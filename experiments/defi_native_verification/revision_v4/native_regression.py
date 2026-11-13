import argparse,json,subprocess
from pathlib import Path
import numpy as np
from row_codec import loadrows
from evaluate import EVENT,request_tokens

def call(cmd, reject=False):
    r=subprocess.run(list(map(str,cmd)),capture_output=True,text=True)
    if reject:
        assert r.returncode==2 and 'conflicting authenticated event identity' in r.stderr,(cmd,r.stdout,r.stderr)
    else:
        assert r.returncode==0,(cmd,r.stdout,r.stderr)
    return r

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--results',type=Path,required=True);ap.add_argument('--out',type=Path,required=True);args=ap.parse_args()
    args.out.mkdir(exist_ok=False)
    checks=[]
    for method in ['mbft','vchain']:
        cfg=json.loads((args.results/method/'small/S1_w0/client.json').read_text())
        manifest=json.loads(Path(cfg['trusted_manifest']).read_text());q=json.loads(Path(cfg['query']).read_text())
        binary=args.out/(method+'.bin');jsonl=args.out/(method+'.jsonl')
        for dest in [binary,jsonl]:call([cfg['exe'],'verify',cfg['proofs'][0],manifest['shards'][0]['trusted'],request_tokens(q),dest])
        expected=np.array([tuple(json.loads(s)) for s in jsonl.read_text().splitlines()],dtype=EVENT)
        actual=loadrows([binary]);np.testing.assert_array_equal(actual,expected)
        checks.append({'method':method,'binary_json_identical':True,'rows':len(actual)})


    source=args.out/'collision.jsonl';store=args.out/'collision.store';roots=args.out/'collision.roots';proof=args.out/'collision.proof'
    rows=[]
    for i in [1,2]:
        rows.append({'event_id':'same','event_type':'borrow_positive','canonical_event_type':'borrow',
                     'row':[i,1,1,i,1,0,0,2,1],'mbft_block':1,'mbft_amount':1})
    source.write_text(''.join(json.dumps(r)+'\n' for r in rows))
    exe=json.loads((args.results/'mbft/small/S1_w0/client.json').read_text())['exe']
    call([exe,'seal',source,roots,'borrow_positive',store])
    call([exe,'prove',store,roots,'borrow_positive',proof])
    call([exe,'verify',proof,roots,'borrow_positive',args.out/'collision.bin'],reject=True)
    checks.append({'conflicting_authenticated_identity_rejected':True})
    (args.out/'report.json').write_text(json.dumps({'passed':True,'checks':checks},indent=2))
    print(json.dumps(checks))

if __name__=='__main__':main()
