import argparse,hashlib,importlib.util,json,time
from pathlib import Path
import numpy as np
from evaluate import exact,normalized,request_tokens

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--out',type=Path,required=True);args=ap.parse_args()
    args.out.mkdir(exist_ok=False);src=Path('/suite/source')
    spec=importlib.util.spec_from_file_location('workloads',src/'research/defi_scc/prepare.py')
    w=importlib.util.module_from_spec(spec);spec.loader.exec_module(w)
    data=src/'research/defi_interfaces';a=np.load(data/'events.npy')
    state={'state':'running','rows':len(a),'completed':[],'formal_repeats':0}
    def save():(args.out/'status.json').write_text(json.dumps(state,indent=2))
    save()
    for label,win,_,q in w.workloads():
        key=f'{label}_w{win}';state['active']=key;save();stats={};t=time.perf_counter()
        got=exact(a,q,stats)
        with np.load(data/f'{key}_answers.npz') as z:expected=normalized(z['full'])
        assert np.array_equal(got,expected),(key,'full binding mismatch')
        r={'query':key,'answers':len(got),'FP':0,'FN':0,'kernel':stats,
           'seconds_including_reference_check':time.perf_counter()-t,'atomic_request':request_tokens(q),
           'answer_sha256':hashlib.sha256(got.tobytes()).hexdigest()}
        (args.out/(key+'.json')).write_text(json.dumps(r,indent=2));state['completed'].append(r);save()
        print(key,len(got),stats['total_seconds'],flush=True)
    state['state']='passed';save()

if __name__=='__main__':main()
