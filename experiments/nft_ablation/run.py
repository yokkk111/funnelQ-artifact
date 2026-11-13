from pathlib import Path
import datetime,fcntl,hashlib,json,os,shutil,signal,statistics,subprocess,sys,time,traceback
import psutil

HERE=Path(__file__).resolve().parent
BASE=Path(os.environ.get('FUNNELQ_SUITE', '/suite'))
N=BASE/'nft-query-v1'
SOURCE=N/'source'
OUT=BASE/'results/nft_ablation_v1'
SCC=Path('/suite/results/nft_scc_formal')
EXE=Path('/suite/source/FunnelQ_Columnar_Count_Experiment/target/release/funnelq_columnar_count_experiment')
QUERY=('full','no_anchor','no_refined','no_pruning')
LAYOUT=('shared','complete','single_max','independent')

def put(p,obj):
    t=p.with_suffix('.next');t.write_text(json.dumps(obj,indent=2));t.replace(p)
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(8<<20),b''):h.update(b)
    return h.hexdigest()
def one(s,a,b):
    assert s.count(a)==1,a
    return s.replace(a,b,1)

def build():
    src=SOURCE/'systems/nft/hvceq/local_executor_rust';dst=OUT/'crate'
    shutil.copytree(src,dst,ignore=shutil.ignore_patterns('target'))
    original=src/'src/main.rs';native=(SOURCE/'driver/funnelq_resident.rs').read_text()
    assert sha(original)=='85826380b58976584b25033d8be9fab274c63b9129a75a4417331ecfea22d6a1'
    core=one(original.read_text(),'fn main() -> Result<()> {','fn legacy_main() -> Result<()> {')

    core=one(core,'let mut by_src: HashMap','let build_posting = Ablation::parse(&std::env::var("NFT_ABLATION")?)?.use_refined();\n    let mut by_src: HashMap')
    core=one(core,'        by_src\n            .entry','        if build_posting { by_src\n            .entry')
    core=one(core,'            .push(idx);\n        edges.push(edge);','            .push(idx); }\n        edges.push(edge);')
    native=one(native,'let (anchors, _) = load_anchor_directory(&conn)?;',
        'let name=std::env::var("NFT_ABLATION")?; let variant=Ablation::parse(&name)?;\n'
        '    let anchors=if variant.use_anchor_directory(){Some(load_anchor_directory(&conn)?.0)}else{None};\n'
        '    if runtime.by_src.is_empty()==variant.use_refined(){bail!("posting isolation");}')
    native=one(native,'let (candidates, cand0, cand1, _, _)','let (mut candidates, cand0, cand1, _, _)')
    native=one(native,'kind, Ablation::Full, &summaries, Some(&anchors)','kind, variant, &summaries, anchors.as_deref()')
    native=one(native,'= enumerate_selected_matches_fused(kind, &runtime, &candidates, delta, usize::MAX, &pool);',
        '= if variant.use_refined(){enumerate_selected_matches_fused(kind, &runtime, &candidates, delta, usize::MAX, &pool)}else{\n'
        '            candidates.sort_by_key(|s|(s.anchor_order,s.closure_id));\n'
        '            let (r,n,e)=enumerate_selected_matches_generic(kind,&runtime,&candidates,delta,&pool);(r,n,e,candidates.len())};')
    native=one(native,'"frame":"result","matches"','"frame":"result","variant":name,"use_anchor_directory":anchors.is_some(),"use_base":variant.use_base(),"use_refined":variant.use_refined(),"universe":summaries.len(),"matches"')
    (dst/'src/main.rs').write_text(core+'\n'+native)
    tc=Path(shutil.which('cargo')).parent
    subprocess.run([str(tc/'cargo'),'build','--manifest-path',str(dst/'Cargo.toml'),'--offline','--locked','--release',
                    '--config','source.crates-io.replace-with="vendored-sources"','--config',f'source.vendored-sources.directory="{N}/build/vendor"'],
                   check=True,env={**os.environ,'RUSTC':str(tc/'rustc')})
    binary=dst/'target/release/nft_hvceq_local_executor'
    put(OUT/'versions.json',{'core_sha256':sha(original),'variant_wrapper_and_core_sha256':sha(dst/'src/main.rs'),
        'query_binary':sha(binary),'standard_query_driver':sha(SOURCE/'driver/run.py'),'scc_binary':sha(EXE)})
    return binary

def run_query(v,binary):
    job=json.loads((N/'jobs/funnelq.json').read_text());job.update(executable=str(binary),binary_sha256=sha(binary),ablation=v)
    jf=OUT/(v+'_job.json');put(jf,job)
    subprocess.run([sys.executable,str(SOURCE/'support/resident_watchdog.py'),'--out',str(OUT/(v+'_monitor')),
                    '--session-seconds','39600','--',sys.executable,str(SOURCE/'driver/run.py'),'--method','funnelq',
                    '--job',str(jf),'--out',str(OUT/v),'--repeats','10'],env={**os.environ,'NFT_ABLATION':v},check=True)
    groups=[]
    for q in range(1,6):
        rr=[json.loads((OUT/v/f'repeat_{i:02}'/f'Q{q}/result.json').read_text()) for i in range(1,11)]
        for r in rr:
            d=r['details'];assert r['measured'] and r['full_binding_equal'] and r['FP']==r['FN']==0
            assert d['variant']==v and d['use_anchor_directory']==(v in ('full','no_refined')) and d['use_base']==(v!='no_pruning') and d['use_refined']==(v in ('full','no_anchor'))
            assert d['universe']>=d['cand0']>=d['cand1']>=d['cand2']
            if v in ('no_pruning','no_refined'):assert d['cand2']==d['cand1']
            if v=='no_pruning':assert d['cand2']==d['universe']
            if v!='full':
                ref=json.loads((OUT/'full'/f'repeat_{r["repeat"]:02}'/f'Q{q}/result.json').read_text())
                assert r['full_result_digest']==ref['full_result_digest']
                if v=='no_anchor':assert all(d[k]==ref['details'][k] for k in ['cand0','cand1','cand2'])
        groups.append(dict(case=f'Q{q}',variant=v,n=10,median_s=statistics.median(r['query_latency_s'] for r in rr),
                           median_peak_rss_bytes=statistics.median(r['query_peak_rss_bytes'] for r in rr),
                           **{k:rr[0]['details'][k] for k in ['universe','cand0','cand1','cand2']}))
    return groups

def scc_case(v,q):
    label=f'q{q}_w1800';dest=OUT/'scc'/f'{label}_{v}'

    cmd=[str(EXE),'scc-ablation-case',str(SCC/f'sealed_{v}/bundle.json'),str(SCC/(label+'.query.json')),
         str(SCC/(label+'.expected.json')),str(dest),'10']
    with (OUT/'scc'/f'{label}_{v}.log').open('x') as log:
        p=subprocess.Popen(cmd,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            root=psutil.Process(p.pid);start=time.monotonic()
            while p.poll() is None:
                rss=0
                try:procs=[root,*root.children(recursive=True)]
                except psutil.NoSuchProcess:procs=[]
                for x in procs:
                    try:rss+=x.memory_info().rss
                    except psutil.NoSuchProcess:pass
                if rss>64*2**30 or time.monotonic()-start>7200:raise RuntimeError('SCC case exceeded safety limit')
                time.sleep(.1)
            if p.returncode:raise RuntimeError(f'SCC failed: see {log.name}')
        finally:
            if p.poll() is None:os.killpg(p.pid,signal.SIGKILL);p.wait()
    a=json.loads((dest/'summary.json').read_text());assert a['variant']==v and a['client_fresh_process_each_run']
    assert [r['rep'] for r in a['runs']]==list(range(11))
    for r in a['runs']:
        assert r['warmup']==(r['rep']==0) and r['FP']==r['FN']==0 and {c['mode'] for c in r['clients']}=={'count','replay'}
        assert all(c['accepted'] and c['answers']==r['answers'] and c['client_read_verify_seconds']<600 for c in r['clients'])
    rr=a['runs'][1:]; groups={}
    for mode in ['count','replay']:
        groups[mode]=statistics.median(next(c['client_read_verify_seconds'] for c in r['clients'] if c['mode']==mode) for r in rr)
    return dict(case=f'Q{q}',variant=v,n=10,median_client_s=groups,domain_rows=a['domain_rows'],opened_rows=a['opened_rows'],
                median_response_bytes=statistics.median(r['sizes']['response_bytes'] for r in rr))

def main():
    assert __debug__ and sorted(os.sched_getaffinity(0))==[4,5,6,7]
    OUT.mkdir(parents=True,exist_ok=False);(OUT/'scc').mkdir()
    state={'state':'waiting_for_lane','query_done':[],'scc_done':[],'failed':[]}
    def save(**kw):state.update(kw,utc=datetime.datetime.now(datetime.timezone.utc).isoformat());put(OUT/'status.json',state)
    save()
    try:
        with (BASE/'results/query_lane.lock').open('a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX);save(state='building');binary=build()
            query=[];verify=[]
            for v in QUERY:
                save(state='query',active=v)
                try:query.extend(run_query(v,binary));state['query_done'].append(v);put(OUT/'query_summary.json',query)
                except Exception:state['failed'].append(dict(job='query/'+v,error=traceback.format_exc()))
                save()
            for q in range(1,6):
                for i in range(4):
                    v=LAYOUT[(q-1+i)%4];key=f'Q{q}/{v}';save(state='scc',active=key)
                    try:verify.append(scc_case(v,q));state['scc_done'].append(key);put(OUT/'scc_summary.json',verify)
                    except Exception:state['failed'].append(dict(job='scc/'+key,error=traceback.format_exc()))
                    save()
            save(state='passed' if not state['failed'] else 'needs_attention',active=None)
    except BaseException:save(state='failed',error=traceback.format_exc());raise
if __name__=='__main__':main()
