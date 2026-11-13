import copy,csv,datetime,fcntl,hashlib,importlib.util,json,os,shutil,sqlite3,struct,subprocess,sys,time,traceback
from pathlib import Path
import numpy as np

BASE=Path('/suite');HERE=Path(__file__).resolve().parent
OUT=Path(os.environ.get('NFT_SCALE_OUT',str(BASE/'results/nft_scalability_v1')))
N=BASE/'nft-query-v1';SOURCE=N/'source'
CODEC=BASE/'verification-codec-v1'
CHAIN=BASE/'chain-verification-v1'
SCC=BASE/'results/nft_scc_formal'
REL='FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin'
CUTS={20:1177453,40:2357538,60:3532155,80:4709518,100:5886895}
DT=np.dtype([('order','<u8'),('time','<u8'),('src','<u4'),('dst','<u4'),('nft','<u4'),('collection','<u4'),('tx','<u4'),('type','u1')])
STATE={'state':'starting','query_measurements':0,'verification_measurements':0,'completed_scales':[]}

def put(p,x):
    p=Path(p);t=p.with_suffix('.next');t.write_text(json.dumps(x,indent=2));t.replace(p)
def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(8<<20),b''):h.update(b)
    return h.hexdigest()
def status(stage,**kw):
    STATE.update(state=stage,utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),**kw)
    put(OUT/'status.json',STATE);print(json.dumps(STATE),flush=True)
def run(cmd,log,timeout=7200,env=None):
    log=Path(log)
    if log.exists():log=log.with_name(log.stem+f'_retry_{time.time_ns()}'+log.suffix)
    with Path(log).open('x') as f:
        print('RUN',list(map(str,cmd)),flush=True)
        subprocess.run(list(map(str,cmd)),stdout=f,stderr=subprocess.STDOUT,check=True,timeout=timeout,env=env)
def module(name,p):
    s=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
def ro(p):return sqlite3.connect('file:'+str(p)+'?mode=ro',uri=True)

def compile_verifier():
    src=BASE/'source/FunnelQ_Columnar_Count_Experiment';dst=OUT/'crate'
    built=OUT/'build_provenance.json'
    if built.exists():
        exe=dst/'target/release/funnelq_columnar_count_experiment'
        assert sha(exe)==json.loads(built.read_text())['binary_sha256']
        return exe
    if not dst.exists():shutil.copytree(src,dst,ignore=shutil.ignore_patterns('target','.git'))

    for name in ['Q1CertifiedCountTreeSimulator','FunnelQ_Residual_Obligation','FunnelQ_Runtime_Verification','experiments']:
        if not (OUT/name).exists():(OUT/name).symlink_to(BASE/'source'/name,target_is_directory=True)
    file=dst/'src/hybrid_key_audit.rs';old=(src/'src/hybrid_key_audit.rs').read_text()
    a='rows.len() == 1_123_403 && rows.windows(2).all(|w| w[0].order < w[1].order)'
    assert old.count(a)==1
    file.write_text(old.replace(a,'!rows.is_empty() && rows.windows(2).all(|w| w[0].order < w[1].order)'))
    tc=Path(shutil.which('cargo')).parent
    cmd=[tc/'cargo','build','--release','--offline','--locked','--manifest-path',dst/'Cargo.toml',
         '--config','source.crates-io.replace-with="vendored-sources"',
         '--config','source.vendored-sources.directory="/suite/rust_vendor"']
    env={**os.environ,'RUSTC':str(tc/'rustc'),'CARGO_BUILD_JOBS':'4'}
    run(cmd,OUT/'compile.log',env=env)
    exe=dst/'target/release/funnelq_columnar_count_experiment'
    run([tc/'cargo','test','--release','--offline','--locked','--manifest-path',dst/'Cargo.toml',*cmd[-4:]],OUT/'unit_tests.log',env=env)
    changed=[]
    for p in src.glob('src/**/*.rs'):
        if sha(p)!=sha(dst/p.relative_to(src)):changed.append(str(p.relative_to(src)))
    assert changed==['src/hybrid_key_audit.rs']
    put(OUT/'build_provenance.json',dict(original_loader_sha256=hashlib.sha256(old.encode()).hexdigest(),
        changed_files=changed,change='remove dataset-size constant only; preserve nonempty and unique order checks',binary_sha256=sha(exe)))
    return exe

def prefix(scale,cut,original,events,builder):
    d=OUT/f'scale_{scale:03}';d.mkdir();db=d/'query.sqlite'
    src=ro(original['db']);c=sqlite3.connect(db);builder.configure(c);builder.create_schema(c)
    c.execute('ATTACH DATABASE ? AS src',('file:'+original['db']+'?mode=ro',))
    assert list(c.execute('SELECT * FROM nft_event_type_dict'))==list(src.execute('SELECT * FROM nft_event_type_dict'))
    for table in ['nft_build_meta','nft_role_value_dict']:
        c.execute(f'INSERT INTO {table} SELECT * FROM src.{table}')
    c.execute('INSERT INTO nft_role_transition_edges SELECT * FROM src.nft_role_transition_edges WHERE order_seq<?',(cut,));c.commit()
    report=builder.build_window_summaries(c,max_delta=86400,insert_batch=100000,progress_every=1000000,resume=False)
    validation=builder.validate_store(c);c.execute('ANALYZE main');c.commit()
    data=events[events['order']<cut]
    assert len(data)==validation['events']
    path=d/REL;path.parent.mkdir(parents=True);data.tofile(path)

    c.execute('ATTACH DATABASE ? AS txdb',('file:'+original['tx_db']+'?mode=ro',))
    h=hashlib.sha256()
    for r in c.execute('SELECT e.order_seq,e.block_ts,e.src_addr_id,e.dst_addr_id,e.nft_id,e.collection_id,t.tx_id,e.event_type_id FROM nft_role_transition_edges e JOIN txdb.nft_event_tx_ids t USING(order_seq) ORDER BY e.collection_id,e.order_seq'):
        h.update(struct.pack('<QQIIIIIB',*r))
    assert h.hexdigest()==sha(path),'query/verification source mismatch'
    assert len(np.unique(data['order']))==len(data)
    assert src.execute('select max(block_ts) from nft_role_transition_edges where order_seq<?',(cut,)).fetchone()[0] < (src.execute('select min(block_ts) from nft_role_transition_edges where order_seq>=?',(cut,)).fetchone()[0] or float('inf'))
    put(d/'dataset.json',dict(scale_percent=scale,raw_prefix_rows=cut,valid_events=len(data),trade_events=int(sum(data['type']==1)),
        source_bridge_sha256=h.hexdigest(),boundary='historical complete-block chronological raw prefix; verified no shared timestamp across boundary',
        summaries_rebuilt=report,validation=validation))
    c.close();src.close()
    job=copy.deepcopy(original);job['db']=str(db)
    for case in job['cases']:
        q=case['case'];out=d/(q+'.reference.csv');original_ref=Path(case['reference'])
        with original_ref.open(newline='') as f,out.open('x',newline='') as g:
            reader=csv.DictReader(f);w=csv.DictWriter(g,reader.fieldnames);w.writeheader();keep=[]
            for row in reader:
                orders=json.loads(row['event_orders_json'])
                if all(int(x)<cut for x in orders.values()):w.writerow(row);keep.append(row)
        qdoc=json.loads((SCC/f'{q.lower()}_w1800.query.json').read_text())
        rolemap={b['id']:[a['id'] for a in b['atoms']] for b in qdoc['branches']}
        expected=[dict(branch=r['branch_id'],events=['order:'+str(json.loads(r['event_orders_json'])[role]) for role in rolemap[r['branch_id']]]) for r in keep]
        put(d/(q+'.expected.json'),expected);put(d/(q+'.query.json'),qdoc)
        case['reference']=str(out)
        put(d/(q+'.oracle.json'),dict(source=str(original_ref),source_sha256=sha(original_ref),count=len(keep),
            derivation='restrict all event orders of independently cross-checked full answers to positive-query chronological prefix'))
    put(d/'query_job.json',job);return d,job

def stats(values):
    a,b,c=np.percentile(values,[25,50,75]);return dict(q25=float(a),median=float(b),q75=float(c))

def main():
    if OUT.exists():
        prior=json.loads((OUT/'status.json').read_text())
        assert os.environ.get('NFT_SCALE_RESUME')=='1' and prior['state']=='failed' and prior['query_measurements']==prior['verification_measurements']==0
        for partial in OUT.glob('scale_*'):
            assert partial.is_dir() and partial.parent==OUT
            partial.rename(OUT/(f'failed_prefix_{time.time_ns()}_'+partial.name))
        put(OUT/f'failed_attempt_{time.time_ns()}.json',prior)
    else:OUT.mkdir(parents=True,exist_ok=False)
    assert sorted(os.sched_getaffinity(0))==[4,5,6,7]
    sys.path.insert(0,str(CODEC));import trust
    sys.path.insert(1,str(CHAIN));import binding
    supervisor=module('scale_supervisor',CHAIN/'run.py');supervisor.HERE=CODEC
    builder=module('scale_compact_builder',HERE/'build_nft_funnelq_compact_store.py')
    with (BASE/'results/query_lane.lock').open('a') as lock:
        status('waiting_for_exclusive_lane');fcntl.flock(lock,fcntl.LOCK_EX)
        original=json.loads((N/'jobs/funnelq.json').read_text());assert sha(original['executable'])==original['binary_sha256']
        assert json.loads((N/'oracle_agreement.json').read_text())
        frozen={p:sha(p) for p in [original['db'],original['tx_db'],original['executable'],str(BASE/'source'/REL),str(SOURCE/'driver/run.py')]}
        put(OUT/'frozen_sources.json',frozen)
        put(OUT/'contract.json',dict(scales=list(CUTS),queries=['Q1','Q2','Q3','Q4','Q5'],window_seconds=1800,repeats=10,warmup=1,
            query='unchanged nft_warm_resident_full_bindings_v1 driver; startup excluded; per-request loading, preparation, full materialization included',
            verification='unchanged fresh-client unified wrapper; parent start to authenticated decision includes chain reads, file reads and native subprocess',
            size='native full response physical bytes (proof+answer), same as latest NFT main plot; pure proof separately reported',
            statistic='median and interquartile range; deterministic sizes not synthetic repeated measurements',
            prefixes='same chronological raw-row prefixes as old complete-block experiment; original IDs retained, boundary summaries rebuilt',
            cache='ambient OS pages; resident query-independent query index; no candidate/answer/verification caches',
            affinity=[4,5,6,7],threads=4,originals_modified=False))
        status('compiling_scale_capable_verifier');exe=compile_verifier()
        events=np.fromfile(BASE/'source'/REL,dtype=DT);assert len(events)==3201667
        rpc=trust.Rpc('http://127.0.0.1:28547');pin0=trust.parse((BASE/'results/chain_verification_v2/nft/trusted_chain_pin.json').read_bytes())
        assert int(rpc.call('eth_chainId',[]),16)==31338
        assert trust.sha(bytes.fromhex(rpc.call('eth_getCode',[pin0['registry'],'latest'])[2:]))==pin0['runtime_sha256']
        sender=rpc.call('eth_accounts',[])[0];groups=[]
        for scale,cut in CUTS.items():
            status('building_prefix_and_query_independent_indexes',scale=scale)
            d,job=prefix(scale,cut,original,events,builder)
            run([exe,'scc-ablation-seal',d,d/'sealed_shared','shared'],d/'seal.log')
            bundle=d/'sealed_shared/bundle.json';b=trust.parse(bundle.read_bytes())
            assert b['full_rows']==int(sum((events['order']<cut)&(events['type']==1)))
            if scale==100:
                old=trust.parse((SCC/'sealed_shared/bundle.json').read_bytes())
                assert b==old,'100% generic loader changed sealed data'
            epoch='0x'+trust.sha((str(OUT)+str(scale)).encode())
            e=dict(version=trust.VERSION,dataset='nft',method='funnelq',epoch_id=epoch,
                files=[dict(role='shared_bundle',bytes=bundle.stat().st_size,sha256=sha(bundle))])
            put(d/'manifest.json',e)
            tx=binding.transaction(rpc,{'from':sender,'to':pin0['registry'],'data':'0x'+trust.COMMIT+trust.hex32(epoch)+trust.hex32(trust.root(e))})
            tag=tx['receipt']['blockNumber'];pin={**pin0,'method':'funnelq','epoch_id':epoch,'block_tag':tag,'block_hash':rpc.call('eth_getBlockByNumber',[tag,False])['hash']}
            put(d/'pin.json',pin);put(d/'commit.json',tx)
            status('query_full_binding_gate_and_50_measurements',scale=scale)
            run([sys.executable,SOURCE/'support/resident_watchdog.py','--out',d/'query_monitor','--session-seconds','39600','--',
                 sys.executable,SOURCE/'driver/run.py','--method','funnelq','--job',d/'query_job.json','--out',d/'query','--repeats','10'],d/'query.log')
            STATE['query_measurements']+=50
            configs={}
            for i in range(1,6):
                q=f'Q{i}';status('proving_and_full_binding_gate',scale=scale,case=q)
                run([exe,'scc-ablation-case',bundle,d/(q+'.query.json'),d/(q+'.expected.json'),d/(q+'_proof'),'1'],d/(q+'_proof.log'))
                s=trust.parse((d/(q+'_proof')/'summary.json').read_bytes())
                assert all(r['FP']==r['FN']==0 for r in s['runs'])
                cfg=dict(method='funnelq',case=q,binary=str(exe),query=str(d/(q+'.query.json')),
                    packet=str(d/(q+'_proof')/'rep1.packet'),manifest=str(d/'manifest.json'),pin=str(d/'pin.json'),
                    trust_files={'shared_bundle':str(bundle)},rpc=rpc.url)
                configs[q]=cfg
            status('chain_attack_gate',scale=scale);attacks=[];(d/'attacks').mkdir()
            for attack in ['root_mismatch','wrong_epoch','manifest_tamper']:
                ad=d/'attacks'/attack;ad.mkdir();cfg=copy.deepcopy(configs['Q1']);env=copy.deepcopy(e);pinx=copy.deepcopy(pin)
                if attack=='wrong_epoch':env['epoch_id']='0x'+'33'*32
                elif attack=='manifest_tamper':
                    (ad/'bundle.json').write_bytes(bundle.read_bytes()+b' ');cfg['trust_files']['shared_bundle']=str(ad/'bundle.json')
                else:
                    forged=bundle.read_bytes()+b' ';(ad/'bundle.json').write_bytes(forged)
                    cfg['trust_files']['shared_bundle']=str(ad/'bundle.json');env['files'][0].update(bytes=len(forged),sha256=trust.sha(forged))
                put(ad/'manifest.json',env);cfg['manifest']=str(ad/'manifest.json')
                r=supervisor.invoke(cfg,ad/'client');assert not r['accepted'] and not (ad/'client/kernel_started.json').exists()
                attacks.append(dict(attack=attack,rejected=True,error=r['error']))
            put(d/'attack_results.json',attacks)
            status('verification_warmup_and_50_measurements',scale=scale)
            records={q:[] for q in configs}
            for rep in range(11):
                rd=d/('verify_warmup' if rep==0 else f'verify_{rep:02}');rd.mkdir()
                for q,cfg in configs.items():
                    r=supervisor.invoke(cfg,rd/q)
                    expected=trust.parse((d/(q+'.oracle.json')).read_bytes())['count']
                    assert r['accepted'] and r['answer']['count']==expected
                    put(rd/(q+'.measurement.json'),r)
                    if rep:records[q].append(r);STATE['verification_measurements']+=1
                status('verification_repetition_complete',scale=scale,repeat=rep)
            for q,rs in records.items():
                qs=[trust.parse((d/'query'/f'repeat_{i:02}'/q/'result.json').read_bytes()) for i in range(1,11)]
                assert len(rs)==10 and all(x['full_binding_equal'] and x['FP']==x['FN']==0 for x in qs)
                s=trust.parse((d/(q+'_proof')/'summary.json').read_bytes())
                groups.append(dict(scale=scale,query=q,n=10,query_seconds=stats([x['query_latency_s'] for x in qs]),
                    verification_seconds=stats([x['client_e2e_ms']/1000 for x in rs]),
                    response_bytes=rs[0]['response_bytes'],proof_sizes=s['runs'][-1]['sizes'],result_count=qs[0]['count'],
                    chain_read_ms=stats([x['chain']['chain_binding_ms'] for x in rs])))
            STATE['completed_scales'].append(scale);put(OUT/'plot_data.json',groups)
        assert all(sha(p)==h for p,h in frozen.items()),'original source changed'
        assert len(groups)==25 and STATE['query_measurements']==STATE['verification_measurements']==250
        put(OUT/'acceptance.json',dict(status='PASS',groups=25,queries=250,verifications=250,chain_attacks_rejected=15,source_hashes_unchanged=True))
        status('passed')

if __name__=='__main__':
    try:main()
    except BaseException as ex:
        if OUT.exists():status('failed',error=repr(ex));(OUT/'failure.log').write_text(traceback.format_exc())
        raise
