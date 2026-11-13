from __future__ import annotations
import argparse,csv,hashlib,importlib.util,json,os,subprocess,sys,time
from pathlib import Path

HERE=Path(__file__).resolve().parent
SOURCE=HERE.parents[1]
sys.path.append('/usr/lib/python3/dist-packages')                                 
sys.path.insert(0,str(SOURCE/'experiments/defi_query'))
sys.path.insert(0,str(SOURCE/'experiments/server_support'))
from resident_query import Memory,deadline,phase,ro_connect
sys.path.insert(0,str(SOURCE))
from systems.nft.common.canonical_results import canonicalize_rows,canonical_full_result_digest,canonical_full_result_digest_csv,write_normalized_csv
from systems.nft.common.full_binding_spec import parse_full_binding_plan
from systems.nft.hvceq.selected5_query_contract import validate_selected5_query_contract

CONTRACT={
    'protocol':'nft_warm_resident_full_bindings_v1',
    'output':'all eight canonical fields, not count-only',
    'scope':'Q1-Q5 Episode 1800 seconds, original full selected5 QuerySpecs',
    'startup':'one query-independent data/index load per method session; separately reported',
    'timer':'direct wall time: JSON decoding/validation/compilation + retrieval + intermediate transport + query-dependent preparation + exact matching + result projection/canonicalization',
    'excluded':'offline index build; runtime startup; final experiment CSV/report writes; independent correctness audit',
    'warmup':'one full request per query, also full-binding gate; then ten round-robin repetitions',
    'allowed_cache':['OS page cache','DB buffer cache','query-independent resident data/index'],
    'forbidden_cache':['candidates','filtered adapter indexes','answers','previous timing values'],
    'cache_flush':False,'cold_cache_claimed':False,
    'cpu_affinity':[4,5,6,7],'memory_limit_bytes':64*2**30,
    'request_timeout_seconds':600,'startup_timeout_seconds':3600,
    'memory':'sampled aggregate process-tree RSS including DB server; baseline not subtracted; not Windows Commit',
    'historical_measurements_reused':False,
}

def save(p,value):
    with p.open('x',encoding='utf-8') as f:json.dump(value,f,indent=2,ensure_ascii=False)

def update(p,value):
    tmp=p.with_suffix('.next.json');tmp.write_text(json.dumps(value,indent=2));tmp.replace(p)

class Native:
    def __init__(self,cmd,out):
        self.log=(out/'native.stderr.log').open('x')
        self.p=subprocess.Popen(list(map(str,cmd)),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1)
        self.ready=self.read()
        if self.ready.get('frame')!='ready' or self.ready.get('load_count')!=1:raise ValueError(self.ready)
    def read(self):
        line=self.p.stdout.readline()
        if not line:raise RuntimeError(f'native exited {self.p.poll()}; see native.stderr.log')
        r=json.loads(line)
        if r.get('frame')=='error':raise RuntimeError(r)
        return r
    def query(self,req):
        self.p.stdin.write(json.dumps(req,separators=(',',':'))+'\n');self.p.stdin.flush()
        result=self.read()
        if result.get('frame')!='result':raise ValueError(result)
        return result
    def close(self):
        if self.p.poll() is None:
            self.p.stdin.write('{"op":"close"}\n');self.p.stdin.flush();self.p.wait(timeout=30)
        self.log.close()

class Adapter:
    def __init__(self,args,job):self.args=args;self.job=job;self.native=None;self.conn=None;self.request_id=0
    def prepare(self):
        a,j=self.args,self.job;m=a.method
        if m=='funnelq':
            from systems.nft.hvceq import query_nft_hvceq_full as fq
            self.engine=fq; self.conn=ro_connect(j['db']);self.conn.row_factory=__import__('sqlite3').Row
            self.conn.execute('PRAGMA cache_size=-131072');self.conn.execute('PRAGMA mmap_size=1073741824')
            self.native=Native([j['executable'],'--db',j['db'],'--tx-sidecar-db',j['tx_db'],'--threads','4'],a.out)
        elif m=='hgmatch':

            self.native=Native([j['executable'],'--db',j['db'],'--query-json','unused','--outdir','unused','--threads','4'],a.out)
        elif m=='sqlite':
            from systems.nft.sql import query_nft_sql_full as sq
            from systems.nft.common.nft_semantics import configure_sqlite
            self.engine=sq;self.conn=ro_connect(j['db']);self.conn.row_factory=__import__('sqlite3').Row;configure_sqlite(self.conn)
        elif m=='postgresql':
            import psycopg2
            from systems.nft.postgresql import query_nft_postgresql_full as pg
            self.engine=pg
            self.conn=psycopg2.connect(**j['connection'],options='-c max_parallel_workers_per_gather=4 -c work_mem=64MB -c jit=off -c default_transaction_read_only=on -c statement_timeout=600000')
        elif m=='mongodb':
            from pymongo import MongoClient


            from systems.nft.mongodb import query_nft_mongodb_full as pinned_mongo
            if not Path(pinned_mongo.__file__).is_relative_to(SOURCE):raise ValueError('Mongo source drift')
            import mongo_native_join as mo
            self.engine=mo;self.client=MongoClient(j['uri'],serverSelectionTimeoutMS=10000)
            coll=self.client[j['database']][j['collection']]
            expected=set(mo.KEYS)|{'type','_time_eq_cf'}
            if not expected.issubset(coll.index_information()):raise ValueError('native Mongo indexes absent')
            class Limited:
                def aggregate(_,pipeline,**kw):
                    kw['maxTimeMS']=600000
                    return coll.aggregate(pipeline,**kw)
            self.conn=Limited()
        elif m=='mbft':
            from systems.nft.mbft import nft_semantic_matcher as mb
            self.engine=mb
            self.native=Native([j['executable'],j['manifest']],a.out)
        else:raise ValueError(m)
        return self.native.ready if self.native else {'load_count':1,'persistent_connection':True}

    def execute(self,doc,plan):
        m=self.args.method; self.request_id+=1
        if m=='funnelq':
            r=self.native.query({'query_id':plan.query_id,'delta_seconds':plan.delta_seconds})
            matches=[tuple(x) for x in r.pop('matches')]
            events,values=self.engine._load_matched_event_projection(self.conn,matches)
            rows=self.engine._materialize_selected_exact_matches(plan,matches,events,values)
            for row in rows:
                ids=list(json.loads(row['event_bindings_json']).values())
                if len(set(ids))!=len(ids):raise ValueError('non-injective FunnelQ event IDs')
            rows,meta=canonicalize_rows(rows)
            return rows,{**r,**meta}
        if m=='hgmatch':
            r=self.native.query(doc);rows=r.pop('rows');return rows,r
        if m in {'sqlite','postgresql'}:
            compiled=[self.engine.compile_branch_sql(b) for b in plan.branches]
            if m=='postgresql':
                compiled=[(self.engine.postgres_sql(s),p,v) for s,p,v in compiled]
                rows,meta=self.engine.execute_compiled(self.conn,plan,compiled);self.conn.rollback()
            else:rows,meta=self.engine._execute_compiled(self.conn,plan,compiled)
            return rows,meta
        if m=='mongodb':
            compiled=[self.engine.compile_native(b,collection=self.job['collection'],use_time_index=True)[0] for b in plan.branches]
            return self.engine.execute_compiled(self.conn,plan,compiled)
        if m=='mbft':
            path=self.args.out/f'candidate_{self.request_id:04}.jsonl'
            types=sorted({t.lower() for b in plan.branches for p in b.event_patterns for t in p.event_types})
            t=time.perf_counter();r=self.native.query({'types':types,'candidate_path':str(path)})
            transport=time.perf_counter()-t
            t=time.perf_counter();events,duplicates=self.engine.load_jsonl([path]);load=time.perf_counter()-t
            strict=any(c.get('type')=='strict_time_order' for b in plan.branches for c in b.constraints)
            t=time.perf_counter();index=self.engine.AdapterIndex(events,need_strict_time=strict,need_order_time=not strict);prepare=time.perf_counter()-t
            t=time.perf_counter();rows=[]
            for root in index.roots:
                closure=index.closure(root,plan.delta_seconds,strict_time_order=strict)
                for branch in plan.branches:rows.extend(self.engine.match_branch(index,plan,branch,root,closure))
            matching=time.perf_counter()-t
            rows,meta=canonicalize_rows(rows)

            path.unlink()
            return rows,{**meta,'atomic_retrieval_and_write_s':transport,'candidate_load_s':load,'adapter_prepare_s':prepare,'match_s':matching,'candidate_rows':r['events'],'identical_candidate_duplicates':duplicates}
        raise ValueError(m)

    def close(self):
        if self.native:self.native.close()
        if self.args.method=='mongodb':self.client.close()
        elif self.conn is not None:self.conn.close()

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--method',required=True)
    ap.add_argument('--job',type=Path,required=True);ap.add_argument('--out',type=Path,required=True)
    ap.add_argument('--external-memory-file',type=Path);ap.add_argument('--repeats',type=int,default=10)
    args=ap.parse_args();args.out.mkdir(parents=True,exist_ok=False)
    job=json.loads(args.job.read_text());save(args.out/'contract.json',CONTRACT)
    save(args.out/'job.json',job)

    references={c['case']:canonical_full_result_digest_csv(Path(c['reference'])) for c in job['cases']}
    save(args.out/'reference_digests.json',references)
    cases=[]
    for c in job['cases']:
        raw=Path(c['query_file']).read_text();doc=json.loads(raw)
        validate_selected5_query_contract(doc)
        if parse_full_binding_plan(doc).delta_seconds!=1800:raise ValueError('NFT window must be 1800s')
        cases.append((c['case'],raw,hashlib.sha256(raw.encode()).hexdigest()))
    state={'state':'startup','completed':[],'protocol':CONTRACT['protocol'],'repeats':args.repeats}
    update(args.out/'progress.json',state)
    adapter=Adapter(args,job)
    try:
        with Memory(external_memory_file=args.external_memory_file) as memory:
            phase(args.out,'startup',3600);t=time.perf_counter();info=adapter.prepare()
            save(args.out/'startup.json',{'seconds':time.perf_counter()-t,'resident_rss_bytes':memory.begin(),**info})

            for rep in range(args.repeats+1):
                tag='gate_warmup' if rep==0 else f'repeat_{rep:02}'
                for label,raw,qhash in cases:
                    dest=args.out/tag/label;dest.mkdir(parents=True)
                    state.update(state='running',active=tag+'/'+label);update(args.out/'progress.json',state)
                    phase(args.out,tag+'/'+label,600);baseline=memory.begin()
                    with deadline(600):
                        start=time.perf_counter()
                        doc=json.loads(raw);plan=parse_full_binding_plan(doc)

                        validate_selected5_query_contract(doc)
                        rows,details=adapter.execute(doc,plan)
                        elapsed=time.perf_counter()-start
                    peak=memory.end();phase(args.out,'audit_and_report',600)
                    got=canonical_full_result_digest(rows)
                    if got!=references[label]:raise ValueError({'case':label,'expected':references[label],'actual':got})
                    if details.get('duplicate_count',0)!=0:raise ValueError('duplicate results')
                    write_normalized_csv(dest/'instances.csv',rows)
                    report={'method':args.method,'case':label,'repeat':rep,'query_sha256':qhash,
                        'query_latency_s':elapsed,'count':len(rows),'full_result_digest':got[0],
                        'full_binding_equal':True,'FP':0,'FN':0,'query_peak_rss_bytes':peak,
                        'resident_before_query_rss_bytes':baseline,'measured':rep>0,'details':details}
                    save(dest/'result.json',report);state['completed'].append({k:v for k,v in report.items() if k!='details'})
                    update(args.out/'progress.json',state);print(json.dumps({k:report[k] for k in ['method','case','repeat','count','query_latency_s']}),flush=True)
                    del rows,details
            state.update(state='passed',active=None);update(args.out/'progress.json',state)
            phase(args.out,'close',60)
    finally:adapter.close()

if __name__=='__main__':main()
