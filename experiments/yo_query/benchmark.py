import argparse
import collections
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from business import SPEC, validate, motif, canonical_result, digest

def sql_candidates(table, placeholder):

    return f'''WITH eligible AS (
      SELECT tx FROM {table} GROUP BY tx HAVING
      MAX(CASE WHEN kind='aggregator_intent' THEN amount ELSE 0 END)>={placeholder}
      AND SUM(CASE WHEN kind='pool_swap' THEN 1 ELSE 0 END)>={placeholder}
      AND COUNT(DISTINCT CASE WHEN kind='pool_swap' AND pool<>'' THEN pool END)>={placeholder}
      AND COUNT(DISTINCT CASE WHEN kind='pool_swap' AND dex<>'' THEN dex END)>={placeholder})
      SELECT e.tx FROM eligible e WHERE EXISTS (
      SELECT 1 FROM {table} a JOIN {table} b ON b.tx=a.tx AND b.kind='pool_swap' AND b.src=a.dst AND b.seq>a.seq
      JOIN {table} c ON c.tx=b.tx AND c.kind='pool_swap' AND c.src=b.dst AND c.seq>b.seq
      WHERE a.tx=e.tx AND a.kind='pool_swap' AND a.dst<>'' AND b.dst<>'')'''

def mongo_pipeline(q):

    join=lambda output,token,order: {'$lookup':{'from':'swaps','let':{'tx':'$tx','token':token,'order':order},'pipeline':[
        {'$match':{'$expr':{'$and':[{'$eq':['$tx','$$tx']},{'$eq':['$src','$$token']},{'$ne':['$$token','']},{'$gt':['$seq','$$order']}]}}}], 'as':output}}
    return [join('next','$dst','$seq'),{'$unwind':'$next'},join('last','$next.dst','$next.seq'),
        {'$match':{'last.0':{'$exists':True}}},{'$group':{'_id':'$tx'}},
        {'$lookup':{'from':'transactions','localField':'_id','foreignField':'_id','as':'t'}},
        {'$unwind':'$t'},{'$match':{'t.max_amount':{'$gte':q['minimum_aggregator_usd']},'t.n_swaps':{'$gte':q['minimum_swaps']},'t.n_pools':{'$gte':q['minimum_pools']},'t.n_dexes':{'$gte':q['minimum_dexes']}}},
        {'$project':{'_id':1,'rows':'$t.rows'}}]

class Engine:
    def __init__(self,a):
        self.a=a;self.child=None;self.request=0;self.groups=None
        m=a.method
        if m in ('funnelq','hgmatch','mbft'):
            self.groups=json.loads((a.store/'groups.json').read_text())
        if m=='funnelq':
            sys.path.insert(0,str(a.source))
            from systems.hvceq.same_tx import query_same_tx_generic as fq
            self.fq=fq
            self.conn=sqlite3.connect((a.store/'funnelq.db').resolve().as_uri()+'?mode=ro',uri=True)
            if not fq.rust_kernel_available():raise RuntimeError('Original Rust refinement kernel required; no silent downgrade')
            self.rt=fq.EncodedSameTxRuntime(self.conn,build_tx_type_buckets=False,role_fields=['token_sold','token_bought','pool'],numeric_fields=['amount'])
            fq.prepare_numpy_runtime(self.rt)
            self.txids={v:k for k,v in json.loads((a.store/'txids.json').read_text()).items()}
        elif m=='sqlite':
            self.conn=sqlite3.connect((a.store/'events.db').resolve().as_uri()+'?mode=ro',uri=True)
        elif m=='postgresql':
            sys.path.append('/usr/lib/python3/dist-packages')
            import psycopg2
            self.conn=psycopg2.connect(host='127.0.0.1',port=55432,dbname='funnelq',user='funnelq',password='funnelq')
            self.conn.autocommit=True
            with self.conn.cursor() as c:
                c.execute("SET statement_timeout='600s'");c.execute("SET max_parallel_workers_per_gather=3");c.execute("SET work_mem='64MB'")
        elif m=='mongodb':
            sys.path.append('/usr/lib/python3/dist-packages')
            from pymongo import MongoClient
            self.conn=MongoClient('mongodb://mongo:27018',serverSelectionTimeoutMS=15000)
            self.db=self.conn[a.namespace]
        elif m in ('hgmatch','mbft'):
            cmd=([str(a.native/'hgmatch/target/release/hgmatch_same_tx_rust_fair'),'serve',str(a.store/'hgmatch.db')] if m=='hgmatch' else [str(a.native/'mbft/build/mbft_resident'),str(a.store/'mbft_manifest.json')])
            self.child=subprocess.Popen(cmd,stdin=subprocess.PIPE,stdout=subprocess.PIPE,text=True,bufsize=1)
            ready=self.read()
            if ready.get('frame')!='ready':raise ValueError(('native startup failed',ready))

    def read(self):
        raw=self.child.stdout.readline()
        if not raw:raise RuntimeError('native worker ended')
        frame=json.loads(raw)
        if frame.get('frame')=='error':raise RuntimeError(frame)
        return frame

    def execute(self,request):
        q=json.loads(request);validate(q);self.request+=1
        m=self.a.method;groups=self.groups
        if m=='funnelq':
            query=motif();anchor,_=self.fq.select_anchor(self.rt,query)
            cand0,_=self.fq.anchor_recall(self.rt,query,anchor)
            cand1=self.fq.base_filter(self.rt,query,cand0)
            cand2=self.fq.rust_refined_filter(self.rt,query,cand1)
            ids=[self.txids[int(tx)] for tx in cand2]
        elif m in ('sqlite','postgresql'):
            table='events' if m=='sqlite' else self.a.namespace+'.events'
            mark='?' if m=='sqlite' else '%s'
            cur=self.conn.cursor()
            cur.execute(sql_candidates(table,mark),[q['minimum_aggregator_usd'],q['minimum_swaps'],q['minimum_pools'],q['minimum_dexes']])
            ids=[r[0] for r in cur.fetchall()]
            groups=collections.defaultdict(list)
            for lo in range(0,len(ids),300):
                batch=ids[lo:lo+300]
                cur.execute('SELECT payload FROM '+table+' WHERE tx IN ('+','.join([mark]*len(batch))+')',batch)
                for (payload,) in cur.fetchall():
                    r=json.loads(payload);groups[r['tx']].append(r)
            cur.close()
        elif m=='mongodb':
            docs=list(self.db.swaps.aggregate(mongo_pipeline(q),allowDiskUse=True,maxTimeMS=600000))
            groups={d['_id']:d['rows'] for d in docs};ids=list(groups)
        elif m=='hgmatch':
            self.child.stdin.write(json.dumps({'request_id':self.request,'query':motif()})+'\n');self.child.stdin.flush()
            result=self.read()
            if result.get('request_id')!=self.request:raise ValueError('native sequence')
            ids=result['transactions']
        elif m=='mbft':


            self.child.stdin.write(json.dumps({'request_id':self.request,'types':['aggregator_intent']})+'\n');self.child.stdin.flush()
            ids=set();seen={};total=0
            while True:
                frame=self.read()
                if frame.get('request_id')!=self.request:raise ValueError('native sequence')
                if frame['frame']=='end':
                    if total!=frame['rows']:raise ValueError('native truncated stream')
                    break
                if frame['frame']!='selection':raise ValueError('native frame')
                for _ in range(frame['rows']):
                    row=self.read();total+=1;key=row['event_id']
                    if key in seen and seen[key]!=row:raise ValueError('conflicting event identity')
                    seen[key]=row
                    if float(row['attrs'].get('amount',0))>=q['minimum_aggregator_usd']:ids.add(row['tx_hash'])
        else:raise ValueError(m)
        return canonical_result(groups,ids,q)

    def close(self):
        if self.child:
            try:self.child.stdin.write('{"op":"close"}\n');self.child.stdin.flush();self.child.wait(timeout=15)
            except Exception:self.child.kill();self.child.wait()
        if hasattr(self,'conn'):self.conn.close()

def main(a):
    sys.path.insert(0,str(a.helpers))
    from resident_query import Memory
    a.out.mkdir(parents=True,exist_ok=False)
    request=(a.store/'query.json').read_text()

    expected=json.loads(a.reference.read_text())['selected_transactions']
    def timeout(_s,_f):raise TimeoutError('600 second complete-query deadline')
    signal.signal(signal.SIGALRM,timeout)
    results=[]
    with Memory(external_memory_file=a.external_memory_file) as memory:
        t=time.perf_counter();engine=Engine(a);startup=time.perf_counter()-t
        try:
            for repeat in range(11):
                phase=a.out/'phase.json';tmp=a.out/'phase.tmp'
                tmp.write_text(json.dumps({'started_monotonic':time.monotonic(),'limit_seconds':600,'phase':'warmup' if repeat==0 else 'query','repeat':repeat}));tmp.replace(phase)
                memory.begin();signal.alarm(600)
                t=time.perf_counter();answer=engine.execute(request);elapsed=time.perf_counter()-t
                signal.alarm(0)
                peak=memory.end()
                tmp.write_text(json.dumps({'started_monotonic':time.monotonic(),'limit_seconds':600,'phase':'answer_gate','repeat':repeat}));tmp.replace(phase)
                missing=sorted(set(expected)-set(answer));extra=sorted(set(answer)-set(expected))
                report=dict(method=a.method,repeat=repeat,warmup=repeat==0,query_seconds=elapsed,request_peak_rss_bytes=peak,count=len(answer),result_digest=digest(answer),missing=missing,extra=extra,status='PASS' if not missing and not extra else 'FAIL',query_spec_digest=digest(SPEC),candidate_cache_used=False,answers=answer)
                (a.out/f'run_{repeat:02d}.json').write_text(json.dumps(report,indent=2))
                print(json.dumps({k:v for k,v in report.items() if k!='answers'}),flush=True)
                if missing or extra:raise ValueError('Full transaction-set gate failed; formal timing stopped')
                if repeat:results.append({k:v for k,v in report.items() if k not in ('answers','missing','extra')})
        finally:engine.close()
    (a.out/'summary.json').write_text(json.dumps(dict(status='PASS',method=a.method,startup_seconds=startup,load_count=1,formal_repeats=len(results),query_semantics=SPEC,measurements=results,memory_note='process tree RSS including resident data; DB methods also include entire necessary database service tree, matching DeFi accounting'),indent=2))

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--method',required=True);p.add_argument('--store',type=Path,required=True);p.add_argument('--native',type=Path,required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--helpers',type=Path,required=True);p.add_argument('--reference',type=Path,required=True);p.add_argument('--out',type=Path,required=True);p.add_argument('--namespace',default='yo_query_v1');p.add_argument('--external-memory-file',type=Path);main(p.parse_args())
