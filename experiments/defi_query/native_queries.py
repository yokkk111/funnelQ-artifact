import argparse,hashlib,io,json,os,sys,time,traceback
from pathlib import Path
import numpy as np
HERE=Path(__file__).resolve().parent
V4=Path(os.environ.get('DEFI_V4',str(HERE.parent/'defi_native_verification/revision_v4')))
sys.path.insert(0,str(V4))
sys.path.append('/usr/lib/python3/dist-packages')                                
from legacy_evaluate import EVENT,brute
from compiler import pg_sql,mongo_pipeline,validate

SOURCE=Path('/suite/source/research/defi_interfaces')
OLD=Path('/defi-native-v3/results/mbft/full')
SCHEMA='defi_completion_v1'
INDEXES=[('kind','tx','eid'),('kind','actor','time','eid'),('kind','asset','time','eid'),
         ('kind','pool','time','eid'),('kind','actor','asset','time','eid'),('kind','actor','out','time','eid')]

def save(p,d):
    with Path(p).open('x',encoding='utf-8') as f:json.dump(d,f,indent=2)
def sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(8*1024*1024),b''):h.update(b)
    return h.hexdigest()
def cases():
    out=[]
    for p in sorted(OLD.glob('*/query.json')):
        q=json.loads(p.read_text());validate(q);out.append((p.parent.name,p,q))
    if len(out)!=20:raise RuntimeError('expected 20 QuerySpecs')
    return out
def connect(method):
    if method=='postgresql':
        import psycopg2
        c=psycopg2.connect(host=os.environ.get('DEFI_PGHOST','127.0.0.1'),port=int(os.environ.get('DEFI_PGPORT','55432')),dbname='funnelq',user='funnelq',password=os.environ.get('DEFI_PGPASSWORD','funnelq'),connect_timeout=15)
        c.autocommit=True
        with c.cursor() as s:
            s.execute("SET statement_timeout='600s'");s.execute("SET max_parallel_workers_per_gather=3");s.execute("SET work_mem='64MB'")
        return c
    from pymongo import MongoClient
    return MongoClient(os.environ.get('DEFI_MONGO_URI','mongodb://mongo:27018'),serverSelectionTimeoutMS=15000)
def table_name(name):
    import re
    if not re.fullmatch('[a-z][a-z0-9_]*',name):raise ValueError('table name')
    return SCHEMA+'.'+name
def build(method,name,rows):
    start=time.perf_counter();c=connect(method);table_name(name)
    if rows.dtype!=EVENT or len(np.unique(rows['eid']))!=len(rows):raise ValueError('invalid or duplicate source identity')
    try:
        if method=='postgresql':
            with c.cursor() as cur:
                cur.execute('CREATE SCHEMA IF NOT EXISTS '+SCHEMA)
                cur.execute('CREATE TABLE '+table_name(name)+' ('+','.join('"'+n+'" BIGINT NOT NULL' for n in EVENT.names)+',PRIMARY KEY(eid))')
                for lo in range(0,len(rows),10000):
                    buf=io.StringIO('\n'.join('\t'.join(str(int(v)) for v in r) for r in rows[lo:lo+10000])+'\n')
                    cur.copy_expert('COPY '+table_name(name)+' FROM STDIN',buf)
                for i,keys in enumerate(INDEXES):cur.execute('CREATE INDEX ON '+table_name(name)+' ('+','.join('"'+k+'"' for k in keys)+')')
                cur.execute('ANALYZE '+table_name(name));cur.execute('SELECT count(*) FROM '+table_name(name));count=cur.fetchone()[0]
                cur.execute('SELECT version()');version=cur.fetchone()[0]
        else:
            db=c[SCHEMA];target=db.create_collection(name)
            for lo in range(0,len(rows),10000):
                docs=[{'_id':int(r['eid']),**{n:int(r[n]) for n in EVENT.names}} for r in rows[lo:lo+10000]]
                if docs:target.insert_many(docs,ordered=True)
            target.create_index('eid',unique=True)
            for keys in INDEXES:target.create_index([(k,1) for k in keys])
            count=target.count_documents({});version=c.server_info()['version']
        if count!=len(rows):raise RuntimeError('ingest count mismatch')
        return {'method':method,'rows':count,'build_seconds':time.perf_counter()-start,'version':version,'indexes':INDEXES,
                'schema':SCHEMA,'table':name,'query_independent_indexes':True,'existing_data_modified':False}
    finally:c.close()

def execute(c,method,name,q,count):
    t=time.perf_counter();ids=validate(q)
    compiled=pg_sql(q,table_name(name),count=count) if method=='postgresql' else mongo_pipeline(q,name,count=count)
    compiled_at=time.perf_counter()
    if method=='postgresql':
        with c.cursor() as cur:
            cur.execute(compiled);raw=cur.fetchall()
            value=int(raw[0][0]) if count else raw
    else:
        raw=list(c[SCHEMA][name].aggregate(compiled,allowDiskUse=True,maxTimeMS=600000,comment='defi_query'))
        value=int(raw[0]['n']) if count and raw else 0 if count else [tuple(int(r['e'+str(i)]) for i in range(len(ids))) for r in raw]
    fetched=time.perf_counter()
    if not count:
        value=np.array(value,dtype=[(i,'<i8') for i in ids]);value.sort()
        if len(np.unique(value))!=len(value):raise ValueError('duplicate full binding')
    ended=time.perf_counter()
    return value,{'compile_seconds':compiled_at-t,'execute_fetch_seconds':fetched-compiled_at,'normalize_seconds':ended-fetched,
                  'query_seconds':ended-t,'count_only':count,'compiled':compiled}

def main():
    ap=argparse.ArgumentParser();ap.add_argument('stage',choices=['build','fixture','fixture-check','gate','measure']);ap.add_argument('--method',choices=['mongodb','postgresql'],required=True)
    ap.add_argument('--out',type=Path,required=True);ap.add_argument('--case');ap.add_argument('--ready',type=Path);ap.add_argument('--go',type=Path)
    args=ap.parse_args();args.out.mkdir(parents=True,exist_ok=False);method=args.method
    if args.stage=='build':
        rows=np.load(SOURCE/'events.npy',mmap_mode='r');r=build(method,'events',rows);r['source_sha256']=sha(SOURCE/'events.npy');save(args.out/'result.json',r);return
    if args.stage in ['fixture','fixture-check']:
        a=np.load(SOURCE/'events.npy',mmap_mode='r');wanted=set(map(int,a['eid'][:12]))
        for key,p,q in cases():
            with np.load(SOURCE/(key+'_answers.npz')) as z:ref=z['full']
            for row in ref[:1]:wanted.update(map(int,row))
        small=a[np.isin(a['eid'],list(wanted))]
        if args.stage=='fixture':build(method,'fixture',small)
        checks=[];c=connect(method)
        try:
            for key,p,q in cases():
                expected=brute(small,q);got,_=execute(c,method,'fixture',q,False);n,_=execute(c,method,'fixture',q,True)
                np.testing.assert_array_equal(got,expected);assert n==len(expected)
                checks.append({'case':key,'full_bindings':n,'FP':0,'FN':0})


            from test_evaluator import pattern
            boundary=np.array([(1,1,9,1,2,0,0,2,1),(2,2,10,1,2,3,0,1,0),(3,3,19,1,3,0,0,2,-1),(4,3,20,1,3,0,0,2,-1)],dtype=EVENT)
            if args.stage=='fixture':build(method,'boundary',boundary)
            for w in [0,9,10,11]:
                q=pattern(w=w);q['query_id']='renamed';q['events']=[q['events'][1],q['events'][0],q['events'][2]]
                got,_=execute(c,method,'boundary',q,False);np.testing.assert_array_equal(got,brute(boundary,q))
            save(args.out/'result.json',{'passed':True,'checks':checks,'boundary_cases':4,'source_rows':len(small)})
        finally:c.close()
        return
    key,p,q=next(x for x in cases() if x[0]==args.case)
    c=connect(method)
    try:
        if args.stage=='gate':
            got,r=execute(c,method,'events',q,False)
            with np.load(SOURCE/(key+'_answers.npz')) as z:expected=z['full'].copy()
            expected.sort();np.testing.assert_array_equal(got,expected)
            n,nr=execute(c,method,'events',q,True);assert n==len(got)
            save(args.out/'result.json',{'passed':True,'FP':0,'FN':0,'rows':n,'full_binding_gate':r,'count_gate':nr,'query_sha256':sha(p)});return
        warm,wr=execute(c,method,'events',q,True)
        if args.ready:
            if not args.go:raise ValueError('ready requires go')
            save(args.ready,{'warmed':True})
            limit=time.monotonic()+120
            while not args.go.exists():
                if time.monotonic()>limit:raise TimeoutError('measurement barrier')
                time.sleep(.02)
        n,r=execute(c,method,'events',q,True)
        with np.load(SOURCE/(key+'_answers.npz')) as z:expected=len(z['full'])
        assert n==warm==expected
        save(args.out/'result.json',{'passed':True,'method':method,'case':key,'count':n,'timing':r,'warmup':wr,
              'warmup_runs':1,'query_sha256':sha(p),'offline_build_excluded':True,'count_only':True,
              'full_binding_correctness_gate_separate':True,'database_fetch_and_compile_included':True})
    finally:c.close()

if __name__=='__main__':main()
