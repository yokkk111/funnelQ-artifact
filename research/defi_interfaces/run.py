import os
import argparse
import csv
import gc
import importlib.util
import json
import math
import sqlite3
import sys
import threading
import time
import zlib
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE / "deps"))
import duckdb
import numpy as np
import pandas as pd
import psutil

SC = [8, 16, 32, 64, 128, 256, 512, 1024]
PORTS = ['F', 'B+', 'B-', 'I', 'O']
DB = Path(os.environ.get('FUNNELQ_DEFI_DB', str(Path(__file__).resolve().parents[2] / 'data/defi/anchored_episode_runtime_sidecar.db')))
EVENT = np.dtype([('eid','<i8'),('tx','<u4'),('time','<i8'),('actor','<u4'),('asset','<u4'),('out','<u4'),('pool','<u4'),('kind','u1'),('sign','i1')])
DIRECTORY = np.dtype([('profile','<u4'),('offset','<u8'),('count','<u8')])
_spec = importlib.util.spec_from_file_location('prior_fixture', HERE.parent / 'defi_scc/prepare.py')
prior = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(prior)


def save(name, obj):
    (HERE/name).write_text(json.dumps(obj,indent=2,ensure_ascii=False),encoding='utf-8')


class Monitor:
    def __init__(self):
        self.peak = 0
        self.stop = False
        self.stage = 'startup'
        self.peaks = {}
        self.thread = threading.Thread(target=self.work,daemon=True)
        self.thread.start()
    def work(self):
        p = psutil.Process()
        while not self.stop:
            rss = p.memory_info().rss
            self.peak = max(self.peak,rss)
            self.peaks[self.stage] = max(self.peaks.get(self.stage,0),rss)
            time.sleep(.05)


def load_events():
    cache = HERE/'events.npy'
    if cache.exists():
        return np.load(cache), {'cached': True, 'bytes':cache.stat().st_size}
    t0=time.perf_counter()
    c=sqlite3.connect(DB.as_uri()+'?mode=ro',uri=True)
    c.execute('PRAGMA query_only=ON')
    counts={t:c.execute('SELECT count(*) FROM support_'+t).fetchone()[0] for t in prior.FIELDS}
    a=np.empty(sum(counts.values()),dtype=EVENT)
    timeline=np.array(c.execute('SELECT global_tx_ord,block_time_unix,tx_hash_id FROM episode_tx_timeline ORDER BY global_tx_ord').fetchall(),dtype=np.int64)
    assert np.all(np.diff(timeline[:,0])==1)
    assert np.all(np.diff(timeline[:,1])>=0), 'this experiment requires monotone canonical time'
    assert len(np.unique(timeline[:,2]))==len(timeline), 'tx ID mapping must be injective'
    times=np.zeros(int(timeline[-1,0])+1,dtype=np.int64)
    times[timeline[:,0]]=timeline[:,1]
    offset=0
    for kind,(typ,roles) in enumerate(prior.FIELDS.items()):
        fields=['global_event_ord','global_tx_ord',*(r+'_id' for r in roles)]
        if typ=='borrow': fields.append('amount_num')
        cur=c.execute('SELECT '+','.join(fields)+' FROM support_'+typ+' ORDER BY global_event_ord')
        while True:
            batch=cur.fetchmany(100000)
            if not batch: break
            n=len(batch); v=a[offset:offset+n]
            for j,key in enumerate(['eid','tx']):v[key]=[r[j] for r in batch]
            v['actor']=[r[2] for r in batch]
            v['asset']=[r[3] for r in batch]
            v['out']=[r[4] for r in batch] if typ=='swap' else 0
            v['pool']=[r[5 if typ=='swap' else 4] for r in batch]
            v['time']=times[v['tx']];v['kind']=kind
            if typ=='borrow':
                amount=np.array([r[-1] for r in batch],dtype=np.float64)
                assert np.all(np.isfinite(amount))
                v['sign']=np.sign(amount).astype(np.int8)
            else:v['sign']=0
            offset+=n
    assert len(np.unique(a['eid']))==len(a)
    np.save(cache,a)
    meta={'source_db':str(DB),'source_file_size':DB.stat().st_size,'counts':counts,'events':len(a),'transactions':len(timeline),
          'load_seconds_including_save':time.perf_counter()-t0,'event_record_bytes':EVENT.itemsize,'event_cache_bytes':cache.stat().st_size,
          'zero_borrow':int(np.sum((a['kind']==2)&(a['sign']==0))), 'amount':'exact sign of finite stored REAL; no general Decimal support claimed'}
    save('source.json',meta);c.close()
    return a,meta


def group_masks(keys, bins, own, adjacent):

    starts=np.r_[0,1+np.flatnonzero((keys[1:]!=keys[:-1])|(bins[1:]!=bins[:-1]))]
    masks=np.bitwise_or.reduceat(np.left_shift(np.uint8(1),own),starts)
    if adjacent:
        good=(keys[starts[1:]]==keys[starts[:-1]])&(bins[starts[1:]]==bins[starts[:-1]]+1)
        out=masks.copy()
        out[1:] |= np.where(good,masks[:-1],0).astype(np.uint8)
        out[:-1] |= np.where(good,masks[1:],0).astype(np.uint8)
        masks=out
    lengths=np.diff(np.r_[starts,len(keys)])
    return np.repeat(masks,lengths),len(starts)


def write_index(name, codes, row_ids):
    t=time.perf_counter()
    order=np.lexsort((row_ids,codes))
    sorted_codes=codes[order]
    starts=np.r_[0,1+np.flatnonzero(sorted_codes[1:]!=sorted_codes[:-1])]
    dr=np.empty(len(starts),dtype=DIRECTORY)
    dr['profile']=sorted_codes[starts];dr['offset']=starts;dr['count']=np.diff(np.r_[starts,len(codes)])
    members=row_ids[order]
    np.save(HERE/(name+'_directory.npy'),dr)
    np.save(HERE/(name+'_members.npy'),members)
    return {'profiles':len(dr),'members':len(members),'directory_bytes':dr.nbytes,'members_bytes':members.nbytes,
            'modeled_member_roots_bytes_not_proofs':32*len(dr),'sort_and_write_seconds':time.perf_counter()-t}


def build(a, monitor):
    if (HERE/'offline.json').exists():return json.loads((HERE/'offline.json').read_text())
    monitor.stage='offline_build';t0=time.perf_counter()
    ids=[np.flatnonzero(a['kind']==0),np.flatnonzero((a['kind']==2)&(a['sign']>0)),np.flatnonzero((a['kind']==2)&(a['sign']<0)),np.flatnonzero(a['kind']==1),np.flatnonzero(a['kind']==1)]
    rows=np.concatenate(ids).astype(np.uint32)
    own=np.concatenate([np.full(len(x),i,dtype=np.uint8) for i,x in enumerate(ids)])
    assets=np.concatenate([a['asset'][x] if i!=4 else a['out'][x] for i,x in enumerate(ids)])
    keys=(a['actor'][rows].astype(np.uint64)<<32)|assets.astype(np.uint64)
    order=np.lexsort((a['tx'][rows],keys))
    keys=keys[order];own=own[order];rows=rows[order]
    del ids,assets,order
    tx=a['tx'][rows];ts=a['time'][rows]
    same,ng=group_masks(keys,tx,own,False)
    st=write_index('same_tx',(own.astype(np.uint32)<<5)|same,rows)
    st['joint_tx_buckets']=ng
    del same
    beta=np.full(len(rows),sum(8<<(4*p) for p in range(5)),dtype=np.uint32)
    previous=np.zeros(len(rows),dtype=np.uint8);scans=[]
    for level,h in enumerate(SC):
        t=time.perf_counter();mask,ng=group_masks(keys,ts//h,own,True)
        assert np.all((previous&mask)==previous),'nonmonotone capability'
        for p in range(5):
            born=((mask&(1<<p))!=0)&((previous&(1<<p))==0)
            beta[born] &= np.uint32(0xffffffff ^ (15<<(4*p)))
            beta[born] |= np.uint32(level<<(4*p))
        scans.append({'h':h,'base_bins':ng,'seconds':time.perf_counter()-t,'full_capability_port_incidence':int(np.sum(mask==31))})
        previous=mask
        print('BUILD SCALE',h,scans[-1],flush=True)
    assert np.all(((beta>>(4*own.astype(np.uint32)))&15)==0)
    ep=write_index('episode',(own.astype(np.uint32)<<20)|beta,rows)
    meta={'scales':SC,'ports':PORTS,'port_incidence':len(rows),'same_tx':st,'episode':ep,'scans':scans,
          'offline_seconds_excluding_source_load':time.perf_counter()-t0,'peak_process_rss_bytes':monitor.peak,
          'additional_index_bytes':sum((HERE/(n+'_'+k+'.npy')).stat().st_size for n in ['episode','same_tx'] for k in ['directory','members']),
          'trust':'structural exact builder, no cryptographic construction correctness proof',
          'no_queries_read_during_build':True,'no_pair_support_edges':True}
    save('offline.json',meta);return meta


def atom_info(q,a):
    typ=a['type'];roles=a['roles']
    if typ=='flashloan':return [(0,(roles['recipient'],roles['asset']))]
    if typ=='swap':return [(3,(roles['taker'],roles['token_sold'])),(4,(roles['taker'],roles['token_bought']))]
    filt=[x for x in q.get('attr_filters',[]) if x['event']==a['id']]
    assert len(filt)==1 and filt[0]['field']=='amount' and filt[0]['value']=='0' and filt[0]['op'] in ('>','<'), 'unproven polarity; do not prune'
    return [(1 if filt[0]['op']=='>' else 2,(roles['borrower'],roles['asset']))]


def compile_query(q):
    info={a['id']:atom_info(q,a) for a in q['events']}
    groups={}
    for aid,ports in info.items():
        for port,key in ports:
            assert all(isinstance(v,str) and v.startswith('$') for v in key),'unsupported constant interface for this runner'
            groups.setdefault(key,[]).append((aid,port))
    req={a['id']:[] for a in q['events']}
    for key,occ in groups.items():
        if len({x[0] for x in occ})<2:continue
        need=0
        for _,p in occ:need|=1<<p
        for aid,p in occ:req[aid].append((p,need))
    return info,req


FIELD={'borrower':'actor','recipient':'actor','taker':'actor','asset':'asset','token_sold':'asset','token_bought':'out','pool':'pool'}


def atom_mask(a,q,atom):
    m=a['kind']=={'flashloan':0,'swap':1,'borrow':2}[atom['type']]
    for f in q.get('attr_filters',[]):
        if f['event']==atom['id']:
            assert f['field']=='amount' and f['value']=='0'
            m &= a['sign']>0 if f['op']=='>' else a['sign']<0
    return m


def sql(q,domain=False,count=False):
    terms={};cond=[];tables=[]
    for atom in q['events']:
        aid=atom['id'];tables.append(f'{"d_" if domain else "b_"}{aid} {aid}')
        for f,v in atom['roles'].items():
            exp=aid+'.'+FIELD[f]
            if v in terms:cond.append(exp+'='+terms[v])
            else:terms[v]=exp
    for x,y in q['order']:cond.append(x+'.eid<'+y+'.eid')
    for x,f,y,g in q.get('neq_constraints',[]):cond.append(x+'.'+FIELD[f]+'!='+y+'.'+FIELD[g])
    anchor=q['events'][0]['id'] if q['scope']=='same_tx' else q['scope_params']['semantic_anchor_event']
    for i,atom in enumerate(q['events']):
        aid=atom['id']
        for other in q['events'][:i]:cond.append(aid+'.eid!='+other['id']+'.eid')
        if q['scope']=='same_tx':cond.append(aid+'.tx='+anchor+'.tx')
        else:
            w=int(q['scope_params']['forward_window_sec'])
            cond += [aid+'.eid>='+anchor+'.eid',aid+'.time>='+anchor+'.time',f'{aid}.time<={anchor}.time+{w}']
    cols='count(*)' if count else ','.join(x['id']+'.eid AS '+x['id'] for x in q['events'])
    return f'SELECT {cols} FROM '+','.join(tables)+' WHERE '+' AND '.join(cond)


def record_array(results):
    names=list(results)
    a=np.empty(len(results[names[0]]),dtype=[(x,'<i8') for x in names])
    for x in names:a[x]=results[x]
    a.sort();return a


def load_index(name):
    return np.load(HERE/(name+'_directory.npy')),np.load(HERE/(name+'_members.npy'),mmap_mode='r')


def evaluate(a,monitor):
    monitor.stage='runtime_structure';n=len(a)
    indexes={k:load_index(k) for k in ['same_tx','episode']}
    db=duckdb.connect()
    db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
    db.execute("SET temp_directory='"+str(HERE/'duckdb_temp').replace('\\','/')+"'")
    histories={}
    old=DB.parents[2]/'final_queryset_4scheme_once/results.csv'
    if old.exists():
        for r in csv.DictReader(old.open()):
            if r['system']=='hvceq':histories[(r['label'],int(r['window_sec']))]=int(r['num_matches'])
    reports=[]
    for label,w,path,q in prior.workloads():
        monitor.stage=f'query_{label}_{w}';start=time.perf_counter();info,req=compile_query(q)
        mode='same_tx' if w==0 else 'episode';dr,members=indexes[mode]
        level=next((i for i,h in enumerate(SC) if h>=w),None) if w else None
        selected_classes=set();cache={};domains={};atom_stats=[]
        for atom in q['events']:
            aid=atom['id'];base=atom_mask(a,q,atom);mask=base.copy();raw=0
            for port,need in req[aid]:
                k=(port,need)
                if k not in cache:
                    code=dr['profile'];hit=(code>>(5 if mode=='same_tx' else 20))==port
                    if mode=='same_tx':hit &= (code&need)==need
                    else:
                        for target in range(5):
                            if need&(1<<target):hit &= ((code>>(4*target))&15)<=level
                    classes=np.flatnonzero(hit);cm=np.zeros(n,dtype=bool);count=0
                    for j in classes:
                        lo=int(dr[j]['offset']);hi=lo+int(dr[j]['count']);cm[members[lo:hi]]=True;count+=hi-lo
                    cache[k]=(cm,count,classes)
                cm,count,classes=cache[k];mask &= cm;raw+=count;selected_classes.update(map(int,classes))
            domains[aid]=mask
            atom_stats.append({'atom':aid,'type':atom['type'],'local_domain':int(base.sum()),'reduced_domain':int(mask.sum()),'logical_member_ids_before_atom_intersection':raw,
                               'interfaces':[{'own':PORTS[p],'required':[PORTS[t] for t in range(5) if need&(1<<t)]} for p,need in req[aid]]})
            for prefix,mm in [('b_',base),('d_',mask)]:

                frame=pd.DataFrame.from_records(a[mm])
                db.register(prefix+aid+'_frame',frame)
                db.execute(f'CREATE OR REPLACE TEMP TABLE {prefix+aid} AS SELECT * FROM {prefix+aid}_frame')
                db.unregister(prefix+aid+'_frame')
            del base,mask
        union=np.zeros(n,dtype=bool)
        for mask in domains.values():union|=mask
        payload=a[union]
        posting_n=0;posting_compressed=0
        for j in sorted(selected_classes):
            lo=int(dr[j]['offset']);count=int(dr[j]['count']);posting_n+=count
            posting_compressed+=len(zlib.compress(members[lo:lo+count].tobytes(),1))
        structure_s=time.perf_counter()-start
        t=time.perf_counter();full=record_array(db.execute(sql(q)).fetchnumpy());full_s=time.perf_counter()-t
        t=time.perf_counter();reduced=record_array(db.execute(sql(q,True)).fetchnumpy());replay_s=time.perf_counter()-t
        t=time.perf_counter();count=int(db.execute(sql(q,True,True)).fetchone()[0]);count_s=time.perf_counter()-t
        fn=len(np.setdiff1d(full,reduced,assume_unique=True));fp=len(np.setdiff1d(reduced,full,assume_unique=True))
        assert fp==fn==0 and count==len(full) and len(np.unique(full))==len(full),(label,w,fp,fn)
        hist=histories.get((label,w))
        assert hist is None or hist==len(full),('historical result mismatch',label,w,hist,len(full))

        directory_modeled=dr.nbytes+32*len(dr)
        compressed_events=len(zlib.compress(payload.tobytes(),1));compressed_answers=len(zlib.compress(full.tobytes(),1))
        actor,cnt=np.unique(payload['actor'],return_counts=True);top=np.argsort(cnt)[-10:][::-1]
        r={'label':label,'window':w,'scale':None if level is None else SC[level],'query_name':q['query_name'], 'atoms':atom_stats,
           'full_event_rows':n,'local_domain_sum':sum(x['local_domain'] for x in atom_stats),'reduced_domain_sum':sum(x['reduced_domain'] for x in atom_stats),
           'selected_event_union':int(union.sum()),'selected_classes':len(selected_classes),'catalog_classes':len(dr),'opened_member_ids_dedup_classes':posting_n,
           'posting_raw_bytes':4*posting_n,'posting_zlib1_bytes':posting_compressed,'event_kernel_raw_bytes':payload.nbytes,'event_kernel_zlib1_bytes':compressed_events,
           'catalog_bytes_with_modeled_32byte_roots':directory_modeled,'answer_count':count,'answer_raw_bytes':full.nbytes,'answer_zlib1_bytes':compressed_answers,
           'structural_compressed_response_excluding_merkle_frontier':directory_modeled+posting_compressed+compressed_events+compressed_answers,
           'false_positive':fp,'false_negative':fn,'historical_full_count':hist,'full_sql_replay_s':full_s,'reduced_sql_replay_s':replay_s,'reduced_sql_count_s':count_s,
           'structure_and_sql_table_setup_s':structure_s,'total_single_trial_s':time.perf_counter()-start,'top10_actors_event_share':float(cnt[top].sum()/max(1,len(payload))),
           'top_actors':[{'actor_id':int(actor[i]),'events':int(cnt[i])} for i in top], 'peak_process_rss_bytes':monitor.peak}
        reports.append(r);save('results.json',reports)
        print('QUERY',label,w,'domain',r['selected_event_union'],'ids',posting_n,'answers',count,'FP/FN',fp,fn,'estMiB',round(r['structural_compressed_response_excluding_merkle_frontier']/2**20,3),flush=True)
        np.savez(HERE/(f'{label}_w{w}_answers.npz'),full=full,reduced=reduced)
        del cache,domains,payload,full,reduced,union;gc.collect()
    db.close();return reports


def small_tests():

    rng=np.random.default_rng(22027)
    for h in [8,16,32,64]:
        key=np.repeat(np.arange(4,dtype=np.uint64),40)
        ts=np.concatenate([np.sort(rng.integers(0,257,40)) for _ in range(4)])
        own=rng.integers(0,5,len(key),dtype=np.uint8)
        got,_=group_masks(key,ts//h,own,True)
        for i in range(len(key)):
            want=0
            for j in [int(ts[i]//h)-1,int(ts[i]//h)]:
                ids=(key==key[i])&(ts>=j*h)&(ts<(j+2)*h)
                for p in own[ids]:want|=1<<int(p)
            assert got[i]==want

    got,_=group_masks(np.array([1,1],dtype=np.uint64),np.array([0,20]),np.array([1,3],dtype=np.uint8),True)
    assert list(got)==[2,8]
    return {'explicit_cover_checks':640,'distant_no_support':True}


def main():
    monitor=Monitor()
    try:
        tests=small_tests();save('unit_checks.json',tests)
        a,source=load_events();offline=build(a,monitor)
        reports=evaluate(a,monitor)
        save('run_summary.json',{'configurations':len(reports),'false_positive':sum(r['false_positive'] for r in reports),'false_negative':sum(r['false_negative'] for r in reports),
            'peak_rss_bytes':monitor.peak,'stage_peaks':monitor.peaks,'status':'complete','scope':'full three-month data structural test; no cryptographic E2E timings',
            'duckdb_version':duckdb.__version__,'numpy_version':np.__version__})
    finally:monitor.stop=True


if __name__=='__main__':main()
