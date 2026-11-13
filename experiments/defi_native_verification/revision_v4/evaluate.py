import itertools
import time
import numpy as np
from legacy_evaluate import EVENT,TYPES,FIELDS,normalized,brute
from legacy_evaluate import validate as old_validate,sql as legacy_sql

def validate(q):
    ids=old_validate(q)
    assert ids,'empty pattern unsupported'
    assert q['scope']=='same_tx' or 0<=int(q['scope_params']['forward_window_sec'])<=2**62
    allowed={'flashloan':{'recipient','asset'},'swap':{'taker','token_sold','token_bought','pool'},'borrow':{'borrower','asset','pool'}}
    for e in q['events']:assert set(e['roles'])<=allowed[e['type']],'role not in event schema'
    return ids

def request_tokens(q):

    validate(q);tokens=set()
    for e in q['events']:
        if e['type']!='borrow':tokens.add(e['type']);continue
        signs={-1,0,1}
        for f in q.get('attr_filters',[]):
            if f['event']==e['id']:signs &= ({1} if f['op']=='>' else {-1})
        tokens.update({-1:'borrow_negative',0:'borrow_zero',1:'borrow_positive'}[s] for s in signs)
    assert tokens,'empty atomic request unsupported'
    return ','.join(sorted(tokens))

def row_token(row):
    kind=int(row['kind'])
    return ['flashloan','swap'][kind] if kind<2 else {-1:'borrow_negative',0:'borrow_zero',1:'borrow_positive'}[int(row['sign'])]

def sql(q):
    ids=validate(q)

    text=legacy_sql(q)

    text=text.replace(' FROM '+','.join('events '+i for i in ids)+' WHERE ',
                      ' FROM '+','.join('atom_'+str(n)+' '+i for n,i in enumerate(ids))+' WHERE ')
    if q['scope']=='anchored_episode':
        anchor=q['scope_params']['semantic_anchor_event'];w=int(q['scope_params']['forward_window_sec'])
        text=text.replace(f'{anchor}.time+{w}',f'(CAST({anchor}.time AS HUGEINT)+{w})')
    text += ''.join(f' AND {a}.__scope_key={b}.__scope_key' for a,b in itertools.combinations(ids,2))

    import re
    for aid in sorted(ids,key=len,reverse=True):
        text=re.sub(r'\b'+re.escape(aid)+r'\.',lambda _: '"'+aid+'".',text)
    text=re.sub(r'(atom_\d+) ([A-Za-z][A-Za-z0-9_]*)',lambda m:m[1]+' "'+m[2]+'"',text)
    text=re.sub(r' AS ([A-Za-z][A-Za-z0-9_]*)(?=,| FROM)',lambda m:' AS "'+m[1]+'"',text)
    return text

def exact(a,q,metrics=None):
    import duckdb,pandas as pd
    start=time.perf_counter();ids=validate(q)
    assert a.dtype==EVENT and np.all(a['time']>=0) and np.all(a['eid']>=0),'canonical unsigned identity/time domain'
    db=duckdb.connect();db.execute("SET threads=4");db.execute("SET memory_limit='16GB'")
    anchor=ids[0] if q['scope']=='same_tx' else q['scope_params']['semantic_anchor_event']
    stats=[]
    try:
        for n,e in enumerate(q['events']):
            mask=a['kind']==TYPES[e['type']]
            for f in q.get('attr_filters',[]):
                if f['event']==e['id']:mask &= (a['sign']>0 if f['op']=='>' else a['sign']<0)
            rows=a[mask];frame=pd.DataFrame.from_records(rows)
            if q['scope']=='same_tx':frame['__scope_key']=rows['tx'].astype(np.int64)
            else:
                w=int(q['scope_params']['forward_window_sec']);frame['__scope_key']=rows['time']//max(w,1)
                if e['id']!=anchor and w>0:
                    prev=frame.copy();prev['__scope_key']-=1;frame=pd.concat([frame,prev],ignore_index=True)
            db.register('input_frame',frame)
            db.execute(f'CREATE TABLE atom_{n} AS SELECT * FROM input_frame')
            db.unregister('input_frame');db.execute(f'ANALYZE atom_{n}')
            stats.append({'atom':e['id'],'local_rows':len(rows),'partition_rows':len(frame)})
        prepared=time.perf_counter();statement=sql(q)
        result=db.execute(statement).fetchnumpy();executed=time.perf_counter()
        out=np.empty(len(next(iter(result.values()))),dtype=[(i,'<i8') for i in ids])
        for i in ids:out[i]=result[i]
        out=normalized(out);finished=time.perf_counter()
        if metrics is not None:metrics.update(prepare_seconds=prepared-start,execute_seconds=executed-prepared,
            normalize_seconds=finished-executed,total_seconds=finished-start,atoms=stats,sql=statement,
            query_id_dispatch=False,answer_cache=False)
        return out
    finally:db.close()
