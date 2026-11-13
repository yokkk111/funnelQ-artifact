import itertools
import re
import numpy as np
EVENT=np.dtype([('eid','<i8'),('tx','<u4'),('time','<i8'),('actor','<u4'),('asset','<u4'),('out','<u4'),('pool','<u4'),('kind','u1'),('sign','i1')])
TYPES={'flashloan':0,'swap':1,'borrow':2}
FIELDS={'recipient':'actor','taker':'actor','borrower':'actor','asset':'asset','token_sold':'asset','token_bought':'out','pool':'pool'}

def validate(q):
    allowed={'name','query_name','query_id','description','scope','scope_params','events','order','neq_constraints','attr_filters'}
    assert not set(q)-allowed, ('unsupported query keys',set(q)-allowed)
    assert q['scope'] in ('same_tx','anchored_episode')
    ids=[e['id'] for e in q['events']]
    assert len(ids)==len(set(ids)) and all(re.fullmatch('[A-Za-z][A-Za-z0-9_]*',i) for i in ids)
    for e in q['events']:
        assert not set(e)-{'id','type','roles'}
        assert e['type'] in TYPES
        assert all(k in FIELDS and isinstance(v,str) and v.startswith('$') for k,v in e['roles'].items())
    for a,b in q['order']:assert a in ids and b in ids
    for a,f,b,g in q.get('neq_constraints',[]):assert a in ids and b in ids and f in FIELDS and g in FIELDS
    for f in q.get('attr_filters',[]):
        assert set(f)=={'event','field','op','value'} and f['event'] in ids
        assert f['field']=='amount' and str(f['value'])=='0' and f['op'] in ('>','<')
        assert next(e for e in q['events'] if e['id']==f['event'])['type']=='borrow'
    if q['scope']=='anchored_episode':
        assert q['scope_params']['semantic_anchor_event'] in ids
        assert int(q['scope_params']['forward_window_sec'])>=0
    return ids

def sql(q):
    ids=validate(q);conditions=[];variables={}
    for e in q['events']:
        i=e['id'];conditions.append(f'{i}.kind={TYPES[e["type"]]}')
        for f,v in e['roles'].items():
            col=f'{i}.{FIELDS[f]}'
            if v in variables:conditions.append(col+'='+variables[v])
            else:variables[v]=col
    for a,b in q['order']:conditions.append(f'{a}.eid<{b}.eid')
    for a,f,b,g in q.get('neq_constraints',[]):conditions.append(f'{a}.{FIELDS[f]}!={b}.{FIELDS[g]}')
    for f in q.get('attr_filters',[]):conditions.append(f'{f["event"]}.sign{f["op"]}0')
    anchor=ids[0] if q['scope']=='same_tx' else q['scope_params']['semantic_anchor_event']
    for i in ids:
        if q['scope']=='same_tx':conditions.append(f'{i}.tx={anchor}.tx')
        else:
            w=int(q['scope_params']['forward_window_sec'])
            conditions += [f'{i}.eid>={anchor}.eid',f'{i}.tx>={anchor}.tx',f'{i}.time>={anchor}.time',f'{i}.time<={anchor}.time+{w}']
    conditions += [f'{a}.eid!={b}.eid' for a,b in itertools.combinations(ids,2)]
    return 'SELECT '+','.join(f'{i}.eid AS {i}' for i in ids)+' FROM '+','.join('events '+i for i in ids)+' WHERE '+' AND '.join(conditions)

def normalized(a):
    a=a.copy();a.sort()
    assert len(np.unique(a))==len(a),'duplicate bindings'
    return a

def exact(a,q):
    import duckdb,pandas as pd
    db=duckdb.connect();db.execute("SET threads=4");db.execute("SET memory_limit='16GB'")
    db.register('events',pd.DataFrame.from_records(a))
    try:
        result=db.execute(sql(q)).fetchnumpy()
        out=np.empty(len(next(iter(result.values()))),dtype=[(i,'<i8') for i in validate(q)])
        for i in out.dtype.names:out[i]=result[i]
        return normalized(out)
    finally:db.close()

def brute(a,q):
    ids=validate(q);bound={};answers=[]
    def valid():
        if len({int(r['eid']) for r in bound.values()})!=len(bound):return False
        vars_={}
        for e in q['events']:
            if e['id'] not in bound:continue
            r=bound[e['id']]
            for f,v in e['roles'].items():
                x=int(r[FIELDS[f]])
                if v in vars_ and vars_[v]!=x:return False
                vars_[v]=x
        for x,y in q['order']:
            if x in bound and y in bound and bound[x]['eid']>=bound[y]['eid']:return False
        for x,f,y,g in q.get('neq_constraints',[]):
            if x in bound and y in bound and bound[x][FIELDS[f]]==bound[y][FIELDS[g]]:return False
        for f in q.get('attr_filters',[]):
            if f['event'] in bound and not (int(bound[f['event']]['sign'])>0 if f['op']=='>' else int(bound[f['event']]['sign'])<0):return False
        anchor=ids[0] if q['scope']=='same_tx' else q['scope_params']['semantic_anchor_event']
        if anchor in bound:
            ar=bound[anchor]
            for r in bound.values():
                if q['scope']=='same_tx':
                    if r['tx']!=ar['tx']:return False
                elif not (r['eid']>=ar['eid'] and r['tx']>=ar['tx'] and 0<=int(r['time'])-int(ar['time'])<=int(q['scope_params']['forward_window_sec'])):return False
        return True
    def visit(d):
        if d==len(ids):answers.append(tuple(int(bound[i]['eid']) for i in ids));return
        e=q['events'][d]
        for r in a[a['kind']==TYPES[e['type']]]:
            bound[e['id']]=r
            if valid():visit(d+1)
        bound.pop(e['id'],None)
    visit(0)
    return normalized(np.array(answers,dtype=[(i,'<i8') for i in ids]))
