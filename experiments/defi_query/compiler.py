import itertools
import re

FIELDS = {'recipient':'actor','taker':'actor','borrower':'actor','asset':'asset',
          'token_sold':'asset','token_bought':'out','pool':'pool'}
TYPES = {'flashloan':0,'swap':1,'borrow':2}
ROLES = {'flashloan':{'recipient','asset'},'swap':{'taker','token_sold','token_bought','pool'},'borrow':{'borrower','asset','pool'}}

def validate(q):
    def require(ok, why):
        if not ok: raise ValueError(why)
    require(not set(q)-{'name','query_name','query_id','description','scope','scope_params','events','order','neq_constraints','attr_filters'}, 'unknown QuerySpec field')
    require(q['scope'] in ('same_tx','anchored_episode'),'scope')
    es=q['events'];ids=[e['id'] for e in es]
    require(bool(ids) and len(ids)==len(set(ids)),'empty/duplicate event IDs')
    require(all(isinstance(i,str) and re.fullmatch('[A-Za-z][A-Za-z0-9_]*',i) for i in ids),'event identifiers')
    for e in es:
        require(not set(e)-{'id','type','roles'},'unknown atom field')
        require(e['type'] in TYPES and set(e['roles'])<=ROLES[e['type']],'roles')
        require(all(isinstance(v,str) and v.startswith('$') for v in e['roles'].values()),'variable encoding')
    for pair in q.get('order',[]):require(len(pair)==2 and all(i in ids for i in pair),'order')
    for a,f,b,g in q.get('neq_constraints',[]):
        require(a in ids and b in ids,'neq event')
        require(f in ROLES[es[ids.index(a)]['type']] and g in ROLES[es[ids.index(b)]['type']],'neq role')
    for f in q.get('attr_filters',[]):
        require(set(f)=={'event','field','op','value'} and f['event'] in ids,'filter')
        require(f['field']=='amount' and str(f['value'])=='0' and f['op'] in ('>','<'),'unsupported amount filter')
        require(es[ids.index(f['event'])]['type']=='borrow','amount only on borrow')
    sp=q.get('scope_params',{})
    if q['scope']=='anchored_episode':
        require({'semantic_anchor_event','forward_window_sec'}<=set(sp) and not set(sp)-{'semantic_anchor_event','forward_window_sec','execution_anchor_event'},'episode parameters')
        require(sp.get('execution_anchor_event','auto')=='auto' or sp['execution_anchor_event'] in ids,'execution anchor hint')
        require(sp['semantic_anchor_event'] in ids,'anchor')
        require(type(sp['forward_window_sec']) is int and 0<=sp['forward_window_sec']<=2**62,'window')
    else:require(not sp,'unsupported same_tx parameters')
    return ids

def predicates(q):
    ids=validate(q);ps=[];variables={}
    col=lambda i,f:('col',i,f)
    val=lambda x:('value',x)
    for i,e in enumerate(q['events']):
        ps.append(('=',col(i,'kind'),val(TYPES[e['type']])))
        for f,v in e['roles'].items():
            c=col(i,FIELDS[f])
            if v in variables:ps.append(('=',c,variables[v]))
            else:variables[v]=c
    for a,b in q.get('order',[]):ps.append(('<',col(ids.index(a),'eid'),col(ids.index(b),'eid')))
    for a,f,b,g in q.get('neq_constraints',[]):ps.append(('!=',col(ids.index(a),FIELDS[f]),col(ids.index(b),FIELDS[g])))
    for f in q.get('attr_filters',[]):ps.append((f['op'],col(ids.index(f['event']),'sign'),val(0)))
    anchor=0 if q['scope']=='same_tx' else ids.index(q['scope_params']['semantic_anchor_event'])
    for i in range(len(ids)):
        if i==anchor:continue
        if q['scope']=='same_tx':ps.append(('=',col(i,'tx'),col(anchor,'tx')))
        else:
            ps += [('>=',col(i,f),col(anchor,f)) for f in ('eid','tx','time')]
            ps.append(('<=',col(i,'time'),('add',col(anchor,'time'),val(q['scope_params']['forward_window_sec']))))
    ps += [('!=',col(i,'eid'),col(j,'eid')) for i,j in itertools.combinations(range(len(ids)),2)]
    return ids,anchor,ps

def pg_sql(q,table,*,count=False):
    if not re.fullmatch('[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*',table):raise ValueError('qualified table')
    ids,_,ps=predicates(q)
    def term(x):
        if x[0]=='col':return f'a{x[1]}."{x[2]}"'
        if x[0]=='value':return str(x[1])
        return '(CAST('+term(x[1])+' AS NUMERIC)+'+term(x[2])+')'
    select='COUNT(*)' if count else ','.join(f'a{i}.eid' for i in range(len(ids)))
    return 'SELECT '+select+' FROM '+','.join(table+f' a{i}' for i in range(len(ids)))+' WHERE '+' AND '.join(term(a)+' '+op+' '+term(b) for op,a,b in ps)

def mongo_pipeline(q,collection,*,count=False):
    ids,anchor,ps=predicates(q)


    rank={'flashloan':0,'borrow':1,'swap':2}
    first=min(range(len(ids)),key=lambda i:(rank[q['events'][i]['type']],-sum(f['event']==ids[i] for f in q.get('attr_filters',[])),i))
    if q['scope']=='anchored_episode':
        w=q['scope_params']['forward_window_sec']


        ps += [('>=',('col',anchor,'time'),('add',('col',i,'time'),('value',-w))) for i in range(len(ids)) if i!=anchor]
    def deps(x):
        if x[0]=='col':return {x[1]}
        if x[0]=='value':return set()
        return deps(x[1])|deps(x[2])
    def pd(p):return deps(p[1])|deps(p[2])

    order=[first];remaining=set(range(len(ids)))-{first}
    while remaining:
        def score(i):return (sum(p[0]=='=' and i in pd(p) and bool(pd(p)&set(order)) for p in ps),-i)
        chosen=max(remaining,key=score);order.append(chosen);remaining.remove(chosen)
    ops={'=':'$eq','!=':'$ne','<':'$lt','>':'$gt','<=':'$lte','>=':'$gte'}
    stages=[];bound=set()
    for i in order:
        available=bound|{i};selected=[p for p in ps if pd(p)<=available and not pd(p)<=bound]
        def term(x):
            if x[0]=='col':return ('$'+x[2]) if x[1]==i else ('$$v'+str(x[1])+'.'+x[2])
            if x[0]=='value':return x[1]
            return {'$add':[{'$toDecimal':term(x[1])},term(x[2])]}
        expressions=[{ops[op]:[term(a),term(b)]} for op,a,b in selected]

        match={'kind':TYPES[q['events'][i]['type']],'$expr':{'$and':expressions}}
        if not bound:
            stages += [{'$match':match},{'$project':{'_id':0,'a'+str(i):'$$ROOT'}}]
        else:
            alias='a'+str(i)
            stages += [{'$lookup':{'from':collection,'let':{'v'+str(j):'$a'+str(j) for j in sorted(bound)},
                                   'pipeline':[{'$match':match}], 'as':alias}}, {'$unwind':'$'+alias}]
        bound.add(i)
    stages.append({'$count':'n'} if count else {'$project':{'_id':0,**{'e'+str(i):'$a'+str(i)+'.eid' for i in range(len(ids))}}})
    return stages
