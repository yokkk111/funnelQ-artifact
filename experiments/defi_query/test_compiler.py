import copy,sqlite3,sys,unittest
from pathlib import Path
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'defi_native_verification/revision_v4'))
from legacy_evaluate import EVENT,brute
from test_evaluator import pattern
from compiler import pg_sql,mongo_pipeline,validate

def interpret_pipeline(rows,pipeline):

    import operator
    ops={'$eq':operator.eq,'$ne':operator.ne,'$gt':operator.gt,'$gte':operator.ge,'$lt':operator.lt,'$lte':operator.le,'$add':operator.add}
    source=[{k:int(r[k]) for k in rows.dtype.names} for r in rows]
    def expr(x,row,variables):
        if isinstance(x,str) and x.startswith('$'):
            if x=='$$ROOT':return row
            if x.startswith('$$'):
                parts=x[2:].split('.');value=variables[parts[0]];parts=parts[1:]
            else:parts=x[1:].split('.');value=row
            for p in parts:value=value[p]
            return value
        if isinstance(x,dict):
            name,args=next(iter(x.items()))
            if name=='$and':return all(expr(a,row,variables) for a in args)
            if name=='$toDecimal':return expr(args,row,variables)
            return ops[name](*(expr(a,row,variables) for a in args))
        return x
    def execute(items,stages,variables):
        for stage in stages:
            kind,v=next(iter(stage.items()))
            if kind=='$match':items=[r for r in items if r['kind']==v['kind'] and expr(v['$expr'],r,variables)]
            elif kind=='$project':items=[{k:expr(e,r,variables) for k,e in v.items() if k!='_id'} for r in items]
            elif kind=='$lookup':
                items=[{**r,v['as']:execute(list(source),v['pipeline'],{k:expr(e,r,variables) for k,e in v['let'].items()})} for r in items]
            elif kind=='$unwind':items=[{**r,v[1:]:child} for r in items for child in r[v[1:]]]
            else:raise AssertionError(kind)
        return items
    return sorted(tuple(r['e'+str(i)] for i in range(len(r))) for r in execute(source,pipeline,{}))

class CompilerTests(unittest.TestCase):
    def check(self,a,q):
        db=sqlite3.connect(':memory:');db.execute("ATTACH ':memory:' AS bench")
        db.execute('CREATE TABLE bench.events ('+','.join('"'+n+'" INTEGER' for n in EVENT.names)+')')
        db.executemany('INSERT INTO bench.events VALUES ('+','.join('?' for _ in EVENT.names)+')',[tuple(map(int,r)) for r in a])
        got=sorted(db.execute(pg_sql(q,'bench.events')).fetchall());expected=sorted(tuple(map(int,r)) for r in brute(a,q))
        self.assertEqual(got,expected);self.assertEqual(db.execute(pg_sql(q,'bench.events',count=True)).fetchone()[0],len(expected));db.close()
        self.assertEqual(interpret_pipeline(a,mongo_pipeline(q,'events')),expected)
    def test_randomized(self):
        for seed in range(32):
            g=np.random.default_rng(seed);a=np.zeros(20,dtype=EVENT);a['eid']=np.arange(20)
            for n,h in [('tx',4),('time',25),('actor',3),('asset',3),('out',3),('pool',2),('kind',3)]:a[n]=g.integers(0,h,len(a))
            a['sign']=g.integers(-1,2,len(a));q=pattern('same_tx' if seed%2 else 'anchored_episode',seed%12)
            if q['scope']=='same_tx':q['scope_params']={}
            if seed%3==0:q['order']=[]
            if seed%4==0:q['neq_constraints']=[['a','asset','b','asset']]
            if seed%5==0:q['events'][1]['roles']['token_bought']='$x'
            self.check(a,q)
    def test_boundaries_and_reorder(self):
        a=np.array([(1,1,9,1,2,0,0,2,1),(2,2,10,1,2,3,0,1,0),(3,3,19,1,3,0,0,2,-1),(4,3,20,1,3,0,0,2,-1)],dtype=EVENT)
        for w in [0,1,9,10,11]:
            q=pattern(w=w);q['query_id']='renamed';q['events']=q['events'][1:2]+q['events'][:1]+q['events'][2:];self.check(a,q)
    def test_reject_unknown(self):
        q=pattern();q['scope_params']['ignored']=True
        with self.assertRaises(ValueError):validate(q)
        q=pattern();q['attr_filters'][0]['value']='1'
        with self.assertRaises(ValueError):validate(q)

if __name__=='__main__':unittest.main()
