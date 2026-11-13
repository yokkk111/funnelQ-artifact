import random
import unittest
from business import SPEC, finish, validate

def row(kind,seq,src='',dst='',**kw):
    r=dict(kind=kind,seq=seq,src=src,dst=dst,pool='p'+str(seq%2),dex='d'+str(seq%2),amount=0,token='')
    r.update(kw);return r

def quadratic(rows):
    swaps=sorted([r for r in rows if r['kind']=='pool_swap'],key=lambda r:r['seq'])
    aggs=[r for r in rows if r['kind']=='aggregator_intent']
    if max([r['amount'] for r in aggs] or [0])<100000 or len(swaps)<3:return False
    if len({r['pool'] for r in swaps}-{''})<2 or len({r['dex'] for r in swaps}-{''})<2:return False
    dp=[1]*len(swaps);prev=[-1]*len(swaps)
    for j in range(len(swaps)):
        for i in range(j):
            if swaps[i]['dst'] and swaps[i]['dst']==swaps[j]['src'] and dp[i]+1>dp[j]:dp[j]=dp[i]+1;prev[j]=i
    end=max(range(len(dp)),key=dp.__getitem__)
    if dp[end]<3:return False
    tokens={r[k] for r in aggs for k in ('src','dst')}-{''}
    while end>=0:tokens.update((swaps[end]['src'],swaps[end]['dst']));end=prev[end]
    tokens.discard('')
    return bool(tokens & {r['token'] for r in rows if r['kind']=='token_transfer'})

class Tests(unittest.TestCase):
    def valid(self):
        return [row('aggregator_intent',10,amount=100000),row('pool_swap',0,'a','b'),row('pool_swap',1,'b','c'),row('pool_swap',2,'c','d'),row('token_transfer',9,token='b')]
    def test_threshold_and_late_aggregator(self):
        rs=self.valid();self.assertTrue(finish(rs,SPEC));rs[0]['amount']=99999;self.assertFalse(finish(rs,SPEC))
    def test_no_loss_filter(self):self.assertTrue(finish(self.valid(),SPEC))
    def test_no_transfer_support(self):
        rs=self.valid();rs[-1]['token']='unrelated';self.assertFalse(finish(rs,SPEC))
    def test_order(self):
        rs=self.valid();rs[2]['seq']=4;self.assertFalse(finish(rs,SPEC))
    def test_short_and_null(self):
        self.assertFalse(finish([],SPEC));rs=self.valid();rs[2]['src']='';self.assertFalse(finish(rs,SPEC))
    def test_spec_drift(self):
        with self.assertRaises(ValueError):validate(dict(SPEC,minimum_pools=1))
    def test_random_against_independent_quadratic(self):
        rng=random.Random(42)
        for _ in range(3000):
            rs=[row('aggregator_intent',100,src=rng.choice(['','a']),amount=rng.choice([99999,100000,200000]))]
            for i in range(rng.randrange(1,25)):
                rs.append(row('pool_swap',i,rng.choice(['','a','b','c','d']),rng.choice(['','a','b','c','d'])))
            rs.append(row('token_transfer',101,token=rng.choice(['','a','b','c','d'])))
            self.assertEqual(finish(rs,SPEC),quadratic(rs))

if __name__=='__main__':unittest.main()
