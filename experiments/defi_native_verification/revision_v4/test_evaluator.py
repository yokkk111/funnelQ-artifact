import copy,unittest
import numpy as np
from evaluate import EVENT,exact,brute,request_tokens

def pattern(scope='anchored_episode',w=10):
    return {'scope':scope,'scope_params':{'semantic_anchor_event':'a','forward_window_sec':w},
        'events':[{'id':'a','type':'borrow','roles':{'borrower':'$u','asset':'$x'}},
                  {'id':'s','type':'swap','roles':{'taker':'$u','token_sold':'$x','token_bought':'$y'}},
                  {'id':'b','type':'borrow','roles':{'borrower':'$u','asset':'$y'}}],
        'order':[['a','s'],['s','b']], 'attr_filters':[{'event':'a','field':'amount','op':'>','value':'0'},
             {'event':'b','field':'amount','op':'<','value':'0'}], 'neq_constraints':[]}

class Equivalence(unittest.TestCase):
    def check(self,a,q):
        self.assertTrue(np.array_equal(exact(a,q),brute(a,q)),str(q))
    def test_boundaries_and_identity(self):
        a=np.array([(1,1,9,1,2,0,0,2,1),(2,2,10,1,2,3,0,1,0),
            (3,3,19,1,3,0,0,2,-1),(4,3,20,1,3,0,0,2,-1),
            (5,3,19,1,3,0,0,2,-1),(6,3,8,1,3,0,0,2,-1)],dtype=EVENT)
        self.check(a,pattern());self.assertEqual(len(exact(a,pattern())),2)
        for w in [0,1,9,10,11,20]:self.check(a,pattern(w=w))
        for scope in ['same_tx','anchored_episode']:
            q=pattern(scope);a['tx']=1;self.check(a,q)
        a['time']=np.iinfo(np.int64).max-2;self.check(a,pattern(w=10))
    def test_no_q_dispatch_renamed_anchor_not_first(self):
        a=np.array([(1,1,9,1,2,0,0,2,1),(2,2,10,1,2,3,0,1,0),(3,3,19,1,3,0,0,2,-1)],dtype=EVENT)
        q=pattern();q['events']=[q['events'][1],q['events'][0],q['events'][2]]
        q['query_id']='never-seen-before';self.check(a,q)
        mapping={'a':'select','s':'from','b':'where'}
        for e in q['events']:e['id']=mapping[e['id']]
        q['scope_params']['semantic_anchor_event']='select'
        q['order']=[[mapping[x],mapping[y]] for x,y in q['order']]
        for f in q['attr_filters']:f['event']=mapping[f['event']]
        self.check(a,q)
    def test_randomized(self):
        for seed in range(36):
            g=np.random.default_rng(seed);a=np.zeros(24,dtype=EVENT);a['eid']=np.arange(24)
            for name,hi in [('tx',4),('time',35),('actor',3),('asset',3),('out',3),('pool',2),('kind',3)]:a[name]=g.integers(0,hi,24)
            a['sign']=g.integers(-1,2,24)
            q=pattern('same_tx' if seed%2 else 'anchored_episode',seed%12)
            if seed%3==0:q['order']=[]
            if seed%4==0:q['neq_constraints']=[['a','asset','b','asset']]
            if seed%5==0:q['events'][1]['roles']['token_bought']='$x'
            self.check(a,q)
    def test_self_join_empty_and_single_atom(self):
        a=np.array([(1,1,10,1,2,2,0,1,0),(2,1,10,1,2,2,0,1,0)],dtype=EVENT)
        q={'scope':'anchored_episode','scope_params':{'semantic_anchor_event':'x','forward_window_sec':0},
           'events':[{'id':x,'type':'swap','roles':{'taker':'$u','token_sold':'$a','token_bought':'$a'}} for x in ['x','y']],
           'order':[]}
        self.check(a,q);self.assertEqual(len(exact(a,q)),1)
        self.check(a[:0],q);q['events']=q['events'][:1];self.check(a,q)
    def test_atomic_request(self):
        q=pattern();self.assertEqual(request_tokens(q),'borrow_negative,borrow_positive,swap')
        q['attr_filters']=[];self.assertEqual(request_tokens(q),'borrow_negative,borrow_positive,borrow_zero,swap')
        q=pattern();q['attr_filters']=[q['attr_filters'][0]];q['events']=q['events'][:1];q['order']=[]
        self.assertEqual(request_tokens(q),'borrow_positive')
        q['attr_filters'][0]['value']='2'
        with self.assertRaises(AssertionError):request_tokens(q)

if __name__=='__main__':unittest.main()
