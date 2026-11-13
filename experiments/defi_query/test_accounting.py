import unittest,json,tempfile,sys,types
from pathlib import Path
from query_accounting import instrument_funnelq,instrument_matcher,instrument_sql,mbft_components

class Accounting(unittest.TestCase):
    def test_overlay(self):
        root=Path(__file__).resolve().parents[2]/'FunnelQ_code_release/defi'
        for mode,file in [('same_tx','query_same_tx_generic.py'),('episode','query_episode_generic.py')]:
            p=root/'systems/hvceq'/mode/file
            compile(instrument_funnelq(p.read_text(encoding='utf-8'),mode),str(p),'exec')
        p=root/'MBFT-Adapt_MergeBloom_QueryOnly_v3_source/mbft_adapt/python/neutral_matcher.py'
        compile(instrument_matcher(p.read_text(encoding='utf-8')),str(p),'exec')
        for mode,file in [('same_tx','query_same_tx_summary_reuse_indexes.py'),('episode','query_episode_summary_reuse_indexes.py')]:
            p=root/'systems/sql'/mode/file
            compile(instrument_sql(p.read_text(encoding='utf-8'),mode),str(p),'exec')
    def test_sum_and_timeout(self):
        a={'selections':{'borrow':{'mbft_tree_query_time_us':1e6,'candidate_write_time_us':2e6,'candidate_merge_time_us':3e6}}}
        r={'client_candidate_load_ms':4000,'client_temporary_index_build_ms':5000,'client_complex_match_ms':6000}
        x=mbft_components(a,r,['borrow']);self.assertEqual(x['query_seconds'],21);self.assertFalse(x['query_timeout_exceeded'])
        r['client_candidate_load_ms']=700000;self.assertTrue(mbft_components(a,r,['borrow'])['query_timeout_exceeded'])
        del a['selections']['borrow']['candidate_merge_time_us']
        with self.assertRaises(KeyError):mbft_components(a,r,['borrow'])
    def test_conflicting_candidate_identity(self):
        p=Path(__file__).resolve().parents[2]/'FunnelQ_code_release/defi/MBFT-Adapt_MergeBloom_QueryOnly_v3_source/mbft_adapt/python/neutral_matcher.py'
        mod=types.ModuleType('accounting_test_matcher');mod.__file__=str(p);sys.modules[mod.__name__]=mod
        exec(compile(instrument_matcher(p.read_text(encoding='utf-8')),str(p),'exec'),mod.__dict__)
        with tempfile.TemporaryDirectory() as td:
            f=Path(td)/'events.jsonl';event={'event_id':'one','tx_hash':'tx','event_type':'borrow','roles':{'borrower':'a','asset':'x'},'attrs':{'amount':1}}
            f.write_text(json.dumps(event)+'\n'+json.dumps(event)+'\n')
            self.assertEqual(len(mod.load_verified_events([f])),1)
            changed={**event,'attrs':{'amount':-1}}
            f.write_text(json.dumps(event)+'\n'+json.dumps(changed)+'\n')
            with self.assertRaisesRegex(ValueError,'conflicting event_id'):mod.load_verified_events([f])

if __name__=='__main__':unittest.main()
