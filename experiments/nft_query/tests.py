import ast,json,unittest
from pathlib import Path

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[1]

class ProtocolTest(unittest.TestCase):
    def test_all_python_parses(self):
        for p in HERE.glob('*.py'):ast.parse(p.read_text(encoding='utf-8'),filename=str(p))
    def test_no_source_mutation_in_native_entrypoints(self):
        for name in ['funnelq','hgmatch']:
            text=(HERE/(name+'_resident.rs')).read_text()
            self.assertNotIn('CREATE TABLE',text)
            self.assertIn('load_count',text)
        self.assertIn('SQLITE_OPEN_READ_ONLY',(HERE/'funnelq_resident.rs').read_text())
    def test_direct_timer_and_retrieval_inside_request(self):
        text=(HERE/'run.py').read_text()
        self.assertIn('elapsed=time.perf_counter()-start',text)
        self.assertLess(text.index('rows,details=adapter.execute'),text.index('elapsed=time.perf_counter()-start'))
        self.assertGreater(text.index('canonical_full_result_digest(rows)'),text.index('elapsed=time.perf_counter()-start'))
        self.assertNotIn('resume-atomic',text)
    def test_native_queries_are_not_cached(self):
        for name in ['funnelq_resident.rs','hgmatch_resident.rs','mbft_resident.cpp']:
            text=(HERE/name).read_text();self.assertIn('ready',text);self.assertIn('result',text)
        text=(HERE/'mbft_resident.cpp').read_text()
        self.assertLess(text.index('while(std::getline'),text.index('selectAllChainNoProof'))
    def test_five_templates_and_window(self):
        from systems.nft.hvceq.selected5_query_contract import validate_selected5_query_contract
        paths=list((ROOT/'queries/nft/selected5').glob('Q*.json'));self.assertEqual(len(paths),5)
        for p in paths:
            q=json.loads(p.read_text());validate_selected5_query_contract(q)
            self.assertEqual(q['full_binding_plan']['delta_seconds'],1800)

if __name__=='__main__':unittest.main()
