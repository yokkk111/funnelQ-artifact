import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from resident_query import CONTRACT, PROTOCOL, SQLite, ro_connect, run_requests


class FakeAdapter:
    def __init__(self):
        self.loads = 0
        self.calls = []

    def prepare(self):
        self.loads += 1

    def execute(self, q, out):
        self.calls.append(q['n'])
        answer = q.pop('n')                                                       
        return answer, {}


class ResidentTests(unittest.TestCase):
    def test_one_load_full_warmup_and_repeated_execution(self):
        adapter = FakeAdapter()
        adapter.prepare()
        cases = [{'case': 'a', 'request': '{"n": 1}', 'expected_count': 1},
                 {'case': 'b', 'request': '{"n": 2}', 'expected_count': 2}]
        with tempfile.TemporaryDirectory() as d:
            rows = run_requests(adapter, cases, Path(d), 10, timeout=None)
        self.assertEqual(adapter.loads, 1)
        self.assertEqual(adapter.calls, [1, 2] * 11)
        self.assertEqual(sum(not r['warmup'] for r in rows), 20)
        self.assertTrue(all(r['protocol'] == PROTOCOL for r in rows))
        for r in rows:
            self.assertAlmostEqual(sum(r['components_seconds'].values()), r['query_seconds'])

    def test_answer_mismatch_stops_before_measurements(self):
        adapter = FakeAdapter()
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaisesRegex(ValueError, 'answer mismatch'):
                run_requests(adapter, [{'case': 'a', 'request': '{"n":3}', 'expected_count': 4}], Path(d), 10, timeout=None)
            self.assertFalse((Path(d) / 'repeat_01').exists())
            self.assertFalse(json.loads((Path(d) / 'warmup/a/result.json').read_text())['passed'])

    def test_readonly_database(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / 'input.db'
            c = sqlite3.connect(p)
            c.execute('CREATE TABLE data (n INTEGER)')
            c.close()
            c = ro_connect(p)
            with self.assertRaises(sqlite3.OperationalError):
                c.execute('INSERT INTO data VALUES (1)')
            c.close()

    def test_no_v2_mixing_or_cold_cache_claim(self):
        self.assertFalse(CONTRACT['historical_v2_samples_compatible'])
        self.assertFalse(CONTRACT['cold_cache_claimed'])
        self.assertIn('answers', CONTRACT['forbidden_cache'])

    def test_sql_recompiles_and_executes_each_request(self):
        from types import SimpleNamespace
        calls = []
        m = SimpleNamespace(
            validate_query=lambda q: calls.append('validate'),
            choose_event_order_query_order=lambda q: q,
            build_index_signatures=lambda q, specs: (None, {}, None),
            build_select_sql=lambda q, specs, aliases: calls.append('compile') or 'SELECT 1',
            run_count=lambda c, sql: calls.append('execute') or (1, 1, .001))
        adapter = SQLite(Path('.'), Path('unused'), 'episode')
        adapter.m, adapter.c = m, object()
        for _ in range(3):
            self.assertEqual(adapter.execute({}, Path('.'))[0], 1)
        self.assertEqual(calls, ['validate', 'compile', 'execute'] * 3)

    def test_duplicate_cases_and_old_output_rejected(self):
        case = {'case': 'a', 'request': '{"n":1}', 'expected_count': 1}
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                run_requests(FakeAdapter(), [case, case], Path(d), 1, timeout=None)
            run_requests(FakeAdapter(), [case], Path(d), 1, timeout=None)
            with self.assertRaises(FileExistsError):
                run_requests(FakeAdapter(), [case], Path(d), 1, timeout=None)

    def test_diagnostic_hashing_is_after_query_timer(self):
        import time
        class WithDiagnostics(FakeAdapter):
            def diagnostics(self):
                time.sleep(.04)
                return {'audit_hash': 'outside'}
        with tempfile.TemporaryDirectory() as d:
            rows = run_requests(WithDiagnostics(),
                [{'case': 'a', 'request': '{"n":1}', 'expected_count': 1}],
                Path(d), 1, timeout=None)
        for r in rows:
            self.assertLess(r['query_seconds'], .04)
            self.assertEqual(r['diagnostics']['audit_hash'], 'outside')


if __name__ == '__main__':
    unittest.main()
