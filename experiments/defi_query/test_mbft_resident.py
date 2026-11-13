import io
import json
import unittest
from dataclasses import dataclass
from mbft_resident import MBFT, receive_events


@dataclass
class Event:
    event_id: str
    event_type: str
    value: int

    @staticmethod
    def from_json(x):
        return Event(x['event_id'], x['event_type'], x['value'])


def stream(records):
    return io.StringIO(''.join(json.dumps(r)+'\n' for r in records))


def selection(shard=0, rows=1):
    return {'frame': 'selection', 'request_id': 1, 'shard': shard, 'type': 'borrow', 'rows': rows}


def end(rows=1, selections=1):
    return {'frame': 'end', 'request_id': 1, 'rows': rows, 'selections': selections,
            'retrieval_executed': True, 'candidate_cache_used': False}


class ProtocolTests(unittest.TestCase):
    def test_query_specific_matcher_cycles_are_released(self):
        import weakref
        from types import SimpleNamespace
        refs = []
        class Matcher:
            def __init__(self, events):
                self.events, self.cycle = events, self
                refs.append(weakref.ref(self))
            def run(self, q, materialize_limit):
                return {'result_count': 1}
        a = MBFT('.', 'unused', 'unused', 'same_tx', '.')
        a.m = SimpleNamespace(Event=Event, SameTxMatcher=Matcher)
        a.worker = SimpleNamespace(pid=1, stdin=io.StringIO(), stdout=stream([
            selection(), {'event_id':'a','event_type':'borrow','value':1}, end()]))
        a.sequence, a.shards = 0, 1
        n, detail = a.execute({'scope':'same_tx','events':[{'type':'borrow'}]}, '.')
        self.assertEqual(n, 1)
        self.assertIsNone(refs[0]())
        self.assertGreater(detail['exclusive_adapter_seconds']['release_query_state_s'], 0)

    def test_complete_empty_and_cross_shard_dedup(self):
        e = {'event_id': 'a', 'event_type': 'borrow', 'value': 1}
        rows, report = receive_events(stream([selection(), e, selection(1), e, end(2, 2)]), Event, 1, ['borrow'], 2)
        self.assertEqual(len(rows), 1)
        rows, _ = receive_events(stream([selection(rows=0), end(0)]), Event, 1, ['borrow'], 1)
        self.assertEqual(rows, [])

    def test_missing_duplicate_and_cached_retrieval_rejected(self):
        bad = [
            [end(0, 0)],
            [selection(rows=0), selection(rows=0), end(0, 2)],
            [selection(rows=0), {**end(0), 'candidate_cache_used': True}],
            [selection(rows=0), {**end(0), 'retrieval_executed': False}],
            [selection(rows=0), {**end(0), 'request_id': 2}],
        ]
        for records in bad:
            with self.subTest(records=records), self.assertRaises(ValueError):
                receive_events(stream(records), Event, 1, ['borrow'], 1)

    def test_conflicting_identity_rejected(self):
        a = {'event_id': 'a', 'event_type': 'borrow', 'value': 1}
        b = {**a, 'value': 2}
        with self.assertRaisesRegex(ValueError, 'conflicting'):
            receive_events(stream([selection(rows=2), a, b, end(2)]), Event, 1, ['borrow'], 1)

    def test_truncation_and_wrong_type(self):
        with self.assertRaisesRegex(RuntimeError, 'truncated'):
            receive_events(stream([selection()]), Event, 1, ['borrow'], 1)
        with self.assertRaisesRegex(ValueError, 'type mismatch'):
            receive_events(stream([selection(), {'event_id': 'a', 'event_type': 'swap', 'value': 1}]), Event, 1, ['borrow'], 1)


if __name__ == '__main__':
    unittest.main()
