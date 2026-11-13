import gc
import json
import subprocess
import sys
import time
import types
from pathlib import Path


def read_json(stream):
    line = stream.readline()
    if not line:
        raise RuntimeError('native worker exited/truncated stream')
    obj = json.loads(line)
    if obj.get('frame') == 'error':
        raise RuntimeError(obj['error'])
    return obj


def receive_events(stream, event_class, request_id, kinds, shards):
    events, seen, frames, raw_rows = [], {}, set(), 0
    expected = {(s, t) for s in range(shards) for t in kinds}
    while True:
        frame = read_json(stream)
        if frame.get('request_id') != request_id:
            raise ValueError('request ID mismatch')
        if frame.get('frame') == 'end':
            if (frames != expected or frame.get('rows') != raw_rows or
                frame.get('selections') != len(frames) or
                frame.get('retrieval_executed') is not True or frame.get('candidate_cache_used') is not False):
                raise ValueError('incomplete or cached native retrieval')
            return events, frame
        if frame.get('frame') != 'selection':
            raise ValueError('invalid protocol frame')
        key = (frame.get('shard'), frame.get('type'))
        if key not in expected or key in frames:
            raise ValueError('duplicate/unexpected selection')
        frames.add(key)
        rows = frame.get('rows')
        if type(rows) is not int or rows < 0:
            raise ValueError('invalid row count')
        for _ in range(rows):
            raw = read_json(stream)
            event = event_class.from_json(raw)
            if not event.event_id or event.event_type != key[1]:
                raise ValueError('candidate identity/type mismatch')
            if event.event_id in seen:
                if seen[event.event_id] != event:
                    raise ValueError('conflicting candidate event_id')
            else:
                seen[event.event_id] = event
                events.append(event)
        raw_rows += rows


class MBFT:
    def __init__(self, source, executable, shard_manifest, scope, log_dir):
        self.source, self.executable, self.manifest = Path(source), Path(executable), Path(shard_manifest)
        self.scope, self.log_dir = scope, Path(log_dir)
        self.worker = None

    def prepare(self):
        path = self.source / 'MBFT-Adapt_MergeBloom_QueryOnly_v3_source/mbft_adapt/python/neutral_matcher.py'
        self.m = types.ModuleType('resident_neutral_matcher')
        self.m.__file__ = str(path)
        sys.modules[self.m.__name__] = self.m
        exec(compile(path.read_text(encoding='utf-8'), str(path), 'exec'), self.m.__dict__)
        self.log = (self.log_dir / 'native_stderr.log').open('x')
        self.worker = subprocess.Popen([str(self.executable), str(self.manifest)],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.log,
            text=True, encoding='utf-8', bufsize=1)
        ready = read_json(self.worker.stdout)
        if ready.get('frame') != 'ready' or ready.get('protocol') != 'mbft_resident_jsonl_v1' or ready.get('load_count') != 1:
            raise ValueError('native startup protocol')
        manifest = json.loads(self.manifest.read_text())
        if ready.get('source_rows') != manifest['expected_source_rows'] or ready.get('shards') != len(manifest['shards']):
            raise ValueError('native startup source mismatch')
        self.shards, self.sequence = ready['shards'], 0
        return {**ready, 'candidate_transport': 'streamed JSONL pipe; no candidate-file cache',
                'native_pid': self.worker.pid, 'input_is_query_independent': True}

    def execute(self, q, out):
        t0 = time.perf_counter()
        scope = 'same_tx' if self.scope == 'same_tx' else 'anchored_episode'
        if not q.get('events') or q.get('scope') != scope:
            raise ValueError('empty query or wrong scope')
        kinds = sorted({str(e['type']).lower() for e in q['events']})
        if not set(kinds) <= {'flashloan', 'swap', 'borrow'}:
            raise ValueError('unsupported event types')
        self.sequence += 1
        self.worker.stdin.write(json.dumps({'request_id': self.sequence, 'types': kinds}) + '\n')
        self.worker.stdin.flush()
        events, native = receive_events(self.worker.stdout, self.m.Event, self.sequence, kinds, self.shards)
        t1 = time.perf_counter()
        matcher = self.m.SameTxMatcher(events) if self.scope == 'same_tx' else self.m.EpisodeMatcher(events)
        t2 = time.perf_counter()
        result = matcher.run(q, materialize_limit=0)
        count = int(result['result_count'])
        t3 = time.perf_counter()


        del matcher, events, result


        gc.collect()
        t4 = time.perf_counter()
        return count, {'engine': 'original_mbft_native_retrieval+neutral_matcher',
            'request_id': self.sequence, 'native_pid': self.worker.pid,
            'exclusive_adapter_seconds': {'retrieve_transfer_parse_s': t1-t0,
                'temporary_adapter_index_s': t2-t1, 'match_count_s': t3-t2,
                'release_query_state_s': t4-t3},
            'native_diagnostics_not_additive': native,
            'candidate_cache_used': False}

    def close(self):
        if self.worker:
            if self.worker.poll() is None:
                try:
                    self.worker.stdin.write('{"op":"close"}\n')
                    self.worker.stdin.flush()
                    self.worker.wait(timeout=5)
                except (BrokenPipeError, subprocess.TimeoutExpired):
                    self.worker.terminate()
                    try:
                        self.worker.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.worker.kill()
                        self.worker.wait()
            self.worker.stdin.close()
            self.worker.stdout.close()
        if hasattr(self, 'log'):
            self.log.close()
