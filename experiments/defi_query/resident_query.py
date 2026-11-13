from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import signal
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

PROTOCOL = 'defi_warm_resident_count_v3'
CONTRACT = {
    'protocol': PROTOCOL,
    'output': 'count_only; full-binding correctness gates are separate',
    'startup': 'once per method/scope session, reported separately; no QuerySpec passed',
    'query': 'request JSON decode/validation, compilation, retrieval, query-dependent preparation, matching and count normalization',
    'warmup': 'one complete request per case; then round-robin measured repetitions in the same session',
    'allowed_cache': ['OS pages', 'DB buffer pool', 'query-independent data/index/encoding'],
    'forbidden_cache': ['Cand0/Cand1/Cand2', 'query-filtered adapter input/index', 'answers', 'previous timing samples'],
    'cold_cache_claimed': False,
    'cache_flush': 'none; do not flush shared-server OS caches',
    'cache_bytes_equal': False,
    'resources': 'same CPU allocation and memory ceiling; report resident and request peak separately',
    'timeout': '600 seconds per complete warmup/measured request; no warmup subtraction from a timeout',
    'historical_v2_samples_compatible': False,
}


def save(path, value):
    with Path(path).open('x', encoding='utf-8') as f:
        json.dump(value, f, ensure_ascii=False, indent=2)


def phase(out, name, seconds):

    p = Path(out) / 'phase.json'
    tmp = p.with_suffix('.tmp')
    tmp.write_text(json.dumps({'phase': name, 'started_monotonic': time.monotonic(),
                              'limit_seconds': seconds}), encoding='utf-8')
    tmp.replace(p)


def ro_connect(path):
    c = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True)
    c.execute('PRAGMA query_only=ON')
    return c


def module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


@contextmanager
def deadline(seconds):

    if not hasattr(signal, 'setitimer'):
        raise RuntimeError('Hard request deadlines require Linux; Windows supports unit tests only')
    def expired(signum, frame):
        raise TimeoutError('complete request exceeded its time limit')
    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


class Memory:


    def __init__(self, external_pids=(), limit_bytes=64 * 2**30, external_memory_file=None):
        import psutil
        self.psutil = psutil
        self.roots = [psutil.Process()] + [psutil.Process(p) for p in external_pids]
        self.limit = limit_bytes
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.peak = self.current = self.phase_peak = 0
        self.exceeded = False
        self.external_memory_file = external_memory_file

    def sample(self):
        procs = {}
        for root in self.roots:
            try:
                for p in [root] + root.children(recursive=True):
                    procs[p.pid] = p
            except self.psutil.NoSuchProcess:
                continue
        total = 0
        for p in procs.values():
            try:
                total += p.memory_info().rss
            except self.psutil.NoSuchProcess:
                pass
        with self.lock:
            from external_memory import read_external_memory
            total += read_external_memory(self.external_memory_file)
            self.current = total
            self.peak = max(self.peak, total)
            self.phase_peak = max(self.phase_peak, total)
            self.exceeded |= total > self.limit

    def loop(self):
        while not self.stop.wait(.05):
            self.sample()

    def __enter__(self):
        self.sample()
        self.thread = threading.Thread(target=self.loop, daemon=True)
        self.thread.start()
        return self

    def begin(self):
        self.sample()
        with self.lock:
            self.phase_peak = self.current
            return self.current

    def end(self):
        self.sample()
        with self.lock:
            if self.exceeded:
                raise MemoryError('session RSS ceiling exceeded; sample is not eligible')
            return self.phase_peak

    def __exit__(self, *args):
        self.stop.set()
        self.thread.join()


class FunnelQ:
    def __init__(self, source, db, scope):
        self.source, self.db, self.scope = source, db, scope

    def prepare(self):
        sys.path.insert(0, str(self.source))
        self.m = module(self.source / f'systems/hvceq/{self.scope}/query_{self.scope}_generic.py', 'resident_funnelq')
        if not self.m.rust_kernel_available():
            raise RuntimeError('Expected Rust integer kernel; refusing silent Python fallback')


        self.original_digest = self.m.digest_ids
        self.pending_frontiers = []
        def defer_digest(ids):
            self.pending_frontiers.append(ids)
            return None
        self.m.digest_ids = defer_digest
        self.m.write_binding_headers = lambda out: (out / 'not_materialized_instances.csv', out / 'not_materialized_bindings.csv')
        with ro_connect(self.db) as c:
            if self.scope == 'same_tx':

                self.rt = self.m.EncodedSameTxRuntime(c, load_event_ids=False,
                    build_tx_type_buckets=False, role_fields=None, numeric_fields=None, event_types=None)
                self.m.prepare_numpy_runtime(self.rt)
            else:


                windows = [int(r[0]) for r in c.execute('SELECT DISTINCT window_sec FROM episode_window_endpoints ORDER BY window_sec')]
                if not windows:
                    raise ValueError('no prebuilt Episode window indexes')
                self.rt = self.m.EncodedRuntime(c, windows[0],
                    enable_terminal_complete_dirs=False, enable_terminal_anti_join_dirs=False,
                    load_tx_type_positions=False, enable_role_postings=False, event_types=None)
                self.windows = {windows[0]: self.rt.window_end}
                for window in windows[1:]:
                    self.rt.window_sec = window
                    self.windows[window] = self.rt._load_window_endpoints()
                self.acc = self.m.EpisodeAccessor(self.rt, self.m.load_value_ids(c))
        c.close()
        return {'unfiltered_source_load': True, 'load_count': 1,
                'window_indexes': sorted(self.windows) if self.scope == 'episode' else [],
                'rust_required': True}

    def execute(self, q, out):
        self.pending_frontiers = []
        expected_scope = 'same_tx' if self.scope == 'same_tx' else 'anchored_episode'
        if q.get('scope') != expected_scope or not q.get('events'):
            raise ValueError('wrong scope or empty query')
        for key in ['order', 'attr_filters', 'neq_constraints']:
            q.setdefault(key, [])
        if self.scope == 'same_tx':
            r = self.m.run_generic(self.rt, q, out, False, 'auto', False)
        else:
            w = int(q['scope_params']['forward_window_sec'])
            if w not in self.windows:
                raise ValueError('window not in the prebuilt store; no implicit query-specific offline build')
            self.rt.window_sec, self.rt.window_end = w, self.windows[w]
            r = self.m.run_generic(self.acc, q, out, False, False)
        if not r.get('matching_engine', '').startswith('rust_'):
            raise RuntimeError('unexpected matcher fallback')
        return int(r['num_matches']), {'engine': r['matching_engine'],
            'kernel_seconds_diagnostic_only': r['query_latency_s'],
            'frontier_hashing_outside_query_timer': True}

    def diagnostics(self):
        try:
            return {'frontier_digests': dict(zip(['cand0_sha256', 'cand1_sha256', 'cand2_sha256'],
                (self.original_digest(ids) for ids in self.pending_frontiers)))}
        finally:
            self.pending_frontiers = []

    def close(self):
        pass


class SQLite:
    def __init__(self, source, db, scope):
        self.source, self.db, self.scope = source, db, scope

    def prepare(self):
        self.m = module(self.source / f'systems/sql/{self.scope}/query_{self.scope}_summary_reuse_indexes.py', 'resident_sql')
        self.c = ro_connect(self.db)
        self.m.configure_pragmas(self.c, 'MEMORY', 200)
        return {'connection_count': 1, 'index_build': False, 'read_only': True}

    def execute(self, q, out):

        self.m.validate_query(q)
        specs = self.m.choose_event_order_query_order(q)
        _, aliases, _ = self.m.build_index_signatures(q, specs)
        if self.scope == 'episode':
            sql = self.m.build_select_sql(q, specs, aliases)
            n, _, _ = self.m.run_count(self.c, sql)
        else:
            sql, _, _, _ = self.m.build_select_sql(q, specs, aliases)
            sql = self.m.strip_final_order_by(sql)

            sql = 'SELECT COUNT(*) AS num_matches, COUNT(DISTINCT tx_hash) AS num_residual_closures FROM (' + sql + ')'
            n, _, _ = self.m.timed_summary_sql(self.c, sql)
        return int(n), {'engine': 'sqlite', 'compile_each_request': True}

    def close(self):
        if hasattr(self, 'c'):
            self.c.close()


class NativeDB:
    def __init__(self, method):
        self.method = method

    def prepare(self):
        import native_queries
        self.m = native_queries
        self.c = self.m.connect(self.method)

        if self.method == 'mongodb':
            self.c.admin.command('ping')
        return {'connection_count': 1, 'index_build': False, 'server_resident': True}

    def execute(self, q, out):
        n, detail = self.m.execute(self.c, self.method, 'events', q, True)
        return int(n), {'engine': self.method, 'compile_each_request': True}

    def close(self):
        if hasattr(self, 'c'):
            self.c.close()


def run_requests(adapter, cases, out, repeats, memory=None, timeout=600):


    if repeats < 1 or not cases or len({c['case'] for c in cases}) != len(cases):
        raise ValueError('invalid repeats/cases')
    results = []
    for rep in range(repeats + 1):
        for case in cases:
            folder = out / ('warmup' if rep == 0 else f'repeat_{rep:02}') / case['case']
            folder.mkdir(parents=True, exist_ok=False)
            baseline = memory.begin() if memory else None
            if timeout is not None:
                phase(out, f'request/{rep}/{case["case"]}', timeout)
            started = time.perf_counter()
            def request():
                q = json.loads(case['request'])
                parsed = time.perf_counter()
                count, diagnostics = adapter.execute(q, folder)
                normalized = int(count)
                return normalized, diagnostics, parsed
            if timeout is None:
                n, diagnostics, parsed = request()
            else:
                with deadline(timeout):
                    n, diagnostics, parsed = request()
            ended = time.perf_counter()
            if timeout is not None:
                phase(out, 'diagnostics_and_reporting', 120)
            peak = memory.end() if memory else None
            if hasattr(adapter, 'diagnostics'):
                diagnostics.update(adapter.diagnostics())

            passed = n == case['expected_count']
            r = {'protocol': PROTOCOL, 'case': case['case'], 'repeat': rep,
                 'warmup': rep == 0, 'count': n, 'passed': passed,
                 'query_seconds': ended - started,
                 'components_seconds': {'decode_request_s': parsed-started,
                                        'compile_retrieve_prepare_match_normalize_s': ended-parsed},
                 'resident_rss_before_bytes': baseline, 'request_peak_rss_bytes': peak,
                 'request_sha256': hashlib.sha256(case['request'].encode()).hexdigest(),
                 'diagnostics': diagnostics, 'count_only': True}
            save(folder / 'result.json', r)
            if not passed:
                raise ValueError(('answer mismatch', case['case'], n, case['expected_count']))
            results.append(r)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--method', required=True, choices=['funnelq', 'sqlite', 'postgresql', 'mongodb', 'mbft', 'hgmatch'])
    ap.add_argument('--source', type=Path, default=Path('/suite/source/FunnelQ_code_release/defi'))
    ap.add_argument('--db', type=Path)
    ap.add_argument('--scope', required=True, choices=['same_tx', 'episode'])
    ap.add_argument('--cases', type=Path, required=True, help='JSON list: case, query_file, expected_count')
    ap.add_argument('--out', type=Path, required=True)
    ap.add_argument('--repeats', type=int, default=2)
    ap.add_argument('--pilot', action='store_true', help='Required until all-method release gates pass')
    ap.add_argument('--timing-only', action='store_true', help='Ten measured rounds; publication remains blocked pending binding gates')
    ap.add_argument('--server-pid', action='append', type=int, default=[])
    ap.add_argument('--external-memory-file', type=Path)
    ap.add_argument('--hgmatch-executable', type=Path)
    ap.add_argument('--mbft-executable', type=Path)
    ap.add_argument('--shard-manifest', type=Path)
    args = ap.parse_args()
    if args.pilot == args.timing_only:
        ap.error('Choose exactly one of --pilot or --timing-only')
    if args.pilot and args.repeats not in [1, 2]:
        ap.error('Pilot requires one or two measured rounds')
    if args.timing_only and args.repeats != 10:
        ap.error('Timing-only requires ten rounds and an adapter with completed configuration preflight')
    if args.method in ['postgresql', 'mongodb'] and not (args.server_pid or args.external_memory_file):
        ap.error('resident server PID(s) required for comparable memory accounting')
    if args.method in ['funnelq', 'sqlite', 'hgmatch'] and args.db is None:
        ap.error('--db required')
    if args.method == 'mbft' and (args.mbft_executable is None or args.shard_manifest is None):
        ap.error('MBFT requires --mbft-executable and --shard-manifest')
    if args.method == 'hgmatch' and args.hgmatch_executable is None:
        ap.error('HGMatch requires --hgmatch-executable')
    args.out.mkdir(parents=True, exist_ok=False)
    save(args.out / 'contract.json', CONTRACT)
    factory = {'funnelq': FunnelQ, 'sqlite': SQLite}
    if args.method == 'hgmatch':
        from hgmatch_resident import HGMatch
        adapter = HGMatch(args.hgmatch_executable, args.db, args.scope, args.out)
    elif args.method == 'mbft':
        from mbft_resident import MBFT
        adapter = MBFT(args.source, args.mbft_executable, args.shard_manifest, args.scope, args.out)
    else:
        adapter = factory[args.method](args.source, args.db, args.scope) if args.method in factory else NativeDB(args.method)
    try:
        with Memory(args.server_pid, external_memory_file=args.external_memory_file) as memory:
            phase(args.out, 'startup', 1800)
            started = time.perf_counter()
            with deadline(1800):
                details = adapter.prepare()
            startup = time.perf_counter() - started
            memory.end()
            save(args.out / 'startup.json', {'startup_seconds': startup, 'details': details,
                'startup_peak_rss_bytes': memory.peak, 'method': args.method})
            cases = json.loads(args.cases.read_text(encoding='utf-8'))
            for c in cases:
                if Path(c['case']).name != c['case'] or c['case'] in ['.', '..']:
                    raise ValueError('unsafe case ID')
                c['request'] = Path(c['query_file']).read_text(encoding='utf-8')
            rows = run_requests(adapter, cases, args.out, args.repeats, memory)
            save(args.out / 'summary.json', {'protocol': PROTOCOL, 'method': args.method,
                'state': 'passed', 'measured_samples': sum(not r['warmup'] for r in rows),
                'warmups': len(cases), 'load_count': 1, 'session_peak_rss_bytes': memory.peak,
                'publication_ready': False, 'reason': 'all-method protocol audit and gates still required'})
    except Exception as exc:
        save(args.out / 'failure.json', {'state': 'failed', 'error': repr(exc)})
        raise
    finally:
        phase(args.out, 'closing', 30)
        adapter.close()


if __name__ == '__main__':
    main()
