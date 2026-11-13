import argparse
import copy
import json
import importlib.util
import sys
import time
from pathlib import Path

ROOT = Path('/suite/source/_server_mongodb_work')
sys.path.insert(0, str(ROOT))
from pymongo import MongoClient
from systems.nft.mongodb.query_nft_mongodb_full import (
    EVENT_FIELDS, compile_branch_pipeline, execute_compiled)
from systems.nft.common.full_binding_spec import load_full_binding_plan
from systems.nft.common.canonical_results import (
    canonical_full_result_digest, write_normalized_csv)

KEYS = {
    '_eq_cf': ('event_type', 'collection_contract', 'move_from'),
    '_eq_cfn': ('event_type', 'collection_contract', 'move_from', 'nft'),
    '_eq_cft': ('event_type', 'collection_contract', 'move_from', 'move_to'),
    '_eq_cftn': ('event_type', 'collection_contract', 'move_from', 'move_to', 'nft'),
}
TARGET = 'nft_events_native'
BUCKET_SECONDS = 3600


def key_projection():
    return {**{f: 1 for f in EVENT_FIELDS}, **{
        name: {f: '$' + f for f in fields} for name, fields in KEYS.items()}}


def reverse_keys():

    swap = {'move_from': 'move_to', 'move_to': 'move_from'}
    return {key.replace('_eq_', '_rev_'): {f: '$' + swap.get(f, f) for f in fields}
            for key, fields in KEYS.items()}


def time_keys():
    bucket = {'$floor': {'$divide': ['$block_ts', BUCKET_SECONDS]}}
    base = {f: '$' + f for f in KEYS['_eq_cf']}
    rev = {**base, 'move_from': '$move_to'}
    return {'_time_eq_cf': {**base, 'bucket': bucket},
            '_time_rev_cf': [{**rev, 'bucket': bucket},
                             {**rev, 'bucket': {'$add': [bucket, 1]}}]}


def bounded_future(branch, earlier, later):


    reaches = {(str(a), str(b)) for c in branch.constraints if c.get('type') == 'strict_time_order'
               for a,b in zip(c.get('sequence', []), c.get('sequence', [])[1:])}
    while True:
        extended = reaches | {(a,d) for a,b in reaches for c,d in reaches if b == c}
        if extended == reaches: break
        reaches = extended
    if (earlier, later) not in reaches: return False
    return any(c.get('type') == 'within' and str(c.get('earlier')) == earlier
        and 0 < int(c.get('delta_seconds') or 86400) <= BUCKET_SECONDS
        and (str(c.get('later')) == later or (later, str(c.get('later'))) in reaches)
        for c in branch.constraints)


def map_expression(value, local, alias):
    if isinstance(value, dict):
        if '$literal' in value:
            return value
        return {k: map_expression(v, local, alias) for k, v in value.items()}
    if isinstance(value, list):
        return [map_expression(v, local, alias) for v in value]
    if isinstance(value, str):
        if value.startswith('$$'):
            return local[value[2:]]
        if value.startswith('$'):
            return '$' + alias + '.' + value[1:]
    return value


def compile_native(branch, collection=TARGET, use_time_index=False):
    original, ids, names = compile_branch_pipeline(branch)
    output, paths = [], []
    for stage in original:
        if '$lookup' not in stage:
            output.append(stage)
            continue
        lookup = stage['$lookup']
        static = lookup['pipeline'][0]['$match']
        predicates = lookup['pipeline'][1]['$match']['$expr']['$and']
        known = {f: {'$literal': v} for f, v in static.items() if f != 'event_type'}
        for p in predicates:
            eq = p.get('$eq')
            if (eq and len(eq) == 2 and isinstance(eq[0], str)
                    and eq[0].startswith('$') and not eq[0].startswith('$$')
                    and isinstance(eq[1], str) and eq[1].startswith('$$')):
                known.setdefault(eq[0][1:], lookup['let'][eq[1][2:]])
        known['event_type'] = None
        eligible = [(key, fields) for key, fields in KEYS.items() if set(fields) <= set(known)]
        if not eligible:
            fallback = copy.deepcopy(stage)
            fallback['$lookup']['from'] = collection
            output.append(fallback)
            paths.append({'operator': 'correlated_fallback'})
            continue
        key, fields = max(eligible, key=lambda x: len(x[1]))
        tuples = [{f: ({'$literal': typ} if f == 'event_type' else known[f])
                   for f in fields} for typ in static['event_type']['$in']]
        alias = lookup['as']


        local_field = None
        for prior in branch.event_patterns:
            if prior.pattern_id == branch.event_patterns[0].pattern_id:
                prefix = ''
            else:
                prefix = 'j_' + prior.pattern_id + '.'
            if (len(prior.event_types) != 1
                    or list(prior.event_types) != static['event_type']['$in']):
                continue
            for reverse in (False, True):
                swap = {'move_from': 'move_to', 'move_to': 'move_from'} if reverse else {}
                if all(known[f] == '$' + prefix + swap.get(f, f)
                       for f in fields if f != 'event_type'):
                    local_field = prefix + (key.replace('_eq_', '_rev_') if reverse else key)
                    break
            if local_field:
                break
        if local_field is None:
            output.append({'$set': {'_native_lookup_key': tuples}})
            local_field = '_native_lookup_key'
        foreign_key = key


        if use_time_index and key == '_eq_cf' and local_field.endswith('_rev_cf'):
            owner = 'root' if local_field == '_rev_cf' else local_field[2:].split('.')[0]
            current = alias[2:]
            if bounded_future(branch, owner, current):
                local_field = local_field.replace('_rev_cf', '_time_rev_cf')
                foreign_key = '_time_eq_cf'
        output.append({'$lookup': {'from': collection, 'localField': local_field,
                                  'foreignField': foreign_key, 'as': alias}})


        residual = [map_expression(p, lookup['let'], alias) for p in predicates]
        residual += [{'$eq': ['$' + alias + '.' + f, {'$literal': v}]}
                     for f, v in static.items() if f != 'event_type']
        def filter_ref(v):
            if isinstance(v, dict):
                return v if '$literal' in v else {k: filter_ref(x) for k, x in v.items()}
            if isinstance(v, list):
                return [filter_ref(x) for x in v]
            if isinstance(v, str) and v.startswith('$' + alias + '.'):
                return '$$candidate.' + v[len(alias)+2:]
            return v
        output.append({'$set': {alias: {'$filter': {'input': '$' + alias, 'as': 'candidate',
                                                  'cond': {'$and': filter_ref(residual)}}}}})
        paths.append({'operator': 'native_equality', 'key': foreign_key, 'fields': fields,
                      'local_field': local_field})
    return (output, ids, names), paths


def build(db, out):
    if TARGET in db.list_collection_names():
        raise RuntimeError('Refusing to replace existing collection ' + TARGET)
    started = time.perf_counter()
    source = db['nft_events']
    expected = source.count_documents({})

    list(source.aggregate([{'$project': key_projection()}, {'$out': TARGET}],
                          allowDiskUse=True, comment='funnelq_native_build'))
    target = db[TARGET]
    assert target.count_documents({}) == expected
    copy_s = time.perf_counter() - started
    for key in KEYS:
        target.create_index([(key, 1)], name=key)
    target.create_index([('event_type', 1)], name='type')
    report = {'source': source.full_name, 'target': target.full_name, 'rows': expected,
              'copy_s': copy_s, 'total_build_s': time.perf_counter()-started,
              'stats': db.command('collStats', TARGET)}
    (out/'build.json').write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({k: v for k, v in report.items() if k != 'stats'}), flush=True)


def fixture(client, out):
    db = client['funnelq_audit_v2']
    if TARGET not in db.list_collection_names():
        build(db, out)
    db[TARGET].update_many({}, [{'$set': reverse_keys()}])
    db[TARGET].update_many({}, [{'$set': time_keys()}])
    db[TARGET].create_index([('_time_eq_cf', 1)], name='_time_eq_cf')
    oracle_path = Path(__file__).with_name('mongo_fixture_generic_exact.py')
    spec = importlib.util.spec_from_file_location('independent_exact_oracle', oracle_path)
    oracle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oracle)
    events = list(db['nft_events'].find({}, {'_id': 0}))
    reports = []
    for f in sorted((ROOT/'queries/nft/selected5').glob('Q[1-5]_*.json')):
        _, plan = load_full_binding_plan(f)
        base = [compile_branch_pipeline(b) for b in plan.branches]
        native = [compile_native(b, use_time_index=True)[0] for b in plan.branches]
        a, _ = execute_compiled(db['nft_events'], plan, base)
        b, _ = execute_compiled(db[TARGET], plan, native)
        assert canonical_full_result_digest(a) == canonical_full_result_digest(b), f.name
        exact = list(oracle.iter_plan_matches(plan, events))
        assert canonical_full_result_digest(exact) == canonical_full_result_digest(b), f.name

        from dataclasses import replace
        renamed = replace(plan, query_id='UnseenRuntimeQuery')
        assert [compile_native(x, use_time_index=True)[0] for x in renamed.branches] == native
        reports.append({'query': f.name, 'count': len(a), 'full_rows_equal': True,
                        'independent_oracle_equal': True, 'renaming_invariant': True})
    (out/'fixture.json').write_text(json.dumps(reports, indent=2))
    print(json.dumps(reports), flush=True)


def run(client, args, out):
    db = client[args.database]
    _, plan = load_full_binding_plan(Path(args.query))
    pairs = ([compile_native(b, use_time_index=args.time_index) for b in plan.branches] if args.engine == 'native'
             else [(compile_branch_pipeline(b), [{'operator': 'original_correlated'}]) for b in plan.branches])
    collection_name = TARGET if args.engine == 'native' else 'nft_events'
    compiled = [x[0] for x in pairs]
    if args.limit:
        for pipeline, _, _ in compiled:


            ids = [x['_id'] for x in db['nft_events'].find(pipeline[0]['$match'], {'_id': 1}).limit(args.limit)]
            pipeline.insert(1, {'$match': {'_id': {'$in': ids}}})
    (out/'pipeline.json').write_text(json.dumps(compiled, indent=2, default=str))
    for i, (pipeline, _, _) in enumerate(compiled):
        explain = db.command('explain', {'aggregate': collection_name, 'pipeline': pipeline,
                             'cursor': {}, 'allowDiskUse': True}, verbosity='queryPlanner')
        (out/f'explain_{i}.json').write_text(json.dumps(explain, indent=2, default=str))
    start = time.perf_counter()
    class BoundedCollection:
        def aggregate(self, pipeline, **kwargs):
            return db[collection_name].aggregate(pipeline, maxTimeMS=args.timeout_ms,
                comment='funnelq_native_development', **kwargs)
    rows, report = execute_compiled(BoundedCollection(), plan, compiled)
    elapsed = time.perf_counter()-start
    digest, count = canonical_full_result_digest(rows)
    write_normalized_csv(out/'instances.csv', rows)
    report.update(query=plan.query_id, engine=args.engine, query_s=elapsed, full_digest=digest,
                  paths=[x[1] for x in pairs], root_limit=args.limit,
                  environment='shared-load development, not formal timing',
                  sample_id_selection_excluded=bool(args.limit),
                  offline_build_excluded=True, output_write_excluded=True)
    (out/'report.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('action', choices=['fixture', 'build', 'augment', 'augment-time', 'run'])
    p.add_argument('--database', default='funnelq')
    p.add_argument('--out', required=True, type=Path)
    p.add_argument('--query')
    p.add_argument('--limit', type=int, default=0)
    p.add_argument('--engine', choices=['native', 'original'], default='native')
    p.add_argument('--timeout-ms', type=int, default=300000)
    p.add_argument('--time-index', action='store_true')
    args = p.parse_args()
    args.out.mkdir(parents=True, exist_ok=False)
    client = MongoClient('mongodb://mongo:27017', serverSelectionTimeoutMS=15000)
    if args.action == 'fixture': fixture(client, args.out)
    elif args.action == 'build': build(client[args.database], args.out)
    elif args.action in ('augment', 'augment-time'):
        target = client[args.database][TARGET]
        assert target.estimated_document_count() > 0
        start = time.perf_counter()
        r = target.update_many({}, [{'$set': time_keys() if args.action == 'augment-time' else reverse_keys()}])
        if args.action == 'augment-time':
            target.create_index([('_time_eq_cf', 1)], name='_time_eq_cf')
        stats = client[args.database].command('collStats', TARGET)
        report = {'matched': r.matched_count, 'modified': r.modified_count,
                  'seconds': time.perf_counter()-start, 'stats': stats}
        (args.out/'augment.json').write_text(json.dumps(report, indent=2, default=str))
        print(json.dumps({k:v for k,v in report.items() if k != 'stats'}), flush=True)
    else: run(client, args, args.out)
