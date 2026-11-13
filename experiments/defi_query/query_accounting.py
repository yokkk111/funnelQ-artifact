import argparse,hashlib,json,sys,types
from pathlib import Path

def replace_once(text,old,new):
    if text.count(old)!=1:raise ValueError(('source drift',old,text.count(old)))
    return text.replace(old,new)

def instrument_funnelq(text,mode):

    text=replace_once(text,'    query = load_query'+('_json(args.query_json)' if mode=='same_tx' else '(Path(args.query_json))'),
                      '    request_start = time.perf_counter()\n    query = load_query'+('_json(args.query_json)' if mode=='same_tx' else '(Path(args.query_json))'))
    if mode=='episode':
        text=replace_once(text,'    acc = EpisodeAccessor(rt, value_to_id)','    acc = EpisodeAccessor(rt, value_to_id)\n    setup1 = time.perf_counter()')
    text=replace_once(text,'"query_latency_s": result["query_latency_s"],',
        '"query_kernel_s": result["query_latency_s"],\n            "query_dependent_input_prepare_s": setup1-request_start,\n            "query_latency_s": setup1-request_start+result["query_latency_s"],\n            "query_timeout_exceeded": setup1-request_start+result["query_latency_s"] > 600,')
    return text

def instrument_matcher(text):
    text=replace_once(text,'    seen: set[str] = set()','    seen: dict[str, Event] = {}')
    text=replace_once(text,'                if event.event_id in seen:\n                    continue\n                seen.add(event.event_id)',
        '                if event.event_id in seen:\n                    if seen[event.event_id] != event:\n                        raise ValueError(f"conflicting event_id at {path}:{line_no}")\n                    continue\n                seen[event.event_id] = event')
    text=replace_once(text,'        "client_match_total_ms": (t3 - t0) * 1000.0,',
        '        "client_match_total_ms": ((t1-t0)+(t2-t1)+(t3-t_query))*1000.0,\n        "query_input_load_and_prepare_included": True,')
    return text

def instrument_sql(text,mode):
    text=replace_once(text,'    query = load_query(args.query_json)',
                      '    request_start = time.perf_counter()\n    query = load_query(args.query_json)')
    text=replace_once(text,'        for _ in range(warmup_runs):',
                      '        query_prepare_s = time.perf_counter()-request_start\n        for _ in range(warmup_runs):')
    text=replace_once(text,'    report = {','    report = {\n        "query_compile_prepare_s": query_prepare_s,')
    return text

def mbft_components(atomic,result,kinds):
    fields={'atomic_retrieval_s':'mbft_tree_query_time_us','candidate_write_s':'candidate_write_time_us',
            'candidate_merge_s':'candidate_merge_time_us'}
    v={name:sum(float(atomic['selections'][k][source]) for k in kinds)/1e6 for name,source in fields.items()}
    v.update(candidate_load_s=result['client_candidate_load_ms']/1000,
             adapter_prepare_s=result['client_temporary_index_build_ms']/1000,
             semantic_match_s=result['client_complex_match_ms']/1000)
    total=sum(v.values())
    return {'components_seconds':v,'query_seconds':total,'query_timeout_exceeded':total>600}

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--kind',choices=['funnelq','mbft','sql'],required=True)
    ap.add_argument('--mode',choices=['same_tx','episode']);ap.add_argument('--source',type=Path,required=True)
    args,extra=ap.parse_known_args()
    if extra and extra[0]=='--':extra=extra[1:]
    source=args.source.read_text(encoding='utf-8')
    transformed=instrument_funnelq(source,args.mode) if args.kind=='funnelq' else instrument_sql(source,args.mode) if args.kind=='sql' else instrument_matcher(source)
    mod=types.ModuleType('instrumented_query');mod.__file__=str(args.source);sys.modules[mod.__name__]=mod
    sys.path.insert(0,str(args.source.parent));sys.argv=[str(args.source)]+extra
    exec(compile(transformed,str(args.source),'exec'),mod.__dict__)
    mod.main()

if __name__=='__main__':main()
