#!/usr/bin/env python3
from __future__ import annotations
import argparse, bisect, csv, importlib.util, json, sys, time
from collections import defaultdict
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
COMMON_PATH = HERE / "vendor" / "funnelq_verifier_common.py"
spec = importlib.util.spec_from_file_location("funnelq_verifier_common", COMMON_PATH)
mod = importlib.util.module_from_spec(spec); assert spec and spec.loader
sys.modules[spec.name] = mod; spec.loader.exec_module(mod)
NftLocalVerifier = mod.NftLocalVerifier

FINAL5 = ["C1","C2","L3_RETURN","L6_NEG_RETURN_SUPPORT","H5_23"]
ZERO = "0x0000000000000000000000000000000000000000"

def load_jsonl(paths: list[Path]) -> list[dict[str,Any]]:
    by_id: dict[str,dict[str,Any]] = {}
    for path in paths:
        with path.open("r",encoding="utf-8") as f:
            for line in f:
                if not line.strip(): continue
                obj=json.loads(line)
                by_id[str(obj.get("event_id") or json.dumps(obj,sort_keys=True))]=obj
    rows=list(by_id.values())
    rows.sort(key=lambda x:(int(x.get("order_seq") or 0),str(x.get("event_id") or "")))
    return rows

def norm(s: Any)->str:
    return str(s or "").strip().lower()

def build_edges(events:list[dict[str,Any]]):
    addresses=sorted({norm(e.get("move_from")) for e in events}|{norm(e.get("move_to")) for e in events} - {"",ZERO})
    addr_id={v:i+1 for i,v in enumerate(addresses)}
    nfts=sorted({norm(e.get("nft")) for e in events if norm(e.get("nft"))})
    nft_id={v:i+1 for i,v in enumerate(nfts)}
    colls=sorted({norm(e.get("collection_contract")) for e in events if norm(e.get("collection_contract"))})
    coll_id={v:i+1 for i,v in enumerate(colls)}
    et_map={"nfttrade":1,"nfttransfer":2}
    edges=[]
    for e in events:
        et=norm(e.get("event_type"))
        src=norm(e.get("move_from")); dst=norm(e.get("move_to"))
        edges.append({
            "closure_id":0,
            "edge_id":int(e.get("order_seq") or 0)+1,
            "order_seq":int(e.get("order_seq") or 0),
            "event_type_id":et_map.get(et,0),
            "src_addr_id":0 if src in ("",ZERO) else addr_id[src],
            "dst_addr_id":0 if dst in ("",ZERO) else addr_id[dst],
            "nft_id":nft_id.get(norm(e.get("nft")),0),
            "collection_id":coll_id.get(norm(e.get("collection_contract")),0),
            "block_ts":int(e.get("block_time_epoch") or 0),
            "event_id":str(e.get("event_id") or ""),
        })
    dictionary={
        "dictionaryVersion":"nft_dictionary_v1",
        "eventTypes":[{"event_type_id":1,"event_type":"NFTTrade"},{"event_type_id":2,"event_type":"NFTTransfer"}],
        "addressCanon":[{"addr_id":i,"value_text":v} for v,i in sorted(addr_id.items(),key=lambda x:x[1])],
        "roleValues":[],
    }
    return edges,dictionary

def evidence_key(row:dict[str,Any])->tuple[int,int,str]:
    return (int(row["closure_id"]),int(row["anchor_order"]),str(row["evidence_kind"]))

def write_rows(path:Path,rows:list[dict[str,Any]]):
    fields=["query_id","closure_id","anchor_order","evidence_kind","evidence_orders","evidence_json"]
    path.parent.mkdir(parents=True,exist_ok=True)
    with path.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)

def read_rows(path:Path)->list[dict[str,str]]:
    with path.open("r",newline="",encoding="utf-8") as f:return list(csv.DictReader(f))

def load_query(query_dir:Path,qid:str)->dict[str,Any]:
    return json.loads((query_dir/f"{qid}.json").read_text(encoding="utf-8"))

def run_match(events:list[dict[str,Any]],query_dir:Path,qids:list[str],outdir:Path,root_limit:int=0):
    tprep=time.perf_counter()
    edges,dictionary=build_edges(events)
    verifier=NftLocalVerifier(None,committed_payload_only=True,dictionary_snapshot=dictionary)
    by_coll:dict[int,list[dict[str,Any]]]=defaultdict(list)
    for e in edges: by_coll[int(e["collection_id"])].append(e)
    orders={c:[int(e["order_seq"]) for e in es] for c,es in by_coll.items()}
    roots=[e for e in edges if int(e["src_addr_id"])!=0 and int(e["dst_addr_id"])!=0]
    if root_limit>0: roots=roots[:root_limit]
    prep_s=time.perf_counter()-tprep
    reports=[]
    for qid in qids:
        query=load_query(query_dir,qid)
        label=str((query.get("local_plan") or {}).get("output_label") or query.get("query_name") or qid)
        t0=time.perf_counter(); out=[]
        windows=0; local_edges=0
        base=query.get("base_min_counts") or {}
        for root in roots:
            coll=int(root["collection_id"]); es=by_coll.get(coll,[]); os_=orders.get(coll,[])
            i=bisect.bisect_left(os_,int(root["order_seq"]))
            end_ts=int(root["block_ts"])+86400
            window=[]
            for e in es[i:]:
                if int(e["block_ts"])>end_ts and int(e["order_seq"])>int(root["order_seq"]):break
                window.append(e)
            windows+=1; local_edges+=len(window)
            if base:
                if len(window)<int(base.get("transition_count") or 0): continue
                if int(base.get("addr_count") or 0):
                    addrs={int(e["src_addr_id"]) for e in window if int(e["src_addr_id"])!=0}|{int(e["dst_addr_id"]) for e in window if int(e["dst_addr_id"])!=0}
                    if len(addrs)<int(base["addr_count"]):continue
                if int(base.get("nft_count") or 0):
                    ns={int(e["nft_id"]) for e in window if int(e["nft_id"])!=0}
                    if len(ns)<int(base["nft_count"]):continue
            ok,info=verifier.verify_with_edges(query,int(root["order_seq"])+1,int(root["order_seq"]),window,end_ts=end_ts)
            if not ok: continue
            witness_orders=[int(x) for x in (info.get("orders") or [])]
            payload={"schema":"mbft_nft_compressed_evidence_v1","query_id":qid,
                     "closure_id":int(root["order_seq"])+1,"anchor_order":int(root["order_seq"]),
                     "evidence_kind":label,"operator":info.get("operator"),"witness":info}
            out.append({"query_id":qid,"closure_id":int(root["order_seq"])+1,
                        "anchor_order":int(root["order_seq"]),"evidence_kind":label,
                        "evidence_orders":",".join(str(x) for x in witness_orders),
                        "evidence_json":json.dumps(payload,ensure_ascii=False,sort_keys=True,separators=(",",":"))})

        dedup={evidence_key(r):r for r in out}
        rows=[dedup[k] for k in sorted(dedup)]
        qdir=outdir/qid
        write_rows(qdir/"compressed_evidence.csv",rows)
        dt=time.perf_counter()-t0
        rep={"query":qid,"result_rows":len(rows),"query_s":dt,"root_closures_examined":len(roots),
             "windows_examined":windows,"local_edges_examined":local_edges,
             "evidence_csv":str(qdir/"compressed_evidence.csv")}
        (qdir/"match_report.json").write_text(json.dumps(rep,indent=2),encoding="utf-8")
        reports.append(rep)
    report={"mode":"MBFT_NFT_neutral_closure_matcher_v1","event_count":len(events),
            "in_scope_root_closures":len(roots),"prepare_s":prep_s,"queries":reports}
    (outdir/"matcher_report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
    return report

def compare_claims(claim_dir:Path,recompute_dir:Path,qids:list[str]):
    rows=[];all_pass=True
    for q in qids:
        a=read_rows(claim_dir/q/"compressed_evidence.csv");b=read_rows(recompute_dir/q/"compressed_evidence.csv")
        ka=[evidence_key(x) for x in a];kb=[evidence_key(x) for x in b]
        dup_a=len(ka)!=len(set(ka));dup_b=len(kb)!=len(set(kb))
        sa=set(ka);sb=set(kb)
        ok=not dup_a and not dup_b and sa==sb
        all_pass &= ok
        rows.append({"query":q,"claimed_rows":len(a),"recomputed_rows":len(b),
                     "missing_rows":len(sb-sa),"unexpected_rows":len(sa-sb),
                     "duplicate_claims":dup_a,"duplicate_recomputed":dup_b,"all_pass":ok})
    return all_pass,rows

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events-jsonl",nargs="+",required=True)
    ap.add_argument("--query-dir",required=True)
    ap.add_argument("--queries",default=",".join(FINAL5))
    ap.add_argument("--outdir",required=True)
    ap.add_argument("--root-limit",type=int,default=0)
    ap.add_argument("--compare-claimed-dir",default="")
    args=ap.parse_args()
    qids=[q.strip().upper() for q in args.queries.split(",") if q.strip()]
    out=Path(args.outdir).resolve();out.mkdir(parents=True,exist_ok=True)
    events=load_jsonl([Path(x).resolve() for x in args.events_jsonl])
    rep=run_match(events,Path(args.query_dir).resolve(),qids,out,args.root_limit)
    if args.compare_claimed_dir:
        ok,cmp=compare_claims(Path(args.compare_claimed_dir).resolve(),out,qids)
        rep["comparison"]=cmp;rep["all_pass"]=ok
        (out/"matcher_report.json").write_text(json.dumps(rep,indent=2),encoding="utf-8")
    print(json.dumps(rep,indent=2))
    return 0 if rep.get("all_pass",True) else 2
if __name__=="__main__":raise SystemExit(main())
