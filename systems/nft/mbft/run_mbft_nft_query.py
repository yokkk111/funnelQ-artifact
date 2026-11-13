#!/usr/bin/env python3
from __future__ import annotations
import argparse,csv,json,subprocess,sys,time
import psutil
from pathlib import Path
from typing import Any

HERE=Path(__file__).resolve().parent
FINAL5=["C1","C2","L3_RETURN","L6_NEG_RETURN_SUPPORT","H5_23"]

def run(cmd,cwd):
    t=time.perf_counter()
    p=subprocess.Popen(cmd,cwd=str(cwd),text=True,encoding="utf-8",errors="replace",stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    peak=0
    while p.poll() is None:
        try:
            proc=psutil.Process(p.pid);rss=proc.memory_info().rss
            for child in proc.children(recursive=True):
                try:rss+=child.memory_info().rss
                except (psutil.NoSuchProcess,psutil.AccessDenied):pass
            peak=max(peak,rss)
        except (psutil.NoSuchProcess,psutil.AccessDenied):pass
        time.sleep(.05)
    stdout,stderr=p.communicate();dt=time.perf_counter()-t
    if p.returncode!=0:raise RuntimeError(f"{' '.join(cmd)}\n{(stdout or '')[-4000:]}\n{(stderr or '')[-4000:]}")
    return {"wall_s":dt,"peak_tree_rss_mb":peak/(1024.0*1024.0)},stdout,stderr
def ensure_exe(exe:str)->Path:
    if exe:
        p=Path(exe).resolve()
        if not p.exists():raise FileNotFoundError(p)
        return p
    build=HERE/"build";build.mkdir(exist_ok=True)
    run(["cmake","-S",str(HERE/"mbft"),"-B",str(build),"-DCMAKE_BUILD_TYPE=Release"],HERE)
    run(["cmake","--build",str(build),"--config","Release","--target","mbft_nft_adapt","-j","2"],HERE)
    for p in [build/"mbft_nft_adapt",build/"Release"/"mbft_nft_adapt.exe",build/"mbft_nft_adapt.exe"]:
        if p.exists():return p
    raise FileNotFoundError("mbft_nft_adapt executable")
def write_csv(path,rows):
    fields=[]
    for r in rows:
        for k in r:
            if k not in fields:fields.append(k)
    with Path(path).open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)
def main():
    ap=argparse.ArgumentParser(description="MBFT-NFT query-only: original MergeBloom retrieval plus server-side closure matcher.")
    ap.add_argument("--events",required=True);ap.add_argument("--outdir",required=True);ap.add_argument("--exe",default="")
    ap.add_argument("--queries",default=",".join(FINAL5));ap.add_argument("--combine",type=int,default=0)
    ap.add_argument("--fp-rate",type=float,default=.001);ap.add_argument("--root-limit",type=int,default=0)
    args=ap.parse_args();out=Path(args.outdir).resolve();out.mkdir(parents=True,exist_ok=True)
    exe=ensure_exe(args.exe);atomic=out/"atomic"
    atomic_proc,so,se=run([str(exe),"query-only","--events",str(Path(args.events).resolve()),"--outdir",str(atomic),
                    "--combine",str(args.combine),"--fp-rate",str(args.fp_rate)],HERE)
    (out/"atomic.stdout.txt").write_text(so,encoding="utf-8");(out/"atomic.stderr.txt").write_text(se,encoding="utf-8")
    summ=json.loads((atomic/"atomic_selection_summary.json").read_text())
    qids=[q.strip().upper() for q in args.queries.split(",") if q.strip()]
    match=out/"server_match"
    cmd=[sys.executable,str(HERE/"nft_semantic_matcher.py"),"--events-jsonl",
         str(atomic/"server_candidates"/"nfttrade.jsonl"),str(atomic/"server_candidates"/"nfttransfer.jsonl"),
         "--query-dir",str(HERE/"queries"),"--queries",",".join(qids),"--outdir",str(match)]
    if args.root_limit:cmd+=["--root-limit",str(args.root_limit)]
    matcher_proc,mso,mse=run(cmd,HERE)
    (out/"matcher.stdout.txt").write_text(mso,encoding="utf-8");(out/"matcher.stderr.txt").write_text(mse,encoding="utf-8")
    mr=json.loads((match/"matcher_report.json").read_text())
    atomic_s=sum(float(v.get("query_time_us",0))/1e6 for v in summ["query_only"].values())
    candidate_write_s=sum(float(v.get("candidate_write_time_us",0))/1e6 for v in summ["query_only"].values())
    candidate_load_s=float(mr.get("candidate_load_s") or 0.0)
    matcher_prepare_s=float(mr.get("prepare_s") or 0.0)
    rows=[]
    for r in mr["queries"]:
        semantic_s=float(r["match_s"])+float(r.get("result_canonicalize_s",0.0))
        rows.append({"system":"MBFT-NFT-Query","query":r["query"],"result_semantics":"full_event_binding_v1",
                     "result_count":r["result_count"],"result_digest_sha256":r["result_digest_sha256"],
                     "instances_csv":r["instances_csv"],
                     "atomic_retrieval_s":atomic_s,"candidate_jsonl_write_s":candidate_write_s,
                     "candidate_jsonl_load_s":candidate_load_s,
                     "server_candidate_prepare_s":matcher_prepare_s,
                     "server_match_s":semantic_s,"query_only_s":atomic_s+candidate_write_s+candidate_load_s+matcher_prepare_s+semantic_s,
                     "query_with_adapter_setup_s":atomic_s+candidate_write_s+candidate_load_s+matcher_prepare_s+semantic_s,
                     "candidate_events":mr["event_count"],"candidate_root_closures":mr["root_count"],
                     "index_build_s":float(summ["build"]["mbft_build_time_us"])/1e6,
                     "index_bytes":summ["build"]["total_ads_bytes"],"hash":"original MurmurHash3_x86_32",
                     "vo_enabled":False,"atomic_process_peak_tree_rss_mb":atomic_proc["peak_tree_rss_mb"],
                     "matcher_process_peak_tree_rss_mb":matcher_proc["peak_tree_rss_mb"],
                     "pipeline_peak_tree_rss_mb":max(atomic_proc["peak_tree_rss_mb"],matcher_proc["peak_tree_rss_mb"])})
    write_csv(out/"mbft_nft_query_summary.csv",rows)
    report={"mode":"MBFT_NFT_query_only_full_binding_v2","atomic_process_wall_s":atomic_proc["wall_s"],
            "atomic_process_peak_tree_rss_mb":atomic_proc["peak_tree_rss_mb"],
            "matcher_process_wall_s":matcher_proc["wall_s"],"matcher_process_peak_tree_rss_mb":matcher_proc["peak_tree_rss_mb"],
            "rows":rows,"source_adapter":"original MBFT MergeBloom tree; VO disabled; server-side independent full-binding NFT matcher"}
    (out/"mbft_nft_query_report.json").write_text(json.dumps(report,indent=2),encoding="utf-8")
    print(json.dumps(report,indent=2));return 0
if __name__=="__main__":raise SystemExit(main())
