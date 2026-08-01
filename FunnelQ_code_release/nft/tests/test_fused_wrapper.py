from __future__ import annotations
import json, os, sqlite3, sys, tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from systems.nft.hvceq.query_nft_hvceq_full import run_query

work=Path(tempfile.mkdtemp())
db=work/'main.db'; tx=work/'tx.db'; out=work/'out'
with sqlite3.connect(db) as c:
    c.executescript('''
    CREATE TABLE nft_role_transition_edges(order_seq INTEGER PRIMARY KEY,event_id TEXT,event_type_id INTEGER,block_ts INTEGER,src_addr_id INTEGER,dst_addr_id INTEGER,nft_id INTEGER,collection_id INTEGER);
    CREATE TABLE nft_role_value_dict(value_kind TEXT,value_id INTEGER,value_text TEXT,PRIMARY KEY(value_kind,value_id)) WITHOUT ROWID;
    ''')
    c.executemany('INSERT INTO nft_role_transition_edges VALUES (?,?,?,?,?,?,?,?)',[
      (10,'e10',1,1000,1,2,1,1),(11,'e11',1,1010,2,1,1,1)])
    c.executemany('INSERT INTO nft_role_value_dict VALUES (?,?,?)',[
      ('address',1,'0xa'),('address',2,'0xb'),('nft',1,'0xcol:1'),('collection',1,'0xcol')])
with sqlite3.connect(tx) as c:
    c.execute('CREATE TABLE nft_event_tx_ids(order_seq INTEGER PRIMARY KEY,tx_id INTEGER)')
    c.executemany('INSERT INTO nft_event_tx_ids VALUES (?,?)',[(10,1),(11,2)])

fake=work/'fake_executor.py'
fake.write_text(r'''#!/usr/bin/env python3
import argparse,csv,json
from pathlib import Path
p=argparse.ArgumentParser()
for name in ['db','query','query-json','tx-sidecar-db','out-csv','report-json','mode','ablation','limit','anchor-limit','threads','warmup-runs','phase-marker']:
    p.add_argument('--'+name,default='')
a=p.parse_args()
if a.phase_marker:
    Path(a.phase_marker).parent.mkdir(parents=True,exist_ok=True);Path(a.phase_marker).write_text('query\n')
with open(a.out_csv,'w',newline='') as f:
    w=csv.writer(f);w.writerow(['root_order','middle_order','back_order']);w.writerow([10,'',11])
r={'query':a.query,'ablation':a.ablation,'output_mode':'query','rows':1,'exact_rows_before_dedup':1,'exact_duplicate_rows_removed':0,'closure_universe':2,'cand0_anchor_hits':2,'cand1_base_hits':2,'cand2_refined_hits':1,'candidate_count':1,'closure_count':1,'local_edges_examined':1,'edge_rows_loaded':2,'query_s':0.01,'local_exact_s':0.02,'csv_write_s':0.003,'exact_engine':'refined_posting_fused','uses_refined_posting':True,'anchor_engine':'anchor_directory','uses_anchor_directory':True,'anchor_engine_isolation_contract_ok':True,'search_only_direct_timer':True,'search_only_s':0.03}
Path(a.report_json).write_text(json.dumps(r))
''')
os.chmod(fake,0o755)
q=ROOT/'systems/nft/hvceq/selected5_templates/Q1_RAPID_SAME_NFT_ROUNDTRIP.json'
exe=[sys.executable, str(fake)] if os.name == 'nt' else fake
r=run_query(db,q,out,exe=exe,tx_sidecar_db=tx,warmup_runs=1)
assert r['result_count']==1,r
assert r['duplicate_count']==0
assert r['timing']['closure_reloading_eliminated'] is True
parts=['candidate_query_s','candidate_csv_write_s','candidate_csv_read_s','closure_input_load_s','exact_match_s','result_projection_load_s','result_materialize_s','result_canonicalize_s']
assert abs(sum(r['timing'][x] for x in parts)-r['timing']['query_latency_s'])<1e-9
print('WRAPPER_OK',r['result_digest_sha256'],r['timing']['query_latency_s'])
