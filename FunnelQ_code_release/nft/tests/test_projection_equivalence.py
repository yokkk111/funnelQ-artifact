from __future__ import annotations
import json, sqlite3, sys, tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from systems.nft.hvceq.query_nft_hvceq_full import (
    _load_matched_event_projection,
    _materialize_selected_exact_matches,
    _match_selected_cycle,
)
from systems.nft.common.full_binding_spec import load_full_binding_plan
from systems.nft.common.canonical_results import canonicalize_rows

T=ROOT/'systems/nft/hvceq/selected5_templates'
ADDR={1:'0xa',2:'0xb',3:'0xc'}
NFT={1:'0xcol:1',2:'0xcol:2',3:'0xcol:3'}
COL={1:'0xcol'}

def make_db(events):
    handle=tempfile.NamedTemporaryFile(suffix='.db', delete=False)
    p=Path(handle.name)
    handle.close()
    c=sqlite3.connect(p)
    c.executescript('''
    CREATE TABLE nft_role_transition_edges(
      order_seq INTEGER PRIMARY KEY,event_id TEXT,event_type_id INTEGER,block_ts INTEGER,
      src_addr_id INTEGER,dst_addr_id INTEGER,nft_id INTEGER,collection_id INTEGER);
    CREATE TABLE nft_role_value_dict(value_kind TEXT,value_id INTEGER,value_text TEXT,
      PRIMARY KEY(value_kind,value_id)) WITHOUT ROWID;
    ''')
    c.executemany('INSERT INTO nft_role_value_dict VALUES (?,?,?)',
      [('address',k,v) for k,v in ADDR.items()]+[('nft',k,v) for k,v in NFT.items()]+[('collection',k,v) for k,v in COL.items()])
    c.executemany('INSERT INTO nft_role_transition_edges VALUES (?,?,?,?,?,?,?,?)', events)
    c.commit(); return p,c

def old_event(row,tx):
    order,eid,_,ts,src,dst,nft,col=row
    return {'event_id':eid,'event_type':'NFTTrade','nft':NFT[nft],
      'collection_contract':COL[col],'move_from':ADDR[src],'move_to':ADDR[dst],
      'block_ts':ts,'order_seq':order,'tx_hash':tx}

cases = {
 'Q1_RAPID_SAME_NFT_ROUNDTRIP': [
   (10,'e10',1,1000,1,2,1,1),(11,'e11',1,1010,2,1,1,1)],
 'Q2_CROSS_NFT_RECIPROCAL': [
   (20,'e20',1,2000,1,2,1,1),(21,'e21',1,2010,2,1,2,1)],
 'Q3_SAME_NFT_THREE_ACTOR_CYCLE': [
   (30,'e30',1,3000,1,2,1,1),(31,'e31',1,3010,2,3,1,1),(32,'e32',1,3020,3,1,1,1)],
 'Q4_THREE_NFT_THREE_ACTOR_CYCLE': [
   (40,'e40',1,4000,1,2,1,1),(41,'e41',1,4010,2,3,2,1),(42,'e42',1,4020,3,1,3,1)],
 'Q5_TWO_NFT_RETURN_CYCLE': [
   (50,'e50',1,5000,1,2,1,1),(51,'e51',1,5010,2,3,2,1),(52,'e52',1,5020,3,1,1,1)],
}
for qid, rows in cases.items():
    path=T/f'{qid}.json'
    _,plan=load_full_binding_plan(path)
    db,conn=make_db(rows)
    try:
        match=(rows[0][0], None if len(rows)==2 else rows[1][0], rows[-1][0])
        events,values=_load_matched_event_projection(conn,[match])
        new=_materialize_selected_exact_matches(plan,[match],events,values)
        old_events=[old_event(r,f'tx{i}') for i,r in enumerate(rows)]
        old=_match_selected_cycle(plan,old_events,old_events[0])
        n1,m1=canonicalize_rows(new); n2,m2=canonicalize_rows(old)
        assert n1==n2,(qid,n1,n2)
        assert m1['result_digest_sha256']==m2['result_digest_sha256']
        print(qid,'OK',m1['result_digest_sha256'][:12])
    finally:
        conn.close(); db.unlink(missing_ok=True)
print('ALL_OK')
