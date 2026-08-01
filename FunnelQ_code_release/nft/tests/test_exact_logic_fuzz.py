from __future__ import annotations
import random, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from systems.nft.hvceq.query_nft_hvceq_full import _match_selected_cycle
from systems.nft.common.full_binding_spec import load_full_binding_plan

T=ROOT/'systems/nft/hvceq/selected5_templates'
QIDS=[p.stem for p in T.glob('Q*.json')]

def rust_sim(qid, evs):
    root=evs[0]; deadline=root['block_ts']+1800
    out=[]
    for e1 in evs[1:]:
        if e1['event_type']!='NFTTrade' or e1['move_from']!=root['move_to'] or not(root['block_ts']<e1['block_ts']<=deadline) or e1['tx_hash']==root['tx_hash']:
            continue
        if qid.startswith('Q1_'):
            if e1['move_to']==root['move_from'] and e1['nft']==root['nft']:
                out.append((root['order_seq'],None,e1['order_seq']))
            continue
        if qid.startswith('Q2_'):
            if e1['move_to']==root['move_from'] and e1['nft']!=root['nft']:
                out.append((root['order_seq'],None,e1['order_seq']))
            continue
        if qid.startswith('Q3_') and e1['nft']!=root['nft']: continue
        if (qid.startswith('Q4_') or qid.startswith('Q5_')) and e1['nft']==root['nft']: continue
        if e1['move_to']==root['move_from']: continue
        for e2 in evs[1:]:
            if e2['move_from']!=e1['move_to'] or e2['order_seq']<=e1['order_seq']: continue
            if e2['event_type']!='NFTTrade' or not(e1['block_ts']<e2['block_ts']<=deadline) or e2['move_to']!=root['move_from']:
                continue
            if len({root['tx_hash'],e1['tx_hash'],e2['tx_hash']})<3: continue
            ok=(qid.startswith('Q3_') and e2['nft']==root['nft']) or (qid.startswith('Q4_') and len({root['nft'],e1['nft'],e2['nft']})==3) or (qid.startswith('Q5_') and e2['nft']==root['nft'])
            if ok: out.append((root['order_seq'],e1['order_seq'],e2['order_seq']))
    return sorted(set(out))

def old_orders(plan,evs):
    rows=_match_selected_cycle(plan,evs,evs[0])
    import json
    out=[]
    for r in rows:
        d=json.loads(r['event_orders_json'])
        out.append((d['root'],d.get('middle'),d['back']))
    return sorted(set(out))

random.seed(20260731)
addresses=['a','b','c','d']
nfts=['x','y','z']
txs=['t1','t2','t3','t4']
for qid in QIDS:
    _,plan=load_full_binding_plan(T/f'{qid}.json')
    for case in range(5000):
        a,b=random.sample(addresses,2)
        evs=[{'event_id':'e0','event_type':'NFTTrade','nft':random.choice(nfts),'collection_contract':'col','move_from':a,'move_to':b,'block_ts':1000,'order_seq':0,'tx_hash':random.choice(txs)}]
        ts=1000
        for i in range(1,random.randint(2,9)):
            ts+=random.randint(0,700)
            s,d=random.sample(addresses,2)
            evs.append({'event_id':f'e{i}','event_type':random.choice(['NFTTrade','NFTTransfer']),'nft':random.choice(nfts),'collection_contract':'col','move_from':s,'move_to':d,'block_ts':ts,'order_seq':i,'tx_hash':random.choice(txs)})
        x=rust_sim(qid,evs); y=old_orders(plan,evs)
        if x!=y:
            print('MISMATCH',qid,case,x,y,evs); raise SystemExit(1)
    print(qid,'FUZZ_OK')
print('ALL_FUZZ_OK')
