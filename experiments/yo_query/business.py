import csv
import hashlib
import json
import math
from collections import defaultdict

SPEC = {
    'version': 'yo_structural_v1', 'scope': 'same_tx',
    'minimum_aggregator_usd': 100000, 'minimum_swaps': 3,
    'minimum_pools': 2, 'minimum_dexes': 2, 'minimum_chain_length': 3,
    'chain_order': 'evt_index,source_order,source_row',
    'chain_tie': 'first predecessor and first terminal in canonical order',
    'transfer_support': 'aggregator tokens UNION selected longest-chain tokens',
    'output': 'sorted unique transaction hashes', 'loss_filter': False,
}

def validate(q):

    if q != SPEC:
        raise ValueError('Unsupported YO QuerySpec; rebuild the adapter for changed semantics')

def norm(x):
    x = (x or '').strip().lower()
    return '' if x in ('', '<nil>', 'none', 'null', 'nan') else x

def num(x):
    v = float(x) if norm(x) else 0.0
    if not math.isfinite(v):
        raise ValueError('Non-finite source number')
    return v

def load_source(path):
    groups = defaultdict(list)
    with open(path, encoding='utf-8-sig', newline='') as f:
        for i, raw in enumerate(csv.DictReader(f), 1):
            if None in raw or any(v is None for v in raw.values()):
                raise ValueError('Malformed source row')
            tx = norm(raw['closure_tx_hash'])
            if len(tx) != 66 or norm(raw['tx_hash']) != tx:
                raise ValueError('Source transaction disagreement')
            groups[tx].append(dict(eid=i, tx=tx, kind=norm(raw['event_type']),
                evt=int(num(raw['evt_index'])), source_order=num(raw['source_order']),
                src=norm(raw['token_in_address']), dst=norm(raw['token_out_address']),
                pool=norm(raw['project_contract_address']), dex=norm(raw['project']),
                amount=num(raw['amount_usd']), token=norm(raw['transfer_token_address']),
                block=int(num(raw['block_number'])), time=raw['closure_block_time']))
    for tx, rows in groups.items():
        rows.sort(key=lambda r: (r['evt'], r['source_order'], r['eid']))
        for seq, r in enumerate(rows):
            r['seq'] = seq
    return dict(groups)

def finish(rows, q):


    aggs = [r for r in rows if r['kind'] == 'aggregator_intent']
    swaps = sorted((r for r in rows if r['kind'] == 'pool_swap'), key=lambda r:r['seq'])
    if max((r['amount'] for r in aggs), default=0) < q['minimum_aggregator_usd']:
        return False
    if len(swaps) < q['minimum_swaps']:
        return False
    if len({r['pool'] for r in swaps} - {''}) < q['minimum_pools']:
        return False
    if len({r['dex'] for r in swaps} - {''}) < q['minimum_dexes']:
        return False
    best_token, depth, prev = {}, [], []
    terminal = -1
    for j, r in enumerate(swaps):
        parent = best_token.get(r['src'], -1) if r['src'] else -1
        depth.append(1 if parent < 0 else depth[parent] + 1)
        prev.append(parent)
        if terminal < 0 or depth[j] > depth[terminal]:
            terminal = j
        old = best_token.get(r['dst'], -1)
        if r['dst'] and (old < 0 or depth[j] > depth[old]):
            best_token[r['dst']] = j
    if terminal < 0 or depth[terminal] < q['minimum_chain_length']:
        return False
    tokens = {r[k] for r in aggs for k in ('src','dst')} - {''}
    while terminal >= 0:
        tokens.update((swaps[terminal]['src'], swaps[terminal]['dst']))
        terminal = prev[terminal]
    tokens.discard('')
    return any(r['kind']=='token_transfer' and r['token'] in tokens for r in rows)

def canonical_result(groups, candidate_ids, q):
    validate(q)
    return sorted(tx for tx in set(candidate_ids) if finish(groups[tx], q))

def motif():


    return dict(query_name='YO_required_three_swap_path', scope='same_tx', events=[
        {'id':'s1','type':'pool_swap','roles':{'token_bought':'$x'}},
        {'id':'s2','type':'pool_swap','roles':{'token_sold':'$x','token_bought':'$y'}},
        {'id':'s3','type':'pool_swap','roles':{'token_sold':'$y'}},
    ], order=[['s1','s2'],['s2','s3']], attr_filters=[], neq_constraints=[])

def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
