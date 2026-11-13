import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path

TARGET = '0x6aff59e800dc219ff0d1614b3dc512e7a07159197b2a6a26969a9ca25c3e33b4'
NULLS = {'', '<nil>', 'null', 'none', 'nan'}

def text(v):
    v = (v or '').strip().lower()
    return '' if v in NULLS else v

def number(v):
    return float(v) if text(v) else 0.0

def historical_screen(rows):


    aggs = [r for r in rows if r['event_type'] == 'aggregator_intent']
    swaps = sorted((r for r in rows if r['event_type'] == 'pool_swap'),
                   key=lambda r: (number(r['evt_index']), number(r['source_order'])))
    transfers = [r for r in rows if r['event_type'] == 'token_transfer']
    if max((number(r['amount_usd']) for r in aggs), default=0) < 100000:
        return False
    if len(swaps) < 3 or len({text(r['project_contract_address']) for r in swaps if text(r['project_contract_address'])}) < 2:
        return False
    if len({text(r['project']) for r in swaps if text(r['project'])}) < 2:
        return False
    dp = [1] * len(swaps)
    prev = [-1] * len(swaps)
    for j, b in enumerate(swaps):
        for i in range(j):
            a = swaps[i]
            if text(a['token_out_address']) and text(a['token_out_address']) == text(b['token_in_address']) and dp[i] + 1 > dp[j]:
                dp[j], prev[j] = dp[i] + 1, i
    best = max(range(len(swaps)), key=dp.__getitem__)
    if dp[best] < 3:
        return False
    chain = []
    while best >= 0:
        chain.append(swaps[best]); best = prev[best]
    tokens = {text(r[k]) for r in aggs + chain for k in ('token_in_address', 'token_out_address')} - {''}
    return bool(tokens & {text(r['transfer_token_address']) for r in transfers} - {''})

def audit(source, reference):
    groups = collections.defaultdict(list)
    types = collections.Counter()
    dates = collections.Counter()
    identities = collections.Counter()
    malformed = 0
    with source.open(encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        for r in reader:
            if None in r or any(v is None for v in r.values()):
                malformed += 1
            tx = text(r['closure_tx_hash'])
            groups[tx].append(r)
            types[r['event_type']] += 1
            dates[r['closure_block_time'][:10]] += 1
            identities[(tx, r['source_order'])] += 1
    chosen = {tx for tx, rows in groups.items() if historical_screen(rows)}
    with reference.open(encoding='utf-8-sig', newline='') as f:
        old = {text(r['tx_hash']) for r in csv.DictReader(f)}
    return {
        'source': str(source.resolve()), 'bytes': source.stat().st_size,
        'sha256': hashlib.file_digest(source.open('rb'), 'sha256').hexdigest(),
        'columns': header, 'rows': sum(types.values()), 'event_types': types,
        'transaction_count': len(groups), 'date_counts': dates,
        'malformed_rows': malformed, 'duplicate_tx_source_order': sum(n-1 for n in identities.values() if n>1),
        'target_present': TARGET in groups, 'target_selected': TARGET in chosen,
        'reconstructed_structural_count': len(chosen), 'historical_reference_count': len(old),
        'missing_reference_transactions_in_input': sorted(old - groups.keys()),
        'missing_vs_reference': sorted(old-chosen), 'extra_vs_reference': sorted(chosen-old),
        'selected_transactions': sorted(chosen),
        'warning': 'Source and semantic preflight only; no performance results. Old snapshot hash unavailable.',
    }

if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--reference', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    result = audit(a.source, a.reference)
    a.output.parent.mkdir(parents=True, exist_ok=True)
    with a.output.open('x', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(json.dumps({k:v for k,v in result.items() if k not in ('selected_transactions', 'missing_vs_reference', 'extra_vs_reference','missing_reference_transactions_in_input')}, ensure_ascii=False))
    print('missing',len(result['missing_vs_reference']),'extra',len(result['extra_vs_reference']))
