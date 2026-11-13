import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from business import load_source, SPEC

def prepare(source, out):
    out.mkdir(parents=True, exist_ok=False)
    groups = load_source(source)
    with (out/'groups.json').open('x') as f:
        json.dump(groups,f,separators=(',',':'))
    with (out/'query.json').open('x') as f: json.dump(SPEC,f,indent=2)
    rows = [r for tx in sorted(groups) for r in groups[tx]]
    db = sqlite3.connect(out/'events.db')
    db.execute('CREATE TABLE events(eid INTEGER PRIMARY KEY,tx TEXT,kind TEXT,seq INTEGER,src TEXT,dst TEXT,pool TEXT,dex TEXT,amount REAL,payload TEXT)')
    db.executemany('INSERT INTO events VALUES(?,?,?,?,?,?,?,?,?,?)',
        [(r['eid'],r['tx'],r['kind'],r['seq'],r['src'],r['dst'],r['pool'],r['dex'],r['amount'],json.dumps(r,separators=(',',':'))) for r in rows])
    for columns in ('tx,kind,seq','kind,tx,src,seq','kind,amount,tx'):
        db.execute('CREATE INDEX e_'+columns.replace(',','_')+' ON events('+columns+')')
    db.execute('ANALYZE');db.commit();db.close()

    db = sqlite3.connect(out/'funnelq.db')
    db.execute('CREATE TABLE event_type_dict(event_type_id INTEGER,event_type TEXT)')
    db.execute('CREATE TABLE value_dict(value_id INTEGER,value_text TEXT)')
    types={v:i+1 for i,v in enumerate(sorted({r['kind'] for r in rows}))}
    values={v:i+1 for i,v in enumerate(sorted({r[k] for r in rows for k in ('src','dst','pool','dex')} - {''}))}
    txids={v:i+1 for i,v in enumerate(sorted(groups))}
    db.executemany('INSERT INTO event_type_dict VALUES(?,?)',[(v,k) for k,v in types.items()])
    db.executemany('INSERT INTO value_dict VALUES(?,?)',[(v,k) for k,v in values.items()])
    db.execute('CREATE TABLE same_tx_event_encoded(event_ord INTEGER PRIMARY KEY,event_id TEXT,tx_ord INTEGER,seq_index INTEGER,evt_index INTEGER,event_type_id INTEGER,protocol_id INTEGER,token_sold_id INTEGER,token_bought_id INTEGER,pool_id INTEGER,amount_num REAL)')
    db.executemany('INSERT INTO same_tx_event_encoded VALUES(?,?,?,?,?,?,?,?,?,?,?)',
        [(r['eid'],str(r['eid']),txids[r['tx']],r['seq'],r['evt'],types[r['kind']],values.get(r['dex'],0),values.get(r['src'],0),values.get(r['dst'],0),values.get(r['pool'],0),r['amount']) for r in rows])
    db.commit();db.close()
    with (out/'txids.json').open('x') as f: json.dump(txids,f)
    atoms=[]
    for r in rows:
        roles={k:v for k,v in {'token_sold':r['src'],'token_bought':r['dst'],'pool':r['pool']}.items() if v}
        atoms.append(dict(event_id=str(r['eid']),closure_id=r['tx'],tx_hash=r['tx'],event_type=r['kind'],protocol=r['dex'],seq_index=r['seq'],roles=roles,attrs={'amount':str(r['amount'])}))
    with (out/'hg_atoms.json').open('x') as f:json.dump(atoms,f,separators=(',',':'))
    columns=['event_id','closure_id','tx_hash','block_number','block_time','evt_index','event_type','protocol','version','contract','roles_json','attrs_json','raw_source','raw_row_id','src_rule_id','seq_index']
    shards=[]

    for offset in range(0,len(rows),20000):
        path=out/f'mbft_{offset:06d}.csv';shards.append(str(path.resolve()))
        with path.open('x',newline='') as f:
            w=csv.DictWriter(f,fieldnames=columns);w.writeheader()
            for r,a in zip(rows[offset:offset+20000],atoms[offset:offset+20000]):
                w.writerow(dict(event_id=str(r['eid']),closure_id=r['tx'],tx_hash=r['tx'],block_number=r['block'],block_time=r['time'][:19],evt_index=r['evt'],event_type=r['kind'],protocol=r['dex'],version='',contract=r['pool'],roles_json=json.dumps(a['roles']),attrs_json=json.dumps(a['attrs']),raw_source='larc_evidence_atoms',raw_row_id=r['eid'],src_rule_id='yo_source_v1',seq_index=r['seq']))
    with (out/'mbft_manifest.json').open('x') as f: json.dump({'shards':shards,'expected_source_rows':len(rows)},f)
    print(json.dumps({'rows':len(rows),'transactions':len(groups),'out':str(out)}),flush=True)

def native(out, namespace):
    import re
    if not re.fullmatch('yo_[a-z0-9_]+',namespace): raise ValueError('namespace')
    sys.path.append('/usr/lib/python3/dist-packages')
    import psycopg2
    from psycopg2.extras import execute_values
    from pymongo import MongoClient
    groups=json.loads((out/'groups.json').read_text())
    pg=psycopg2.connect(host='127.0.0.1',port=55432,dbname='funnelq',user='funnelq',password='funnelq')

    with pg,pg.cursor() as c:
        c.execute('CREATE SCHEMA '+namespace)
        c.execute('CREATE TABLE '+namespace+'.events(eid BIGINT PRIMARY KEY,tx TEXT,kind TEXT,seq BIGINT,src TEXT,dst TEXT,pool TEXT,dex TEXT,amount DOUBLE PRECISION,payload TEXT)')
        rows=[r for rs in groups.values() for r in rs]
        execute_values(c,'INSERT INTO '+namespace+'.events VALUES %s',[(r['eid'],r['tx'],r['kind'],r['seq'],r['src'],r['dst'],r['pool'],r['dex'],r['amount'],json.dumps(r,separators=(',',':'))) for r in rows],page_size=5000)
        for columns in ('tx,kind,seq','kind,tx,src,seq','kind,amount,tx'):
            c.execute('CREATE INDEX ON '+namespace+'.events('+columns+')')
        c.execute('ANALYZE '+namespace+'.events')
    pg.close()
    client=MongoClient('mongodb://mongo:27018',serverSelectionTimeoutMS=15000)
    if namespace in client.list_database_names(): raise ValueError('Mongo namespace already exists')
    db=client[namespace]
    docs=[]
    for tx,rs in groups.items():
        swaps=[r for r in rs if r['kind']=='pool_swap']
        docs.append(dict(_id=tx,rows=rs,max_amount=max((r['amount'] for r in rs if r['kind']=='aggregator_intent'),default=0),n_swaps=len(swaps),n_pools=len({r['pool'] for r in swaps}-{''}),n_dexes=len({r['dex'] for r in swaps}-{''})))
    db.transactions.insert_many(docs)
    swaps=[dict(r,_id=r['eid']) for rs in groups.values() for r in rs if r['kind']=='pool_swap']
    db.swaps.insert_many(swaps)
    db.transactions.create_index([('max_amount',1),('n_swaps',1)])
    db.swaps.create_index([('tx',1),('src',1),('seq',1)])
    db.swaps.create_index([('tx',1),('seq',1)])
    client.close()
    print('Native stores built: '+namespace,flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--source',type=Path);p.add_argument('--out',type=Path,required=True);p.add_argument('--native',action='store_true');p.add_argument('--namespace',default='yo_query_v1');a=p.parse_args()
    if a.native:native(a.out,a.namespace)
    else:prepare(a.source,a.out)
