import collections
import hashlib
import json
import math
import struct
import zlib

KINDS=['aggregator_intent','pool_swap','token_transfer','transaction_meta']
TYPES=','.join(KINDS[:3])
SCHEMA=['eid','tx','kind','seq','src','dst','pool','dex','amount_lo','amount_hi','token']
MAGIC=b'YOVER001'

def need(x,msg):
    if not x:raise ValueError(msg)

def pairs(items):
    d={}
    for k,v in items:need(k not in d,'duplicate JSON key');d[k]=v
    return d

def parse(b):
    return json.loads(b,object_pairs_hook=pairs,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite')))

def js(x):return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(b):return hashlib.sha256(b).hexdigest()

def encode(groups):
    txs=sorted(groups)
    values=sorted({''}|{r[k] for rows in groups.values() for r in rows for k in ('src','dst','pool','dex','token')})
    ids={v:i for i,v in enumerate(values)}
    d=dict(version='yo-canonical/v1',schema=SCHEMA,kinds=KINDS,transactions=txs,values=values)
    encoded=[];seen=set()
    for ti,tx in enumerate(txs):
        rows=groups[tx]
        need([r['seq'] for r in rows]==list(range(len(rows))),'source order')
        for r in rows:
            need(r['eid'] not in seen,'duplicate event identity');seen.add(r['eid'])
            lo,hi=struct.unpack('<II',struct.pack('<d',r['amount']))
            encoded.append([r['eid'],ti,KINDS.index(r['kind']),r['seq'],ids[r['src']],ids[r['dst']],ids[r['pool']],ids[r['dex']],lo,hi,ids[r['token']]])
    return d,encoded

def decode(rows,d):
    need(set(d)=={'version','schema','kinds','transactions','values'} and d['version']=='yo-canonical/v1' and d['schema']==SCHEMA and d['kinds']==KINDS,'dictionary schema')
    txs=d['transactions'];vs=d['values']
    need(txs==sorted(set(txs)) and vs==sorted(set(vs)) and vs[0]=='','dictionary order')
    groups=collections.defaultdict(list);seen=set();positions=set()
    for r in rows:
        need(len(r)==11 and all(type(v) is int and 0<=v<2**32 for v in r),'event schema')
        eid,ti,kind,seq,src,dst,pool,dex,lo,hi,token=r
        need(eid not in seen and (ti,seq) not in positions,'duplicate event identity or position')
        seen.add(eid);positions.add((ti,seq))
        need(ti<len(txs) and kind<4 and all(v<len(vs) for v in (src,dst,pool,dex,token)),'dictionary index')
        amount=struct.unpack('<d',struct.pack('<II',lo,hi))[0];need(math.isfinite(amount),'amount')
        groups[txs[ti]].append(dict(eid=eid,tx=txs[ti],kind=KINDS[kind],seq=seq,src=vs[src],dst=vs[dst],pool=vs[pool],dex=vs[dex],amount=amount,token=vs[token]))
    for rs in groups.values():rs.sort(key=lambda r:r['seq'])
    return groups

def summary(rows):

    c=[0]*4;amounts=[];pools=set();dexes=set();outputs=set();linked=0
    for r in rows:
        c[r[2]]+=1
        if r[2]==0:amounts.append(struct.unpack('<d',struct.pack('<II',r[8],r[9]))[0])
        if r[2]==1:
            if r[4] and r[4] in outputs:linked+=1
            if r[5]:outputs.add(r[5])
            if r[6]:pools.add(r[6])
            if r[7]:dexes.add(r[7])
    return dict(counts=c,pools=len(pools),dexes=len(dexes),max_aggregator_usd=max(amounts) if amounts else None,linked_swaps=linked)

def stage(s,q):
    if not s['counts'][0]:return 'anchor_negative'
    if (s['max_aggregator_usd']<q['minimum_aggregator_usd'] or s['counts'][1]<q['minimum_swaps'] or s['pools']<q['minimum_pools'] or s['dexes']<q['minimum_dexes'] or not s['counts'][2]):return 'base_negative'
    if s['linked_swaps']+1<q['minimum_chain_length']:return 'refined_negative'
    return 'residual'

def seal(rows,transaction_count):
    grouped=collections.defaultdict(list)
    for r in rows:grouped[r[1]].append(r)
    need(sorted(grouped)==list(range(transaction_count)),'complete transaction domain')
    directory=[];payloads={}
    for ti in range(transaction_count):
        rs=sorted(grouped[ti],key=lambda r:r[3]);need([r[3] for r in rs]==list(range(len(rs))),'complete transaction input')
        directory.append(dict(tx=ti,rows=len(rs),summary=summary(rs),payload_sha256=sha(b'YO-TX/v1\0'+js(rs))))
        payloads[ti]=rs
    meta=dict(version='yo-closure-ads/v1',transactions=transaction_count,events=len(rows),directory_sha256=sha(js(directory)))
    return meta,directory,payloads

def prove(directory,payloads,q):
    return dict(directory=directory,opened=[[x['tx'],payloads[x['tx']]] for x in directory if stage(x['summary'],q)=='residual'])

def verify(proof,meta,q):
    need(set(proof)=={'directory','opened'},'proof schema')
    directory=proof['directory'];need(sha(js(directory))==meta['directory_sha256'],'directory authentication')
    need(len(directory)==meta['transactions'] and [x['tx'] for x in directory]==list(range(len(directory))),'domain coverage')
    need(sum(x['rows'] for x in directory)==meta['events'],'event coverage')
    selected=[x['tx'] for x in directory if stage(x['summary'],q)=='residual']
    need([x[0] for x in proof['opened']]==selected,'residual coverage')
    rows=[]
    for ti,rs in proof['opened']:
        entry=directory[ti]
        need(len(rs)==entry['rows'] and sha(b'YO-TX/v1\0'+js(rs))==entry['payload_sha256'],'payload authentication')
        need(all(r[1]==ti for r in rs) and [r[3] for r in rs]==list(range(len(rs))),'payload boundaries')
        need(summary(rs)==entry['summary'],'summary consistency')
        rows.extend(rs)
    return rows,dict(collections.Counter(stage(x['summary'],q) for x in directory))

def packet(parts):
    raw=MAGIC+struct.pack('<I',len(parts))+b''.join(struct.pack('<Q',len(b))+b for b in parts)
    return zlib.compress(raw,6)

def unpack(blob):
    d=zlib.decompressobj();raw=d.decompress(blob,512*2**20+1)
    need(d.eof and not d.unused_data and not d.unconsumed_tail and len(raw)<=512*2**20,'compression framing/limit')
    need(raw[:8]==MAGIC and len(raw)>=12,'packet header')
    n,=struct.unpack_from('<I',raw,8);need(2<=n<=32,'part count');pos=12;parts=[]
    for _ in range(n):
        need(pos+8<=len(raw),'part header');size,=struct.unpack_from('<Q',raw,pos);pos+=8
        need(pos+size<=len(raw),'part length');parts.append(raw[pos:pos+size]);pos+=size
    need(pos==len(raw),'trailing bytes');return parts
