import argparse
import gc
import hashlib
import importlib.util
import json
import re
import struct
import time
import zlib
from pathlib import Path
import column_codec

HERE = Path(__file__).resolve().parent
INDEX = HERE / 'compact_indexes'
V1 = HERE.parent / 'defi_interfaces'
spec = importlib.util.spec_from_file_location('interface_v1', V1 / 'run.py')
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)
np, pd, duckdb = base.np, base.pd, base.duckdb
DIR = np.dtype([('key','<u8'),('offset','<u8'),('count','<u8')])
MAGIC = b'DJADS001'
ADAPTIVE = False


def js(x):
    return json.dumps(x, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


def save(name, x):
    (HERE / name).write_text(json.dumps(x, indent=2, ensure_ascii=False), encoding='utf-8')


def h(x):
    return hashlib.sha256(x).digest()


def require(ok, message):
    if not ok:
        raise ValueError(message)


def pack_columns(a):
    if ADAPTIVE:return column_codec.pack(a)

    chunks = []
    for name in a.dtype.names:
        x = np.ascontiguousarray(a[name])
        chunks.append(x.view(np.uint8).reshape(len(a), x.dtype.itemsize).T.tobytes())
    return zlib.compress(b''.join(chunks), 1)


def unpack_columns(blob, dtype, n):
    if ADAPTIVE:return column_codec.unpack(blob,dtype,n)
    expected = dtype.itemsize * n
    dec = zlib.decompressobj()
    raw = dec.decompress(blob, expected + 1)
    require(len(raw) == expected and dec.eof and not dec.unused_data and not dec.unconsumed_tail, 'column length/compression')
    a = np.empty(n, dtype=dtype)
    pos = 0
    for name in dtype.names:
        dt = dtype.fields[name][0]
        end = pos + n * dt.itemsize
        a[name] = np.frombuffer(raw[pos:end], dtype=np.uint8).reshape(dt.itemsize, n).T.copy().reshape(-1).view(dt)
        pos = end
    return a


def leaf(ctx, j, rows):
    return h(b'defi-joint-class/v1' + ctx + struct.pack('<QQ', j, len(rows)) + rows.tobytes())


def parent(a, b):
    return h(b'defi-joint-parent/v1' + a + b)


def seal(a, mode, p0, p1, source_hash):
    t = time.perf_counter()

    typ = np.where(a['kind'] == 0, 0, np.where(a['kind'] == 1, 1, np.where(a['sign'] > 0, 2, np.where(a['sign'] < 0, 3, 4)))).astype(np.uint64)
    keys = (typ << 40) | (p1.astype(np.uint64) << 20) | p0.astype(np.uint64)
    order = np.lexsort((a['eid'], keys))
    keys = keys[order]
    starts = np.r_[0, 1 + np.flatnonzero(keys[1:] != keys[:-1])]
    dr = np.empty(len(starts), dtype=DIR)
    dr['key'] = keys[starts]; dr['offset'] = starts
    dr['count'] = np.diff(np.r_[starts, len(a)])
    rows = a[order]
    meta = {'version':'defi-joint-class-ads/v1', 'mode':mode, 'scales':base.SC,
            'source_canonical_sha256':source_hash, 'source_rows':len(a),
            'schema':base.EVENT.descr, 'directory_sha256':h(dr.tobytes()).hex(),
            'classes':len(dr), 'trust':'independent honest full-data sealer; NOT authenticated source derivation',
            'source_semantics':'complete supported canonical event cache, finite stored REAL amount sign only'}
    ctx = h(js(meta))
    width = 1 << (len(dr) - 1).bit_length()
    tree = [b''] * (2 * width)
    for j in range(width):
        if j < len(dr):
            lo = int(dr[j]['offset']); n = int(dr[j]['count'])
            tree[width+j] = leaf(ctx, j, rows[lo:lo+n])
        else:
            tree[width+j] = h(b'defi-joint-padding/v1' + ctx + struct.pack('<Q', j))
    for j in range(width-1, 0, -1):
        tree[j] = parent(tree[2*j], tree[2*j+1])
    meta['root'] = tree[1].hex()
    INDEX.mkdir(exist_ok=True)
    np.save(INDEX / (mode+'_directory.npy'), dr)


    np.save(INDEX / (mode+'_members.npy'), order.astype(np.uint32))
    (INDEX / (mode+'_tree.bin')).write_bytes(b''.join(tree[1:]))
    (INDEX / (mode+'_manifest.json')).write_text(json.dumps(meta,indent=2),encoding='utf-8')
    return {'mode':mode, 'classes':len(dr), 'stored_event_rows':len(rows),
            'shared_event_bytes_not_duplicated':rows.nbytes, 'member_bytes':4*len(order),'directory_bytes':dr.nbytes,
            'tree_bytes':32*(2*width-1), 'seal_sort_hash_write_s':time.perf_counter()-t,
            'root':meta['root']}


def build(a):

    t = time.perf_counter()
    source_hash = h(a.tobytes()).hex()
    ids = [np.flatnonzero(a['kind']==0), np.flatnonzero((a['kind']==2)&(a['sign']>0)),
           np.flatnonzero((a['kind']==2)&(a['sign']<0)), np.flatnonzero(a['kind']==1), np.flatnonzero(a['kind']==1)]
    rows = np.concatenate(ids).astype(np.uint32)
    own = np.concatenate([np.full(len(v), p, dtype=np.uint8) for p,v in enumerate(ids)])
    assets = np.concatenate([a['out'][v] if p==4 else a['asset'][v] for p,v in enumerate(ids)])
    keys = (a['actor'][rows].astype(np.uint64)<<32) | assets.astype(np.uint64)
    order = np.lexsort((a['tx'][rows],keys))
    keys=keys[order]; own=own[order]; rows=rows[order]
    del ids, assets, order
    primary = own != 4
    def joint(codes):
        p0=np.zeros(len(a),dtype=np.uint32);p1=np.zeros(len(a),dtype=np.uint32)
        p0[rows[primary]]=codes[primary];p1[rows[~primary]]=codes[~primary]
        return p0,p1
    mask,_=base.group_masks(keys,a['tx'][rows],own,False)
    p0,p1=joint(mask)
    same=seal(a,'same_tx',p0,p1,source_hash)
    del p0,p1,mask
    beta=np.full(len(rows),sum(8<<(4*p) for p in range(5)),dtype=np.uint32)
    previous=np.zeros(len(rows),dtype=np.uint8)
    ts=a['time'][rows]
    for level,scale in enumerate(base.SC):
        mask,_=base.group_masks(keys,ts//scale,own,True)
        require(np.all((previous&mask)==previous),'scale nesting')
        for p in range(5):
            born=((mask&(1<<p))!=0)&((previous&(1<<p))==0)
            beta[born]&=np.uint32(0xffffffff ^ (15<<(4*p)))
            beta[born]|=np.uint32(level<<(4*p))
        previous=mask
    p0,p1=joint(beta)
    episode=seal(a,'episode',p0,p1,source_hash)
    out={'seconds_rebuild_sort_hash_write':time.perf_counter()-t,'indexes':[same,episode],
         'physical_index_bytes':sum((INDEX/f'{mode}_{suffix}').stat().st_size for mode in ['same_tx','episode'] for suffix in ['directory.npy','members.npy','tree.bin','manifest.json']),
         'no_query_pre_registration':True,'event_cache_is_not_a_chain_verified_epoch':True}
    save('offline_compact.json',out)
    return out


class GatherRows:
    def __init__(self,mode):
        self.events=np.load(V1/'events.npy',mmap_mode='r')
        self.members=np.load(INDEX/(mode+'_members.npy'),mmap_mode='r')
    def __getitem__(self,index):return self.events[self.members[index]]


def load(mode):
    dr=np.load(INDEX/(mode+'_directory.npy'))
    rows=GatherRows(mode)
    meta=json.loads((INDEX/(mode+'_manifest.json')).read_text())
    wire=(INDEX/(mode+'_tree.bin')).read_bytes()
    tree=[b'']+[wire[i:i+32] for i in range(0,len(wire),32)]
    return dr,rows,meta,tree


def select(dr,q):
    allowed={'query_name','scope','events','order','attr_filters','neq_constraints','scope_params'}
    require(set(q)<=allowed,'unsupported QuerySpec field; do not silently ignore')
    require(q['scope'] in ('same_tx','anchored_episode'),'unsupported scope')
    atom_ids=[atom['id'] for atom in q['events']]
    require(bool(atom_ids) and len(set(atom_ids))==len(atom_ids),'unique atoms')
    require(all(re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*',x) for x in atom_ids),'atom identifier')
    roles_by_type={'flashloan':{'recipient','asset','pool'},'swap':{'taker','token_sold','token_bought','pool'},'borrow':{'borrower','asset','pool'}}
    for atom in q['events']:
        require(set(atom)=={'id','type','roles'} and atom['type'] in roles_by_type,'atom schema')
        require(set(atom['roles'])<=roles_by_type[atom['type']],'unsupported role')
        require(all(isinstance(v,str) and v.startswith('$') for v in atom['roles'].values()),'only variable roles supported')
    for f in q.get('attr_filters',[]):
        require(set(f)=={'event','field','op','value'} and f['event'] in atom_ids and f['field']=='amount' and f['value']=='0' and f['op'] in ('>','<'),'unsupported attribute filter')
        require(next(a for a in q['events'] if a['id']==f['event'])['type']=='borrow','amount filter on non-borrow')
    require(all(len(pair)==2 and all(x in atom_ids for x in pair) for pair in q['order']),'order references')
    for x,f,y,g in q.get('neq_constraints',[]):
        require(x in atom_ids and y in atom_ids and f in base.FIELD and g in base.FIELD,'inequality references')
    info,req=base.compile_query(q)
    if q['scope']=='anchored_episode':
        params=q['scope_params']
        require(params['semantic_anchor_event'] in atom_ids,'semantic anchor')
        require(set(params)<= {'semantic_anchor_event','execution_anchor_event','forward_window_sec'},'scope parameters')
        window=int(params['forward_window_sec'])
        require(0<=window<=base.SC[-1],'unsupported window; requires complete-input fallback')
        level=next(i for i,h0 in enumerate(base.SC) if h0>=window)
    else:level=None
    types=dr['key']>>40
    domains={}
    for atom in q['events']:
        aid=atom['id']
        ports=[p for p,_ in info[aid]]
        typ=0 if ports==[0] else 1 if ports==[3,4] else 2 if ports==[1] else 3
        m=types==typ
        for port,need in req[aid]:
            code=(dr['key']>>(20 if port==4 else 0))&0xfffff
            if level is None:m &= (code&need)==need
            else:
                for target in range(5):
                    if need&(1<<target):m &= ((code>>(4*target))&15)<=level
        domains[aid]=set(map(int,np.flatnonzero(m)))
    return domains,sorted(set().union(*domains.values()))


def frontier(tree, selected):
    width=len(tree)//2; opened={width+j for j in selected}; proof={}
    while opened and opened!={1}:
        for j in opened:
            if j^1 not in opened:proof[j^1]=tree[j^1]
        opened={j//2 for j in opened}
    if not selected:proof[1]=tree[1]
    return proof


def frontier_positions(width, selected):
    opened={width+j for j in selected}; expected=set()
    while opened and opened!={1}:
        expected.update(j^1 for j in opened if j^1 not in opened)
        opened={j//2 for j in opened}
    return expected if selected else {1}


def packet(dr,rows,meta,tree,q,answers):
    t=time.perf_counter();_,sel=select(dr,q)
    directory=zlib.compress(dr.tobytes(),1)
    parts=[MAGIC,h(js(q)),bytes.fromhex(meta['root']),struct.pack('<Q',len(directory)),directory,struct.pack('<I',len(sel))]
    event_bytes=0;row_zlib_bytes=0
    for j in sel:
        lo=int(dr[j]['offset']);n=int(dr[j]['count']);block=rows[lo:lo+n]
        comp=pack_columns(block)
        event_bytes+=len(comp)

        parts.extend([struct.pack('<IQ',j,len(comp)),comp])
    fr=frontier(tree,sel)
    parts.append(struct.pack('<I',len(fr)))
    for j,value in sorted(fr.items()):parts.extend([struct.pack('<I',j),value])
    ab=pack_columns(answers)
    parts.extend([struct.pack('<QQ',len(answers),len(ab)),ab])
    blob=b''.join(parts)
    return blob,{'packet_generation_s':time.perf_counter()-t,'wire_bytes':len(blob),
                 'event_column_zlib_bytes':event_bytes,'directory_zlib_bytes':len(directory),
                 'answer_zlib_bytes':len(ab),'frontier_nodes':len(fr),'frontier_bytes':36*len(fr),
                 'selected_classes':len(sel),'selected_rows':sum(int(dr[j]['count']) for j in sel)}


class Reader:
    def __init__(self,b):self.b=b;self.i=0
    def take(self,n):
        require(0<=n<=len(self.b)-self.i,'truncated packet')
        b=self.b[self.i:self.i+n];self.i+=n;return b
    def num(self,fmt):return struct.unpack(fmt,self.take(struct.calcsize(fmt)))


def authenticate(blob,trusted,q):
    t=time.perf_counter();r=Reader(blob)
    require(r.take(8)==MAGIC,'version')
    require(r.take(32)==h(js(q)),'query binding')
    require(r.take(32)==bytes.fromhex(trusted['root']),'epoch/root binding')
    n,=r.num('<Q');dblob=r.take(n)
    dec=zlib.decompressobj();size=trusted['classes']*DIR.itemsize
    raw=dec.decompress(dblob,size+1)
    require(len(raw)==size and dec.eof and not dec.unused_data and not dec.unconsumed_tail,'directory length')
    require(h(raw).hex()==trusted['directory_sha256'],'complete directory')
    dr=np.frombuffer(raw,dtype=DIR)
    require(len(dr)>0 and int(dr[0]['offset'])==0 and np.all(dr['count']>0),'directory shape')
    require(np.all(dr['offset'][1:]==(dr['offset']+dr['count'])[:-1]) and int(dr[-1]['offset']+dr[-1]['count'])==trusted['source_rows'],'directory partition')
    require(np.all(dr['key'][1:]>dr['key'][:-1]),'directory key order')
    mode='same_tx' if q['scope']=='same_tx' else 'episode'
    require(trusted['mode']==mode and trusted['scales']==base.SC,'index semantics')
    domains,sel=select(dr,q)
    nt,=r.num('<I');require(nt==len(sel),'complete class count')
    ctx=h(js({k:v for k,v in trusted.items() if k!='root'}))
    width=1<<(len(dr)-1).bit_length();hashes={};blocks={}
    for expected in sel:
        j,length=r.num('<IQ');require(j==expected,'complete class identity/order')
        rows=unpack_columns(r.take(length),base.EVENT,int(dr[j]['count']))
        require(len(rows)<2 or np.all(rows['eid'][1:]>rows['eid'][:-1]),'within-class event uniqueness/order')
        blocks[j]=rows;hashes[width+j]=leaf(ctx,j,rows)
    nf,=r.num('<I');require(nf<=2*width,'frontier bound');proof={}
    for _ in range(nf):
        j,=r.num('<I');require(1<=j<2*width and j not in proof and j not in hashes,'duplicate/invalid frontier');proof[j]=r.take(32)


    require(set(proof)==frontier_positions(width,sel),'exact frontier positions')
    used=set()
    def root(j):
        if j in hashes:return hashes[j]
        if j in proof:used.add(j);return proof[j]
        require(j<width,'missing class/frontier')
        return parent(root(j*2),root(j*2+1))
    require(root(1).hex()==trusted['root'] and used==set(proof),'Merkle root/frontier coverage')
    n,length=r.num('<QQ')
    require(n<=len(blob)*64,'untrusted answer allocation limit')
    dtype=np.dtype([(x['id'],'<i8') for x in q['events']])
    answers=unpack_columns(r.take(length),dtype,n)
    require(r.i==len(blob),'trailing bytes')
    return blocks,domains,answers,{'auth_decode_hash_s':time.perf_counter()-t}


def sound_unique(blocks,answers,q):

    t=time.perf_counter()
    allrows=np.concatenate(list(blocks.values())) if blocks else np.empty(0,dtype=base.EVENT)
    allrows.sort(order='eid')
    require(len(allrows)<2 or np.all(allrows['eid'][1:]>allrows['eid'][:-1]),'cross-class event identity')
    require(len(np.unique(answers))==len(answers),'duplicate answer')
    bindings={};vars={}
    for atom in q['events']:
        aid=atom['id'];ids=answers[aid];pos=np.searchsorted(allrows['eid'],ids)
        require(np.all(pos<len(allrows)),'answer event missing')
        rows=allrows[pos];require(np.all(rows['eid']==ids),'answer event identity')
        require(np.all(base.atom_mask(rows,q,atom)),'answer atom predicate')
        bindings[aid]=rows
        for field,var in atom['roles'].items():
            col=rows[base.FIELD[field]]
            if var in vars:require(np.array_equal(vars[var],col),'answer equality')
            else:vars[var]=col
    for x,y in q['order']:require(np.all(bindings[x]['eid']<bindings[y]['eid']),'answer order')
    for x,f,y,g in q.get('neq_constraints',[]):require(np.all(bindings[x][base.FIELD[f]]!=bindings[y][base.FIELD[g]]),'answer inequality')
    anchor=q['events'][0]['id'] if q['scope']=='same_tx' else q['scope_params']['semantic_anchor_event']
    for i,atom in enumerate(q['events']):
        aid=atom['id'];rows=bindings[aid];ar=bindings[anchor]
        for other in q['events'][:i]:require(np.all(rows['eid']!=bindings[other['id']]['eid']),'answer injectivity')
        if q['scope']=='same_tx':require(np.all(rows['tx']==ar['tx']),'answer tx')
        else:
            require(np.all((rows['eid']>=ar['eid'])&(rows['time']>=ar['time'])&(rows['time']<=ar['time']+int(q['scope_params']['forward_window_sec']))),'answer window')
    return time.perf_counter()-t


def tables(db,blocks,domains,q):
    t=time.perf_counter()
    for atom in q['events']:
        aid=atom['id'];parts=[blocks[j] for j in sorted(domains[aid])]
        rows=np.concatenate(parts) if parts else np.empty(0,dtype=base.EVENT)
        require(np.all(base.atom_mask(rows,q,atom)),'sealed class type')
        db.register('frame',pd.DataFrame.from_records(rows))
        db.execute(f'CREATE OR REPLACE TEMP TABLE d_{aid} AS SELECT * FROM frame')
        db.unregister('frame')
    return time.perf_counter()-t


def attacks(blob,meta,q,blocks,domains,answers,count):
    out={}
    def reject(name,fn):
        try:fn()
        except (ValueError,zlib.error,IndexError):out[name]='rejected';return
        raise AssertionError('accepted attack: '+name)
    altered=bytearray(blob);altered[8]^=1
    reject('different_query',lambda:authenticate(bytes(altered),meta,q))
    wrong=dict(meta);wrong['root']='00'*32
    reject('different_epoch',lambda:authenticate(blob,wrong,q))
    altered=bytearray(blob);n=struct.unpack('<Q',blob[72:80])[0];altered[80+n//2]^=1
    reject('modified_directory',lambda:authenticate(bytes(altered),meta,q))
    if blocks:
        start=80+n+4;_,length=struct.unpack('<IQ',blob[start:start+12]);altered=bytearray(blob);altered[start+12+length//2]^=1
        reject('modified_event_chunk',lambda:authenticate(bytes(altered),meta,q))
        altered=bytearray(blob);altered[start]^=1
        reject('omitted_or_reordered_class',lambda:authenticate(bytes(altered),meta,q))
        rr=Reader(blob);rr.take(80+n);nc,=rr.num('<I')
        for _ in range(nc):
            _,sz=rr.num('<IQ');rr.take(sz)
        fr_start=rr.i;nf,=rr.num('<I');rr.take(nf*36);fr_end=rr.i
        shadow=blob[:fr_start]+struct.pack('<II',1,1)+bytes.fromhex(meta['root'])+blob[fr_end:]
        reject('root_frontier_bypasses_opened_events',lambda:authenticate(shadow,meta,q))
    reject('truncated_packet',lambda:authenticate(blob[:-1],meta,q))
    if len(answers):
        duplicate=np.concatenate([answers,answers[:1]])
        reject('duplicate_binding',lambda:sound_unique(blocks,duplicate,q))
        forged=answers.copy();forged[forged.dtype.names[0]][0]=-1
        reject('forged_event_binding',lambda:sound_unique(blocks,forged,q))
        reject('missing_answer',lambda:require(len(answers)-1==count,'answer cardinality'))
    return out


def main():
    global ADAPTIVE, MAGIC
    parser=argparse.ArgumentParser();parser.add_argument('--rebuild',action='store_true');parser.add_argument('--limit',type=int);parser.add_argument('--adaptive',action='store_true');parser.add_argument('--build-only',action='store_true')
    args=parser.parse_args();mon=base.Monitor();start=time.perf_counter()
    ADAPTIVE=args.adaptive
    output=HERE/'adaptive_compact' if ADAPTIVE else HERE/'compact_raw'
    output.mkdir(exist_ok=True)
    if ADAPTIVE:MAGIC=b'DJADS002'
    save('codec_tests.json',column_codec.tests())
    try:
        a=np.load(V1/'events.npy',mmap_mode='r')
        if args.rebuild or not (INDEX/'episode_manifest.json').exists():
            mon.stage='offline';print('BUILD',build(a),flush=True)
        if args.build_only:return
        del a;gc.collect()
        indexes={mode:load(mode) for mode in ['same_tx','episode']}
        db=duckdb.connect();db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
        db.execute("SET temp_directory='"+str(HERE/'duckdb_temp').replace('\\','/')+"'")
        old={(r['label'],r['window']):r for r in json.loads((V1/'results.json').read_text())}
        reports=[]
        for k,(label,w,path,q) in enumerate(base.prior.workloads()):
            if args.limit is not None and k>=args.limit:break
            mon.stage=label+'_'+str(w);dr,rows,meta,tree=indexes['same_tx' if w==0 else 'episode']
            t=time.perf_counter();domains,sel=select(dr,q)
            blocks={j:rows[int(dr[j]['offset']):int(dr[j]['offset']+dr[j]['count'])] for j in sel}
            setup=tables(db,blocks,domains,q)
            server_answers=base.record_array(db.execute(base.sql(q,True)).fetchnumpy())
            server_query=time.perf_counter()-t
            blob,stats=packet(dr,rows,meta,tree,q,server_answers)
            (output/f'{label}_w{w}.packet').write_bytes(blob)
            (output/f'{label}_w{w}_query.json').write_bytes(js(q))
            authenticated,client_domains,claimed,auth=authenticate(blob,meta,q)
            client_setup=tables(db,authenticated,client_domains,q)
            t=time.perf_counter();count=int(db.execute(base.sql(q,True,True)).fetchone()[0]);count_s=time.perf_counter()-t
            sound_s=sound_unique(authenticated,claimed,q)
            require(count==len(claimed),'count seal')

            with np.load(V1/f'{label}_w{w}_answers.npz') as baseline:full=baseline['full']
            fp=len(np.setdiff1d(claimed,full));fn=len(np.setdiff1d(full,claimed))
            require(fp==fn==0,'full-source oracle mismatch')
            t=time.perf_counter();replayed=base.record_array(db.execute(base.sql(q,True)).fetchnumpy());replay_s=time.perf_counter()-t
            require(np.array_equal(replayed,full),'replay oracle mismatch')
            tests=attacks(blob,meta,q,authenticated,client_domains,claimed,count)
            row_bytes=sum(len(zlib.compress(blocks[j].tobytes(),1)) for j in sel)
            require(stats['selected_rows']==old[(label,w)]['selected_event_union'],'joint representation must preserve old exact domain')

            repeats=[]
            for rep in range(3):
                times={}
                for method in (['count','replay'] if rep%2==0 else ['replay','count']):
                    t=time.perf_counter()
                    if method=='count':require(int(db.execute(base.sql(q,True,True)).fetchone()[0])==count,'repeat count')
                    else:require(np.array_equal(base.record_array(db.execute(base.sql(q,True)).fetchnumpy()),full),'repeat replay')
                    times[method]=time.perf_counter()-t
                repeats.append(times)
            r={'label':label,'window':w,'query_name':q['query_name'],'answer_count':count,'fp':fp,'fn':fn,
               **stats,**auth,'server_query_including_tables_s':server_query,'client_tables_s':client_setup,
               'client_count_only_s':count_s,'client_sound_unique_s':sound_s,
               'client_total_s':auth['auth_decode_hash_s']+client_setup+count_s+sound_s,
               'e2e_no_network_s':server_query+stats['packet_generation_s']+auth['auth_decode_hash_s']+client_setup+count_s+sound_s,
               'client_replay_enumerate_sort_s':replay_s,'warm_kernel_repeats':repeats,
               'same_classes_rowwise_zlib_bytes':row_bytes,'old_structural_estimate_bytes':old[(label,w)]['structural_compressed_response_excluding_merkle_frontier'],
               'old_opened_member_ids':old[(label,w)]['opened_member_ids_dedup_classes'],
               'new_membership_id_list_bytes':0,'trusted_manifest_bytes':len(js(meta)),
               'attacks':tests,'peak_combined_process_rss_bytes':mon.peak,
               'timing_scope':'local single process Python/numpy/DuckDB; separate logical client; no network/disk/source load/sealing; not GPU or production Rust'}
            reports.append(r);(output/'results.json').write_text(json.dumps(reports,indent=2),encoding='utf-8')
            print('RESULT',label,w,'events',stats['selected_rows'],'MiB',round(len(blob)/2**20,4),'client',round(r['client_total_s'],3),'E2E',round(r['e2e_no_network_s'],3),'FP/FN',fp,fn,flush=True)
            del blocks,authenticated,blob,claimed,server_answers,replayed,full;gc.collect()
        db.close()
        summary={'configurations':len(reports),'complete':len(reports)==20,'fp':sum(r['fp'] for r in reports),'fn':sum(r['fn'] for r in reports),
                             'elapsed_s_including_controls':time.perf_counter()-start,'peak_combined_process_rss_bytes':mon.peak,'stage_peaks':mon.peaks,
                             'trust_boundary':'experimental honest sealer and source cache; no chain provenance or signature verification',
                             'no_production_changes':True}
        (output/'summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    finally:mon.stop=True


if __name__=='__main__':main()
