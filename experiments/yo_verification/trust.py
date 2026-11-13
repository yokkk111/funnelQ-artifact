from protocol import js,sha,parse,need
import time
from binding import Rpc,transaction,hex32,READ,COMMIT

VERSION='yo-native-complete-seal/v1'

def root(m):
    need(set(m)=={'version','dataset','method','epoch_id','source_sha256','events','transactions','shards','files'},'manifest schema')
    need(m['version']==VERSION and m['dataset']=='yo' and m['method'] in ('funnelq','mbft','vchain'),'manifest method')
    hex32(m['source_sha256']);hex32(m['epoch_id'])
    need(type(m['shards']) is int and 0<m['shards']<=32,'shards')
    roles=['dictionary']
    if m['method']=='funnelq':
        need(m['shards']==1,'FunnelQ shard count');roles+=['ads']
    else:
        for i in range(m['shards']):
            roles += [f'{i}_roots'] if m['method']=='mbft' else [f'{i}_headers',f'{i}_pk']
    need(sorted(x['role'] for x in m['files'])==sorted(roles),'complete trust file roles')
    for f in m['files']:
        need(set(f)=={'role','bytes','sha256'} and type(f['bytes']) is int and f['bytes']>0,'file schema');hex32(f['sha256'])
    return '0x'+sha(VERSION.encode()+b'\0'+js(m))

def authenticate(m,buffers,pin,url):
    t=time.perf_counter_ns();r=root(m)
    for k in ('dataset','method','epoch_id'):need(m[k]==pin[k],'pin mismatch: '+k)
    need(set(buffers)=={f['role'] for f in m['files']},'trust buffers')
    for f in m['files']:need(len(buffers[f['role']])==f['bytes'] and sha(buffers[f['role']])==f['sha256'],'trust file mismatch')
    mm=(time.perf_counter_ns()-t)/1e6;t=time.perf_counter_ns();rpc=Rpc(url)
    need(int(rpc.call('eth_chainId',[]),16)==pin['chain_id'],'chain ID')
    block=rpc.call('eth_getBlockByNumber',[pin['block_tag'],False]);need(block and block['hash']==pin['block_hash'],'block pin')
    code=rpc.call('eth_getCode',[pin['registry'],pin['block_tag']]);need(sha(bytes.fromhex(code[2:]))==pin['runtime_sha256'],'registry code')
    got=rpc.call('eth_call',[dict(to=pin['registry'],data='0x'+READ+hex32(pin['epoch_id'])),pin['block_tag']])
    need(got.lower()==r and r!='0x'+'00'*32,'chain root mismatch')
    need(rpc.call('eth_getBlockByNumber',[pin['block_tag'],False])['hash']==pin['block_hash'],'block changed')
    return dict(epoch_root=r,manifest_ms=mm,chain_ms=(time.perf_counter_ns()-t)/1e6,rpc=rpc.calls)
