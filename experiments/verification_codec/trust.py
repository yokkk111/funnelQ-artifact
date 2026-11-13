import hashlib,json,time,urllib.request
from pathlib import Path

VERSION='native-verifier-complete-seal/v1'
READ='a00a4604';COMMIT='c065f56f'
def require(ok,msg):
    if not ok:raise ValueError(msg)
def pairs(items):
    d={}
    for k,v in items:require(k not in d,'duplicate JSON key');d[k]=v
    return d
def parse(b):return json.loads(b,object_pairs_hook=pairs,parse_constant=lambda x:(_ for _ in ()).throw(ValueError('nonfinite JSON')))
def canonical(x):return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(b):return hashlib.sha256(b).hexdigest()
def hex32(v):
    require(isinstance(v,str),'bytes32 type');s=v.removeprefix('0x')
    require(len(s)==64,'bytes32 length');bytes.fromhex(s);return s.lower()
def file_sha(p):
    h=hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda:f.read(1<<20),b''):h.update(b)
    return h.hexdigest()
def roles(method):
    if method=='funnelq':return ['shared_bundle']
    if method=='mbft':return [f'shard_{i:02}' for i in range(1,21)]
    if method=='vchain':return ['config','public_key','trusted_headers']
    raise ValueError('unknown method')
def root(e):
    require(set(e)=={'version','dataset','method','epoch_id','files'},'manifest schema')
    require(e['version']==VERSION and e['dataset']=='nft','manifest version/dataset')
    hex32(e['epoch_id']);require([x['role'] for x in e['files']]==roles(e['method']),'complete native trust roles')
    for x in e['files']:
        require(set(x)=={'role','bytes','sha256'} and type(x['bytes']) is int and x['bytes']>0,'entry schema')
        hex32(x['sha256'])
    return '0x'+sha(VERSION.encode()+b'\0'+canonical(e))
class Rpc:
    def __init__(self,url):self.url=url;self.calls=[]
    def call(self,m,p):
        t=time.perf_counter_ns();req=urllib.request.Request(self.url,canonical(dict(jsonrpc='2.0',id=1,method=m,params=p)),{'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=20) as r:b=parse(r.read())
        require(b.get('id')==1 and 'error' not in b and 'result' in b,'RPC error')
        self.calls.append(dict(method=m,ms=(time.perf_counter_ns()-t)/1e6));return b['result']
def authenticate(e,buffers,pin,rpc):
    t=time.perf_counter_ns();expected=root(e)
    for k in ('dataset','method','epoch_id'):require(e[k]==pin[k],k+' pin mismatch')
    require(set(buffers)==set(roles(e['method'])),'trust file set')
    for x in e['files']:
        b=buffers[x['role']];require(len(b)==x['bytes'] and sha(b)==x['sha256'],'trusted file hash mismatch')
    manifest_ms=(time.perf_counter_ns()-t)/1e6;t=time.perf_counter_ns()
    require(int(rpc.call('eth_chainId',[]),16)==pin['chain_id'],'chain ID mismatch')
    b=rpc.call('eth_getBlockByNumber',[pin['block_tag'],False]);require(b and b['hash']==pin['block_hash'],'pinned block mismatch')
    code=rpc.call('eth_getCode',[pin['registry'],pin['block_tag']]);require(sha(bytes.fromhex(code[2:]))==pin['runtime_sha256'],'registry mismatch')
    v=rpc.call('eth_call',[dict(to=pin['registry'],data='0x'+READ+hex32(pin['epoch_id'])),pin['block_tag']])
    require(v.lower()==expected and expected!='0x'+'00'*32,'on-chain manifest root mismatch')
    b=rpc.call('eth_getBlockByNumber',[pin['block_tag'],False]);require(b and b['hash']==pin['block_hash'],'block changed')
    return dict(manifest_check_ms=manifest_ms,chain_binding_ms=(time.perf_counter_ns()-t)/1e6,
                root_eth_call_ms=next(c['ms'] for c in rpc.calls if c['method']=='eth_call'),epoch_root=expected,rpc_calls=rpc.calls)
