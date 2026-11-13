import hashlib, json, time, urllib.request
from pathlib import Path

VERSION='funnelq-full-manifest-chain-envelope/v1'
COMMIT='c065f56f'
READ='a00a4604'

def require(ok, message):
    if not ok: raise ValueError(message)

def pairs(items):
    out={}
    for k,v in items:
        require(k not in out,'duplicate JSON key');out[k]=v
    return out

def parse(data):
    return json.loads(data,object_pairs_hook=pairs,parse_constant=lambda x: (_ for _ in ()).throw(ValueError('nonfinite JSON')))

def canonical(obj):
    return json.dumps(obj,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def sha(data): return hashlib.sha256(data).hexdigest()

def file_sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(8<<20),b''):h.update(b)
    return h.hexdigest()

def root(envelope):
    require(set(envelope)=={'version','dataset','epoch_id','manifests'},'envelope schema')
    require(envelope['version']==VERSION,'envelope version')
    require(envelope['dataset'] in ('defi','nft'),'dataset')
    hex32(envelope['epoch_id'])
    entries=envelope['manifests']
    expected=['episode','same_tx'] if envelope['dataset']=='defi' else ['shared_bundle']
    require([e['role'] for e in entries]==expected,'complete manifest roles')
    for e in entries:
        require(set(e)=={'role','bytes','sha256'},'manifest entry schema')
        require(type(e['bytes']) is int and e['bytes']>0,'manifest byte length')
        hex32(e['sha256'])
    return '0x'+sha(VERSION.encode()+b'\0'+canonical(envelope))

def hex32(value):
    require(isinstance(value,str),'bytes32 type')
    s=value.removeprefix('0x')
    require(len(s)==64,'bytes32 length')
    bytes.fromhex(s)
    return s.lower()

class Rpc:
    def __init__(self,url):self.url=url;self.calls=[]
    def call(self,method,params):
        start=time.perf_counter_ns()
        data=canonical(dict(jsonrpc='2.0',id=1,method=method,params=params))
        request=urllib.request.Request(self.url,data=data,headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request,timeout=20) as r:body=parse(r.read())
        require(body.get('id')==1 and 'error' not in body and 'result' in body,'RPC failure: '+str(body))
        self.calls.append(dict(method=method,ms=(time.perf_counter_ns()-start)/1e6))
        return body['result']

def authenticate(envelope,manifest_bytes,pin,rpc):

    start=time.perf_counter_ns()
    expected=root(envelope)
    require(envelope['dataset']==pin['dataset'],'dataset pin mismatch')
    require(envelope['epoch_id']==pin['epoch_id'],'epoch pin mismatch')
    require(set(manifest_bytes)=={e['role'] for e in envelope['manifests']},'manifest set mismatch')
    for e in envelope['manifests']:
        b=manifest_bytes[e['role']]
        parse(b)
        require(len(b)==e['bytes'] and sha(b)==e['sha256'],'manifest bytes mismatch: '+e['role'])
    manifest_ms=(time.perf_counter_ns()-start)/1e6
    start=time.perf_counter_ns()
    require(int(rpc.call('eth_chainId',[]),16)==pin['chain_id'],'chain ID mismatch')
    block=rpc.call('eth_getBlockByNumber',[pin['block_tag'],False])
    require(block and block['hash']==pin['block_hash'],'pinned block mismatch')
    code=rpc.call('eth_getCode',[pin['registry'],pin['block_tag']])
    require(sha(bytes.fromhex(code.removeprefix('0x')))==pin['runtime_sha256'],'registry code mismatch')
    value=rpc.call('eth_call',[{'to':pin['registry'],'data':'0x'+READ+hex32(pin['epoch_id'])},pin['block_tag']])
    require(isinstance(value,str) and len(value)==66,'invalid root return')
    require(value.lower()!='0x'+'00'*32,'epoch not committed')
    require(value.lower()==expected,'on-chain manifest root mismatch')
    block_after=rpc.call('eth_getBlockByNumber',[pin['block_tag'],False])
    require(block_after and block_after['hash']==pin['block_hash'],'block changed during read')
    return dict(epoch_root=expected,epoch_id=pin['epoch_id'],chain_id=pin['chain_id'],registry=pin['registry'],
                block_hash=pin['block_hash'],block_tag=pin['block_tag'],manifest_check_ms=manifest_ms,
                chain_binding_ms=(time.perf_counter_ns()-start)/1e6,
                root_eth_call_ms=next(c['ms'] for c in rpc.calls if c['method']=='eth_call'),rpc_calls=rpc.calls)

def transaction(rpc,tx):
    gas=int(rpc.call('eth_estimateGas',[tx]),16);tx={**tx,'gas':hex(gas+50000)}
    start=time.perf_counter_ns();h=rpc.call('eth_sendTransaction',[tx])
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        receipt=rpc.call('eth_getTransactionReceipt',[h])
        if receipt:
            require(int(receipt['status'],16)==1,'transaction reverted')
            return dict(transaction_hash=h,gas_used=int(receipt['gasUsed'],16),wall_ms=(time.perf_counter_ns()-start)/1e6,
                        calldata_bytes=(len(tx['data'])-2)//2,receipt=receipt)
        time.sleep(.01)
    raise TimeoutError('receipt timeout')
