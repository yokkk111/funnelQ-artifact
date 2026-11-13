import copy,struct,tempfile,unittest,zlib
from pathlib import Path
import codec,trust

class Controls(unittest.TestCase):
    def test_roundtrip_determinism(self):
        parts=[b'a'*3000000,b'b'*100,b'']
        a=codec.measure(parts);self.assertTrue(a['roundtrip_verified']);self.assertEqual(a,codec.measure(parts))
    def test_disk_and_memory_equal(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'x';p.write_bytes(b'abc'*10000)
            self.assertEqual(codec.measure([p]),codec.measure([p.read_bytes()]))
    def test_zlib_rejects_trailing(self):
        for b in (zlib.compress(b'x')+b'bad',zlib.compress(b'x')[:-1],b'bad'):
            with self.assertRaises((ValueError,zlib.error)):codec.inflate(b)
    def test_nft_inner_layers_removed(self):
        parts=[b'{"opened":0}',zlib.compress(b'events'),zlib.compress(b'answers')]
        b=b'SCCPFX01'+b''.join(struct.pack('<Q',len(x))+x for x in parts)
        raw=codec.nft_funnelq(b);r=codec.Reader(raw);self.assertEqual(r.take(8),b'SCCRAW01')
        self.assertEqual([r.blob() for _ in range(3)],[parts[0],b'events',b'answers']);r.done()
    def test_defi_inner_layers_removed(self):
        col=struct.pack('<BI',1,len(zlib.compress(b'123')))+zlib.compress(b'123')
        dr=zlib.compress(b'directory')
        b=b'DJADS002'+b'x'*64+struct.pack('<Q',len(dr))+dr+struct.pack('<I',1)+struct.pack('<IQ',3,len(col))+col+struct.pack('<IQQ',0,1,len(col))+col
        raw=codec.defi_funnelq(b);r=codec.Reader(raw);self.assertEqual(r.take(8),b'DJRAW002');r.take(64)
        self.assertEqual(r.blob(),b'directory');self.assertEqual(r.num('<I'),(1,));j,n=r.num('<IQ')
        self.assertEqual(r.take(n),struct.pack('<BI',1,3)+b'123');self.assertEqual(r.num('<I'),(0,))
        count,n=r.num('<QQ');self.assertEqual(r.take(n),struct.pack('<BI',1,3)+b'123');r.done()
    def envelope(self,method):
        return dict(version=trust.VERSION,dataset='nft',method=method,epoch_id='0x'+'11'*32,
            files=[dict(role=r,bytes=1,sha256=trust.sha(b'x')) for r in trust.roles(method)])
    def test_complete_method_schema(self):
        for m in ('funnelq','mbft','vchain'):
            e=self.envelope(m);trust.root(e);e['files'].pop()
            with self.assertRaises(ValueError):trust.root(e)
    def test_unknown_fields_and_method_change(self):
        e=self.envelope('funnelq');e['candidate_mask']=1
        with self.assertRaises(ValueError):trust.root(e)
        e=self.envelope('funnelq');e['method']='mbft'
        with self.assertRaises(ValueError):trust.root(e)
    def test_duplicate_json(self):
        with self.assertRaises(ValueError):trust.parse('{"a":1,"a":2}')
    def test_authentication_and_attacks(self):
        e=self.envelope('funnelq');pin=dict(dataset='nft',method='funnelq',epoch_id=e['epoch_id'],chain_id=31338,
            registry='0x123',block_tag='0x1',block_hash='hash',runtime_sha256=trust.sha(b'code'))
        class Fake:
            def __init__(self,root):self.root=root;self.calls=[]
            def call(self,m,p):
                self.calls.append(dict(method=m,ms=1))
                return {'eth_chainId':hex(31338),'eth_getBlockByNumber':{'hash':'hash'},'eth_getCode':'0x'+b'code'.hex(),'eth_call':self.root}[m]
        self.assertEqual(trust.authenticate(e,{'shared_bundle':b'x'},pin,Fake(trust.root(e)))['epoch_root'],trust.root(e))
        for kind in ('epoch','method','bytes','chain_root','forgery'):
            bad=copy.deepcopy(e);b={'shared_bundle':b'x'};rpc=Fake(trust.root(e))
            if kind=='epoch':bad['epoch_id']='0x'+'33'*32
            elif kind=='method':pin2={**pin,'method':'vchain'}
            elif kind=='bytes':b['shared_bundle']=b'z'
            elif kind=='chain_root':rpc.root='0x'+'00'*32
            else:b['shared_bundle']=b'z';bad['files'][0]['sha256']=trust.sha(b'z')
            with self.assertRaises(ValueError):trust.authenticate(bad,b,pin2 if kind=='method' else pin,rpc)

if __name__=='__main__':unittest.main()
