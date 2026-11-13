import copy,unittest
from binding import *

class FakeRpc:
    def __init__(self,value):self.value=value;self.calls=[]
    def call(self,m,p):
        self.calls.append(dict(method=m,ms=.1))
        return {'eth_chainId':'0x7a6a','eth_getBlockByNumber':{'hash':'block'},'eth_getCode':'0x1234','eth_call':self.value}[m]

class Tests(unittest.TestCase):
    def setUp(self):
        self.b={'shared_bundle':b'{"root":"abc","scales":[512],"trust":"sealer"}'}
        self.e=dict(version=VERSION,dataset='nft',epoch_id='0x'+'11'*32,
                    manifests=[dict(role=k,bytes=len(v),sha256=sha(v)) for k,v in self.b.items()])
        self.pin=dict(dataset='nft',epoch_id=self.e['epoch_id'],chain_id=31338,registry='registry',block_tag='0x2',block_hash='block',runtime_sha256=sha(bytes.fromhex('1234')))
    def check(self,e=None,b=None,p=None,value=None):
        return authenticate(e or self.e,b or self.b,p or self.pin,FakeRpc(value or root(self.e)))
    def test_accept(self):self.assertEqual(self.check()['epoch_root'],root(self.e))
    def test_wrong_root(self):
        with self.assertRaisesRegex(ValueError,'on-chain'):self.check(value='0x'+'22'*32)
    def test_wrong_epoch(self):
        with self.assertRaisesRegex(ValueError,'epoch pin'):self.check(p={**self.pin,'epoch_id':'0x'+'22'*32})
    def test_absent_epoch(self):
        with self.assertRaisesRegex(ValueError,'not committed'):self.check(value='0x'+'00'*32)
    def test_full_metadata_tamper(self):
        b={'shared_bundle':b'{"root":"abc","scales":[1024],"trust":"sealer"}'}
        with self.assertRaisesRegex(ValueError,'manifest bytes'):self.check(b=b)
    def test_self_consistent_forgery(self):
        b={'shared_bundle':b'{"root":"forged"}'};e=copy.deepcopy(self.e)
        e['manifests'][0].update(bytes=len(b['shared_bundle']),sha256=sha(b['shared_bundle']))
        with self.assertRaisesRegex(ValueError,'on-chain'):self.check(e=e,b=b)
    def test_wrong_chain(self):
        with self.assertRaisesRegex(ValueError,'chain ID'):self.check(p={**self.pin,'chain_id':1})
    def test_wrong_block(self):
        with self.assertRaisesRegex(ValueError,'block mismatch'):self.check(p={**self.pin,'block_hash':'bad'})
    def test_wrong_code(self):
        with self.assertRaisesRegex(ValueError,'code mismatch'):self.check(p={**self.pin,'runtime_sha256':'bad'})
    def test_unknown_envelope_field(self):
        with self.assertRaisesRegex(ValueError,'schema'):self.check(e={**self.e,'candidate_mask':1})
    def test_missing_manifest(self):
        e={**self.e,'manifests':[]}
        with self.assertRaisesRegex(ValueError,'roles'):root(e)
    def test_duplicate_json(self):
        with self.assertRaisesRegex(ValueError,'duplicate'):parse('{"a":1,"a":2}')
    def test_nonfinite_json(self):
        with self.assertRaisesRegex(ValueError,'nonfinite'):parse('{"a":NaN}')
    def test_key_order(self):self.assertEqual(root(self.e),root(dict(reversed(list(self.e.items())))))

if __name__=='__main__':unittest.main()
