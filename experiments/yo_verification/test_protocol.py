import copy
import random
import unittest
from protocol import *
from business import SPEC,finish
from test_business import quadratic

class ProtocolTests(unittest.TestCase):
    def fixture(self):
        rows=[]
        for i,(kind,src,dst) in enumerate([(0,0,0),(1,1,2),(1,2,3),(1,3,4),(2,0,0)]):
            lo,hi=struct.unpack('<II',struct.pack('<d',100000 if kind==0 else 0))
            rows.append([i+1,0,kind,i,src,dst,1+i%2,1+i%2,lo,hi,2 if kind==2 else 0])
        return rows
    def test_clean(self):
        rows=self.fixture();m,d,p=seal(rows,1);r,_=verify(prove(d,p,SPEC),m,SPEC);self.assertEqual(rows,r)
    def test_payload_omit(self):
        m,d,p=seal(self.fixture(),1);proof=prove(d,p,SPEC);proof['opened'][0][1]=proof['opened'][0][1][:-1]
        with self.assertRaises(ValueError):verify(proof,m,SPEC)
    def test_coverage(self):
        m,d,p=seal(self.fixture(),1);proof=prove(d,p,SPEC);proof['opened']=[]
        with self.assertRaises(ValueError):verify(proof,m,SPEC)
    def test_directory(self):
        m,d,p=seal(self.fixture(),1);proof=prove(d,p,SPEC);proof['directory'][0]['summary']['linked_swaps']=0
        with self.assertRaises(ValueError):verify(proof,m,SPEC)
    def test_framing(self):
        parts=[b'abc',b'xyz'];self.assertEqual(unpack(packet(parts)),parts)
        with self.assertRaises(ValueError):unpack(packet(parts)+b'junk')
    def test_link_upper_bound(self):
        rng=random.Random(120)
        for _ in range(2000):
            rs=self.fixture()[:1]
            for i in range(rng.randrange(1,30)):rs.append([i+2,0,1,i+1,rng.randrange(5),rng.randrange(5),1+i%2,1+i%2,0,0,0])
            swaps=rs[1:];depth=[]
            for j,r in enumerate(swaps):depth.append(max([depth[i]+1 for i in range(j) if swaps[i][5] and swaps[i][5]==r[4]] or [1]))
            self.assertGreaterEqual(summary(rs)['linked_swaps']+1,max(depth))
    def test_identity(self):
        rows=self.fixture();d=dict(version='yo-canonical/v1',schema=SCHEMA,kinds=KINDS,transactions=['0x'+'0'*64],values=['','a','b','c','d'])
        with self.assertRaises(ValueError):decode(rows+rows[:1],d)

if __name__=='__main__':unittest.main()
