import json,os,struct,tempfile,unittest
from pathlib import Path
import numpy as np
from contract import CASES,METHODS,CONTRACT,answer_id,packet_bytes,response

class ContractTest(unittest.TestCase):
    def test_schedule(self):
        self.assertEqual(len(set(CASES)),20);self.assertEqual(len(METHODS)*len(CASES)*CONTRACT['repetitions'],600)
        self.assertEqual(len([x for x in CASES if x.startswith('E')]),15)
    def test_unique_bytes(self):
        blob=b'P'*72+struct.pack('<Q',3)+b'abc'+struct.pack('<I',1)+struct.pack('<IQ',2,4)+b'abcd'+struct.pack('<I',1)+b'N'*36+struct.pack('<QQ',4,3)+b'ans'
        p,a=packet_bytes(blob);self.assertEqual(a,19);self.assertEqual(response(p,a)['response_bytes'],len(blob))
        for bad in [blob[:-1],blob+b'X']:
            with self.assertRaises(ValueError):packet_bytes(bad)
    def test_oracle_all_roles_unordered(self):
        a=np.array([(3,4),(1,5)],dtype=[('x','<i8'),('y','<i8')])
        self.assertEqual(answer_id(a),answer_id(a[::-1]))
        b=a.copy();b['y'][0]+=1
        self.assertNotEqual(answer_id(a),answer_id(b))
        with self.assertRaises(ValueError):answer_id(np.concatenate([a,a]))
    @unittest.skipUnless(os.name=='posix','Linux process benchmark')
    def test_primary_excludes_postdecision_time(self):
        import bench
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            (root/'client.py').write_text("""import json,time,sys
from pathlib import Path
time.sleep(.12)
d=time.monotonic_ns()
time.sleep(.35)
(Path(sys.argv[2])/'decision.json').write_text(json.dumps(dict(accepted=True, decision_ns=d,
answer={'count':1},wire={'response_bytes':4,'answer_bytes':1,'proof_bytes':3},query_bytes=2,trusted_state_bytes=3)))
""")
            with patch.object(bench,'HERE',root):
                r=bench.invoke(root/'unused',root/'run',{'count':1})
                self.assertGreater(r['process_wall_diagnostic_s']-r['verify_seconds'],.30)
                self.assertGreater(r['memory_samples'],0)
                with self.assertRaises(ValueError):bench.invoke(root/'unused',root/'wrong',{'count':2})

if __name__=='__main__':unittest.main()
