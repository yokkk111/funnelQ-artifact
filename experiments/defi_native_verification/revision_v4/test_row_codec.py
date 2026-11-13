import struct,tempfile,unittest
from pathlib import Path
import numpy as np
from row_codec import loadrows,MAGIC,WIRE
from legacy_evaluate import EVENT

class CodecTests(unittest.TestCase):
    def write(self,p,rows):
        a=np.array(rows,dtype=WIRE)
        p.write_bytes(MAGIC+struct.pack('<Q',len(a))+a.tobytes())
    def test_roundtrip_empty_extremes(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'r.bin'
            for rows in [[],[(2**63-1,2**32-1,2**63-1,0,2**32-1,1,2,2,-1)]]:
                self.write(p,rows);np.testing.assert_array_equal(loadrows([p]),np.array(rows,dtype=EVENT))
    def test_malformed_and_ranges(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'r.bin';row=[1,1,1,1,1,1,1,2,-1]
            self.write(p,[tuple(row)]);good=p.read_bytes()
            for bad in [b'',good[:-1],good+b'x',b'BADMAGIC'+good[8:],MAGIC+struct.pack('<Q',2)+good[16:]]:
                p.write_bytes(bad)
                with self.assertRaises(ValueError):loadrows([p])
            for col,value in [(0,-1),(1,-1),(1,2**32),(7,3),(8,2)]:
                r=row.copy();r[col]=value;self.write(p,[tuple(r)])
                with self.assertRaises(ValueError):loadrows([p])
    def test_duplicate_identity_across_shards(self):
        with tempfile.TemporaryDirectory() as d:
            paths=[Path(d)/f'{i}.bin' for i in range(2)]
            for p in paths:self.write(p,[(1,1,1,1,1,1,1,2,-1)])
            with self.assertRaises(ValueError):loadrows(paths)

if __name__=='__main__':unittest.main()
