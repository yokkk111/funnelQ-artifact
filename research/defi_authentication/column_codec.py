import struct
import zlib
import numpy as np


def pack(a):
    parts=[]
    for name in a.dtype.names:
        x=np.ascontiguousarray(a[name]);u=x.view(np.dtype('<u'+str(x.dtype.itemsize)))
        raw=zlib.compress(u.view(np.uint8).reshape(len(a),u.dtype.itemsize).T.tobytes(),1)
        diff=u.copy()
        if len(diff)>1:diff[1:]=u[1:]-u[:-1]
        zig=((diff<<1) ^ (np.zeros_like(diff)-(diff>>(8*u.dtype.itemsize-1)))).astype(u.dtype)
        delta=zlib.compress(zig.view(np.uint8).reshape(len(a),u.dtype.itemsize).T.tobytes(),1)
        mode,compressed=(1,delta) if len(delta)<len(raw) else (0,raw)
        parts.extend([struct.pack('<BI',mode,len(compressed)),compressed])
    return b''.join(parts)


def unpack(blob,dtype,n):
    out=np.empty(n,dtype=dtype);pos=0
    for name in dtype.names:
        if pos+5>len(blob):raise ValueError('column header')
        mode,size=struct.unpack('<BI',blob[pos:pos+5]);pos+=5
        if mode not in (0,1) or pos+size>len(blob):raise ValueError('column codec/length')
        dt=dtype.fields[name][0];width=dt.itemsize
        dec=zlib.decompressobj();raw=dec.decompress(blob[pos:pos+size],n*width+1);pos+=size
        if len(raw)!=n*width or not dec.eof or dec.unused_data or dec.unconsumed_tail:raise ValueError('column decompression')
        u=np.frombuffer(raw,dtype=np.uint8).reshape(width,n).T.copy().reshape(-1).view(np.dtype('<u'+str(width)))
        if mode:
            diff=((u>>1) ^ (np.zeros_like(u)-(u&np.array(1,dtype=u.dtype)))).astype(u.dtype)
            u=np.cumsum(diff,dtype=u.dtype)
        out[name]=u.view(dt)
    if pos!=len(blob):raise ValueError('column trailing bytes')
    return out


def tests():
    rng=np.random.default_rng(271027)
    dtype=np.dtype([('s8','<i8'),('u4','<u4'),('s1','i1')])
    total=0
    for n in [0,1,2,1000]:
        for kind in ['zeros','random','limits','sequence']:
            a=np.zeros(n,dtype=dtype)
            for name in dtype.names:
                dt=dtype.fields[name][0];limits=np.iinfo(dt)
                if kind=='random':a[name]=rng.integers(limits.min,limits.max,n,dtype=dt,endpoint=True)
                if kind=='limits':a[name][::2]=limits.min;a[name][1::2]=limits.max
                if kind=='sequence':a[name]=np.arange(n).astype(dt)
            if not np.array_equal(a,unpack(pack(a),dtype,n)):raise AssertionError((n,kind))
            total+=1
    return {'roundtrips':total,'signed_overflow_limits':True,'zero_rows':True}


if __name__=='__main__':print(tests())
