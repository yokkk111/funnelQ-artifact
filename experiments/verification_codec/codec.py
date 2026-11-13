import hashlib,struct,zlib
from pathlib import Path

MAGIC=b'VCSIZE01'
CHUNK=1<<20

def require(ok,msg):
    if not ok:raise ValueError(msg)

def inflate(blob):
    d=zlib.decompressobj();raw=d.decompress(blob);raw+=d.flush()
    require(d.eof and not d.unused_data and not d.unconsumed_tail,'invalid zlib framing')
    return raw

class Reader:
    def __init__(self,b):self.b=b;self.i=0
    def take(self,n):
        require(0<=n<=len(self.b)-self.i,'truncated framing')
        b=self.b[self.i:self.i+n];self.i+=n;return b
    def num(self,f):return struct.unpack(f,self.take(struct.calcsize(f)))
    def blob(self):return self.take(self.num('<Q')[0])
    def done(self):require(self.i==len(self.b),'trailing data')

def field_columns(blob):
    r=Reader(blob);out=bytearray()
    while r.i<len(blob):
        mode,n=r.num('<BI');require(mode in (0,1),'column mode')
        b=inflate(r.take(n));out+=struct.pack('<BI',mode,len(b))+b
    r.done();return bytes(out)

def nft_funnelq(blob):
    r=Reader(blob);require(r.take(8)==b'SCCPFX01','NFT packet version')
    header=r.blob();events=inflate(r.blob());answers=inflate(r.blob());r.done()
    return b'SCCRAW01'+b''.join(struct.pack('<Q',len(b))+b for b in (header,events,answers))

def defi_funnelq(blob):
    r=Reader(blob);require(r.take(8)==b'DJADS002','DeFi packet version')
    out=bytearray(b'DJRAW002'+r.take(64));directory=inflate(r.blob())
    out+=struct.pack('<Q',len(directory))+directory
    count,=r.num('<I');out+=struct.pack('<I',count)
    for _ in range(count):
        j,n=r.num('<IQ');b=field_columns(r.take(n));out+=struct.pack('<IQ',j,len(b))+b
    count,=r.num('<I');out+=struct.pack('<I',count)+r.take(count*36)
    count,n=r.num('<QQ');b=field_columns(r.take(n));out+=struct.pack('<QQ',count,len(b))+b
    r.done();return bytes(out)

def chunks(part):
    if isinstance(part,bytes):
        for i in range(0,len(part),CHUNK):yield part[i:i+CHUNK]
    else:
        with Path(part).open('rb') as f:
            yield from iter(lambda:f.read(CHUNK),b'')

def size(part):return len(part) if isinstance(part,bytes) else Path(part).stat().st_size

def measure(parts):

    c=zlib.compressobj(level=6,method=zlib.DEFLATED,wbits=15,memLevel=8,strategy=zlib.Z_DEFAULT_STRATEGY)
    d=zlib.decompressobj();raw=hashlib.sha256();back=hashlib.sha256();wire=hashlib.sha256()
    nb=nc=nd=0
    def encoded(b):
        nonlocal nc,nd
        nc+=len(b);wire.update(b);v=d.decompress(b);nd+=len(v);back.update(v)
    def feed(b):
        nonlocal nb
        nb+=len(b);raw.update(b);encoded(c.compress(b))
    feed(MAGIC+struct.pack('<I',len(parts)))
    for p in parts:
        feed(struct.pack('<Q',size(p)))
        for b in chunks(p):feed(b)
    encoded(c.flush());v=d.flush();nd+=len(v);back.update(v)
    require(d.eof and not d.unused_data and nd==nb and back.digest()==raw.digest(),'roundtrip failure')
    return dict(uncompressed_framed_bytes=nb,zlib6_response_bytes=nc,raw_sha256=raw.hexdigest(),
                compressed_sha256=wire.hexdigest(),roundtrip_verified=True,part_count=len(parts))
