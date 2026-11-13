import hashlib
import json
import struct
from pathlib import Path

METHODS = ('funnelq', 'mbft', 'vchain')
CASES = tuple([f'S{i}_w0' for i in range(1, 6)] +
              [f'E{i}_w{w}' for w in (60, 300, 600) for i in range(1, 6)])
CONTRACT = dict(version='defi_client_accept_e2e_v1', methods=METHODS, cases=CASES,
    repetitions=10, warmups_per_case_method=1, startup='fresh client for every invocation',
    timer='parent immediately before Popen to child monotonic timestamp at authenticated decision',
    includes='interpreter, imports, query/trusted inputs, proof/answer reads, native children, required handoff, setup, authentication, replay and result checks',
    excludes='sealing, proving, transmission, post-decision cleanup, oracle checks, report writes, process teardown',
    wire='response = proof bytes + answer bytes, count each physical byte once; request QuerySpec and pretrusted state separate',
    cache='frozen response files; ambient OS page cache allowed; no authenticated rows, decisions, candidate indexes or previous answers reused in client',
    preparation='honest frozen offline seals; native mechanisms differ; no chain RPC',
    affinity=[4,5,6,7], threads=4, timeout_seconds=600, memory_limit_bytes=64*2**30,
    memory='aggregate client process-tree RSS sampled every 50ms; not Windows Commit')

def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(8<<20),b''):h.update(b)
    return h.hexdigest()

def answer_id(rows):

    import numpy as np
    names=rows.dtype.names
    if not names or rows.ndim!=1 or any(rows.dtype[n].kind not in 'iu' for n in names):
        raise ValueError('invalid answer array')
    names=sorted(names)
    norm=np.empty(len(rows),dtype=[(n,'<i8') for n in names])
    for n in names:norm[n]=rows[n]
    norm.sort(order=names)
    if len(norm)>1 and np.any(norm[1:]==norm[:-1]):raise ValueError('duplicate answer')
    h=hashlib.sha256(json.dumps(names,separators=(',',':')).encode()+b'\0')
    h.update(norm.tobytes())
    return {'count':len(norm),'sha256':h.hexdigest()}

def packet_bytes(blob):

    offset=72
    def read(fmt):
        nonlocal offset
        vals=struct.unpack_from(fmt,blob,offset);offset+=struct.calcsize(fmt)
        return vals
    def skip(n):
        nonlocal offset
        offset+=n
        if offset>len(blob):raise ValueError('truncated packet')
    skip(read('<Q')[0])
    for _ in range(read('<I')[0]):skip(read('<IQ')[1])
    skip(read('<I')[0]*36)
    proof=offset;_,length=read('<QQ');skip(length)
    if offset!=len(blob):raise ValueError('unexpected packet length')
    return proof,16+length

def response(proof,result):
    if proof<0 or result<0:raise ValueError('negative byte count')
    return {'proof_bytes':proof,'answer_bytes':result,'response_bytes':proof+result}
