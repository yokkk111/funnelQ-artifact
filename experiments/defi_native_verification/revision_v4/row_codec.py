import struct
from pathlib import Path
import numpy as np
from legacy_evaluate import EVENT

MAGIC = b'DFROWS4\0'
WIRE = np.dtype([(name, '<i8') for name in EVENT.names])

def loadrows(paths):
    arrays = []
    for path in paths:
        path = Path(path)
        with path.open('rb') as f:
            header = f.read(16)
            if len(header) != 16 or header[:8] != MAGIC:
                raise ValueError('invalid authenticated-row handoff header')
            count, = struct.unpack('<Q', header[8:])
            if path.stat().st_size != 16 + count * WIRE.itemsize:
                raise ValueError('truncated or trailing authenticated-row handoff')
            raw = np.fromfile(f, dtype=WIRE, count=count)
        for name in EVENT.names:
            bounds = np.iinfo(EVENT[name])
            if np.any(raw[name] < bounds.min) or np.any(raw[name] > bounds.max):
                raise ValueError('out-of-range event field: ' + name)
        if np.any(raw['eid'] < 0) or np.any(raw['time'] < 0):
            raise ValueError('negative event identity or timestamp')
        if np.any(raw['kind'] > 2) or np.any(~np.isin(raw['sign'], [-1, 0, 1])):
            raise ValueError('invalid event category')
        arrays.append(raw.astype(EVENT))
    a = np.concatenate(arrays) if arrays else np.empty(0, dtype=EVENT)
    if len(np.unique(a['eid'])) != len(a):
        raise ValueError('duplicate authenticated event identity')
    return a
