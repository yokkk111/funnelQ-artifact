import argparse
import json
import time
from pathlib import Path
import run as protocol


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--manifest',required=True);p.add_argument('--query',required=True)
    p.add_argument('--packet',required=True);p.add_argument('--output',required=True)
    args=p.parse_args()
    mon=protocol.base.Monitor()
    trusted=json.loads(Path(args.manifest).read_text())
    query=json.loads(Path(args.query).read_text())
    blob=Path(args.packet).read_bytes()
    protocol.ADAPTIVE=blob[:8]==b'DJADS002'
    protocol.MAGIC=b'DJADS002' if protocol.ADAPTIVE else b'DJADS001'
    db=protocol.duckdb.connect()
    db.execute("SET memory_limit='1GB'");db.execute('SET threads=4')
    start=time.perf_counter()
    blocks,domains,answers,stats=protocol.authenticate(blob,trusted,query)
    setup=protocol.tables(db,blocks,domains,query)
    t=time.perf_counter()
    count=int(db.execute(protocol.base.sql(query,True,True)).fetchone()[0])
    count_time=time.perf_counter()-t
    sound_time=protocol.sound_unique(blocks,answers,query)
    protocol.require(count==len(answers),'count seal')
    elapsed=time.perf_counter()-start
    result={**stats,'client_tables_s':setup,'count_only_s':count_time,
            'sound_unique_s':sound_time,'client_total_s':elapsed,
            'answer_count':count,'wire_bytes':len(blob),
            'peak_client_process_rss_bytes':max(mon.peak,protocol.base.psutil.Process().memory_info().rss),
            'source_events_loaded':False,'server_index_loaded':False,
            'trusted_inputs':['manifest','runtime query'],'untrusted_input':'packet',
            'excludes':'imports, DB initialization, disk reads, network, sealer verification'}
    Path(args.output).write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)
    db.close();mon.stop=True


if __name__=='__main__':main()
