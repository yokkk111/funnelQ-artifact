import json
import subprocess

class HGMatch:
    def __init__(self, executable, db, scope, out):
        self.executable,self.db,self.scope,self.out=executable,db,scope,out
        self.worker=None
    def prepare(self):
        self.log=(self.out/'native_stderr.log').open('x')
        self.worker=subprocess.Popen([str(self.executable),str(self.db)],stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,stderr=self.log,text=True,encoding='utf-8')
        ready=json.loads(self.worker.stdout.readline())
        if ready != {'frame':'ready','load_count':1,'query_independent':True}: raise ValueError('HGMatch startup')
        self.sequence=0
        return {**ready,'native_pid':self.worker.pid,'unfiltered_source_load':True}
    def execute(self,q,out):
        self.sequence+=1
        self.worker.stdin.write(json.dumps({'request_id':self.sequence,'query':q})+'\n');self.worker.stdin.flush()
        r=json.loads(self.worker.stdout.readline())
        if r.get('frame')!='result' or r.get('request_id')!=self.sequence: raise ValueError('HGMatch protocol')
        return int(r['count']),{'engine':'original_hgmatch','native_pid':self.worker.pid,'request_id':self.sequence}
    def close(self):
        if self.worker:
            if self.worker.poll() is None:
                try:
                    self.worker.stdin.write('{"op":"close"}\n');self.worker.stdin.flush();self.worker.wait(timeout=10)
                except (BrokenPipeError,subprocess.TimeoutExpired): self.worker.kill();self.worker.wait()
            self.worker.stdin.close();self.worker.stdout.close()
        if hasattr(self,'log'):self.log.close()
