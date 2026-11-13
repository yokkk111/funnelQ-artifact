#![allow(dead_code)]
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};
use std::{cell::{Cell, RefCell}, collections::{BTreeMap, BTreeSet, HashMap},
    fs::{self, File}, io::{BufWriter, Write}, path::{Path, PathBuf}, time::Instant};

                                                                           
                                                                                     
#[path = "../../FunnelQ_Residual_Obligation/src/model.rs"] mod model;
#[path = "../../FunnelQ_Residual_Obligation/src/query.rs"] mod query;
#[path = "../../FunnelQ_Residual_Obligation/src/oracle.rs"] mod oracle;
#[path = "plain_index.rs"] mod index;
mod reference;
mod tests;
mod evaluator {
    use crate::model::Scalar;
    use std::collections::{BTreeMap, BTreeSet};
    #[derive(Default)]
    pub struct EvaluationRequest {
        pub allowed_event_ids: Option<BTreeSet<String>>,
        pub fixed_atoms: BTreeMap<String, String>,
    }
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MatchRecord {
        pub query_id: String,
        pub branch_id: String,
        pub event_bindings: BTreeMap<String, String>,
        pub variable_bindings: BTreeMap<String, Scalar>,
    }
}

const SOURCE: &str = "FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin";
const QUERY: &str = "FunnelQ_CSR/nft/queries/nft/selected5/Q1_RAPID_SAME_NFT_ROUNDTRIP.json";
const SPEC: &str = "FunnelQ_CCC_Prototype/sp1/queries/q1.json";
const RULES: [&str; 6] = ["WholeBlockZero", "RangeCount", "CountFactorEliminate",
    "IndependentProduct", "Split", "SingletonExact"];

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Event { order: u64, time: i64, src: u32, dst: u32, nft: u32, col: u32, tx: u32 }
type Key = [u32; 4];                                                               
impl Event {
    fn key(self) -> Key { [self.col, self.nft, self.src, self.dst] }
}
fn reverse(k: Key) -> Key { [k[0], k[1], k[3], k[2]] }

struct Store {
    events: Vec<Event>,
    reads: Cell<u64>,
    seen: RefCell<Vec<bool>>,
    unique: Cell<u64>,
}
impl Store {
    fn new(events: Vec<Event>) -> Self {
        let n = events.len();
        Self { events, reads: Cell::new(0), seen: RefCell::new(vec![false;n]), unique: Cell::new(0) }
    }
    fn read(&self, i: u32) -> Event {
        self.reads.set(self.reads.get() + 1);
        let mut s = self.seen.borrow_mut();
        if !s[i as usize] { s[i as usize] = true; self.unique.set(self.unique.get() + 1); }
        self.events[i as usize]
    }
    fn reset(&self) { self.reads.set(0); self.unique.set(0); self.seen.borrow_mut().fill(false); }
}
#[derive(Clone, Copy, Debug)]
struct Bounds { min_o: u64, max_o: u64, min_t: i64, max_t: i64, min_tx: u32, max_tx: u32 }
fn bounds(store: &Store, ids: &[u32]) -> Bounds {
    let a = store.read(ids[0]);
    let mut b = Bounds { min_o:a.order,max_o:a.order,min_t:a.time,max_t:a.time,min_tx:a.tx,max_tx:a.tx };
    for &id in &ids[1..] {
        let a = store.read(id);
        b.min_o=b.min_o.min(a.order); b.max_o=b.max_o.max(a.order);
        b.min_t=b.min_t.min(a.time); b.max_t=b.max_t.max(a.time);
        b.min_tx=b.min_tx.min(a.tx); b.max_tx=b.max_tx.max(a.tx);
    }
    b
}
struct Group {
    key: Key, lo: u32, hi: u32, ids: Vec<u32>, tx: BTreeMap<u32, Vec<u32>>,
    b: Bounds, monotone: bool,
}
struct Prepared {
    store: Store, permutation: Vec<u32>, groups: Vec<Group>, directory: HashMap<Key,u32>,
    seconds: f64, canonical_reads: u64,
}
fn prepare(events: Vec<Event>) -> Result<Prepared> {
    let start = Instant::now();
    let store = Store::new(events);
    let mut permutation: Vec<_> = (0..store.events.len() as u32).collect();
    permutation.sort_unstable_by_key(|&i| { let e=store.read(i); (e.key(),e.order) });
    let mut directory=HashMap::new(); let mut groups=Vec::new(); let mut lo=0;
    while lo<permutation.len() {
        let key=store.read(permutation[lo]).key();
        let mut hi=lo+1;
        while hi<permutation.len() && store.read(permutation[hi]).key()==key { hi+=1; }
        let ids=permutation[lo..hi].to_vec();
        let b=bounds(&store,&ids);
        let mut tx:BTreeMap<u32,Vec<u32>>=BTreeMap::new();
        let mut monotone=true; let mut previous=None;
        for &id in &ids {
            let e=store.read(id);
            if let Some(t)=previous { monotone &= t<=e.time; }
            previous=Some(e.time);
            tx.entry(e.tx).or_default().push(id);
        }
        directory.insert(key,groups.len() as u32);
        groups.push(Group {key,lo:lo as u32,hi:hi as u32,ids,tx,b,monotone});
        lo=hi;
    }
    let canonical_reads=store.reads.get();
    store.reset();
    Ok(Prepared {store,permutation,groups,directory,seconds:start.elapsed().as_secs_f64(),canonical_reads})
}

#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
enum Rule { WholeBlockZero, RangeCount, CountFactorEliminate, IndependentProduct, Split, SingletonExact }
impl Rule { fn name(self)-> &'static str { RULES[self as usize] } }
#[derive(Serialize)]
struct Node {
    id:u32, parent:Option<u32>, depth:u32,
    anchor_rank_start:u32, anchor_rank_end:u32, domain_size:u64,
    binding_domain_size:u64,                                                
    source_group:Option<u32>, target_group:Option<u32>,
    rule:Rule, reason:&'static str, exact_contribution:u64,
    children:Vec<u32>, range_requests:Vec<u32>,
    own_canonical_event_reads:u64, own_unique_canonical_events:u64,
}
#[derive(Clone, Debug, Serialize, PartialEq, Eq, PartialOrd, Ord)]
struct RequestKey { group:u32, lower_order_exclusive:u64, lower_time_exclusive:i64,
    upper_time_inclusive:i64, equal_tx:Option<u32> }
#[derive(Serialize)]
struct RangeRequest {
    id:u32,node_id:u32, key:RequestKey, lower_rank:usize,upper_rank:usize,count:u64,
    canonical_event_reads:u64, attached_to_leaf:bool,
}
struct Engine {
    p:Prepared, delta:i64, nodes:Vec<Node>, requests:Vec<RangeRequest>,
    directory_lookups:u64, directory_keys:BTreeSet<Key>,
    force_exact:bool,
}
impl Engine {
    fn new(p:Prepared,delta:i64)->Self {
        Self {p,delta,nodes:vec![],requests:vec![],directory_lookups:0,directory_keys:BTreeSet::new(),force_exact:false}
    }
    fn upper_bound(&self,ids:&[u32],p:impl Fn(Event)->bool)->usize {
        let (mut lo,mut hi)=(0,ids.len());
        while lo<hi {let m=(lo+hi)/2;if p(self.p.store.read(ids[m])){lo=m+1}else{hi=m}}
        lo
    }
    fn request(&mut self,node:u32,group:u32,a:Event,tx:Option<u32>)->u32 {
        let before=self.p.store.reads.get();
        let g=&self.p.groups[group as usize];
        let ids:&[u32]=match tx {Some(t)=>g.tx.get(&t).map(Vec::as_slice).unwrap_or(&[]),None=>&g.ids};
        let lo_order=self.upper_bound(ids,|e|e.order<=a.order);
        let lo_time=self.upper_bound(ids,|e|e.time<=a.time);
        let lo=lo_order.max(lo_time);
        let hi=self.upper_bound(ids,|e|e.time<=a.time+self.delta).max(lo);
        let id=self.requests.len() as u32;
        self.requests.push(RangeRequest {id,node_id:node,key:RequestKey {group,
            lower_order_exclusive:a.order,lower_time_exclusive:a.time,upper_time_inclusive:a.time+self.delta,equal_tx:tx},
            lower_rank:lo,upper_rank:hi,count:(hi-lo) as u64,canonical_event_reads:self.p.store.reads.get()-before,
            attached_to_leaf:false});
        id
    }
    fn attach(&mut self,node:u32,ids:Vec<u32>) {
        for &r in &ids {self.requests[r as usize].attached_to_leaf=true;}
        self.nodes[node as usize].range_requests=ids;
    }
    fn add(&mut self,lo:u32,hi:u32,group:Option<u32>,parent:Option<u32>,depth:u32)->u32 {
        let id=self.nodes.len() as u32;
        self.nodes.push(Node{id,parent,depth,anchor_rank_start:lo,anchor_rank_end:hi,domain_size:(hi-lo) as u64,
            binding_domain_size:(hi-lo) as u64*self.p.store.events.len() as u64,
            source_group:group,target_group:None,rule:Rule::Split,reason:"pending",exact_contribution:0,
            children:vec![],range_requests:vec![],own_canonical_event_reads:0,own_unique_canonical_events:0});
        id
    }
    fn terminal(&mut self,id:u32,rule:Rule,reason:&'static str,count:u64,before:u64,unique:u64) {
        let n=&mut self.nodes[id as usize];
        n.rule=rule;n.reason=reason;n.exact_contribution=count;
        n.own_canonical_event_reads=self.p.store.reads.get()-before;
        n.own_unique_canonical_events=self.p.store.unique.get()-unique;
    }
    fn visit(&mut self,lo:u32,hi:u32,gid:u32,parent:u32,depth:u32)->u32 {
        let id=self.add(lo,hi,Some(gid),Some(parent),depth);
        let before=self.p.store.reads.get();let unique=self.p.store.unique.get();
        let key=self.p.groups[gid as usize].key;
        self.directory_lookups+=1; self.directory_keys.insert(reverse(key));
        let target=self.p.directory.get(&reverse(key)).copied();
        self.nodes[id as usize].target_group=target;
        if target.is_none() {
            self.terminal(id,Rule::WholeBlockZero,"complete_reverse_joint_key_absent",0,before,unique);return id;
        }
        let target=target.unwrap();
        let ab=bounds(&self.p.store,&self.p.permutation[lo as usize..hi as usize]);
        let bb=self.p.groups[target as usize].b;
        if bb.max_o<=ab.min_o || bb.max_t<=ab.min_t || bb.min_t>ab.max_t+self.delta {
            self.terminal(id,Rule::WholeBlockZero,"universal_order_or_time_disjointness",0,before,unique);return id;
        }
        if ab.min_tx==ab.max_tx && bb.min_tx==bb.max_tx && ab.min_tx==bb.min_tx {
            self.terminal(id,Rule::WholeBlockZero,"every_back_is_same_transaction",0,before,unique);return id;
        }
        if !self.force_exact && ab.max_o<bb.min_o && ab.max_t<bb.min_t && bb.max_t<=ab.min_t+self.delta
            && (ab.max_tx<bb.min_tx || bb.max_tx<ab.min_tx) {
            let count=(hi-lo) as u64*self.p.groups[target as usize].ids.len() as u64;
            self.terminal(id,Rule::IndependentProduct,"all_pairs_satisfy_all_constraints_and_tx_ranges_disjoint",count,before,unique);return id;
        }
        let source_monotone=self.p.groups[gid as usize].monotone;
        let target_monotone=self.p.groups[target as usize].monotone;
        if !self.force_exact && hi-lo>1 && ab.min_tx==ab.max_tx && source_monotone && target_monotone {
            let first=self.p.store.read(self.p.permutation[lo as usize]);
            let last=self.p.store.read(self.p.permutation[hi as usize-1]);
            let r1=self.request(id,target,first,None);
            let r2=self.request(id,target,last,None);
            let x=&self.requests[r1 as usize];let y=&self.requests[r2 as usize];
            if x.lower_rank==y.lower_rank && x.upper_rank==y.upper_rank {
                                                                                      
                                                                                        
                let all=x.count;
                let tx=self.request(id,target,first,Some(first.tx));
                let bad=self.requests[tx as usize].count;
                assert!(bad<=all);
                self.attach(id,vec![r1,r2,tx]);
                self.terminal(id,Rule::CountFactorEliminate,"identical_complete_back_interval_and_forbidden_tx",
                    (hi-lo) as u64*(all-bad),before,unique);return id;
            }
                                                                                            
        }
        if hi-lo==1 {
            let a=self.p.store.read(self.p.permutation[lo as usize]);
            if !self.force_exact && target_monotone {
                let all=self.request(id,target,a,None);
                let tx=self.request(id,target,a,Some(a.tx));
                let count=self.requests[all as usize].count.checked_sub(self.requests[tx as usize].count).unwrap();
                self.attach(id,vec![all,tx]);
                self.terminal(id,Rule::RangeCount,"exact_joint_key_order_time_range_minus_same_tx",count,before,unique);
            } else {
                let mut count=0;
                for &b in &self.p.groups[target as usize].ids {
                    if exact(a,self.p.store.read(b),self.delta) {count+=1;}
                }
                self.terminal(id,Rule::SingletonExact,"one_anchor_complete_back_enumeration",count,before,unique);
            }
            return id;
        }
        let mid=lo+(hi-lo)/2;
        self.nodes[id as usize].own_canonical_event_reads=self.p.store.reads.get()-before;
        self.nodes[id as usize].own_unique_canonical_events=self.p.store.unique.get()-unique;
        self.nodes[id as usize].reason="balanced_complete_anchor_rank_partition";
        let left=self.visit(lo,mid,gid,id,depth+1);
        let right=self.visit(mid,hi,gid,id,depth+1);
        self.nodes[id as usize].children=vec![left,right];
        self.nodes[id as usize].exact_contribution=self.nodes[left as usize].exact_contribution+self.nodes[right as usize].exact_contribution;
        id
    }
    fn build(&mut self)->Result<()> {
        let n=self.p.permutation.len() as u32;
        let root=self.add(0,n,None,None,0);
        ensure!(root==0,"root id");
        if n==0 {
            self.nodes[0].rule=Rule::WholeBlockZero;self.nodes[0].reason="empty_complete_anchor_domain";return Ok(());
        }
        self.nodes[0].reason="complete_schema_level_joint_key_partition";
        for g in 0..self.p.groups.len() {
            let (lo,hi)=(self.p.groups[g].lo,self.p.groups[g].hi);
            let id=self.visit(lo,hi,g as u32,0,1);
            self.nodes[0].children.push(id);
            self.nodes[0].exact_contribution+=self.nodes[id as usize].exact_contribution;
            if g%100000==0 {eprintln!("TREE groups={g}/{} nodes={} ranges={}",self.p.groups.len(),self.nodes.len(),self.requests.len());}
        }
        self.check_partitions()?;
        Ok(())
    }
    fn check_partitions(&self)->Result<()> {
        let m=self.p.store.events.len();
        ensure!(self.p.permutation.len()==m,"missing anchors");
        let mut seen=vec![false;m];
        for &i in &self.p.permutation {ensure!(!seen[i as usize],"duplicate anchor");seen[i as usize]=true;}
        ensure!(seen.iter().all(|x|*x),"anchor permutation incomplete");
        ensure!(self.nodes[0].anchor_rank_start==0 && self.nodes[0].anchor_rank_end as usize==m,"root incomplete");
        let mut incoming=vec![0usize;self.nodes.len()];
        let mut covered=0u64;let mut contribution=0u64;
        for n in &self.nodes {
            ensure!(n.domain_size==(n.anchor_rank_end-n.anchor_rank_start) as u64,"wrong node size");
            if n.rule==Rule::Split {
                ensure!(!n.children.is_empty(),"empty Split");
                let mut cursor=n.anchor_rank_start;let mut sum=0;
                for &i in &n.children {
                    let c=&self.nodes[i as usize];
                    incoming[i as usize]+=1;
                                                                                  
                                                                                    
                    ensure!(c.parent==Some(n.id) && c.anchor_rank_start==cursor && c.anchor_rank_end>cursor,"gap/overlap/foreign child");
                    ensure!(c.depth==n.depth+1,"bad depth");
                    cursor=c.anchor_rank_end;sum+=c.exact_contribution;
                }
                ensure!(cursor==n.anchor_rank_end && sum==n.exact_contribution,"incomplete union or bad sum");
            } else {
                ensure!(n.children.is_empty(),"terminal node has children");
                covered+=n.domain_size;contribution+=n.exact_contribution;
            }
        }
        ensure!(incoming[0]==0 && incoming[1..].iter().all(|x|*x==1),"disconnected or multiply-used node");
        ensure!(covered==m as u64 && contribution==self.nodes[0].exact_contribution,"leaves do not partition root");
        Ok(())
    }
    fn reconstruct_answers(&self)->Result<BTreeSet<[u64;2]>> {
        let mut out=BTreeSet::new();
        for n in self.nodes.iter().filter(|n|n.rule!=Rule::Split) {
            if n.exact_contribution==0 {continue;}
            let g=&self.p.groups[n.target_group.context("positive leaf without target")? as usize];
            let mut count=0;
            for p in n.anchor_rank_start..n.anchor_rank_end {
                let a=self.p.store.events[self.p.permutation[p as usize] as usize];
                for &b in &g.ids {
                    let b=self.p.store.events[b as usize];
                    if exact(a,b,self.delta) {
                        ensure!(out.insert([a.order,b.order]),"duplicate tree binding");
                        count+=1;
                    }
                }
            }
            ensure!(count==n.exact_contribution,"leaf {} claimed {} but reconstructed {}",n.id,n.exact_contribution,count);
        }
        ensure!(out.len() as u64==self.nodes[0].exact_contribution,"tree count mismatch");
        Ok(out)
    }
}
fn exact(a:Event,b:Event,d:i64)->bool {
    a.col==b.col && a.nft==b.nft && a.src==b.dst && a.dst==b.src && a.order<b.order
        && a.time<b.time && b.time<=a.time+d && a.order!=b.order && a.tx!=b.tx
}

fn load_query(root:&Path)->Result<(query::QuerySpec,i64)> {
    let doc:Value=serde_json::from_slice(&fs::read(root.join(QUERY))?)?;
    let mut q=query::compile_query_document(&doc)?;
    let delta=doc["full_binding_plan"]["delta_seconds"].as_i64().context("missing delta")?;
    q.scope=query::ScopeSpec::Episode{anchor_atom:"root".into(),window_seconds:delta};
    let canonical:query::QuerySpec=serde_json::from_slice(&fs::read(root.join(SPEC))?)?;
    let mut x=q.clone();let mut y=canonical.clone();
    for b in &mut x.branches {b.constraints.sort();}
    for b in &mut y.branches {b.constraints.sort();}
    ensure!(x==y,"formal query and current normalized QuerySpec differ");
    ensure!(q.result_semantics=="full_event_binding_v1" && q.branches.len()==1,"unsupported semantics");
    let b=&q.branches[0];
    ensure!(b.atoms.len()==2 && b.atoms[0].id=="root" && b.atoms[1].id=="back","not Q1 layout");
    ensure!(b.atoms.iter().all(|a|a.event_types==BTreeSet::from(["NFTTrade".into()]) && a.local_predicates.is_empty()),"type pushdown not safe");
    ensure!(!b.constraints.iter().any(|c|matches!(c,query::Constraint::NoEvent{..})),"cannot exclude nontrades under NoEvent");
                                                                             
    let fields=|src:&str,dst:&str|BTreeMap::from([
        ("collection_contract".into(),query::Term::Variable("collection".into())),
        ("nft".into(),query::Term::Variable("x".into())),
        ("move_from".into(),query::Term::Variable(src.into())),
        ("move_to".into(),query::Term::Variable(dst.into()))]);
    ensure!(b.atoms[0].fields==fields("A","B") && b.atoms[1].fields==fields("B","A"),"Q1 field mapping changed");
    let expected=vec![
        query::Constraint::Order{earlier:"root".into(),later:"back".into(),field:"order_seq".into()},
        query::Constraint::Order{earlier:"root".into(),later:"back".into(),field:"block_ts".into()},
        query::Constraint::Within{earlier:"root".into(),later:"back".into(),delta_seconds:delta},
        query::Constraint::DistinctEvents(vec!["root".into(),"back".into()]),
        query::Constraint::DistinctTransactions(vec!["root".into(),"back".into()])];
    ensure!(b.constraints.iter().collect::<BTreeSet<_>>()==expected.iter().collect::<BTreeSet<_>>() && b.event_injective,"Q1 predicate contract changed");
    Ok((q,delta))
}
fn load(root:&Path)->Result<(Vec<Event>,Value)> {
    let t=Instant::now();let bytes=fs::read(root.join(SOURCE))?;
    ensure!(bytes.len()%37==0 && bytes.len()/37==3_201_667,"not full three-month artifact");
    let mut es=Vec::new();let mut kinds=BTreeMap::new();
    for b in bytes.chunks_exact(37) {
        *kinds.entry(b[36]).or_insert(0u64)+=1;
        if b[36]!=1 {continue;}
        let u32at=|i|u32::from_le_bytes(b[i..i+4].try_into().unwrap());
        let order=u64::from_le_bytes(b[0..8].try_into().unwrap());
        let time=u64::from_le_bytes(b[8..16].try_into().unwrap());
        ensure!(order<=i64::MAX as u64 && time<=i64::MAX as u64-3600,"invalid integer range");
        es.push(Event{order,time:time as i64,src:u32at(16),dst:u32at(20),nft:u32at(24),col:u32at(28),tx:u32at(32)});
    }
    ensure!(es.len()==1_123_403,"wrong full anchor count");
    let mut ids:Vec<_>=es.iter().map(|e|e.order).collect();ids.sort_unstable();
    ensure!(ids.windows(2).all(|w|w[0]<w[1]),"duplicate canonical identity");
    ensure!(es.windows(2).all(|w|w[0].col<w[1].col || (w[0].col==w[1].col && w[0].order<w[1].order)),"bad source order");
    let metrics=json!({"source":SOURCE,"records":bytes.len()/37,"bytes":bytes.len(),"record_bytes":37,
        "event_kind_counts":kinds,"all_anchor_count":es.len(),"load_seconds":t.elapsed().as_secs_f64(),
        "source_rows_read":bytes.len()/37,"selection":"ONLY kind=NFTTrade; all such rows retained"});
    Ok((es,metrics))
}

#[cfg(windows)]
fn memory()->Value {
    #[repr(C)] #[derive(Default)]
    struct Counters {cb:u32,faults:u32,peak_rss:usize,rss:usize,peak_paged:usize,paged:usize,
        peak_nonpaged:usize,nonpaged:usize,commit:usize,peak_commit:usize,private:usize}
    #[link(name="kernel32")] extern "system" {fn GetCurrentProcess()->*mut std::ffi::c_void;}
    #[link(name="psapi")] extern "system" {fn GetProcessMemoryInfo(p:*mut std::ffi::c_void,c:*mut Counters,size:u32)->i32;}
    let mut c=Counters::default();c.cb=std::mem::size_of::<Counters>() as u32;
    let ok=unsafe {GetProcessMemoryInfo(GetCurrentProcess(),&mut c,c.cb)};
    json!({"available":ok!=0,"rss_bytes":c.rss,"peak_rss_bytes":c.peak_rss,"commit_bytes":c.commit,
        "peak_commit_bytes":c.peak_commit,"private_bytes":c.private})
}
#[cfg(not(windows))] fn memory()->Value{json!({"available":false})}
fn save(path:&Path,v:&impl Serialize)->Result<()> {
    let f=File::create(path)?;serde_json::to_writer_pretty(BufWriter::new(f),v)?;Ok(())
}
fn jsonl(path:&Path,rows:impl IntoIterator<Item=impl Serialize>)->Result<()> {
    let mut w=BufWriter::new(File::create(path)?);
    for row in rows {serde_json::to_writer(&mut w,&row)?;w.write_all(b"\n")?;}
    w.flush()?;Ok(())
}

fn main()->Result<()> {
    let args:Vec<_>=std::env::args().collect();
    let root=PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap().to_path_buf();
    let mode=args.get(1).map(String::as_str).unwrap_or("tree");
    let out=PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("results/full_once");
    fs::create_dir_all(&out)?;
    let (q,delta)=load_query(&root)?;
    if mode=="report" {return report(&out);}
    let filename=if mode=="reference" {"reference_summary.json"} else {"tree_summary.json"};
    ensure!(!out.join(filename).exists(),"completed run exists; no automatic rerun or overwrite");
    let start=Instant::now();
    let (es,source)=load(&root)?;
    eprintln!("LOADED complete epoch, anchors={}, mode={mode}",es.len());
    if mode=="reference" {
        let (ans,mut v)=reference::run(&es,&q,delta)?;
        v["source"]=source;v["memory"]=memory();v["wall_seconds"]=json!(start.elapsed().as_secs_f64());
        save(&out.join("reference_answers.json"),&ans)?;
        save(&out.join(filename),&v)?;
        eprintln!("REFERENCE COMPLETE count={} seconds={}",ans.len(),v["seconds"]);
        return Ok(());
    }
    ensure!(mode=="tree","unknown command");
    save(&out.join("query.normalized.json"),&q)?;
    let p=prepare(es)?;
    eprintln!("PREP complete groups={} seconds={:.3}",p.groups.len(),p.seconds);
    let mut engine=Engine::new(p,delta);
    let build=Instant::now();engine.build()?;let build_seconds=build.elapsed().as_secs_f64();
    let tree_memory=memory();
    let export=Instant::now();
    jsonl(&out.join("nodes.jsonl"),engine.nodes.iter())?;
    jsonl(&out.join("range_requests.jsonl"),engine.requests.iter())?;
    jsonl(&out.join("anchor_permutation.jsonl"),engine.p.permutation.iter().enumerate().map(|(rank,&i)|
        json!({"rank":rank,"trade_row":i,"event_id":engine.p.store.events[i as usize].order})))?;
    jsonl(&out.join("groups.jsonl"),engine.p.groups.iter().enumerate().map(|(id,g)|
        json!({"id":id,"key":g.key,"anchor_rank_start":g.lo,"anchor_rank_end":g.hi,"event_count":g.ids.len(),"time_monotone":g.monotone})))?;
    let export_seconds=export.elapsed().as_secs_f64();
    let answer_t=Instant::now();let answers=engine.reconstruct_answers()?;
    let answer_seconds=answer_t.elapsed().as_secs_f64();
    save(&out.join("tree_answers.json"),&answers)?;
    let mut summary=statistics(&engine)?;
    summary["source"]=source;summary["tree_build_seconds"]=json!(build_seconds);
    summary["index_prepare_seconds"]=json!(engine.p.seconds);
    summary["prepare_plus_tree_seconds"]=json!(engine.p.seconds+build_seconds);
    summary["export_seconds"]=json!(export_seconds);summary["answer_reconstruction_seconds"]=json!(answer_seconds);
    summary["memory_before_reference"]=tree_memory;summary["whole_process_memory"]=memory();
    summary["wall_seconds"]=json!(start.elapsed().as_secs_f64());
    save(&out.join(filename),&summary)?;
    eprintln!("TREE COMPLETE nodes={} count={} construction={:.3}s",engine.nodes.len(),answers.len(),engine.p.seconds+build_seconds);
    Ok(())
}

fn statistics(e:&Engine)->Result<Value> {
    let anchors=e.p.permutation.len() as u64;
    let leaves:Vec<_>=e.nodes.iter().filter(|n|n.rule!=Rule::Split).collect();
    let mut stats=vec![];
    for name in RULES {
        let ns:Vec<_>=leaves.iter().filter(|n|n.rule.name()==name).collect();
        let mass:u64=ns.iter().map(|n|n.domain_size).sum();
        stats.push(json!({"rule":name,"leaves":ns.len(),"covered_domain_size":mass,
            "avg_domain_per_leaf":if ns.is_empty(){0.0}else{mass as f64/ns.len() as f64},
            "max_domain_per_leaf":ns.iter().map(|n|n.domain_size).max().unwrap_or(0),
            "percent_total_domain":100.0*mass as f64/anchors.max(1) as f64,
            "exact_contribution":ns.iter().map(|n|n.exact_contribution).sum::<u64>()}));
    }
    let attached:Vec<_>=e.requests.iter().filter(|r|r.attached_to_leaf).collect();
    let distinct:BTreeSet<_>=e.requests.iter().map(|r|&r.key).collect();
    let attached_distinct:BTreeSet<_>=attached.iter().map(|r|&r.key).collect();
    let range_ids:BTreeSet<_>=leaves.iter().filter(|n|n.rule==Rule::RangeCount).map(|n|n.id).collect();
    let singleton:Vec<_>=leaves.iter().filter(|n|n.rule==Rule::SingletonExact).collect();
    let single_anchor:Vec<_>=leaves.iter().filter(|n|n.domain_size==1).collect();
    let smass:u64=singleton.iter().map(|n|n.domain_size).sum();
    let logical_objects=e.nodes.len()+attached.len()+e.directory_keys.len();
    let node_ratio=e.nodes.len() as f64/anchors.max(1) as f64;
    let object_ratio=logical_objects as f64/anchors.max(1) as f64;
    let single_ratio=single_anchor.len() as f64/anchors.max(1) as f64;
                                                                          
    let verdict=if node_ratio<=0.25 && object_ratio<=0.5 && single_ratio<=0.25 {"Go"}
        else if node_ratio<1.0 && object_ratio<1.5 && single_ratio<0.75 {"Conditional Go"} else {"No-Go"};
    ensure!(e.nodes.iter().map(|n|n.own_canonical_event_reads).sum::<u64>()==e.p.store.reads.get(),"node read accounting mismatch");
    Ok(json!({
        "simulator":"Q1CertifiedCountTreeSimulator","cryptographic_proof":false,"complete_three_month_data":true,
        "anchors":anchors,"tree_total_nodes":e.nodes.len(),"internal_nodes":e.nodes.len()-leaves.len(),"leaf_nodes":leaves.len(),
        "max_depth":e.nodes.iter().map(|n|n.depth).max().unwrap_or(0),"tree_count":e.nodes[0].exact_contribution,
        "root_domain":"complete anchor permutation [0,M); every node implicitly considers ALL epoch NFTTrade back events",
        "domain_mass_unit":"original anchors, NOT result count; binding mass is anchor mass times M",
        "split_disjoint_union_checks":e.nodes.len()-leaves.len(),"all_split_checks_passed":true,
        "leaf_rule_table":stats,"range_requests_on_range_count_leaves":e.requests.iter().filter(|r|range_ids.contains(&r.node_id)&&r.attached_to_leaf).count(),
        "range_requests_on_count_factor_leaves":attached.iter().filter(|r|!range_ids.contains(&r.node_id)).count(),
        "all_leaf_logical_range_requests":attached.len(),"all_attempted_logical_range_requests":e.requests.len(),
        "failed_rule_attempt_range_requests":e.requests.len()-attached.len(),
        "distinct_leaf_range_requests":attached_distinct.len(),"distinct_all_attempted_range_requests":distinct.len(),
        "directory_lookup_calls":e.directory_lookups,"distinct_directory_lookup_keys":e.directory_keys.len(),
        "canonical_event_reads_all_nodes":e.p.store.reads.get(),"unique_canonical_events_accessed_by_tree":e.p.store.unique.get(),
        "canonical_event_reads_index_preparation":e.p.canonical_reads,
        "singleton_exact_leaves":singleton.len(),"singleton_exact_anchor_mass":smass,
        "singleton_exact_anchor_percent":100.0*smass as f64/anchors.max(1) as f64,
        "singleton_exact_implicit_binding_mass":singleton.iter().map(|n|n.binding_domain_size).sum::<u64>(),
        "single_anchor_leaves_all_rules":single_anchor.len(),"single_anchor_domain_percent_all_rules":single_ratio*100.0,
        "tree_anchor_ratio":node_ratio,"logical_object_proxy":logical_objects,"logical_object_anchor_ratio":object_ratio,
        "structural_verdict":verdict,
        "verdict_rule":"Go: nodes/M<=.25, (nodes+leaf range requests+distinct directory lookups)/M<=.5, singleton-all-rule mass/M<=.25. Conditional: ratios <1,<1.5,<.75. Otherwise No-Go.",
        "count_factor_rule":"Only common exact back interval proven by monotone endpoint ranks AND identical forbidden tx; three explicit requests are recorded. Never an opaque per-anchor count loop.",
        "singleton_warning":"Zero SingletonExact does NOT mean compressed: singleton RangeCount/Zero/Product leaves are counted separately.",
        "scope":"Q1 structural feasibility only. Grouping/rule order fixed before full run; not a lower bound over all possible trees."
    }))
}
fn report(out:&Path)->Result<()> {
    let mut t:Value=serde_json::from_slice(&fs::read(out.join("tree_summary.json"))?)?;
    let r:Value=serde_json::from_slice(&fs::read(out.join("reference_summary.json"))?)?;
    let a:BTreeSet<[u64;2]>=serde_json::from_slice(&fs::read(out.join("tree_answers.json"))?)?;
    let b:BTreeSet<[u64;2]>=serde_json::from_slice(&fs::read(out.join("reference_answers.json"))?)?;
    let fp:Vec<_>=a.difference(&b).copied().collect();let fns:Vec<_>=b.difference(&a).copied().collect();
    let correctness=json!({"tree_count":t["tree_count"],"generic_exact_count":b.len(),
        "false_positive":fp.len(),"false_negative":fns.len(),"all_anchors_evaluated":r["anchors"]==t["anchors"],
        "count_equal":t["tree_count"].as_u64()==Some(b.len() as u64),"full_binding_set_equal":a==b});
    t["correctness"]=correctness.clone();t["generic_reference"]=r;
    save(&out.join("summary.json"),&t)?;
    save(&out.join("correctness.json"),&correctness)?;
    ensure!(fp.is_empty()&&fns.is_empty()&&correctness["count_equal"]==true&&correctness["all_anchors_evaluated"]==true,"correctness FAILED");
    let mut md=String::from("# Q1 完整域计数分解树：全量结构审计\n\n本实验不生成任何密码学证明。仅独立模拟器写入结果，正式系统只读。\n\n");
    md+=&format!("结论：**{}**。TreeCount = GenericExactCount = {}，FP = 0，FN = 0。\n\n",t["structural_verdict"].as_str().unwrap(),a.len());
    md+="## 总表\n\n| Anchors | Tree Nodes | Leaves | Range Requests | Singleton Leaves | Singleton Domain % | Tree/Anchor Ratio | Runtime | Peak Memory |\n|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n";
    md+=&format!("| {} | {} | {} | {} | {} | {:.4}% | {:.4} | {:.3} s | {:.2} MiB RSS |\n\n",
        t["anchors"],t["tree_total_nodes"],t["leaf_nodes"],t["all_leaf_logical_range_requests"],t["singleton_exact_leaves"],
        t["singleton_exact_anchor_percent"].as_f64().unwrap(),t["tree_anchor_ratio"].as_f64().unwrap(),
        t["prepare_plus_tree_seconds"].as_f64().unwrap(),t["memory_before_reference"]["peak_rss_bytes"].as_u64().unwrap_or(0) as f64/1048576.0);
    md+="Runtime 为独立索引准备 + 构树 + Split 检查；导出和全量 oracle 分开计时。表内 Singleton 是 SingletonExact；必须同时看下面跨所有规则的单 anchor 叶比例。\n\n";
    md+="## 核心规则表\n\n| Rule | #Leaves | Covered Domain Size | Avg Domain/Leaf | Max Domain/Leaf | % Total Domain |\n|---|---:|---:|---:|---:|---:|\n";
    for s in t["leaf_rule_table"].as_array().unwrap() {
        md+=&format!("| {} | {} | {} | {:.3} | {} | {:.4}% |\n",s["rule"].as_str().unwrap(),s["leaves"],s["covered_domain_size"],
            s["avg_domain_per_leaf"].as_f64().unwrap(),s["max_domain_per_leaf"],s["percent_total_domain"].as_f64().unwrap());
    }
    md+=&format!("\n原始 domain mass 的单位为完整 anchors。每个节点隐含 back 域是全部 NFTTrade；可重建的 binding mass = anchor mass × M，不能把命中数用作分母。\n\n\
## 不能隐藏的退化与开销\n\n\
- 所有规则合计的单 anchor 叶：{}，覆盖 {:.4}% anchors。\n\
- RangeCount 叶的逻辑范围请求：{}；CountFactorEliminate 叶：{}。\n\
- 叶上 distinct range requests：{}；包含失败规则尝试的范围请求总数：{}，distinct：{}。\n\
- 节点目录访问：{} 次，{} 个不同反向联合键。目录不存在判断也不是免费证明。\n\
- 构树 canonical event 行访问：{} 次，涉及 {} 条不同事件；索引准备：{} 次访问。\n\
- 节点 + 叶范围请求 + 不同目录键的逻辑对象代理值：{}，相当于 anchors 的 {:.4} 倍。它不是未来 VO 字节估计。\n\
- 最大深度：{}；内部节点：{}；Split 的不相交完整覆盖检查全部通过。\n\n",
        t["single_anchor_leaves_all_rules"],t["single_anchor_domain_percent_all_rules"].as_f64().unwrap(),
        t["range_requests_on_range_count_leaves"],t["range_requests_on_count_factor_leaves"],t["distinct_leaf_range_requests"],
        t["all_attempted_logical_range_requests"],t["distinct_all_attempted_range_requests"],t["directory_lookup_calls"],t["distinct_directory_lookup_keys"],
        t["canonical_event_reads_all_nodes"],t["unique_canonical_events_accessed_by_tree"],t["canonical_event_reads_index_preparation"],
        t["logical_object_proxy"],t["logical_object_anchor_ratio"].as_f64().unwrap(),t["max_depth"],t["internal_nodes"]);
    md+=&format!("## 计时与内存\n\n- 读取全量数据：{} s。\n- 索引准备：{} s。\n- 构树与完整覆盖检查：{} s。\n- 节点明细导出：{} s。\n- 答案独立展开：{} s。\n- 原 Generic Residual 精确 oracle：{} s（独立进程，全 anchors；不使用旧答案文件）。\n- 构树进程峰值内存：{}。\n\n",
        t["source"]["load_seconds"],t["index_prepare_seconds"],t["tree_build_seconds"],t["export_seconds"],
        t["answer_reconstruction_seconds"],t["generic_reference"]["seconds"],t["memory_before_reference"]);
    md+="## 固定规则与安全性\n\n完整输入仅按 NFTTrade 类型下推：Q1 两个原子均为 NFTTrade，且没有 NoEvent。读取全部三个月原始规范事件，没有 Cand2、hit mask、QueryKind synopsis、support directory 或冻结候选。\n\n\
根 Split 按单事件 (collection,NFT,src,dst) 联合键完整分组。组内顺序固定。规则优先级：完整反向键不存在 / 普遍时序无交集 / 全同交易 → IndependentProduct → 精确相同范围且同禁止交易的 CountFactorEliminate → 单 anchor RangeCount → Split；非单调索引无法准确范围计数时 SingletonExact 完整枚举。\n\n\
CountFactorEliminate 仅当首尾 anchor 导出的完整 back 范围位置完全相同，且所有 anchor 禁止交易相同才使用；保存首尾两个全范围请求和一个同交易请求。失败尝试单独计数，绝不把循环执行百万个请求藏在一个叶里。\n\n\
IndependentProduct 必须对所有组合满足角色/NFT/collection、严格 order、严格 time、窗口和交易不等；用不相交交易编号区间作为充分条件，不猜测独立性。\n\n\
Split 用稳定 anchor permutation 的半开区间表示。校验完整排列包含每个 anchor 一次；每个 Split 子区间必须前后连续、没有重叠、首尾等于父区间，且贡献和相等；每个非根节点恰有一个父节点。\n\n\
GenericExactCount 直接 source-include 原 oracle.rs；仅替换 index 接口为无认证纯内存适配器，不改变 oracle。逐 collection 处理完整 Episode。计数相等之外比较完整 (root_event_id,back_event_id) 集合。\n\n\
## 判定边界\n\n";
    md+=t["verdict_rule"].as_str().unwrap();
    md+="\n\n这些阈值是全量运行前固定的结构审计标准，不是理论最优性定理。即使 SingletonExact 为 0，大量单 anchor RangeCount/Zero 仍表示逻辑对象未显著缩小。本次只评价当前明确规则与布局；不能证明所有可能的树都失败，也不能因为正确就宣布有效。\n\n\
## 文件\n\nnodes.jsonl 保存每个节点的 domain 区间、size、规则、贡献、children、范围请求引用与自身事件读取；anchor_permutation.jsonl 恢复精确 anchor IDs；groups.jsonl 定义基础完整目录；range_requests.jsonl 保存每次实际请求；summary.json 保存完整指标和正确性。\n";
    fs::write(out.join("report.md"),md)?;
    println!("{}",serde_json::to_string_pretty(&t)?);
    Ok(())
}
