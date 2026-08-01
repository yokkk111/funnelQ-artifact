use anyhow::{bail, Context, Result};
use clap::Parser;
use rusqlite::{params, Connection, OpenFlags};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

const INF: i64 = i64::MAX;
const BUILDER_VERSION: &str = "generic_descriptor_builder_v3";

#[derive(Parser)]
struct Args {
    #[arg(long)] source_db: String,
    #[arg(long)] out_db: String,
    #[arg(long, default_value_t = 100_000)] progress_every: usize,
    #[arg(long, default_value_t = false)] force: bool,
}

#[derive(Clone, Copy)]
struct Edge { order:u32, event_type:u8, ts:i64, src:u32, dst:u32, nft:u32, collection:u32 }
#[derive(Clone, Copy)]
struct Summary {
    closure_id:i64, anchor_order:u32, start_ts:i64, end_ts:i64, end_order:u32,
    collection:u32, transition_count:u32, addr_count:u32, nft_count:u32,
    nft_ge2_count:u32, b_out_event_count:u32,
}
struct Runtime { edges:Vec<Edge>, order_to_index:HashMap<u32,usize>, by_src:HashMap<u64,Vec<usize>> }
fn posting_key(collection:u32, src:u32)->u64 { ((collection as u64)<<32)|src as u64 }
fn lower(posting:&[usize], edges:&[Edge], order:u32)->usize { posting.partition_point(|i| edges[*i].order<order) }
fn upper(posting:&[usize], edges:&[Edge], order:u32)->usize { posting.partition_point(|i| edges[*i].order<=order) }
impl Runtime {
    fn source_range(&self, collection:u32, src:u32, lo:u32, hi:u32)->&[usize] {
        let Some(p)=self.by_src.get(&posting_key(collection,src)) else { return &[]; };
        &p[lower(p,&self.edges,lo)..upper(p,&self.edges,hi)]
    }
    fn root(&self, order:u32)->Option<Edge>{ self.order_to_index.get(&order).map(|i|self.edges[*i]) }
}
#[derive(Clone,Copy)] struct Descriptor { l2_equal:i64,l2_different:i64,l3_all_equal:i64,l3_all_distinct:i64,l3_root_back_equal:i64 }
impl Descriptor { fn empty()->Self{Self{l2_equal:INF,l2_different:INF,l3_all_equal:INF,l3_all_distinct:INF,l3_root_back_equal:INF}} }
fn descriptor(runtime:&Runtime, summary:Summary, trade:u8)->(u8,Descriptor){
    let Some(root)=runtime.root(summary.anchor_order) else{return(0,Descriptor::empty())};
    if root.collection!=summary.collection || root.ts!=summary.start_ts || root.event_type!=trade { return(root.event_type,Descriptor::empty()); }
    let mut out=Descriptor::empty();
    for i1 in runtime.source_range(summary.collection,root.dst,root.order,summary.end_order){
        let e1=runtime.edges[*i1];
        if e1.event_type!=trade || e1.ts<=root.ts || e1.ts>summary.end_ts {continue;}
        let d1=e1.ts-root.ts;
        if e1.dst==root.src { if e1.nft==root.nft {out.l2_equal=out.l2_equal.min(d1)} else {out.l2_different=out.l2_different.min(d1)}; continue; }
        for i2 in runtime.source_range(summary.collection,e1.dst,e1.order,summary.end_order){
            let e2=runtime.edges[*i2];
            if e2.event_type!=trade || e2.ts<=e1.ts || e2.ts>summary.end_ts || e2.dst!=root.src {continue;}
            let d2=e2.ts-root.ts;
            if e1.nft==root.nft && e2.nft==root.nft {out.l3_all_equal=out.l3_all_equal.min(d2);}
            if e1.nft!=root.nft && e2.nft!=root.nft && e2.nft!=e1.nft {out.l3_all_distinct=out.l3_all_distinct.min(d2);}
            if e1.nft!=root.nft && e2.nft==root.nft {out.l3_root_back_equal=out.l3_root_back_equal.min(d2);}
        }
    }
    (root.event_type,out)
}

fn main()->Result<()> {
    let args=Args::parse(); let output_path=Path::new(&args.out_db);
    if output_path.exists(){ if !args.force{bail!("output already exists; pass --force")}; std::fs::remove_file(output_path)?; }
    if let Some(parent)=output_path.parent(){std::fs::create_dir_all(parent)?;}
    let source=Connection::open_with_flags(&args.source_db,OpenFlags::SQLITE_OPEN_READ_ONLY|OpenFlags::SQLITE_OPEN_URI)
        .with_context(||format!("open source {}",args.source_db))?;
    source.execute_batch("PRAGMA query_only=ON; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536;")?;
    for table in ["nft_anchor_directory","nft_closure_summary","nft_role_transition_edges","nft_event_type_dict"]{
        let count:i64=source.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",[table],|r|r.get(0))?;
        if count!=1{bail!("compact v4 source is missing required table {table}");}
    }
    let trade:u8=source.query_row("SELECT event_type_id FROM nft_event_type_dict WHERE event_type='NFTTrade'",[],|r|r.get(0))?;
    let started=Instant::now();
    let mut edges=Vec::new(); let mut order_to_index=HashMap::new(); let mut by_src:HashMap<u64,Vec<usize>>=HashMap::new();
    let mut st=source.prepare("SELECT order_seq,event_type_id,block_ts,src_addr_id,dst_addr_id,nft_id,collection_id FROM nft_role_transition_edges ORDER BY collection_id,order_seq")?;
    for row in st.query_map([],|r|Ok(Edge{order:r.get(0)?,event_type:r.get(1)?,ts:r.get(2)?,src:r.get(3)?,dst:r.get(4)?,nft:r.get(5)?,collection:r.get(6)?}))?{
        let e=row?; let idx=edges.len(); if order_to_index.insert(e.order,idx).is_some(){bail!("duplicate order_seq")};
        by_src.entry(posting_key(e.collection,e.src)).or_default().push(idx); edges.push(e);
    }
    eprintln!("[descriptor] loaded events={} elapsed={:.2}s",edges.len(),started.elapsed().as_secs_f64());
    let runtime=Runtime{edges,order_to_index,by_src};
    let anchor_ids:HashSet<i64>={
        let mut statement=source.prepare("SELECT closure_id FROM nft_anchor_directory")?;
        let rows=statement.query_map([],|r|r.get::<_,i64>(0))?;
        let mut result=HashSet::new();
        for row in rows { result.insert(row?); }
        result
    };

    let mut output=Connection::open(output_path)?;
    output.execute_batch(r#"
      PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF; PRAGMA locking_mode=EXCLUSIVE;
      CREATE TABLE formal_closure_descriptor(
        closure_position INTEGER PRIMARY KEY,is_anchor INTEGER NOT NULL CHECK(is_anchor IN(0,1)),
        closure_id INTEGER NOT NULL UNIQUE,anchor_order_seq INTEGER NOT NULL UNIQUE,start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,end_order_seq INTEGER NOT NULL,collection_id INTEGER NOT NULL,
        transition_count INTEGER NOT NULL,addr_count INTEGER NOT NULL,nft_count INTEGER NOT NULL,nft_ge2_count INTEGER NOT NULL,b_out_event_count INTEGER NOT NULL,root_event_type_id INTEGER NOT NULL,
        l2_ba_nft_equal_min_delta INTEGER NOT NULL,l2_ba_nft_different_min_delta INTEGER NOT NULL,l3_bcca_all_equal_min_delta INTEGER NOT NULL,l3_bcca_all_distinct_min_delta INTEGER NOT NULL,l3_bcca_root_back_equal_min_delta INTEGER NOT NULL);
      CREATE TABLE formal_anchor_descriptor(
        position INTEGER PRIMARY KEY,closure_id INTEGER NOT NULL UNIQUE,anchor_order_seq INTEGER NOT NULL UNIQUE,start_ts INTEGER NOT NULL,end_ts INTEGER NOT NULL,end_order_seq INTEGER NOT NULL,collection_id INTEGER NOT NULL,
        transition_count INTEGER NOT NULL,addr_count INTEGER NOT NULL,nft_count INTEGER NOT NULL,nft_ge2_count INTEGER NOT NULL,b_out_event_count INTEGER NOT NULL,root_event_type_id INTEGER NOT NULL,
        l2_ba_nft_equal_min_delta INTEGER NOT NULL,l2_ba_nft_different_min_delta INTEGER NOT NULL,l3_bcca_all_equal_min_delta INTEGER NOT NULL,l3_bcca_all_distinct_min_delta INTEGER NOT NULL,l3_bcca_root_back_equal_min_delta INTEGER NOT NULL);
      CREATE TABLE descriptor_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL) WITHOUT ROWID;
    "#)?;
    let tx=output.transaction()?;
    let mut insert_closure=tx.prepare("INSERT INTO formal_closure_descriptor VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")?;
    let mut insert_anchor=tx.prepare("INSERT INTO formal_anchor_descriptor VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")?;
    let mut summaries=source.prepare("SELECT closure_id,anchor_order_seq,start_ts,end_ts,end_order_seq,collection_id,transition_count,addr_count,nft_count,nft_ge2_count,b_out_event_count FROM nft_closure_summary ORDER BY anchor_order_seq,closure_id")?;
    let rows=summaries.query_map([],|r|Ok(Summary{closure_id:r.get(0)?,anchor_order:r.get(1)?,start_ts:r.get(2)?,end_ts:r.get(3)?,end_order:r.get(4)?,collection:r.get(5)?,transition_count:r.get(6)?,addr_count:r.get(7)?,nft_count:r.get(8)?,nft_ge2_count:r.get(9)?,b_out_event_count:r.get(10)?}))?;
    let mut closure_count=0usize; let mut anchor_count=0usize;
    for row in rows{
        let s=row?; let is_anchor=anchor_ids.contains(&s.closure_id); let (root_type,d)=descriptor(&runtime,s,trade);
        insert_closure.execute(params![closure_count as i64,if is_anchor{1}else{0},s.closure_id,s.anchor_order,s.start_ts,s.end_ts,s.end_order,s.collection,s.transition_count,s.addr_count,s.nft_count,s.nft_ge2_count,s.b_out_event_count,root_type,d.l2_equal,d.l2_different,d.l3_all_equal,d.l3_all_distinct,d.l3_root_back_equal])?;
        if is_anchor { insert_anchor.execute(params![anchor_count as i64,s.closure_id,s.anchor_order,s.start_ts,s.end_ts,s.end_order,s.collection,s.transition_count,s.addr_count,s.nft_count,s.nft_ge2_count,s.b_out_event_count,root_type,d.l2_equal,d.l2_different,d.l3_all_equal,d.l3_all_distinct,d.l3_root_back_equal])?; anchor_count+=1; }
        closure_count+=1;
        if args.progress_every>0 && closure_count%args.progress_every==0{eprintln!("[descriptor] closures={} anchors={} elapsed={:.2}s",closure_count,anchor_count,started.elapsed().as_secs_f64());}
    }
    if anchor_count!=anchor_ids.len(){bail!("Anchor directory contains closure ids absent from summaries");}
    drop(insert_closure); drop(insert_anchor);
    tx.execute("INSERT INTO descriptor_meta VALUES('complete','true')",[])?;
    tx.execute("INSERT INTO descriptor_meta VALUES('descriptor_vocabulary','generic_role_nft_relation_min_delta_v1')",[])?;
    tx.execute("INSERT INTO descriptor_meta VALUES('anchor_count',?)",[anchor_count.to_string()])?;
    tx.execute("INSERT INTO descriptor_meta VALUES('closure_count',?)",[closure_count.to_string()])?;
    tx.execute("INSERT INTO descriptor_meta VALUES('builder_version',?)",[BUILDER_VERSION])?;
    tx.commit()?;
    output.execute_batch("CREATE INDEX idx_formal_anchor_order ON formal_anchor_descriptor(anchor_order_seq);CREATE INDEX idx_formal_anchor_collection_order ON formal_anchor_descriptor(collection_id,anchor_order_seq);CREATE INDEX idx_formal_closure_order ON formal_closure_descriptor(anchor_order_seq);CREATE INDEX idx_formal_closure_collection_order ON formal_closure_descriptor(collection_id,anchor_order_seq);ANALYZE;")?;
    println!("{{\"status\":\"ok\",\"builderVersion\":\"{}\",\"anchorCount\":{},\"closureCount\":{},\"elapsedSeconds\":{:.6}}}",BUILDER_VERSION,anchor_count,closure_count,started.elapsed().as_secs_f64());
    Ok(())
}
