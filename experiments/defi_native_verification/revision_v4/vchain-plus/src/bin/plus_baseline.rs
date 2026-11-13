                                                                                
                                                                                
                                                                             
use anyhow::{ensure,bail,Context,Result};
use vchain_plus::{SimChain,acc::{AccPublicKey,AccSecretKey},chain::{Parameter,
    block::{Height,BlockHead,BlockContent,build::build_block},object::Object,
    id_tree::{IdTreeNode,IdTreeNodeId},bplus_tree::{BPlusTreeNode,BPlusTreeNodeId},trie_tree::{TrieNode,TrieNodeId},
    traits::ReadInterface,query::{query,query_param::{QueryParam,Node}},strict::verify_expected},digest::{Digest,Digestible},utils::binary_encode};
use serde::{Serialize,Deserialize};
use std::convert::TryInto;
use std::{collections::{HashMap,HashSet,BTreeSet},fs::{self,OpenOptions},io::Write,path::{Path,PathBuf},time::Instant};

#[derive(Clone,Copy,Serialize,Deserialize)]struct Event{order:u64,time:u32,src:u32,dst:u32,nft:u32,col:u32,tx:u32,kind:u8}
impl Event {
 fn object(self,block:u32)->Object<u32>{Object::new(Height(block),vec![self.time,self.order as u32,(self.order>>32) as u32,self.src,self.dst,self.nft,self.col,self.tx,self.kind as u32],HashSet::from([format!("type:{}",self.kind)]))}
 fn from_object(o:&Object<u32>)->Result<Self>{ensure!(o.num_data.len()==9,"invalid schema");let n=&o.num_data;ensure!(n[8]<=255,"bad type");let e=Self{time:n[0],order:n[1] as u64|((n[2]as u64)<<32),src:n[3],dst:n[4],nft:n[5],col:n[6],tx:n[7],kind:n[8] as u8};ensure!(o.keyword_data==HashSet::from([format!("type:{}",e.kind)]),"keyword/payload inconsistency");Ok(e)}
}
fn answers(es:&[Event])->BTreeSet<[u64;2]>{
 let mut groups:HashMap<[u32;4],Vec<Event>>=HashMap::new();
 for &e in es.iter().filter(|e|e.kind==1){groups.entry([e.col,e.nft,e.src,e.dst]).or_default().push(e);}
 let mut result=BTreeSet::new();
 for a in es.iter().filter(|e|e.kind==1){if let Some(bs)=groups.get(&[a.col,a.nft,a.dst,a.src]){for b in bs {
 if a.order<b.order && a.time<b.time && (b.time as u64)<=a.time as u64+1800 && a.tx!=b.tx {result.insert([a.order,b.order]);}
 }}}
 result
}
#[derive(Clone,Serialize,Deserialize)]struct Headers{param:Parameter,headers:HashMap<Height,BlockHead>}
impl ReadInterface for Headers {
 type K=u32;
 fn get_parameter(&self)->Result<Parameter>{Ok(self.param.clone())}
 fn read_block_head(&self,h:Height)->Result<BlockHead>{self.headers.get(&h).cloned().context("untrusted/missing header")}
 fn read_block_content(&self,_:Height)->Result<BlockContent>{bail!("client cannot read block contents")}
 fn read_id_tree_node(&self,_:IdTreeNodeId)->Result<IdTreeNode>{bail!("client cannot read ID index")}
 fn read_bplus_tree_node(&self,_:BPlusTreeNodeId)->Result<BPlusTreeNode<u32>>{bail!("client cannot read range index")}
 fn read_trie_node(&self,_:TrieNodeId)->Result<TrieNode>{bail!("client cannot read keyword index")}
 fn read_object(&self,_:Digest)->Result<Object<u32>>{bail!("client cannot read server objects")}
}
fn save(path:&Path,v:&impl Serialize)->Result<()>{let mut f=OpenOptions::new().create_new(true).write(true).open(path)?;f.write_all(&serde_json::to_vec_pretty(v)?)?;Ok(())}
fn rawsave(path:&Path,b:&[u8])->Result<()>{OpenOptions::new().create_new(true).write(true).open(path)?.write_all(b)?;Ok(())}
fn disk(p:&Path)->u64{fs::read_dir(p).into_iter().flatten().flatten().map(|e|{let p=e.path();if p.is_dir(){disk(&p)}else{e.metadata().map(|m|m.len()).unwrap_or(0)}}).sum()}
fn main()->Result<()> {
 let args:Vec<_>=std::env::args().collect();ensure!(args.len()==6||args.len()==7,"source out row_limit q block_rows [sealed_run_to_reuse]");
 let reuse=args.get(6).map(PathBuf::from);
 let source=PathBuf::from(&args[1]);let out=PathBuf::from(&args[2]);let limit:usize=args[3].parse()?;let q:u64=args[4].parse()?;let block_rows:usize=args[5].parse()?;
 ensure!(!out.exists(),"output exists; refusing overwrite");fs::create_dir_all(&out)?;
                                                                                         
 ensure!(q>=17&&q<=65536,"q out of bounds");let max_id=(q-1) as u16;ensure!((max_id as u64)==q-1 && max_id.is_power_of_two()&&max_id.trailing_zeros()%2==0,"q-1 must be power of four");
 ensure!(block_rows>0 && 2*block_rows<=max_id as usize,"active window exceeds cyclic ID capacity");
 let bytes=fs::read(&source)?;ensure!(bytes.len()%37==0,"incomplete canonical input");let n=if limit==0{bytes.len()/37}else{limit.min(bytes.len()/37)};
 let mut es=Vec::with_capacity(n);let mut identities=HashSet::new();
 for b in bytes.chunks_exact(37).take(n){let get=|i|u32::from_le_bytes(b[i..i+4].try_into().unwrap());let order=u64::from_le_bytes(b[..8].try_into()?);let time=u64::from_le_bytes(b[8..16].try_into()?);ensure!(time<=u32::MAX as u64,"timestamp cannot fit native range field");ensure!(identities.insert(order),"duplicate canonical event ID");es.push(Event{order,time:time as u32,src:get(16),dst:get(20),nft:get(24),col:get(28),tx:get(32),kind:b[36]});}
 drop(bytes);drop(identities);
                                                                          
                                                                              
 let param=Parameter{time_win_sizes:vec![1,2],id_tree_fanout:4,max_id_num:max_id,bplus_tree_fanout:4,num_dim:0};
 save(&out.join("config.json"),&serde_json::json!({"source":source,"rows":n,"full_dataset":limit==0,"q":q,"block_rows":block_rows,"index_blocks":"fixed canonical row pages, not native chain blocks","window_semantics":"Episode predicates evaluated on original time and order","query":"Q1 full_event_binding_v1 delta=1800","setup":"fresh local trusted setup; no secret serialized; not audited MPC"}))?;
 let t=Instant::now();let pk=if let Some(old)=&reuse {let cfg:serde_json::Value=serde_json::from_slice(&fs::read(old.join("config.json"))?)?;ensure!(cfg["rows"]==n&&cfg["q"]==q&&cfg["block_rows"]==block_rows,"reuse configuration mismatch");bincode::deserialize(&fs::read(old.join("public_key.bin"))?)?}else{let sk: vchain_plus::acc::AccSecretKeyWithPowCache=AccSecretKey::rand(rand::rngs::OsRng).into();AccPublicKey::gen_key(&sk,q)};let setup_s=t.elapsed().as_secs_f64();
 let public_bytes=bincode::serialize(&pk)?;rawsave(&out.join("public_key.bin"),&public_bytes)?;
 let db_path=reuse.as_ref().unwrap_or(&out).join("db");let mut chain=if reuse.is_some(){SimChain::open(&db_path)?}else{SimChain::create(&db_path,param.clone())?};let mut heads:Headers=if let Some(old)=&reuse{bincode::deserialize(&fs::read(old.join("trusted_headers.bin"))?)?}else{Headers{param,headers:HashMap::new()}};let mut prev=Digest::zero();let t=Instant::now();
 if reuse.is_none(){for (i,part) in es.chunks(block_rows).enumerate(){let height=(i+1)as u32;let objects=part.iter().map(|e|e.object(height)).collect();let (h,_)=build_block(Height(height),prev,objects,&mut chain,&heads.param,&pk)?;prev=h.to_digest();heads.headers.insert(Height(height),h);if i%100==0 {println!("BUILD {} / {} rows {:.3}s",(i+1)*block_rows,n,t.elapsed().as_secs_f64());std::io::stdout().flush()?;}}}
 let build_s=if reuse.is_none(){t.elapsed().as_secs_f64()}else{0.0};let head_wire=bincode::serialize(&heads)?;rawsave(&out.join("trusted_headers.bin"),&head_wire)?;
 save(&out.join("stage_build.json"),&serde_json::json!({"build_s":build_s,"reuse":reuse,"db_path":db_path,"rows":n}))?;println!("STAGE exact start");std::io::stdout().flush()?;
 let t=Instant::now();let claimed=answers(&es);let server_exact_s=t.elapsed().as_secs_f64();save(&out.join("stage_exact.json"),&serde_json::json!({"seconds":server_exact_s,"answers":claimed.len()}))?;println!("STAGE native recall/proof start");std::io::stdout().flush()?;
 let req=QueryParam{start_blk:1,end_blk:heads.headers.len()as u32,range:vec![],keyword_exp:Some(Node::Input("type:1".into()))};
 let t=Instant::now();let (res,dag,_) =query(false,false,&chain,req.clone(),&pk)?;let server_recall_proof_s=t.elapsed().as_secs_f64();save(&out.join("stage_native.json"),&serde_json::json!({"seconds":server_recall_proof_s,"responses":res.len()}))?;println!("STAGE serialize start");std::io::stdout().flush()?;
 let t=Instant::now();let claimed_rows:Vec<_>=claimed.iter().copied().collect();let wire=bincode::serialize(&(&req,&res,&dag,&claimed_rows))?;let compressed=binary_encode(&(&req,&res,&dag,&claimed_rows))?;let serialize_s=t.elapsed().as_secs_f64();rawsave(&out.join("response.bin"),&wire)?;rawsave(&out.join("response.snappy"),&compressed)?;
 let t=Instant::now();let (received_req,received_res,received_dag,received_rows):(QueryParam<u32>,Vec<(HashMap<vchain_plus::chain::id_tree::ObjId,Object<u32>>,vchain_plus::chain::verify::vo::VO<u32>)>,petgraph::Graph<vchain_plus::chain::query::query_dag::DagNode<u32>,bool>,Vec<[u64;2]>)=bincode::deserialize(&wire)?;
 let received_claimed: BTreeSet<_>=received_rows.iter().copied().collect();ensure!(received_rows.len()==received_claimed.len(),"duplicate answer binding");
 ensure!(received_req==req,"query changed");let decode_s=t.elapsed().as_secs_f64();let t=Instant::now();verify_expected(&heads,&req,&received_res,&received_dag,&pk)?;let client_auth_s=t.elapsed().as_secs_f64();
 let t=Instant::now();let mut client_events=HashMap::new();for(o,_)in &received_res{for obj in o.values(){let e=Event::from_object(obj)?;if let Some(old)=client_events.insert(e.order,e){ensure!(bincode::serialize(&old)?==bincode::serialize(&e)?,"conflicting event across windows");}}}
 let client_events:Vec<_>=client_events.into_values().collect();let exact=answers(&client_events);ensure!(exact==received_claimed,"full answer set mismatch");let client_exact_s=t.elapsed().as_secs_f64();
 let fp=exact.difference(&claimed).count();let fn_=claimed.difference(&exact).count();save(&out.join("answers.json"),&exact)?;
 let mem=fs::read_to_string("/proc/self/status").unwrap_or_default();
 let e2e=server_exact_s+server_recall_proof_s+serialize_s+decode_s+client_auth_s+client_exact_s;
 let result=serde_json::json!({"rows":n,"trade_rows":es.iter().filter(|e|e.kind==1).count(),"client_unique_events":client_events.len(),"answers":exact.len(),"fp_against_server":fp,"fn_against_server":fn_,"setup_s":setup_s,"public_parameter_bytes":public_bytes.len(),"trusted_header_bytes":head_wire.len(),"build_s":build_s,"db_bytes":disk(&db_path),"server_exact_s":server_exact_s,"server_recall_including_proof_s":server_recall_proof_s,"serialization_compression_s":serialize_s,"client_decode_s":decode_s,"client_auth_s":client_auth_s,"client_exact_and_compare_s":client_exact_s,"online_compute_e2e_s":e2e,"response_bytes":wire.len(),"response_snappy_bytes":compressed.len(),"subresponses":res.len(),"host_memory":mem,"network_measured":false,"gpu":false,"optimizations":{"egg":false,"pruned_empty_frontier":false},"generic_oracle_comparison":"must be checked by experiment driver, not inferred from server agreement"});
 save(&out.join("result.json"),&result)?;println!("RESULT {}",result);Ok(())
}

#[cfg(test)]mod tests{
 use super::*;
 #[test]fn q1_complete_bindings_and_boundaries(){
  let mut es=Vec::new();for i in 0..80u64{es.push(Event{order:i+1,time:1000+(i%9)as u32*300,src:(i%3)as u32,dst:((i+1)%3)as u32,nft:(i%2)as u32,col:1,tx:(i%7)as u32,kind:1});}
                                                                               
                                                                      
  for i in 0..10u64{es.push(Event{order:100+i,time:1000+(i as u32)*300,src:1,dst:0,nft:0,col:if i==8{2}else{1},tx:if i==7{0}else{99},kind:if i==9{2}else{1}});}
  let mut expected=BTreeSet::new();for a in &es{for b in &es{if a.kind==1&&b.kind==1&&a.col==b.col&&a.nft==b.nft&&a.src==b.dst&&a.dst==b.src&&a.order<b.order&&a.time<b.time&&b.time as u64<=a.time as u64+1800&&a.tx!=b.tx{expected.insert([a.order,b.order]);}}}
  assert!(!expected.is_empty());assert_eq!(answers(&es),expected);
  for &e in &es{assert_eq!(bincode::serialize(&e).unwrap(),bincode::serialize(&Event::from_object(&e.object(1)).unwrap()).unwrap());}
 }
}
