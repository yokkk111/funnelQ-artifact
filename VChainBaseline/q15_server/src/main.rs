                                                                                          
                                                                               
#![allow(dead_code)]
#[path="../../../FunnelQ_Residual_Obligation/src/model.rs"] mod model;
#[path="../../../FunnelQ_Residual_Obligation/src/query.rs"] mod query;
#[path="../../../FunnelQ_Runtime_Verification/src/client_count.rs"] mod client_count;
use anyhow::{ensure,bail,Context,Result};
use serde::{Serialize,Deserialize};
use serde_json::{Value,json};
use std::{collections::{HashMap,HashSet,BTreeMap,BTreeSet},fs,io::{Read,Write},path::{Path,PathBuf},time::Instant};
use vchain_plus::{SimChain,acc::AccPublicKey,chain::{Parameter,block::{Height,BlockHead,BlockContent},object::Object,id_tree::{IdTreeNode,IdTreeNodeId,ObjId},bplus_tree::{BPlusTreeNode,BPlusTreeNodeId},trie_tree::{TrieNode,TrieNodeId},traits::ReadInterface,query::{query as native_query,query_param::{QueryParam,Node},query_dag::DagNode},strict::verify_expected,verify::vo::VO},digest::Digest};
use model::{CanonicalEvent,Scalar};
use query::{QuerySpec,ScopeSpec,Constraint};
type Answers=BTreeSet<(String,Vec<u64>)>;
#[derive(Serialize,Deserialize)]struct Headers{param:Parameter,headers:HashMap<Height,BlockHead>}
impl ReadInterface for Headers{
 type K=u32;
 fn get_parameter(&self)->Result<Parameter>{Ok(self.param.clone())}
 fn read_block_head(&self,h:Height)->Result<BlockHead>{self.headers.get(&h).cloned().context("missing trusted header")}
 fn read_block_content(&self,_:Height)->Result<BlockContent>{bail!("client has no contents")}
 fn read_id_tree_node(&self,_:IdTreeNodeId)->Result<IdTreeNode>{bail!("client has no index")}
 fn read_bplus_tree_node(&self,_:BPlusTreeNodeId)->Result<BPlusTreeNode<u32>>{bail!("client has no index")}
 fn read_trie_node(&self,_:TrieNodeId)->Result<TrieNode>{bail!("client has no index")}
 fn read_object(&self,_:Digest)->Result<Object<u32>>{bail!("client has no server objects")}
}
#[derive(Serialize,Deserialize)]struct Packet{query:QuerySpec,request:QueryParam<u32>,responses:Vec<(HashMap<ObjId,Object<u32>>,VO<u32>)>,dag:petgraph::Graph<DagNode<u32>,bool>,answers:Vec<(String,Vec<u64>)>}
fn save(p:&Path,v:&impl Serialize)->Result<()>{let f=fs::OpenOptions::new().create_new(true).write(true).open(p)?;serde_json::to_writer_pretty(f,v)?;Ok(())}
fn canonical(order:u64,time:u64,src:u32,dst:u32,nft:u32,col:u32,tx:u32)->Result<CanonicalEvent>{Ok(CanonicalEvent::new(format!("order:{order}"),"NFTTrade",i64::try_from(order)?,i64::try_from(time)?,format!("tx:{tx}"),BTreeMap::from([("move_from".into(),Scalar::Int(src.into())),("move_to".into(),Scalar::Int(dst.into())),("nft".into(),Scalar::Int(nft.into())),("collection_contract".into(),Scalar::Int(col.into()))])))}
fn source(path:&Path)->Result<(usize,Vec<CanonicalEvent>)>{let b=fs::read(path)?;ensure!(b.len()%37==0,"source framing");let mut es=vec![];for r in b.chunks_exact(37){if r[36]!=1{continue}let u=|i|u32::from_le_bytes(r[i..i+4].try_into().unwrap());es.push(canonical(u64::from_le_bytes(r[..8].try_into()?),u64::from_le_bytes(r[8..16].try_into()?),u(16),u(20),u(24),u(28),u(32))?);}Ok((b.len()/37,es))}
fn events(p:&Packet)->Result<Vec<CanonicalEvent>>{let mut out=BTreeMap::new();for(os,_)in &p.responses{for o in os.values(){let n=&o.num_data;ensure!(n.len()==9&&n[8]==1&&o.keyword_data==HashSet::from(["type:1".to_string()]),"authenticated object schema/type");let order=n[1]as u64|((n[2]as u64)<<32);let e=canonical(order,n[0].into(),n[3],n[4],n[5],n[6],n[7])?;if let Some(old)=out.insert(order,e.clone()){ensure!(old==e,"conflicting stable identity")}}}Ok(out.into_values().collect())}
fn exact(es:&[CanonicalEvent],q:&QuerySpec)->Result<Answers>{let rows=client_count::ClientCount::new(es,q)?.enumerate_indexed(2_000_000_000)?;let n=rows.len();let ans=rows.into_iter().map(|(b,ids)|(b,ids.into_iter().map(|i|es[i].order_seq as u64).collect())).collect::<Answers>();ensure!(ans.len()==n,"duplicate enumerated binding");Ok(ans)}
fn check_answers(claimed:&[(String,Vec<u64>)],actual:&Answers)->Result<()>{let set=claimed.iter().cloned().collect::<Answers>();ensure!(set.len()==claimed.len(),"duplicate claimed binding");ensure!(&set==actual,"full binding mismatch");Ok(())}
fn load_query(p:&Path)->Result<QuerySpec>{let v:Value=serde_json::from_reader(fs::File::open(p)?)?;let mut q=query::compile_query_document(&v)?;q.scope=ScopeSpec::Episode{anchor_atom:q.branches.first().context("branch")?.atoms.first().context("anchor")?.id.clone(),window_seconds:v["full_binding_plan"]["delta_seconds"].as_i64().context("delta")?};
                                                                                          
 ensure!(q.branches.iter().all(|b|b.atoms.iter().all(|a|a.event_types==BTreeSet::from(["NFTTrade".into()]))&&b.constraints.iter().all(|c|!matches!(c,Constraint::NoEvent{..}))),"requires another authenticated type/blocker contract");Ok(q)}
fn verify(p:&Packet,q:&QuerySpec,req:&QueryParam<u32>,heads:&Headers,pk:&AccPublicKey)->Result<()>{ensure!(&p.query==q&&&p.request==req,"runtime query/request substitution");verify_expected(heads,req,&p.responses,&p.dag,pk)}
                                                                                
                                                                                       
fn client_only(a:&[String])->Result<()>{
 ensure!(a.len()==7,"client sealed query.json response.zlib output.json clean|gate");
 let sealed=Path::new(&a[2]);let q=load_query(Path::new(&a[3]))?;
 let pk:AccPublicKey=bincode::deserialize(&fs::read(sealed.join("public_key.bin"))?)?;
 let heads:Headers=bincode::deserialize(&fs::read(sealed.join("trusted_headers.bin"))?)?;
 let cfg:Value=serde_json::from_reader(fs::File::open(sealed.join("config.json"))?)?;
 ensure!(cfg.is_object()&&!heads.headers.is_empty(),"invalid sealed configuration");
 let req=QueryParam{start_blk:1,end_blk:heads.headers.len() as u32,range:vec![],keyword_exp:Some(Node::Input("type:1".into()))};
 let compressed=fs::read(&a[4])?;let mut wire=Vec::new();
 let mut dec=flate2::read::ZlibDecoder::new(&compressed[..]);dec.read_to_end(&mut wire)?;
 ensure!(dec.total_in() as usize==compressed.len(),"trailing compressed response");
 let mut p:Packet=bincode::deserialize(&wire)?;drop(wire);
 verify(&p,&q,&req,&heads,&pk)?;
 let es=events(&p)?;let actual=exact(&es,&q)?;check_answers(&p.answers,&actual)?;
 let mut attacks=vec![];
 if a[6]=="gate"{
  p.query.query_id.push('x');ensure!(verify(&p,&q,&req,&heads,&pk).is_err(),"query substitution accepted");p.query=q.clone();attacks.push("query_substitution");
  let removed=p.responses.pop().context("window")?;ensure!(verify(&p,&q,&req,&heads,&pk).is_err(),"missing window accepted");p.responses.push(removed);attacks.push("missing_window");
  let saved=p.answers.clone();if let Some(first)=saved.first(){
   p.answers.push(first.clone());ensure!(check_answers(&p.answers,&actual).is_err(),"duplicate accepted");attacks.push("duplicate_answer");
   p.answers=saved.clone();p.answers.pop();ensure!(check_answers(&p.answers,&actual).is_err(),"omission accepted");attacks.push("missing_answer");
   p.answers=saved.clone();p.answers[0].1[0]=u64::MAX;ensure!(check_answers(&p.answers,&actual).is_err(),"forgery accepted");attacks.push("forged_answer");p.answers=saved;
  }
  let (os,_)=p.responses.iter_mut().find(|(os,_)|!os.is_empty()).context("objects")?;os.values_mut().next().unwrap().num_data[3]^=1;
  ensure!(verify(&p,&q,&req,&heads,&pk).is_err(),"changed object accepted");attacks.push("changed_authenticated_field");
 }else{ensure!(a[6]=="clean","unknown client mode");}
                                                                                     
 save(Path::new(&a[5]),&json!({"accepted":true,"answers":actual.len(),"bindings":actual,
   "client_events":es.len(),"response_bytes":compressed.len(),"attacks_rejected":attacks}))?;
 Ok(())
}
fn main()->Result<()>{
 rayon::ThreadPoolBuilder::new().num_threads(4).build_global()?;
 let a:Vec<_>=std::env::args().collect();if a.get(1).map(String::as_str)==Some("client"){return client_only(&a)}
 ensure!(a.len()==7,"source sealed_index query.json reference.json output gate|full");let out=PathBuf::from(&a[5]);ensure!(!out.exists(),"output exists");fs::create_dir_all(&out)?;
 let q=load_query(Path::new(&a[3]))?;let sealed=Path::new(&a[2]);let cfg:Value=serde_json::from_reader(fs::File::open(sealed.join("config.json"))?)?;
 let (rows,es)=source(Path::new(&a[1]))?;ensure!(cfg["rows"]==rows,"sealed input size mismatch");
 let pk:AccPublicKey=bincode::deserialize(&fs::read(sealed.join("public_key.bin"))?)?;let heads:Headers=bincode::deserialize(&fs::read(sealed.join("trusted_headers.bin"))?)?;let chain=SimChain::open(&sealed.join("db"))?;
 let req=QueryParam{start_blk:1,end_blk:heads.headers.len() as u32,range:vec![],keyword_exp:Some(Node::Input("type:1".into()))};
 let t=Instant::now();let claimed=exact(&es,&q)?;let query_s=t.elapsed().as_secs_f64();eprintln!("SERVER EXACT {} {} {:.3}s",q.query_id,claimed.len(),query_s);
                                                                                                 
 let oracle:Value=serde_json::from_reader(fs::File::open(&a[4])?)?;let ids=es.iter().map(|e|e.order_seq as u64).collect::<HashSet<_>>();let expected=oracle["answers"].as_array().context("full-domain Generic oracle")?.iter().map(|v|v.as_array().unwrap().iter().map(|x|x.as_u64().unwrap()).collect::<Vec<_>>()).filter(|v|v.iter().all(|id|ids.contains(id))).collect::<BTreeSet<_>>();let actual=claimed.iter().map(|x|x.1.clone()).collect::<BTreeSet<_>>();ensure!(actual==expected,"FP={} FN={}",actual.difference(&expected).count(),expected.difference(&actual).count());drop(ids);drop(expected);drop(actual);drop(oracle);
 let t=Instant::now();let(res,dag,_)=native_query(false,false,&chain,req.clone(),&pk)?;let prove_s=t.elapsed().as_secs_f64();eprintln!("NATIVE PROOF {:.3}s",prove_s);
 let packet=Packet{query:q.clone(),request:req.clone(),responses:res,dag,answers:claimed.iter().cloned().collect()};
 let t=Instant::now();let wire=bincode::serialize(&packet)?;let raw_bytes=wire.len();let mut enc=flate2::write::ZlibEncoder::new(Vec::new(),flate2::Compression::new(6));enc.write_all(&wire)?;let compressed=enc.finish()?;let encode_s=t.elapsed().as_secs_f64();let response_bytes=compressed.len();drop(wire);drop(packet);
 let t=Instant::now();let mut wire=Vec::new();flate2::read::ZlibDecoder::new(&compressed[..]).read_to_end(&mut wire)?;let mut received:Packet=bincode::deserialize(&wire)?;let decode_s=t.elapsed().as_secs_f64();drop(wire);
 let t=Instant::now();verify(&received,&q,&req,&heads,&pk)?;let auth_s=t.elapsed().as_secs_f64();
 let t=Instant::now();let client=events(&received)?;let client_rows=client.len();let replay=exact(&client,&q)?;check_answers(&received.answers,&replay)?;let replay_s=t.elapsed().as_secs_f64();
 let mut attacks=vec![];if a[6]=="gate"{
  received.query.query_id.push('x');ensure!(verify(&received,&q,&req,&heads,&pk).is_err(),"query attack");received.query=q.clone();attacks.push("query_substitution");
  let response=received.responses.pop().context("window")?;ensure!(verify(&received,&q,&req,&heads,&pk).is_err(),"missing window attack");received.responses.push(response);attacks.push("missing_window");
  if let Some(first)=received.answers.first().cloned(){received.answers.push(first.clone());ensure!(check_answers(&received.answers,&replay).is_err(),"duplicate attack");received.answers.pop();attacks.push("duplicate_answer");received.answers.pop();ensure!(check_answers(&received.answers,&replay).is_err(),"omission attack");attacks.push("missing_answer");received.answers=claimed.iter().cloned().collect();received.answers[0].1[0]=u64::MAX;ensure!(check_answers(&received.answers,&replay).is_err(),"forged answer");attacks.push("forged_answer");}
  let (os,_)=received.responses.iter_mut().find(|(os,_)|!os.is_empty()).context("nonempty object response")?;let (_,obj)=os.iter_mut().next().unwrap();obj.num_data[3]^=1;ensure!(verify(&received,&q,&req,&heads,&pk).is_err(),"modified object attack");attacks.push("changed_authenticated_field");
 }
 let e2e=query_s+prove_s+encode_s+decode_s+auth_s+replay_s;
 let result=json!({"query_id":q.query_id,"baseline":"vChain+ + Client Exact (runtime QuerySpec adapter)","rows":rows,"trade_rows":es.len(),"client_events":client_rows,"answers":claimed.len(),"fp":0,"fn":0,"server_query_seconds":query_s,"server_recall_including_proof_seconds":prove_s,"encode_seconds":encode_s,"decode_seconds":decode_s,"client_auth_seconds":auth_s,"client_exact_and_compare_seconds":replay_s,"client_total_seconds":decode_s+auth_s+replay_s,"compute_e2e_seconds":e2e,"response_bytes":response_bytes,"raw_response_bytes":raw_bytes,"codec":"zlib6","threads":4,"gpu":false,"shared_load":true,"network_measured":false,"index_reused":true,"trusted_sealer_required":true,"attacks_rejected":attacks,"memory":fs::read_to_string("/proc/self/status").unwrap_or_default()});
 save(&out.join("result.json"),&result)?;save(&out.join("answers.json"),&claimed)?;save(&out.join("query.json"),&q)?;let mut f=fs::OpenOptions::new().create_new(true).write(true).open(out.join("response.zlib"))?;f.write_all(&compressed)?;println!("RESULT {}",result);Ok(())
}
