                                                                                    
use anyhow::{ensure,bail,Context,Result};
use serde::{Serialize,Deserialize};
use serde_json::{json,Value};
use std::{fs,io::{BufRead,BufReader,BufWriter,Write},collections::{HashMap,HashSet,BTreeMap},path::Path,time::Instant};
use vchain_plus::{SimChain,acc::{AccPublicKey,AccSecretKey},chain::{Parameter,
 block::{Height,BlockHead,BlockContent,build::build_block},object::Object,
 id_tree::{ObjId,IdTreeNode,IdTreeNodeId},bplus_tree::{BPlusTreeNode,BPlusTreeNodeId},trie_tree::{TrieNode,TrieNodeId},
 traits::ReadInterface,query::{query,query_param::{QueryParam,Node,OrNode},query_dag::DagNode},strict::verify_expected,verify::vo::VO},digest::{Digest,Digestible}};
#[derive(Serialize,Deserialize)]struct Headers{param:Parameter,headers:HashMap<Height,BlockHead>}
impl ReadInterface for Headers{
 type K=u32;
 fn get_parameter(&self)->Result<Parameter>{Ok(self.param.clone())}
 fn read_block_head(&self,h:Height)->Result<BlockHead>{self.headers.get(&h).cloned().context("missing trusted header")}
 fn read_block_content(&self,_:Height)->Result<BlockContent>{bail!("no server contents")}
 fn read_id_tree_node(&self,_:IdTreeNodeId)->Result<IdTreeNode>{bail!("no index")}
 fn read_bplus_tree_node(&self,_:BPlusTreeNodeId)->Result<BPlusTreeNode<u32>>{bail!("no index")}
 fn read_trie_node(&self,_:TrieNodeId)->Result<TrieNode>{bail!("no index")}
 fn read_object(&self,_:Digest)->Result<Object<u32>>{bail!("no server objects")}
}
#[derive(Serialize,Deserialize)]struct Packet{request:QueryParam<u32>,responses:Vec<(HashMap<ObjId,Object<u32>>,VO<u32>)>,dag:petgraph::Graph<DagNode<u32>,bool>}
                                                                          
                                                                                 
#[derive(Serialize,Deserialize)]struct Bundle{version:u32,parts:Vec<Packet>}
fn categories(s:&str)->Result<Vec<&str>>{
 let v:Vec<_>=s.split(',').collect();ensure!(!v.is_empty(),"empty categories");
 for t in &v{ensure!(["flashloan","swap","borrow_positive","borrow_negative","borrow_zero"].contains(t),"unsupported category");}
 ensure!(v.windows(2).all(|w|w[0]<w[1]),"categories must be sorted and unique");Ok(v)
}
fn write(p:&Path,b:&[u8])->Result<()>{fs::OpenOptions::new().create_new(true).write(true).open(p)?.write_all(b)?;Ok(())}
fn row_object(r:&[i64],height:u32)->Result<Object<u32>>{
 ensure!(r.len()==9&&r[0]>=0&&r[2]>=0&&(-1..=1).contains(&r[8])&&(0..=2).contains(&r[7]),"schema");
 let mut n=vec![r[0] as u32,(r[0] as u64>>32) as u32,u32::try_from(r[1])?,r[2] as u32,(r[2] as u64>>32) as u32];
 for x in &r[3..8]{n.push(u32::try_from(*x)?);}n.push((r[8]+1)as u32);
 Ok(Object::new(Height(height),n,HashSet::from([format!("type={}",if r[7]==0 {"flashloan"} else if r[7]==1 {"swap"} else if r[8]>0 {"borrow_positive"} else if r[8]<0 {"borrow_negative"} else {"borrow_zero"})])) )
}
fn object_row(o:&Object<u32>)->Result<Vec<i64>>{
 let n=&o.num_data;ensure!(n.len()==11&&n[9]<=2&&n[10]<=2,"schema");
 let eid=i64::try_from(n[0] as u64|((n[1]as u64)<<32))?;let time=i64::try_from(n[3]as u64|((n[4]as u64)<<32))?;
 let r=vec![eid,n[2]as i64,time,n[5]as i64,n[6]as i64,n[7]as i64,n[8]as i64,n[9]as i64,n[10]as i64-1];
 ensure!(row_object(&r,o.blk_height.0)?.keyword_data==o.keyword_data,"keyword schema mismatch");Ok(r)
}
fn req(types:&str,h:&Headers)->Result<QueryParam<u32>>{
 let mut nodes=Vec::new();for s in types.split(','){ensure!(["flashloan","swap","borrow","borrow_positive","borrow_negative","borrow_zero"].contains(&s),"type");nodes.push(Node::Input(format!("type={s}")));}
 let mut node=nodes.remove(0);for n in nodes{node=Node::Or(Box::new(OrNode(node,n)));}
 Ok(QueryParam{start_blk:1,end_blk:h.headers.len()as u32,range:vec![],keyword_exp:Some(node)})
}
fn main()->Result<()>{
 rayon::ThreadPoolBuilder::new().num_threads(4).build_global()?;
 let a:Vec<_>=std::env::args().collect();ensure!(a.len()==6,"mode input sealed types output");
 let dir=Path::new(&a[3]);let start=Instant::now();
 if a[1]=="seal"{
  ensure!(!dir.exists(),"existing seal");fs::create_dir_all(dir)?;
  let sk: vchain_plus::acc::AccSecretKeyWithPowCache=AccSecretKey::rand(rand::rngs::OsRng).into();let pk=AccPublicKey::gen_key(&sk,257);
  let param=Parameter{time_win_sizes:vec![1,2],id_tree_fanout:4,max_id_num:256,bplus_tree_fanout:4,num_dim:0};
  let mut c=SimChain::create(&dir.join("db"),param.clone())?;let mut heads=Headers{param,headers:HashMap::new()};let mut prev=Digest::zero();let mut objects=vec![];let mut rows=0;let mut height=1;
  for line in BufReader::new(fs::File::open(&a[2])?).lines(){let v:Value=serde_json::from_str(&line?)?;let r:Vec<i64>=serde_json::from_value(v["row"].clone())?;objects.push(row_object(&r,height)?);rows+=1;
   if objects.len()==128{let(h,_)=build_block(Height(height),prev,std::mem::take(&mut objects),&mut c,&heads.param,&pk)?;prev=h.to_digest();heads.headers.insert(Height(height),h);height+=1;}}
  if !objects.is_empty(){let(h,_)=build_block(Height(height),prev,objects,&mut c,&heads.param,&pk)?;heads.headers.insert(Height(height),h);}
  write(&dir.join("headers.bin"),&bincode::serialize(&heads)?)?;write(&dir.join("pk.bin"),&bincode::serialize(&pk)?)?;
  println!("{}",json!({"rows":rows,"seal_seconds":start.elapsed().as_secs_f64(),"trust":"honest local sealer; local setup; no blockchain consensus claimed"}));return Ok(());
 }
 let h:Headers=bincode::deserialize(&fs::read(dir.join("headers.bin"))?)?;let pk:AccPublicKey=bincode::deserialize(&fs::read(dir.join("pk.bin"))?)?;
 let tokens=categories(&a[4])?;
 if a[1]=="prove"{
  let c=SimChain::open(&dir.join("db"))?;let loaded=Instant::now();let mut parts=Vec::new();
  for token in &tokens{let expected=req(token,&h)?;let(res,dag,_)=query(false,false,&c,expected.clone(),&pk)?;parts.push(Packet{request:expected,responses:res,dag});}
  let proved=Instant::now();let bytes=bincode::serialize(&Bundle{version:4,parts})?;write(Path::new(&a[5]),&bytes)?;
  println!("{}",json!({"proof_write_seconds":loaded.elapsed().as_secs_f64(),"load_seconds":loaded.duration_since(start).as_secs_f64(),"proof_compute_seconds":proved.duration_since(loaded).as_secs_f64(),"proof_encode_write_seconds":proved.elapsed().as_secs_f64(),"bytes":bytes.len()}));
 }else if a[1].starts_with("mutate_"){
  let mut p:Bundle=bincode::deserialize(&fs::read(&a[2])?)?;
  if a[1]=="mutate_payload"{let(os,_)=p.parts.iter_mut().flat_map(|x|x.responses.iter_mut()).find(|(os,_)|!os.is_empty()).context("empty")?;os.values_mut().next().unwrap().num_data[5]^=1;}
  else if a[1]=="mutate_omit"{p.parts.first_mut().context("empty")?.responses.pop().context("empty")?;}
  else if a[1]=="mutate_category"{p.parts.pop().context("empty")?;}
  else if a[1]=="mutate_duplicate"{ensure!(p.parts.len()>1,"need two categories");p.parts[1]=bincode::deserialize(&bincode::serialize(&p.parts[0])?)?;}
  else{bail!("mutation");}
  write(Path::new(&a[5]),&bincode::serialize(&p)?)?;
 }else if a[1]=="verify"{
  let p:Bundle=bincode::deserialize(&fs::read(&a[2])?)?;ensure!(p.version==4&&p.parts.len()==tokens.len(),"category coverage mismatch");
  for (part,token) in p.parts.iter().zip(&tokens){ensure!(part.request==req(token,&h)?,"query substitution or reordered category");}
  let decoded=Instant::now();for (part,token) in p.parts.iter().zip(&tokens){verify_expected(&h,&req(token,&h)?,&part.responses,&part.dag,&pk)?;}let verified=Instant::now();
  let mut rows=BTreeMap::new();for(part,token)in p.parts.iter().zip(&tokens){for(os,_)in &part.responses{for o in os.values(){
   let r=object_row(o)?;ensure!(o.keyword_data==HashSet::from([format!("type={token}")]),"wrong authenticated category");
   if let Some(old)=rows.insert(r[0],r.clone()){ensure!(old==r,"identity conflict");}
  }}}
  let mut f=BufWriter::new(fs::OpenOptions::new().create_new(true).write(true).open(&a[5])?);
  if a[5].ends_with(".bin"){f.write_all(b"DFROWS4\0")?;f.write_all(&(rows.len()as u64).to_le_bytes())?;for r in rows.values(){for x in r{f.write_all(&x.to_le_bytes())?;}}}
  else{for r in rows.values(){writeln!(f,"{}",serde_json::to_string(r)?)?;}}f.flush()?;
  println!("{}",json!({"accepted":true,"rows":rows.len(),"context_and_proof_read_seconds":decoded.duration_since(start).as_secs_f64(),"native_verify_seconds":verified.duration_since(decoded).as_secs_f64(),"payload_export_seconds":verified.elapsed().as_secs_f64(),"read_verify_write_seconds":start.elapsed().as_secs_f64()}));
 }else{bail!("unknown mode");}Ok(())
}
