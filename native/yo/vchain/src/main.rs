                                                                               
use anyhow::{ensure,bail,Context,Result};
use serde::{Serialize,Deserialize};
use serde_json::{json,Value};
use std::{fs,io::{BufRead,BufReader,Write},collections::{HashMap,HashSet,BTreeMap},path::Path,time::Instant};
use vchain_plus::{SimChain,acc::{AccPublicKey,AccSecretKey},chain::{Parameter,
 block::{Height,BlockHead,BlockContent,build::build_block},object::Object,
 id_tree::{ObjId,IdTreeNode,IdTreeNodeId},bplus_tree::{BPlusTreeNode,BPlusTreeNodeId},trie_tree::{TrieNode,TrieNodeId},
 traits::ReadInterface,query::{query,query_param::{QueryParam,Node,OrNode},query_dag::DagNode},strict::verify_expected,verify::vo::VO},digest::{Digest,Digestible}};
#[derive(Serialize,Deserialize)]struct Headers{param:Parameter,headers:HashMap<Height,BlockHead>}
impl ReadInterface for Headers{
 type K=u32;
 fn get_parameter(&self)->Result<Parameter>{Ok(self.param.clone())}
 fn read_block_head(&self,h:Height)->Result<BlockHead>{self.headers.get(&h).cloned().context("missing trusted header")}
 fn read_block_content(&self,_:Height)->Result<BlockContent>{bail!("no source access")}
 fn read_id_tree_node(&self,_:IdTreeNodeId)->Result<IdTreeNode>{bail!("no source access")}
 fn read_bplus_tree_node(&self,_:BPlusTreeNodeId)->Result<BPlusTreeNode<u32>>{bail!("no source access")}
 fn read_trie_node(&self,_:TrieNodeId)->Result<TrieNode>{bail!("no source access")}
 fn read_object(&self,_:Digest)->Result<Object<u32>>{bail!("no source access")}
}
#[derive(Serialize,Deserialize)]struct Packet{request:QueryParam<u32>,responses:Vec<(HashMap<ObjId,Object<u32>>,VO<u32>)>,dag:petgraph::Graph<DagNode<u32>,bool>}
fn write(p:&Path,b:&[u8])->Result<()>{fs::OpenOptions::new().create_new(true).write(true).open(p)?.write_all(b)?;Ok(())}
fn row_object(r:&[u32],height:u32)->Result<Object<u32>>{
 ensure!(r.len()==11&&r[2]<4,"YO schema");let kinds=["aggregator_intent","pool_swap","token_transfer","transaction_meta"];
 Ok(Object::new(Height(height),r.to_vec(),HashSet::from([format!("type={}",kinds[r[2] as usize])])) )
}
fn req(types:&str,h:&Headers)->Result<QueryParam<u32>>{
 ensure!(types=="aggregator_intent,pool_swap,token_transfer","unsupported request");
 let mut node=Node::Input("type=aggregator_intent".into());
 for s in ["pool_swap","token_transfer"]{node=Node::Or(Box::new(OrNode(node,Node::Input(format!("type={s}")))));}
 Ok(QueryParam{start_blk:1,end_blk:h.headers.len()as u32,range:vec![],keyword_exp:Some(node)})
}
fn main()->Result<()>{
 rayon::ThreadPoolBuilder::new().num_threads(4).build_global()?;
 let a:Vec<_>=std::env::args().collect();ensure!(a.len()==6,"mode input sealed types output");let dir=Path::new(&a[3]);let start=Instant::now();
 if a[1]=="seal"{
  ensure!(!dir.exists(),"existing seal");fs::create_dir_all(dir)?;
  let sk:vchain_plus::acc::AccSecretKeyWithPowCache=AccSecretKey::rand(rand::rngs::OsRng).into();let pk=AccPublicKey::gen_key(&sk,257);
  let param=Parameter{time_win_sizes:vec![1,2],id_tree_fanout:4,max_id_num:256,bplus_tree_fanout:4,num_dim:0};
  let mut c=SimChain::create(&dir.join("db"),param.clone())?;let mut heads=Headers{param,headers:HashMap::new()};let mut prev=Digest::zero();let mut objects=vec![];let mut rows=0;let mut height=1;
  for line in BufReader::new(fs::File::open(&a[2])?).lines(){let v:Value=serde_json::from_str(&line?)?;let r:Vec<u32>=serde_json::from_value(v["row"].clone())?;objects.push(row_object(&r,height)?);rows+=1;
   if objects.len()==128{let(h,_)=build_block(Height(height),prev,std::mem::take(&mut objects),&mut c,&heads.param,&pk)?;prev=h.to_digest();heads.headers.insert(Height(height),h);height+=1;}}
  if !objects.is_empty(){let(h,_)=build_block(Height(height),prev,objects,&mut c,&heads.param,&pk)?;heads.headers.insert(Height(height),h);}
  write(&dir.join("headers.bin"),&bincode::serialize(&heads)?)?;write(&dir.join("pk.bin"),&bincode::serialize(&pk)?)?;
  println!("{}",json!({"rows":rows,"seconds":start.elapsed().as_secs_f64()}));return Ok(());
 }
 let h:Headers=bincode::deserialize(&fs::read(dir.join("headers.bin"))?)?;let pk:AccPublicKey=bincode::deserialize(&fs::read(dir.join("pk.bin"))?)?;let expected=req(&a[4],&h)?;
 if a[1]=="prove"{
  let c=SimChain::open(&dir.join("db"))?;let(res,dag,_)=query(false,false,&c,expected.clone(),&pk)?;let p=Packet{request:expected,responses:res,dag};write(Path::new(&a[5]),&bincode::serialize(&p)?)?;
 }else if a[1].starts_with("mutate_"){
  let mut p:Packet=bincode::deserialize(&fs::read(&a[2])?)?;
  if a[1]=="mutate_payload"{let(os,_)=p.responses.iter_mut().find(|(os,_)|!os.is_empty()).context("empty")?;os.values_mut().next().unwrap().num_data[4]^=1;}
  else if a[1]=="mutate_omit"{p.responses.pop().context("empty")?;}else{bail!("mutation");}
  write(Path::new(&a[5]),&bincode::serialize(&p)?)?;
 }else if a[1]=="verify"{
  let p:Packet=bincode::deserialize(&fs::read(&a[2])?)?;ensure!(p.request==expected,"query substitution");verify_expected(&h,&expected,&p.responses,&p.dag,&pk)?;
  let mut rows=BTreeMap::new();for(os,_)in &p.responses{for o in os.values(){let r=&o.num_data;ensure!(r.len()==11&&r[2]<3,"YO schema");ensure!(row_object(r,o.blk_height.0)?.keyword_data==o.keyword_data,"keyword mismatch");if let Some(old)=rows.insert(r[0],r.clone()){ensure!(old==*r,"identity conflict");}}}
  let mut f=fs::OpenOptions::new().create_new(true).write(true).open(&a[5])?;for r in rows.values(){writeln!(f,"{}",serde_json::to_string(r)?)?;}
 }else{bail!("unknown mode");}
 println!("{}",json!({"accepted":true,"seconds":start.elapsed().as_secs_f64(),"bytes":fs::metadata(&a[5])?.len()}));Ok(())
}
