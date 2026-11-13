                                                                               
                                                                           
use anyhow::{ensure,Result};
use serde::{Serialize,Deserialize};
use std::{collections::{HashMap,BTreeMap,HashSet},fs,io::Write};
use petgraph::{Graph,graph::NodeIndex};
use vchain_plus::{acc::Set,chain::{block::Height,object::Object,id_tree::{self,ObjId},trie_tree,
    query::{query_param::QueryParam,query_dag::DagNode},verify::vo::{VONode,MerkleProof,VO}},digest::Digest};
#[derive(Serialize,Deserialize)]struct DagMirror{output_sets:HashMap<NodeIndex,Set>,dag_content:HashMap<NodeIndex,VONode<u32>>}
#[derive(Serialize,Deserialize)]struct VoMirror{vo_dag_content:DagMirror,trie_proofs:HashMap<Height,trie_tree::proof::Proof>,id_tree_proof:id_tree::proof::Proof,cur_obj_id:ObjId,merkle_proofs:HashMap<Height,MerkleProof>}
fn size<T:Serialize>(x:&T)->u64{bincode::serialized_size(x).unwrap()}
fn counts(v:&serde_json::Value,out:&mut BTreeMap<String,u64>){match v{
 serde_json::Value::Object(m)=>{for(k,v)in m{if ["Hash","Leaf","NonLeaf","NonLeafRoot"].contains(&k.as_str()){*out.entry(k.clone()).or_default()+=1;}counts(v,out);}},
 serde_json::Value::Array(a)=>for v in a{counts(v,out);},_=>{}}}
fn main()->Result<()>{
 let a:Vec<_>=std::env::args().collect();ensure!(a.len()==3,"response.bin new-output.json");
 let wire=fs::read(&a[1])?;
 type Packet=(QueryParam<u32>,Vec<(HashMap<ObjId,Object<u32>>,VoMirror)>,Graph<DagNode<u32>,bool>,Vec<[u64;2]>);
 let (req,res,dag,ans):Packet=bincode::deserialize(&wire)?;
 let mut b=BTreeMap::<String,u64>::new();let mut types=BTreeMap::<String,u64>::new();let mut nodes=BTreeMap::new();
 let mut ids=HashSet::new();let mut objects=0;let mut vo_total=0;
 let mut add=|k:&str,n:u64|{*b.entry(k.to_string()).or_default()+=n;};
 add("query",size(&req));add("query_dag",size(&dag));add("answer_bindings",size(&ans));add("response_vector_header",8);
 for (o,v) in &res {
                                                                             
  let native:VO<u32>=bincode::deserialize(&bincode::serialize(v)?)?;ensure!(size(&native)==size(v),"mirror layout mismatch");vo_total+=size(v);
  add("event_objects_and_map",size(o));objects+=o.len();for e in o.values(){ids.insert(e.num_data[1] as u64|((e.num_data[2]as u64)<<32));}
  add("vo_output_sets",size(&v.vo_dag_content.output_sets));add("vo_dag_nodes",size(&v.vo_dag_content.dag_content));
  add("vo_trie_proofs",size(&v.trie_proofs));add("vo_id_tree_proof",size(&v.id_tree_proof));add("vo_current_object_id",size(&v.cur_obj_id));add("vo_merkle_proofs",size(&v.merkle_proofs));
  for n in v.vo_dag_content.dag_content.values(){let name=match n{VONode::Range(_)=>"range",VONode::Keyword(_)=>"keyword",VONode::BlkRt(_)=>"block_root",VONode::InterUnion(_)=>"inter_union",VONode::FinalUnion(_)=>"final_union",VONode::InterIntersec(_)=>"inter_intersection",VONode::FinalIntersec(_)=>"final_intersection",VONode::InterDiff(_)=>"inter_difference",VONode::FinalDiff(_)=>"final_difference"};*types.entry(name.into()).or_default()+=1;}
  counts(&serde_json::to_value(&v.id_tree_proof)?,&mut nodes);
 }
 let sum:u64=b.values().sum();ensure!(sum==wire.len()as u64,"byte accounting mismatch");
 let result=serde_json::json!({"source":a[1],"wire_bytes":wire.len(),"sum_component_bytes":sum,"vo_bytes":vo_total,"components":b,"vo_node_counts":types,"id_tree_node_counts":nodes,"subresponses":res.len(),"objects_with_repetition":objects,"unique_events":ids.len(),"answers":ans.len(),"mirror_native_size_check":true,"note":"Additive original-wire bytes, no component re-compression; no proof generation or verification benchmark."});
 let mut f=fs::OpenOptions::new().create_new(true).write(true).open(&a[2])?;f.write_all(&serde_json::to_vec_pretty(&result)?)?;println!("{}",result);Ok(())
}
