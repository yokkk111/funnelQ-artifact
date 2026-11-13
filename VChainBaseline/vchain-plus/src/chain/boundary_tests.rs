use super::*;
use crate::{acc::{Set,AccValue},chain::{strict::verify_expected,query::query_param::{Node,AndNode},verify::vo::VONode}};

fn param()->Parameter {Parameter{time_win_sizes:vec![1,2],id_tree_fanout:4,max_id_num:16,bplus_tree_fanout:4,num_dim:1}}
fn qp(keyword:Option<Node>,range:Vec<Range<u32>>)->QueryParam<u32>{QueryParam{start_blk:1,end_blk:3,keyword_exp:keyword,range}}
fn duplicate<T:serde::Serialize+serde::de::DeserializeOwned>(v:&T)->T{bincode::deserialize(&bincode::serialize(v).unwrap()).unwrap()}

#[test]
fn strict_honest_and_adversarial_gate()->Result<()> {
    let chain=build_chain(TEST_DATA_1,&param())?;
    let mut passed=Vec::new();
    let requests=vec![
        ("keyword",qp(Some(Node::Input("a".into())),vec![])),
        ("absent_keyword",qp(Some(Node::Input("absent".into())),vec![])),
        ("range",qp(None,vec![Range::new(2,3)])),
        ("mixed",qp(Some(Node::Input("a".into())),vec![Range::new(1,3)])),
        ("intersection",qp(Some(Node::And(Box::new(AndNode(Node::Input("a".into()),Node::Input("b".into()))))),vec![]))
    ];
    for (name,q) in &requests {
        let (res,dag,_)=query(false,false,&chain,q.clone(),&PUB_KEY).with_context(||format!("query {}",name))?;
        verify_expected(&&chain,q,&res,&dag,&PUB_KEY).with_context(||format!("verify {}",name))?;
        let actual:HashSet<_>=res.iter().flat_map(|(o,_)|o.values().map(|o|o.to_digest())).collect();
        let expected:HashSet<_>=chain.objects.values().filter(|o|{
            let numeric=q.range.iter().enumerate().all(|(i,r)|r.is_in_range(o.num_data[i]));
            let kw=match &q.keyword_exp {None=>true,Some(Node::Input(s))=>o.keyword_data.contains(s),Some(Node::And(n))=>match (&n.0,&n.1){(Node::Input(a),Node::Input(b))=>o.keyword_data.contains(a)&&o.keyword_data.contains(b),_=>false},_=>false};
            numeric&&kw
        }).map(|o|o.to_digest()).collect();
        assert_eq!(actual,expected,"honest scan equality: {}",name);
        passed.push(format!("honest_{}",name));
    }
    let q=requests[0].1.clone();
    let (res,dag,_)=query(false,false,&chain,q.clone(),&PUB_KEY)?;
    macro_rules! reject {($name:expr,$r:expr,$d:expr,$q:expr,$c:expr)=>{{assert!(verify_expected(&$c,&$q,&$r,&$d,&PUB_KEY).is_err(),"accepted {}",$name);passed.push($name.to_string());}}}
    let mut r=duplicate(&res);r.pop();reject!("drop_window",r,dag,q,&chain);
    let r:Vec<_>=Vec::new();reject!("empty_responses",r,dag,q,&chain);
    let mut r=duplicate(&res);r[1]=duplicate(&r[0]);reject!("duplicate_window",r,dag,q,&chain);
    let mut r=duplicate(&res);let k=*r[0].0.keys().next().unwrap();r[0].0.remove(&k);reject!("drop_object",r,dag,q,&chain);
    let mut r=duplicate(&res);r[0].0.values_mut().next().unwrap().num_data[0]+=99;reject!("forge_object",r,dag,q,&chain);
    let mut other=q.clone();other.keyword_exp=Some(Node::Input("b".into()));reject!("substitute_query",res,dag,other,&chain);
    let mut shorter=q.clone();shorter.end_blk=2;reject!("shrink_client_domain",res,dag,shorter,&chain);
    let mut d=dag.clone();d.clear();reject!("substitute_dag",res,d,q,&chain);
    let mut r=duplicate(&res);r[0].1.vo_dag_content.dag_content.clear();reject!("drop_proof_nodes",r,dag,q,&chain);
    let mut r=duplicate(&res);r[0].1.merkle_proofs.clear();reject!("drop_merkle",r,dag,q,&chain);
    let mut r=duplicate(&res);for (_,p) in &mut r[0].1.merkle_proofs {p.id_tree_root_hash=Some(Digest::zero());}reject!("disconnect_id_root",r,dag,q,&chain);
    let mut bad_chain=build_chain(TEST_DATA_1,&param())?;for h in bad_chain.block_head.values_mut(){h.ads_root_hash=Digest::zero();}reject!("different_epoch",res,dag,q,&bad_chain);
                                                                                        
    let mut r=duplicate(&res);r[0].0.clear();r[0].1.vo_dag_content.output_sets.values_mut().for_each(|s|*s=Set::new());
    for n in r[0].1.vo_dag_content.dag_content.values_mut(){if let VONode::Keyword(k)=n{k.acc=AccValue::from_set(&Set::new(),&PUB_KEY);}}
    for p in r[0].1.trie_proofs.values_mut(){let h=p.root_hash();*p=crate::chain::trie_tree::proof::Proof::from_root_hash(None,"",h);}
    reject!("opaque_trie_false_absence",r,dag,q,&chain);
    let mut r=duplicate(&res);r[0].0.clear();r[0].1.vo_dag_content.output_sets.values_mut().for_each(|s|*s=Set::new());reject!("leaf_output_omission",r,dag,q,&chain);
                                                                         
    let mut r=duplicate(&res);r[0].0.values_mut().next().unwrap().num_data[0]+=99;
    assert!(verify(&chain,&r,&dag,&PUB_KEY).is_err());passed.push("legacy_error_propagation".into());
    let rq=requests[2].1.clone();let (rr,rd,_)=query(false,false,&chain,rq.clone(),&PUB_KEY)?;
    let mut r=duplicate(&rr);for p in r[0].1.merkle_proofs.values_mut(){p.extra_bplus_rt_hashes.insert(0,Digest::zero());}reject!("overwrite_authenticated_range_root",r,rd,rq,&chain);
    println!("BOUNDARY_GATE {}",serde_json::json!({"passed":passed,"all_passed":true}));
    Ok(())
}
