                                                                             
                                                                                 
use super::{block::Height, id_tree::ObjId, object::Object, query::{query_param::QueryParam,
    query_dag::{DagNode,gen_parallel_query_dag},select_win_size}, traits::{Num,ReadInterface}, verify::{inner_verify,vo::{VO,VONode}}};
use crate::acc::{AccPublicKey,AccValue};
use anyhow::{ensure,bail,Context,Result};
use petgraph::{Graph,EdgeDirection::{Incoming,Outgoing}};
use serde::Serialize;
use std::collections::{HashMap,HashSet};

pub fn verify_expected<K:Num+Serialize,T:ReadInterface<K=K>>(
    trusted:&T, expected:&QueryParam<K>, responses:&[(HashMap<ObjId,Object<K>>,VO<K>)],
    server_dag:&Graph<DagNode<K>,bool>, pk:&AccPublicKey
)->Result<()> {
    ensure!(expected.start_blk>0 && expected.start_blk<=expected.end_blk,"invalid requested interval");
    let param=trusted.get_parameter()?;
    ensure!(!param.time_win_sizes.is_empty() && param.time_win_sizes.iter().all(|s|*s>0),"invalid trusted windows");
    let dag=gen_parallel_query_dag(&expected.gen_query_content())?;
    ensure!(dag.node_count()>0,"empty query unsupported");
                                                                         
    ensure!(serde_json::to_value(&dag)?==serde_json::to_value(server_dag)?,"query plan differs from client query");
    ensure!(!dag.node_indices().any(|i|matches!(dag.node_weight(i),Some(DagNode::BlkRt(_)))),"NOT/universe root requires separate authentication; unsupported");
    let roots:HashSet<_>=dag.node_indices().filter(|i|dag.neighbors_directed(*i,Incoming).next().is_none()).collect();
    ensure!(roots.len()==1,"query must have one root");
    let windows=select_win_size(&param.time_win_sizes,expected.gen_time_win())?;
    ensure!(responses.len()==windows.len(),"missing or extra response windows");
    let expected_windows:HashSet<_>=windows.iter().map(|(w,s)|(w.end_blk,*s)).collect();
    ensure!(expected_windows.len()==windows.len(),"duplicate expected windows");
    let mut covered=expected.start_blk as u64;
    for (w,s) in &windows {
        ensure!(w.start_blk>=expected.start_blk && w.end_blk<=expected.end_blk && w.start_blk as u64<=covered,"window outside query or coverage hole");
        ensure!(w.end_blk-w.start_blk+1==*s as u32,"invalid window size");
        covered=covered.max(w.end_blk as u64+1);
    }
    ensure!(covered==expected.end_blk as u64+1,"incomplete interval");
    let mut seen=HashSet::new();
    for (objects,vo) in responses {
        ensure!(vo.vo_dag_content.dag_content.len()==dag.node_count(),"missing or extra proof node (pruned mode unsupported)");
        ensure!(vo.vo_dag_content.output_sets.keys().copied().collect::<HashSet<_>>()==roots,"wrong output roots");
        let mut scope=None;
        for idx in dag.node_indices() {
            let node=vo.vo_dag_content.dag_content.get(&idx).context("missing expected node")?;
            let leaf=match node {VONode::Range(n)=>Some((n.blk_height.0,n.win_size)),VONode::Keyword(n)=>Some((n.blk_height.0,n.win_size)),_=>None};
            if let Some(s)=leaf {
                if let Some(old)=scope {ensure!(old==s,"mixed window leaves");} else {scope=Some(s);}
            }
            if roots.contains(&idx) && dag.neighbors_directed(idx,Outgoing).next().is_none() {
                let set=vo.vo_dag_content.output_sets.get(&idx).context("missing leaf output")?;
                ensure!(*node.get_acc()?==AccValue::from_set(set,pk),"leaf output differs from authenticated set");
            }
            if !roots.contains(&idx) { ensure!(node.get_acc().is_ok(),"non-root final proof"); }
        }
        let (end,size)=scope.context("no authenticated leaves")?;
        ensure!(expected_windows.contains(&(end,size)) && seen.insert((end,size)),"unexpected or duplicate response window");
        ensure!(vo.merkle_proofs.len()==1 && vo.merkle_proofs.contains_key(&Height(end)),"wrong root scope");
        ensure!(vo.merkle_proofs[&Height(end)].id_tree_root_hash.is_none(),"output ID root disconnected from epoch root");
        for obj in objects.values() {ensure!(obj.blk_height.0<=end && obj.blk_height.0+size as u32>end,"object outside window");}
        inner_verify(trusted,objects,vo,&dag,pk)?;
    }
    if seen!=expected_windows {bail!("incomplete windows");}
    Ok(())
}
