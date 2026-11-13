#[cfg(test)]
mod tests {
    use crate::*;
    fn event(o:u64,t:i64,s:u32,d:u32,n:u32,tx:u32)->Event {
        Event {order:o,time:t,src:s,dst:d,nft:n,col:1,tx}
    }
    fn q(delta:i64)->query::QuerySpec {
        let root=Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let (mut q,_)=load_query(root).unwrap();
        q.scope=query::ScopeSpec::Episode{anchor_atom:"root".into(),window_seconds:delta};
        for c in &mut q.branches[0].constraints {
            if let query::Constraint::Within{delta_seconds,..}=c {*delta_seconds=delta;}
        }
        q
    }
    fn check(mut events:Vec<Event>,delta:i64,force_exact:bool)->Engine {
        events.sort_by_key(|e|(e.col,e.order));
        let query=q(delta);
        let (expected,_)=reference::run(&events,&query,delta).unwrap();
        let mut e=Engine::new(prepare(events).unwrap(),delta);
        e.force_exact=force_exact;
        e.build().unwrap();
        let actual=e.reconstruct_answers().unwrap();
        assert_eq!(actual,expected);
        assert_eq!(e.nodes[0].exact_contribution,expected.len() as u64);
        statistics(&e).unwrap();
        e
    }
    #[test] fn whole_block_zero_complete_missing_reverse() {
        let es=(0..1000).map(|i|event(i+1,i as i64,1,2,7,i as u32)).collect();
        let e=check(es,1800,false);
        assert_eq!(e.nodes.len(),2);
        assert_eq!(e.nodes[1].rule,Rule::WholeBlockZero);
        assert_eq!(e.nodes[1].domain_size,1000);
    }
    #[test] fn product_requires_every_pair_valid() {
        let e=check(vec![event(1,0,1,2,7,1),event(2,1,1,2,7,1),event(3,2,2,1,7,2)],1800,false);
        assert!(e.nodes.iter().any(|n|n.rule==Rule::IndependentProduct && n.domain_size==2 && n.exact_contribution==2));
    }
    #[test] fn factor_same_complete_interval_not_just_same_count() {
        let e=check(vec![event(1,0,1,2,7,10),event(2,1,1,2,7,10),
            event(3,2,2,1,7,10),event(4,3,2,1,7,11)],1800,false);
        assert!(e.nodes.iter().any(|n|n.rule==Rule::CountFactorEliminate && n.exact_contribution==2));
        let e=check(vec![event(1,0,1,2,7,10),event(2,1,2,1,7,11),
            event(3,20,1,2,7,10),event(4,21,2,1,7,12)],5,false);
        assert!(!e.nodes.iter().any(|n|n.rule==Rule::CountFactorEliminate && n.domain_size>1));
    }
    #[test] fn range_retains_time_order_tx_and_exact_upper_boundary() {
        let e=check(vec![event(1,100,1,2,7,10),event(2,100,2,1,7,11),
            event(3,101,2,1,7,10),event(4,110,2,1,7,12),event(5,111,2,1,7,13)],10,false);
        assert_eq!(e.nodes[0].exact_contribution,1);
        assert!(e.nodes.iter().any(|n|n.rule==Rule::RangeCount));
    }
    #[test] fn forced_exact_and_nonmonotone_fallback() {
        let es=vec![event(1,100,1,2,7,10),event(2,101,2,1,7,10),
            event(3,105,2,1,7,11),event(4,103,2,1,7,12)];
        for forced in [false,true] {
            let e=check(es.clone(),10,forced);
            assert!(e.nodes.iter().any(|n|n.rule==Rule::SingletonExact));
        }
    }
    #[test] fn split_rejects_missing_overlapping_or_wrong_sum_child() {
        let es=vec![event(1,0,1,2,7,10),event(2,1,2,1,7,11),
            event(3,20,1,2,7,10),event(4,21,2,1,7,12)];
        let mut e=check(es,5,false);
        let child=e.nodes[0].children[0] as usize;
        e.nodes[child].anchor_rank_start+=1;
        assert!(e.check_partitions().is_err());
        e.nodes[child].anchor_rank_start-=1;
        e.nodes[0].exact_contribution+=1;
        assert!(e.check_partitions().is_err());
    }
    #[test] fn deterministic_mixed_identity_and_ties_against_original_oracle() {
        let es=(1..=70).map(|i|event(i,(i/3) as i64,
            (i%3) as u32,((i+1+(i%2))%3) as u32,(i%4) as u32,(i/4) as u32)).collect();
        check(es,7,false);
    }
}
