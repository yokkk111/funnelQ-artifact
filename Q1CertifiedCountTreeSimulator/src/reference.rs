                                                                           
                                                                            
use crate::{evaluator::EvaluationRequest, index::QueryIndependentIndex, model::CanonicalEvent};
use crate::{oracle, query::QuerySpec, Event};
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::{collections::{BTreeMap, BTreeSet}, time::Instant};

pub fn canonical(e: Event) -> CanonicalEvent {
    CanonicalEvent::new(format!("order:{}", e.order), "NFTTrade", e.order as i64,
        e.time, format!("tx:{}", e.tx), BTreeMap::from([
            ("move_from".into(), (e.src as i64).into()),
            ("move_to".into(), (e.dst as i64).into()),
            ("nft".into(), (e.nft as i64).into()),
            ("collection_contract".into(), (e.col as i64).into()),
        ]))
}

pub fn run(es: &[Event], q: &QuerySpec, delta: i64) -> Result<(BTreeSet<[u64; 2]>, Value)> {
    let start = Instant::now();
    let mut pos = 0;
    let mut seen = 0usize;
    let mut reads = 0u64;
    let mut incidence = 0u64;
    let mut answers = BTreeSet::new();
    while pos < es.len() {
        let end = pos + es[pos..].partition_point(|e| e.col == es[pos].col);
        let rows = &es[pos..end];
        let index = QueryIndependentIndex::build_plain(rows.iter().copied().map(canonical).collect())?;
        let ids: Vec<_> = rows.iter().map(|e| format!("order:{}", e.order)).collect();
        let monotone = rows.windows(2).all(|w| w[0].time <= w[1].time);
        for (a, event) in rows.iter().enumerate() {
                                                                                        
                                                                                          
                                                                                          
            let allowed: BTreeSet<String> = if monotone {
                let hi = rows.partition_point(|e| e.time <= event.time + delta).max(a + 1);
                ids[a..hi].iter().cloned().collect()
            } else {
                rows.iter().zip(&ids).filter(|(e, _)| e.order >= event.order && e.time <= event.time + delta)
                    .map(|(_, id)| id.clone()).collect()
            };
            incidence += allowed.len() as u64;
            let request = EvaluationRequest {
                allowed_event_ids: Some(allowed),
                fixed_atoms: BTreeMap::from([("root".into(), ids[a].clone())]),
            };
            for m in oracle::brute_force_query(&index, q, &request)? {
                let id = |s: &str| -> Result<u64> { Ok(m.event_bindings[s].strip_prefix("order:").unwrap().parse()?) };
                ensure!(answers.insert([id("root")?, id("back")?]), "duplicate canonical answer");
            }
            seen += 1;
            if seen % 20000 == 0 {
                eprintln!("REFERENCE anchors={seen}/{} answers={} elapsed={:.1}s", es.len(), answers.len(), start.elapsed().as_secs_f64());
            }
        }
        reads += index.reads.get();
        pos = end;
    }
    ensure!(seen == es.len(), "reference omitted anchors");
    let metrics = json!({"anchors":seen,"answer_count":answers.len(),"seconds":start.elapsed().as_secs_f64(),
        "canonical_row_reads":reads,"eligible_episode_trade_incidence":incidence,
        "oracle":"unmodified FunnelQ_Residual_Obligation/src/oracle.rs::brute_force_query",
        "adapter":"plain in-memory event index; no hashing, commitments or proofs",
        "safe_type_pushdown":"both Q1 atoms are NFTTrade; no NoEvent; all trades retained"});
    Ok((answers, metrics))
}
