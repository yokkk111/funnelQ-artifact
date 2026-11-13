                                                                       
use super::*;
use crate::query::{AtomSpec, FieldRef};
use serde_json::json;
use std::{fs, path::PathBuf, time::Instant};
#[path = "v2_count/client_count.rs"]
mod v2_count;

fn atom(id: &str) -> AtomSpec {
    AtomSpec {
        id: id.into(),
        event_types: BTreeSet::from(["T".into()]),
        fields: BTreeMap::from([("asset".into(), Term::Variable("joint".into()))]),
        local_predicates: vec![],
    }
}
fn expr(atom: &str, field: &str) -> ValueExpr {
    ValueExpr::Field(FieldRef {
        atom: atom.into(),
        field: field.into(),
    })
}
fn events() -> Vec<CanonicalEvent> {
    (0..12)
        .map(|i| {
            CanonicalEvent::new(
                format!("unique:{i}"),
                "T",
                i,
                i / 2,
                "same",
                BTreeMap::from([
                    ("asset".into(), Scalar::Int(i % 2)),
                    ("tag".into(), Scalar::Int(i % 3)),
                ]),
            )
        })
        .collect()
}
fn query(n: usize) -> QuerySpec {
    QuerySpec {
        query_id: "never-registered-guard-test".into(),
        result_semantics: "full_event_binding_v1".into(),
        scope: ScopeSpec::Episode {
            anchor_atom: "a0".into(),
            window_seconds: 6,
        },
        branches: vec![BranchSpec {
            id: "unseen-branch".into(),
            atoms: (0..n).map(|i| atom(&format!("a{i}"))).collect(),
            constraints: vec![],
            event_injective: true,
        }],
    }
}
fn oracle_count(es: &[CanonicalEvent], q: &QuerySpec) -> BigUint {
    BigUint::from(crate::protocol::reference(es, q).unwrap().len())
}

#[test]
fn boundary_guards_unregistered_duplicate_keys_and_distinct_match_oracle() {
    let es = events();
    let types = BTreeSet::from(["T".into()]);
    for n in 2..=4 {
        for variant in 0..4 {
            let mut q = query(n);
            if n >= 3 {
                q.branches[0].constraints.push(Constraint::Compare {
                    left: expr("a1", "tag"),
                    op: CompareOp::Ne,
                    right: expr("a2", "tag"),
                });
            }
            if variant >= 1 {
                q.branches[0].constraints.push(Constraint::Order {
                    earlier: format!("a{}", n - 2),
                    later: format!("a{}", n - 1),
                    field: "block_ts".into(),
                });
            }
            if variant >= 2 && n >= 3 {
                q.branches[0].constraints.push(Constraint::DistinctValues {
                    values: vec![expr("a1", "tag"), expr("a2", "tag")],
                    min_count: 2,
                    max_count: 2,
                    ignore_empty_text: false,
                });
            }
            if variant == 3 {
                q.scope = ScopeSpec::SameTx;
            }
            let c = ClientCount::with_authenticated_blockers(&es, &q, &types).unwrap();
            let off = c.count_policy_early(2_000_000, true, false, false).unwrap();
            let on = c.count_policy_early(2_000_000, true, false, true).unwrap();
            let old = v2_count::ClientCount::with_authenticated_blockers(&es, &q, &types)
                .unwrap()
                .count(2_000_000)
                .unwrap();
            assert_eq!(off.0, old.0);
            assert_eq!(on.0, off.0);
            assert_eq!(on.0, oracle_count(&es, &q));
        }
    }
    let mut q = query(3);
    q.branches[0].constraints.push(Constraint::Compare {
        left: expr("a1", "tag"),
        op: CompareOp::Eq,
        right: expr("a2", "tag"),
    });
    let c = ClientCount::with_authenticated_blockers(&es, &q, &types).unwrap();
    let off = c.count_policy_early(2_000_000, true, false, false).unwrap();
    let on = c.count_policy_early(2_000_000, true, false, true).unwrap();
    assert!(on.1.steps[0].early_boundary_guards > 0);
    assert!(
        on.1.steps[0].output_rows < off.1.steps[0].output_rows,
        "must demonstrate removal of otherwise deferred failing boundaries"
    );
}

#[test]
fn boundary_noevent_keeps_complete_blockers_and_original_obligation() {
    let mut es = events();
    es.push(CanonicalEvent::new(
        "blocker",
        "B",
        8,
        4,
        "same",
        BTreeMap::from([("tag".into(), Scalar::Int(1))]),
    ));
    let types = BTreeSet::from(["T".into(), "B".into()]);
    let mut q = query(3);
    q.scope = ScopeSpec::SameTx;
    q.branches[0].constraints.push(Constraint::NoEvent {
        after: "a1".into(),
        event_types: BTreeSet::from(["B".into()]),
        fields: BTreeMap::from([("tag".into(), expr("a2", "tag"))]),
        within_seconds: 4,
    });
    let c = ClientCount::with_authenticated_blockers(&es, &q, &types).unwrap();
    let off = c.count_policy_early(2_000_000, true, false, false).unwrap();
    let on = c.count_policy_early(2_000_000, true, false, true).unwrap();
    assert_eq!(on.0, off.0);
    assert_eq!(on.0, oracle_count(&es, &q));
    assert!(on.1.steps[0].early_boundary_guards > 0);
    assert!(ClientCount::new(&es, &q).is_err());
    assert!(
        ClientCount::with_authenticated_blockers(&es, &q, &BTreeSet::from(["T".into()])).is_err()
    );
                                                                         
                                                                                 
    assert_eq!(on.1.steps.len(), off.1.steps.len());
}

#[test]
fn episode_noevent_waits_for_anchor_and_uses_full_blocker_envelope() {
    let base = (0..4)
        .map(|i| {
            CanonicalEvent::new(
                format!("episode:{i}"),
                "T",
                i,
                i,
                "same",
                BTreeMap::from([("asset".into(), Scalar::Int(7))]),
            )
        })
        .collect::<Vec<_>>();
    let types = BTreeSet::from(["T".into(), "B".into()]);
    let mut q = query(4);
    q.scope = ScopeSpec::Episode {
        anchor_atom: "a3".into(),
        window_seconds: 6,
    };
    q.branches[0].constraints.push(Constraint::NoEvent {
        after: "a1".into(),
        event_types: BTreeSet::from(["B".into()]),
        fields: BTreeMap::from([("asset".into(), expr("a2", "asset"))]),
        within_seconds: 100,
    });
    let domains = (0..4)
        .map(|i| (("unseen-branch".into(), format!("a{i}")), vec![0, 1, 2, 3]))
        .collect::<BTreeMap<_, _>>();
    let positive_count = oracle_count(&base, &q);
    assert!(!positive_count.is_zero());
    for outside in [true, false] {
        let mut es = base.clone();
        let time = if outside { 100 } else { 4 };
        es.push(CanonicalEvent::new(
            "actual-blocker",
            "B",
            time,
            time,
            "same",
            BTreeMap::from([("asset".into(), Scalar::Int(7))]),
        ));
        let c = ClientCount::from_authenticated_refs(es.iter().collect(), &q, &types, &domains)
            .unwrap();
                                                                           
                                                                              
        assert!(c.branches[0].domains.iter().all(|d| !d.contains(&4)));
        let off = c.count_policy_early(100000, true, false, false).unwrap();
        let on = c.count_policy_early(100000, true, false, true).unwrap();
        assert_eq!(off.0, on.0);
        assert_eq!(on.0, oracle_count(&es, &q));
        if outside {
            assert_eq!(on.0, positive_count);
        } else {
            assert!(on.0.is_zero());
        }
        assert!(on.1.steps[0].early_boundary_guards > 0);
                                                                              
                                                                               
                                                                          
        let no_event = c.branches[0]
            .predicates
            .iter()
            .find(|p| matches!(p, Pred::NoEvent { .. }))
            .unwrap()
            .clone();
        let b = Branch {
            id: "deferred-anchor".into(),
            atoms: vec!["x".into(), "after".into(), "field".into(), "anchor".into()],
            domains: vec![vec![0, 1, 2, 3]; 4],
            predicates: vec![no_event],
        };
        let inputs = vec![Factor {
            vars: vec![0],
            rows: BTreeMap::new(),
            unit: Some(vec![0, 1, 2, 3]),
        }];
        let mut stat = StepStats::default();
        let guarded = fused_eliminate_guarded(
            &c.events,
            &b,
            0,
            &[1, 2],
            &[],
            &inputs,
            1000,
            &mut stat,
            true,
        )
        .unwrap();
        let plain = fused_eliminate_guarded(
            &c.events,
            &b,
            0,
            &[1, 2],
            &[],
            &inputs,
            1000,
            &mut StepStats::default(),
            false,
        )
        .unwrap();
        assert_eq!(stat.early_boundary_guards, 0);
        assert_eq!(guarded, plain);
    }
}

#[test]
fn boundary_guards_preserve_huge_weights_empty_boundary_and_missing_fields() {
    let es = events();
    let refs = es.iter().collect::<Vec<_>>();
    let mut row = vec![None; 3];
    let guard = Pred::Compare(field(1, "tag"), CompareOp::Ne, field(2, "tag"));
    let b = Branch {
        id: "weighted".into(),
        atoms: vec!["x".into(), "y".into(), "z".into()],
        domains: vec![vec![0, 1, 2]; 3],
        predicates: vec![guard.clone()],
    };
    let huge = BigUint::one() << 160usize;
    let inputs = vec![Factor {
        vars: vec![0],
        rows: BTreeMap::from([
            (vec![0], huge.clone()),
            (vec![1], huge.clone()),
            (vec![2], BigUint::one()),
        ]),
        unit: None,
    }];
    let off = fused_eliminate_guarded(
        &refs,
        &b,
        0,
        &[1, 2],
        &[],
        &inputs,
        1000,
        &mut StepStats::default(),
        false,
    )
    .unwrap();
    let on = fused_eliminate_guarded(
        &refs,
        &b,
        0,
        &[1, 2],
        &[],
        &inputs,
        1000,
        &mut StepStats::default(),
        true,
    )
    .unwrap();
    let expected = off
        .into_iter()
        .filter(|(k, _)| {
            row[1] = Some(k[0]);
            row[2] = Some(k[1]);
            guard.holds(&row, &refs)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(on, expected);
    assert!(on.values().all(|v| *v == &huge * 2u8 + 1u8));
    let types = BTreeSet::from(["T".into()]);
    for data in [&es[..], &[][..]] {
        let q = query(1);
        let c = ClientCount::with_authenticated_blockers(data, &q, &types).unwrap();
        assert_eq!(
            c.count_policy_early(1000, true, false, false).unwrap().0,
            c.count_policy_early(1000, true, false, true).unwrap().0
        );
    }
    let mut q = query(3);
    q.branches[0].constraints.push(Constraint::Compare {
        left: expr("a1", "missing"),
        op: CompareOp::Ne,
        right: expr("a2", "tag"),
    });
    let c = ClientCount::with_authenticated_blockers(&es, &q, &types).unwrap();
    assert!(c
        .count_policy_early(100000, true, false, true)
        .unwrap()
        .0
        .is_zero());
    assert_eq!(
        c.count_policy_early(100000, true, false, false).unwrap().0,
        oracle_count(&es, &q)
    );
}

#[test]
#[ignore = "Explicit full-data on/off/replay guard experiment; no authentication benchmark"]
fn full_domain_boundary_guard_count_replay() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf();
    let label = std::env::var("FQ_LOCAL_RUN_LABEL").unwrap_or_else(|_| "initial".into());
    assert!(label.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    let out = root.join(format!(
        "experiments/count_guards/count_guard_{label}.json"
    ));
    assert!(!out.exists(), "preserve results");
    let mut results = vec![];
    for number in 1..=5 {
        let dir = root.join(format!(
            "experiments/count_transport/full_v2/q{number}"
        ));
        let p = crate::shared_wire::decode(
            &fs::read(dir.join("baseline_payload_without_coverage.bin")).unwrap(),
            2 * 1024 * 1024 * 1024,
        )
        .unwrap();
        let mut records = p
            .tables
            .values()
            .flat_map(|t| t.rows.iter().map(|r| &r.event))
            .collect::<Vec<_>>();
        records.sort_unstable_by(|a, b| a.event_id.cmp(&b.event_id));
        let domains = crate::shared_protocol::certified_domains(&p, &records).unwrap();
        let c = ClientCount::from_authenticated_refs(
            records.clone(),
            &p.query,
            &p.plan.blocker_types,
            &domains,
        )
        .unwrap();
        let old = v2_count::ClientCount::from_authenticated_refs(
            records,
            &p.query,
            &p.plan.blocker_types,
            &domains,
        )
        .unwrap()
        .count(5_000_000)
        .unwrap();
        let mut names = p
            .query
            .branches
            .iter()
            .map(|b| b.id.clone())
            .collect::<Vec<_>>();
        names.sort();
        let mut samples = vec![];
        for round in 0..6 {
            for shift in 0..3 {
                let mode = (round + shift) % 3;
                let started = Instant::now();
                let (seconds, n, stats) = if mode == 2 {
                    let mut actual = c
                        .enumerate_indexed(5_000_000)
                        .unwrap()
                        .into_iter()
                        .map(|(b, events)| crate::shared_protocol::Binding {
                            branch: names.binary_search(&b).unwrap(),
                            events,
                        })
                        .collect::<Vec<_>>();
                    actual.sort();
                    let seconds = started.elapsed().as_secs_f64();
                    assert_eq!(
                        actual, p.answers,
                        "complete GenericExact binding set differs"
                    );
                    (
                        seconds,
                        BigUint::from(actual.len()),
                        json!({"full_answer_materialized":true,"fp":0,"fn":0}),
                    )
                } else {
                    let (n, stats) = c
                        .count_policy_early(5_000_000, true, false, mode == 1)
                        .unwrap();
                    let seconds = started.elapsed().as_secs_f64();
                    if mode == 0 {
                        assert_eq!(
                            serde_json::to_value(&stats).unwrap(),
                            serde_json::to_value(&old.1).unwrap(),
                            "default off differs from saved v2"
                        );
                    }
                    (seconds, n, serde_json::to_value(stats).unwrap())
                };
                assert_eq!(n, old.0);
                assert_eq!(n.to_string(), p.answers.len().to_string());
                let mode_name = ["guard_off", "guard_on", "replay"][mode];
                samples.push(json!({"mode":mode_name,"round":round,"warmup":round==0,"seconds":seconds,"count":n.to_string(),"stats":stats}));
            }
        }
        eprintln!(
            "BOUNDARY GUARD Q{number}: {} exact answers, FP=FN=0, saved-v2 agrees",
            p.answers.len()
        );
        results.push(
            json!({"query":number,"answers":p.answers.len(),"fp":0,"fn":0,"samples":samples}),
        );
    }
    serde_json::to_writer_pretty(fs::OpenOptions::new().write(true).create_new(true).open(out).unwrap(),&json!({"scope":"Full unchanged selected domains; 1 warmup+5 rotating rounds of off/on/replay; independent saved full GenericExact answers; count-only, audit packets lack coverage proofs; no protocol or authentication change.","rows":results})).unwrap();
}
