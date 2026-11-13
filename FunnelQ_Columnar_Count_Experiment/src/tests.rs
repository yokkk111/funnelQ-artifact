use super::*;
fn temporal_check(
    es: &[CanonicalEvent],
    query: &QuerySpec,
) -> Result<(usize, crate::temporal_demand::TemporalStats)> {
    use crate::{
        key_projection::KeyPlan,
        temporal_demand::{local_domains, TemporalPlan},
    };
    use std::sync::Arc;
    let seal = seal(es, "temporal-test".into())?;
    let p = KeyPlan::compile(query)?;
    let base = &seal.tables[&p.event_type];
    let mut d = p.execute_domains(&base.columns, base.n)?;
    let st = TemporalPlan::compile(query)?.apply(&p, &base.columns, &mut d)?;
    let ids = Arc::new((0..base.n).filter(|&i| d.union[i]).collect::<Vec<_>>());
    let view = project_table(&p.event_type, base, ids.clone())?;
    let rs = rows(&view)?;
    let ds = local_domains(query, &d, &ids)?;
    let eng = Engine::with_verified_domains(
        &rs,
        query,
        &BTreeSet::from([p.event_type]),
        Some(&d.projections),
        Some(&ds),
    )?;
    let got = eng.replay_rows()?.0;
    assert_eq!(eng.canonical_answers(&got)?, reference(es, query)?);
    assert_eq!(eng.count()?.0, num_bigint::BigUint::from(got.len()));
    eng.sound_unique(&got)?;
    Ok((got.len(), st))
}
#[test]
fn temporal_runtime_queries_match_bruteforce_with_ties_and_nonmonotone_times() -> Result<()> {
    let (mut answers, mut pruned, mut fallback) = (0, 0, 0);
    for seed in 0..8 {
        let es = (0..15)
            .map(|i| {
                CanonicalEvent::new(
                    format!("e{i}"),
                    "T",
                    i,
                    if seed % 2 == 0 { i / 2 } else { (i * 7) % 11 },
                    format!("tx{}", i % 5),
                    BTreeMap::from([
                        ("from".into(), Scalar::Int(i % 3)),
                        ("to".into(), Scalar::Int((i + 1) % 3)),
                        ("group".into(), Scalar::Int(0)),
                    ]),
                )
            })
            .collect::<Vec<_>>();
        let mut query = q(
            "never-registered-temporal-query",
            vec![
                atom("begin", "T", &[("to", "x"), ("group", "g")]),
                atom("end", "T", &[("from", "x"), ("group", "g")]),
            ],
            vec![
                order("begin", "end"),
                Constraint::Order {
                    earlier: "begin".into(),
                    later: "end".into(),
                    field: "block_ts".into(),
                },
                Constraint::DistinctTransactions(vec!["begin".into(), "end".into()]),
            ],
        );
        query.scope = ScopeSpec::Episode {
            anchor_atom: "begin".into(),
            window_seconds: [0, 1, 3, 20][seed / 2],
        };
        let (a, s) = temporal_check(&es, &query)?;
        answers += a;
        pruned += s.before_union - s.after_union;
        fallback += s.fallback_checks;
        assert_eq!(s.monotone_scan, seed % 2 == 0);
        query.branches[0]
            .atoms
            .push(atom("third", "T", &[("from", "x"), ("group", "g")]));
        query.branches[0].constraints.extend([
            order("end", "third"),
            Constraint::Order {
                earlier: "end".into(),
                later: "third".into(),
                field: "block_ts".into(),
            },
        ]);
        temporal_check(&es, &query)?;
    }
    assert!(answers > 0 && pruned > 0 && fallback > 0);
    Ok(())
}
#[test]
fn temporal_within_same_tx_does_not_invent_nonnegative_time() -> Result<()> {
    let es = [(0, 10), (1, 7), (2, 8), (3, 9)]
        .into_iter()
        .map(|(i, t)| {
            CanonicalEvent::new(
                format!("e{i}"),
                "T",
                i,
                t,
                "same",
                BTreeMap::from([("key".into(), Scalar::Int(1))]),
            )
        })
        .collect::<Vec<_>>();
    let mut query = q(
        "negative-time-gap-is-legal-without-episode",
        vec![
            atom("a", "T", &[("key", "v")]),
            atom("b", "T", &[("key", "v")]),
        ],
        vec![Constraint::Within {
            earlier: "a".into(),
            later: "b".into(),
            delta_seconds: 0,
        }],
    );
    query.scope = ScopeSpec::SameTx;
    let (a, st) = temporal_check(&es, &query)?;
    assert_eq!(a, 3);
    assert!(!st.monotone_scan);
    Ok(())
}
#[test]
fn temporal_closed_window_strict_time_and_empty_cycle() -> Result<()> {
    let es = [0, 0, 5, 6]
        .into_iter()
        .enumerate()
        .map(|(i, t)| {
            CanonicalEvent::new(
                format!("e{i}"),
                "T",
                i as i64,
                t,
                format!("t{i}"),
                BTreeMap::from([("key".into(), Scalar::Int(1))]),
            )
        })
        .collect::<Vec<_>>();
    let mut query = q(
        "boundary-check",
        vec![
            atom("a", "T", &[("key", "v")]),
            atom("b", "T", &[("key", "v")]),
        ],
        vec![
            order("a", "b"),
            Constraint::Order {
                earlier: "a".into(),
                later: "b".into(),
                field: "block_ts".into(),
            },
        ],
    );
    query.scope = ScopeSpec::Episode {
        anchor_atom: "a".into(),
        window_seconds: 5,
    };
    assert_eq!(temporal_check(&es, &query)?.0, 3);
    query.branches[0].constraints.push(Constraint::Order {
        earlier: "b".into(),
        later: "a".into(),
        field: "block_ts".into(),
    });
    let (n, st) = temporal_check(&es, &query)?;
    assert_eq!(n, 0);
    assert_eq!(st.after_union, 0);
    Ok(())
}
#[test]
fn temporal_missing_input_and_noevent_are_rejected() -> Result<()> {
    use crate::{key_projection::KeyPlan, temporal_demand::TemporalPlan};
    let es = vec![CanonicalEvent::new(
        "one",
        "T",
        0,
        0,
        "t",
        BTreeMap::from([("key".into(), Scalar::Int(1))]),
    )];
    let mut query = q(
        "missing-input",
        vec![
            atom("a", "T", &[("key", "v")]),
            atom("b", "T", &[("key", "v")]),
        ],
        vec![],
    );
    let s = seal(&es, "source".into())?;
    let p = KeyPlan::compile(&query)?;
    let mut cols = s.tables["T"].columns.clone();
    let mut d = p.execute_domains(&cols, 1)?;
    cols.remove("block_ts");
    assert!(TemporalPlan::compile(&query)?
        .apply(&p, &cols, &mut d)
        .is_err());
    query.branches[0].constraints.push(Constraint::NoEvent {
        after: "a".into(),
        event_types: BTreeSet::from(["T".into()]),
        fields: BTreeMap::new(),
        within_seconds: 1,
    });
    assert!(TemporalPlan::compile(&query).is_err());
    Ok(())
}
#[test]
fn sparse_projection_identity_multiplicity_and_typed_keys() -> Result<()> {
    use crate::key_projection::KeyPlan;
    use std::sync::Arc;
    for seed in 0..6 {
        let es = (0..28)
            .map(|i| {
                let typed = |v: i64| {
                    if seed < 3 && v % 3 == 0 {
                        Scalar::Text((v % 7).to_string())
                    } else {
                        Scalar::Int(v % 7)
                    }
                };
                let mut fields = BTreeMap::from([
                    ("left".into(), typed(i + seed)),
                    ("right".into(), typed(i + seed + 2)),
                    ("group".into(), Scalar::Int(i % 2)),
                    ("amount".into(), Scalar::Int(i)),
                ]);
                if i % 9 == 0 {
                    fields.remove("right");
                }
                if i % 8 == 0 {
                    fields.insert("left".into(), Scalar::Int(500 + i));
                }
                CanonicalEvent::new(
                    format!("unrelated-id-{i}"),
                    "T",
                    i,
                    i,
                    format!("tx{}", i % 4),
                    fields,
                )
            })
            .collect::<Vec<_>>();
        let mut query = q(
            "unregistered-view-regression",
            vec![
                atom("a", "T", &[("left", "a"), ("group", "z")]),
                atom("b", "T", &[("right", "a"), ("group", "z")]),
                atom("c", "T", &[("right", "a"), ("group", "z")]),
            ],
            vec![
                order("a", "b"),
                order("b", "c"),
                Constraint::DistinctTransactions(vec!["a".into(), "b".into()]),
            ],
        );
        query.branches[0].atoms[1]
            .local_predicates
            .push(LocalPredicate {
                field: "amount".into(),
                op: CompareOp::Ge,
                value: Scalar::Int(seed),
            });
        let sealed = seal(&es, "same-source".into())?;
        let full_rows = rows(&sealed.tables)?;
        let types = BTreeSet::from(["T".to_string()]);
        let full = Engine::new(&full_rows, &query, &types)?;
        let (expected, _) = full.replay_rows()?;
        assert_eq!(full.canonical_answers(&expected)?, reference(&es, &query)?);
        let plan = KeyPlan::compile(&query)?;
        let (mask, cache) = plan.execute(&sealed.tables["T"].columns, es.len())?;
        let ids = Arc::new((0..es.len()).filter(|&i| mask[i]).collect::<Vec<_>>());
        assert!(ids.len() < es.len());
        let view = project_table("T", &sealed.tables["T"], ids.clone())?;
        let vr = rows(&view)?;
        let optimized = Engine::with_projections(&vr, &query, &types, Some(&cache))?;
        assert!(optimized.reused_projection_indexes() > 0);
        let (actual, _) = optimized.replay_rows()?;
        assert_eq!(actual, expected);
        assert_eq!(
            optimized.count()?.0,
            num_bigint::BigUint::from(expected.len())
        );
        optimized.sound_unique(&actual)?;
        if let Some(a) = actual.first() {
            let mut dup = actual.clone();
            dup.push(a.clone());
            assert!(optimized.sound_unique(&dup).is_err());
        }
                                                                                                
        let other = seal(&es, "other-source".into())?;
        let other_view = project_table("T", &other.tables["T"], ids)?;
        let other_rows = rows(&other_view)?;
        let fallback = Engine::with_projections(&other_rows, &query, &types, Some(&cache))?;
        assert_eq!(fallback.reused_projection_indexes(), 0);
        assert_eq!(fallback.replay_rows()?.0, expected);
    }
    Ok(())
}
#[test]
fn nested_column_view_preserves_encoding_and_global_ids() -> Result<()> {
    use std::sync::Arc;
    let c = Column::from_values(
        [
            Some(Scalar::Int(9)),
            None,
            Some(Scalar::Text("9".into())),
            Some(Scalar::Int(9)),
        ]
        .into_iter(),
    );
    let first = c.project(Arc::new(vec![0, 2, 3]))?;
    let second = first.project(Arc::new(vec![1, 2]))?;
    assert_eq!(second.get(0), c.get(2));
    assert_eq!(second.get(1), c.get(3));
    let decoded = Column::decode(&second.encode(), 2)?;
    assert_eq!(decoded.get(0), second.get(0));
    assert_eq!(decoded.get(1), second.get(1));
    assert!(c.project(Arc::new(vec![4])).is_err());
    Ok(())
}
#[test]
fn sparse_empty_domain_and_negative_scope_guard() -> Result<()> {
    use crate::key_projection::KeyPlan;
    use std::sync::Arc;
    let es = (0..4)
        .map(|i| {
            CanonicalEvent::new(
                format!("e{i}"),
                "T",
                i,
                i,
                format!("t{i}"),
                BTreeMap::from([
                    ("left".into(), Scalar::Int(1)),
                    ("right".into(), Scalar::Int(2)),
                ]),
            )
        })
        .collect::<Vec<_>>();
    let mut query = q(
        "empty-sparse",
        vec![
            atom("a", "T", &[("left", "v")]),
            atom("b", "T", &[("right", "v")]),
        ],
        vec![order("a", "b")],
    );
    let sealed = seal(&es, "empty-domain-source".into())?;
    let plan = KeyPlan::compile(&query)?;
    let (mask, cache) = plan.execute(&sealed.tables["T"].columns, es.len())?;
    assert!(mask.iter().all(|v| !v));
    let view = project_table("T", &sealed.tables["T"], Arc::new(vec![]))?;
    let rs = rows(&view)?;
    let engine =
        Engine::with_projections(&rs, &query, &BTreeSet::from(["T".into()]), Some(&cache))?;
    assert_eq!(engine.count()?.0, num_bigint::BigUint::from(0u32));
    assert!(engine.replay_rows()?.0.is_empty());
    query.branches[0].constraints.push(Constraint::NoEvent {
        after: "a".into(),
        event_types: BTreeSet::from(["T".into()]),
        fields: BTreeMap::new(),
        within_seconds: 10,
    });
    assert!(KeyPlan::compile(&query).is_err());
    Ok(())
}
#[test]
fn prefix_request_multiplicity_is_not_lost() -> Result<()> {
    let (mut es, mut q) = fanout();
    let first = es.remove(0);
    let mut roots = vec![];
    for i in 0..1000 {
        let mut e = first.clone();
        e.event_id = format!("a{i}");
        e.order_seq = i;
        e.block_ts = 0;
        roots.push(e)
    }
    for e in &mut es {
        e.order_seq += 2000;
    }
    roots.extend(es);
    q.query_id = "unregistered-request-sharing".into();
    let st = check(&roots, &q)?;
    assert_eq!(st.requests, 1000);
    assert_eq!(st.distinct_requests, 1);
    assert_eq!(st.aggregate_requests, 1);
    assert_eq!(st.terminal_probes, 0);
    Ok(())
}
#[test]
fn typed_column_extremes() -> Result<()> {
    let c = Column::from_values(
        [
            None,
            Some(Scalar::Int(i64::MIN)),
            Some(Scalar::Int(i64::MAX)),
            Some(Scalar::Bool(false)),
            Some(Scalar::Text("".into())),
            Some(Scalar::Text("17".into())),
            Some(17.into()),
            None,
        ]
        .into_iter(),
    );
    let d = Column::decode(&c.encode(), c.codes.len())?;
    for i in 0..c.codes.len() {
        assert_eq!(c.get(i), d.get(i))
    }
    Ok(())
}
#[test]
fn missing_blocker_type_is_authenticated_empty() -> Result<()> {
    let mut q = queries().remove(0);
    q.branches[0].constraints.push(Constraint::NoEvent {
        after: "a".into(),
        event_types: BTreeSet::from(["AbsentBlock".into()]),
        fields: BTreeMap::new(),
        within_seconds: 100,
    });
    check(&fixture(), &q)?;
    Ok(())
}
fn check(es: &[CanonicalEvent], q: &QuerySpec) -> Result<engine::Stats> {
    let sealed = seal(es, "test-source".into())?;
    let truth = reference(es, q)?;
    let wire = packet(&sealed, q, truth.clone())?;
    let (ts, claims) = authenticate(&sealed.manifest, q, &wire)?;
    let rs = rows(&ts)?;
    let types = sealed.manifest.types.keys().cloned().collect();
    let engine = Engine::new(&rs, q, &types)?;
    let (actual, _) = engine.replay()?;
    ensure!(
        actual == truth,
        "independent oracle differs for {}",
        q.query_id
    );
    let (n, st) = engine.count()?;
    ensure!(
        n == num_bigint::BigUint::from(truth.len()),
        "count mismatch for {}",
        q.query_id
    );
    engine.sound_unique(&claims)?;
    Ok(st)
}
#[test]
fn grammar_oracle_regression() -> Result<()> {
    for q in queries() {
        check(&fixture(), &q)?;
    }
    Ok(())
}
#[test]
fn cached_domains_do_not_share_resolved_constraints() -> Result<()> {
    let es = fixture();
    let mut query = queries().remove(0);
    let mut second = query.branches[0].clone();
    second.id = "same_atomic_domains_different_constraints".into();
    second.constraints.push(Constraint::Compare {
        left: ValueExpr::Field(FieldRef {
            atom: "a".into(),
            field: "asset".into(),
        }),
        op: CompareOp::Eq,
        right: ValueExpr::Constant(8.into()),
    });
    query.branches.push(second);
    let mut third = query.branches[0].clone();
    third.id = "different_constant_domain".into();
    third.atoms[0]
        .fields
        .insert("asset".into(), Term::Constant(7.into()));
    query.branches.push(third);
    check(&es, &query)?;
    Ok(())
}
#[test]
fn field_slots_across_different_type_schemas() -> Result<()> {
    let (mut es, mut query) = fanout();
    es[0].fields.insert("only_on_anchor".into(), 17.into());
    query.branches[0].atoms[0]
        .fields
        .insert("only_on_anchor".into(), Term::Constant(17.into()));
    query.branches[0].constraints.push(Constraint::Compare {
        left: ValueExpr::Field(FieldRef {
            atom: "b".into(),
            field: "kind".into(),
        }),
        op: CompareOp::Ge,
        right: ValueExpr::Constant(1.into()),
    });
    check(&es, &query)?;
    query.branches[0].atoms[1]
        .fields
        .insert("only_on_anchor".into(), Term::Variable("absent".into()));
    check(&es, &query)?;
    Ok(())
}
#[test]
fn authentication_and_answer_tampering() -> Result<()> {
    let es = fixture();
    let q = queries().remove(0);
    let s = seal(&es, "trusted".into())?;
    let truth = reference(&es, &q)?;
    ensure!(!truth.is_empty(), "test requires answers");
    let raw = packet(&s, &q, truth.clone())?;
    let mut bad = raw.clone();
    *bad.last_mut().unwrap() ^= 1;
    assert!(authenticate(&s.manifest, &q, &bad).is_err());
    assert!(authenticate(&s.manifest, &q, &raw[..raw.len() - 1]).is_err());
    let mut wrong = q.clone();
    wrong.query_id.push('x');
    assert!(authenticate(&s.manifest, &wrong, &raw).is_err());
    let mut wrong = s.manifest.clone();
    wrong.source_digest.push('x');
    assert!(authenticate(&wrong, &q, &raw).is_err());
                                                                             
                                                                   
    let mut b = raw.as_slice();
    let n = read(&mut b)? as usize;
    let mut meta: Value = serde_json::from_slice(take(&mut b, n)?)?;
    meta["columns"].as_array_mut().unwrap().pop();
    let meta = serde_json::to_vec(&meta)?;
    let mut bad = vec![];
    put(&mut bad, meta.len() as u64);
    bad.extend(meta);
    bad.extend(b);
    assert!(authenticate(&s.manifest, &q, &bad).is_err());
    let (ts, claims) = authenticate(&s.manifest, &q, &raw)?;
    let rs = rows(&ts)?;
    let engine = Engine::new(&rs, &q, &s.manifest.types.keys().cloned().collect())?;
    assert_eq!(engine.canonical_answers(&claims)?, truth);
    let mut dup = claims.clone();
    dup.push(claims[0].clone());
    assert!(engine.sound_unique(&dup).is_err());
    let mut fake = claims.clone();
    fake[0].rows[0].0 = "forged".into();
    assert!(engine.sound_unique(&fake).is_err());
    let mut fake = claims.clone();
    fake[0].rows[0].1 = usize::MAX;
    assert!(engine.sound_unique(&fake).is_err());
    let mut fake = claims.clone();
    fake[0].rows.swap(0, 1);
    assert!(engine.sound_unique(&fake).is_err());
    let (n, _) = engine.count()?;
    assert_ne!(n, num_bigint::BigUint::from(truth.len() - 1));
    Ok(())
}
fn fanout() -> (Vec<CanonicalEvent>, QuerySpec) {
    let mut es = vec![CanonicalEvent::new(
        "root",
        "F",
        0,
        0,
        "t0",
        BTreeMap::from([("asset".into(), 7.into())]),
    )];
    for i in 1..=80 {
        es.push(CanonicalEvent::new(
            format!("s{i}"),
            "S",
            i,
            i,
            format!("t{}", i % 4),
            BTreeMap::from([("asset".into(), 7.into()), ("kind".into(), (i % 3).into())]),
        ))
    }
    let mut query = q(
        "never-registered-fanout",
        vec![
            atom("a", "F", &[("asset", "x")]),
            atom("b", "S", &[("asset", "x")]),
        ],
        vec![
            order("a", "b"),
            Constraint::DistinctTransactions(vec!["a".into(), "b".into()]),
        ],
    );
    query.scope = ScopeSpec::Episode {
        anchor_atom: "a".into(),
        window_seconds: 100,
    };
    (es, query)
}
#[test]
fn range_exclusion_and_nonmonotone_fallback() -> Result<()> {
    let (mut es, q) = fanout();
    let st = check(&es, &q)?;
    assert!(st.aggregate_requests > 0);
    es[20].block_ts = 99;
    es[21].block_ts = 1;
    let st = check(&es, &q)?;
    assert_eq!(st.aggregate_requests, 0);
    assert!(st.exact_fallback_requests > 0);
    Ok(())
}
#[test]
fn missing_empty_and_typed_inequality() -> Result<()> {
    let (mut es, mut q) = fanout();
    q.branches[0].constraints.push(Constraint::Compare {
        left: ValueExpr::Field(FieldRef {
            atom: "b".into(),
            field: "kind".into(),
        }),
        op: CompareOp::Ne,
        right: ValueExpr::Constant(Scalar::Int(1)),
    });
    es[8].fields.remove("kind");
    es[9].fields.insert("kind".into(), Scalar::Text("1".into()));
    es[10].fields.insert("kind".into(), Scalar::Text("".into()));
    check(&es, &q)?;
    Ok(())
}
#[test]
fn distinct_values_and_joint_exclusion() -> Result<()> {
    let (es, mut q) = fanout();
    q.branches[0].constraints.push(Constraint::DistinctValues {
        values: vec![
            ValueExpr::Constant(1.into()),
            ValueExpr::Field(FieldRef {
                atom: "b".into(),
                field: "kind".into(),
            }),
        ],
        min_count: 2,
        max_count: 2,
        ignore_empty_text: false,
    });
    let st = check(&es, &q)?;
    assert!(st.aggregate_requests + st.bitmap_requests + st.prepared_scan_requests > 0);
    Ok(())
}
#[test]
fn bitmap_count_matches_bruteforce_at_word_boundaries_and_missing_values() -> Result<()> {
    let mut bitmap_used = 0;
    for len in [1usize, 16, 63, 64, 65, 80] {
        for variant in 0..6 {
            let (mut es, mut query) = fanout();
            es.truncate(len+1);
            es[0].fields.insert("kind".into(), Scalar::Int(1));
                                                                           
                                                          
            for (i, e) in es.iter_mut().enumerate().skip(1) {
                if variant > 0 && i % 7 == 0 { e.fields.remove("kind"); }
                if variant > 0 && i % 11 == 0 { e.fields.insert("kind".into(), Scalar::Text("".into())); }
                if variant > 0 && i % 13 == 0 { e.fields.insert("kind".into(), Scalar::Text("1".into())); }
                if variant == 5 { e.block_ts = ((i*7)%19) as i64; }
            }
            for j in 1..=2 {
                let mut root = es[0].clone();
                root.event_id = format!("extra-root-{j}");
                root.order_seq = -j;
                es.push(root);
            }
            let f = |a: &str| ValueExpr::Field(FieldRef { atom:a.into(), field:"kind".into() });
            query.branches[0].constraints.push(Constraint::Compare {
                left:f("b"), op:if variant == 2 { CompareOp::Eq } else { CompareOp::Ne }, right:f("a"),
            });
            if variant >= 3 {
                query.branches[0].constraints.push(Constraint::DistinctValues {
                    values:vec![f("a"), f("b"), ValueExpr::Constant(Scalar::Text("1".into()))],
                    min_count:2, max_count:2, ignore_empty_text:variant == 4,
                });
            }
            query.query_id = format!("unregistered-word-edge-{len}-{variant}");
            let st = check(&es, &query)?;
            bitmap_used += st.bitmap_requests;
            if len == 64 && variant == 0 { assert!(st.bitmap_requests > 0); }
                                                                           
                                                                            
            if (variant == 5 && len >= 16) || (len > 64 && variant == 0) {
                assert_eq!(st.bitmap_requests, 0);
            }
        }
    }
    assert!(bitmap_used > 0);
    Ok(())
}
#[test]
fn bitmap_count_keeps_parallel_events_and_exact_joint_exclusions() -> Result<()> {
    let (mut es, mut query) = fanout();
    es.truncate(65);
    es[0].fields.insert("kind".into(), Scalar::Int(1));
                                                                         
                                                                                
    query.branches[0].constraints.push(Constraint::Compare {
        left:ValueExpr::Field(FieldRef { atom:"b".into(),field:"kind".into() }),
        op:CompareOp::Ne,
        right:ValueExpr::Field(FieldRef { atom:"a".into(),field:"kind".into() }),
    });
    for e in es.iter_mut().skip(1) { e.block_ts = 1; }
    for j in 1..=2 {
        let mut root = es[0].clone();
        root.event_id = format!("extra-root-{j}");
        root.order_seq = -j;
        es.push(root);
    }
    let st = check(&es, &query)?;
    assert_eq!(st.bitmap_requests,2);
    assert_eq!(st.terminal_probes,64);
    assert!(st.bitmap_counted_bindings > 1);
                                                               
    query.branches[0].constraints.push(Constraint::NoEvent {
        after:"b".into(),event_types:BTreeSet::from(["S".into()]),
        fields:BTreeMap::new(),within_seconds:100,
    });
    let st = check(&es, &query)?;
    assert_eq!(st.bitmap_requests,0);
    Ok(())
}
#[test]
fn wildcard_blocker_complete_catalog() -> Result<()> {
    let mut q = queries().remove(0);
    q.branches[0].constraints.push(Constraint::NoEvent {
        after: "a".into(),
        event_types: BTreeSet::new(),
        fields: BTreeMap::from([("asset".into(), ValueExpr::Variable("x".into()))]),
        within_seconds: 100,
    });
    check(&fixture(), &q)?;
    Ok(())
}
#[test]
fn integer_boundaries() -> Result<()> {
    let (mut es, mut q) = fanout();
    for e in &mut es {
        e.block_ts = i64::MAX - 100 + e.block_ts;
    }
    q.scope = ScopeSpec::Episode {
        anchor_atom: "a".into(),
        window_seconds: 100,
    };
    check(&es, &q)?;
    Ok(())
}
fn atom(id: &str, ty: &str, fields: &[(&str, &str)]) -> AtomSpec {
    AtomSpec {
        id: id.into(),
        event_types: BTreeSet::from([ty.into()]),
        fields: fields
            .iter()
            .map(|(f, v)| (f.to_string(), Term::Variable(v.to_string())))
            .collect(),
        local_predicates: vec![],
    }
}
fn order(a: &str, b: &str) -> Constraint {
    Constraint::Order {
        earlier: a.into(),
        later: b.into(),
        field: "order_seq".into(),
    }
}
fn q(name: &str, atoms: Vec<AtomSpec>, constraints: Vec<Constraint>) -> QuerySpec {
    QuerySpec {
        query_id: name.into(),
        result_semantics: "full_event_binding_v1".into(),
        scope: ScopeSpec::Episode {
            anchor_atom: atoms[0].id.clone(),
            window_seconds: 20,
        },
        branches: vec![BranchSpec {
            id: "dynamic_branch".into(),
            atoms,
            constraints,
            event_injective: true,
        }],
    }
}
pub(crate) fn fixture() -> Vec<CanonicalEvent> {
    let mut es = vec![];
    for (i, ty, src, dst, asset, time, tx) in [
        (1, "F", 1, 2, 7, 0, "T1"),
        (2, "S", 2, 3, 7, 1, "T2"),
        (3, "S", 2, 3, 7, 2, "T3"),
        (4, "B", 3, 1, 7, 3, "T4"),
        (5, "Block", 0, 0, 7, 4, "T5"),
        (6, "F", 4, 5, 8, 10, "T6"),
        (7, "S", 5, 6, 8, 11, "T6"),
        (8, "B", 6, 4, 8, 12, "T6"),
        (9, "F", 8, 9, 9, 40, "T9"),
        (10, "S", 9, 10, 9, 41, "T10"),
        (11, "B", 10, 8, 9, 42, "T11"),
        (12, "Block", 0, 0, 9, 70, "T12"),
        (13, "F", 21, 22, 31, 80, "T13"),
        (14, "S", 41, 42, 51, 81, "T14"),
        (15, "B", 61, 62, 71, 82, "T15"),
    ] {
        es.push(CanonicalEvent::new(
            format!("id:{i:03}"),
            ty,
            i,
            time,
            tx,
            BTreeMap::from([
                ("move_from".into(), src.into()),
                ("move_to".into(), dst.into()),
                ("asset".into(), asset.into()),
                ("amount".into(), (i * 10).into()),
            ]),
        ));
    }
    es
}
pub(crate) fn queries() -> Vec<QuerySpec> {
    let a = || atom("a", "F", &[("asset", "x"), ("move_to", "u")]);
    let b = || {
        atom(
            "b",
            "S",
            &[("asset", "x"), ("move_from", "u"), ("move_to", "v")],
        )
    };
    let c = || atom("c", "B", &[("asset", "x"), ("move_from", "v")]);
    let pair = q("new-pair", vec![a(), b()], vec![order("a", "b")]);
    let chain = q(
        "unseen-three-chain",
        vec![a(), b(), c()],
        vec![order("a", "b"), order("b", "c")],
    );
    let mut cycle = chain.clone();
    cycle.query_id = "unseen-cycle".into();
    cycle.branches[0].atoms[0]
        .fields
        .insert("move_from".into(), Term::Variable("return_to".into()));
    cycle.branches[0].atoms[2]
        .fields
        .insert("move_to".into(), Term::Variable("return_to".into()));
    let tree = q(
        "new-tree",
        vec![a(), b(), atom("side", "B", &[("asset", "x")])],
        vec![order("a", "b"), order("a", "side")],
    );
    let four = q(
        "new-four-event",
        vec![
            a(),
            b(),
            atom("second", "S", &[("asset", "x"), ("move_from", "u")]),
            c(),
        ],
        vec![order("a", "b"), order("b", "second"), order("second", "c")],
    );
    let mut negative = pair.clone();
    negative.query_id = "single-blocker".into();
    negative.branches[0].constraints.push(Constraint::NoEvent {
        after: "a".into(),
        event_types: BTreeSet::from(["Block".into()]),
        fields: BTreeMap::from([("asset".into(), ValueExpr::Variable("x".into()))]),
        within_seconds: 100,
    });
    let noeq = q(
        "no-equality-fallback",
        vec![atom("first", "F", &[]), atom("last", "B", &[])],
        vec![order("first", "last")],
    );
    let mut same = pair.clone();
    same.query_id = "runtime-sametx".into();
    same.scope = ScopeSpec::SameTx;
    let mut local = pair.clone();
    local.query_id = "local-range-and-transaction".into();
    local.branches[0].atoms[0]
        .local_predicates
        .push(LocalPredicate {
            field: "amount".into(),
            op: CompareOp::Ge,
            value: 50.into(),
        });
    local.branches[0]
        .constraints
        .push(Constraint::DistinctTransactions(vec![
            "a".into(),
            "b".into(),
        ]));
    let mut multi = pair.clone();
    multi.query_id = "canonical-branches".into();
    let mut b2 = multi.branches[0].clone();
    b2.id = "second_branch".into();
    multi.branches.push(b2);
    let mut explicit = q(
        "explicit-field-equality",
        vec![atom("left", "F", &[]), atom("right", "S", &[])],
        vec![Constraint::Compare {
            left: ValueExpr::Field(FieldRef {
                atom: "left".into(),
                field: "asset".into(),
            }),
            op: CompareOp::Eq,
            right: ValueExpr::Field(FieldRef {
                atom: "right".into(),
                field: "asset".into(),
            }),
        }],
    );
    explicit.branches[0]
        .constraints
        .push(order("left", "right"));
    let empty = q(
        "unknown-empty-type",
        vec![atom("a", "MissingType", &[])],
        vec![],
    );
    let mut renamed = pair.clone();
    renamed.query_id = "never-registered-name-734921".into();
    vec![
        pair, chain, cycle, tree, four, negative, noeq, same, local, multi, explicit, empty,
        renamed,
    ]
}
