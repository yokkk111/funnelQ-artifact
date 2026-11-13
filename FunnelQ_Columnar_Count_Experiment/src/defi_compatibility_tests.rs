                                                                           
use super::*;
use crate::{model::Scalar, query::{AtomSpec, BranchSpec, CompareOp, FieldRef, LocalPredicate, ValueExpr}};

fn compile(raw: &Value, window: i64) -> Result<QuerySpec> {
    let atoms = raw["events"].as_array().context("events")?.iter().map(|a| {
        let id = a["id"].as_str().context("id")?.to_string();
        let mut local = Vec::new();
        for f in raw["attr_filters"].as_array().into_iter().flatten() {
            if f["event"].as_str() == Some(&id) {
                ensure!(f["field"] == "amount" && f["value"] == "0", "only exact zero/sign transform supported");
                let op = match f["op"].as_str() {Some("<") => CompareOp::Lt, Some(">") => CompareOp::Gt, _ => anyhow::bail!("amount operator")};
                local.push(LocalPredicate { field: "amount_sign".into(), op, value: Scalar::Int(0) });
            }
        }
        let fields = a["roles"].as_object().context("roles")?.iter().map(|(f,v)| {
            Ok((f.clone(), Term::Variable(v.as_str().context("role")?.strip_prefix('$').context("variable")?.into())))
        }).collect::<Result<BTreeMap<_,_>>>()?;
        Ok(AtomSpec {id, event_types: BTreeSet::from([a["type"].as_str().context("type")?.into()]), fields, local_predicates: local})
    }).collect::<Result<Vec<_>>>()?;
    let mut constraints = Vec::new();
    for c in raw["order"].as_array().context("order")? {
        constraints.push(Constraint::Order {earlier: c[0].as_str().unwrap().into(), later: c[1].as_str().unwrap().into(), field: "order_seq".into()});
    }
    for c in raw["neq_constraints"].as_array().into_iter().flatten() {
        constraints.push(Constraint::Compare {
            left: ValueExpr::Field(FieldRef {atom: c[0].as_str().unwrap().into(), field: c[1].as_str().unwrap().into()}),
            op: CompareOp::Ne,
            right: ValueExpr::Field(FieldRef {atom: c[2].as_str().unwrap().into(), field: c[3].as_str().unwrap().into()}),
        });
    }
    Ok(QuerySpec {query_id: raw["query_name"].as_str().context("query_name")?.into(), result_semantics: "full_event_binding_v1".into(),
        scope: if raw["scope"] == "same_tx" {ScopeSpec::SameTx} else {ScopeSpec::Episode {anchor_atom: raw["scope_params"]["semantic_anchor_event"].as_str().unwrap().into(), window_seconds: window}},
        branches: vec![BranchSpec {id: "defi".into(), atoms, constraints, event_injective: true}]})
}

#[test]
#[ignore = "requires prepared real-data fixtures; run explicitly"]
fn defi_real_subsets_count_replay_sql_and_scc_gate() -> Result<()> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../research/defi_scc");
    let fixture: Value = serde_json::from_slice(&fs::read(path.join("fixtures.json"))?)?;
    let dummy = Manifest {version: VERSION.into(), source_sha256: "not-a-defi-manifest".into(), schema: "NFTTrade".into(), trust: "test only".into(), scales: SCALES.to_vec(), prefix_counts: vec![0;SCALES.len()], nrows: 0, page_rows: PAGE, root: [0;32], domain_only: false};
    let mut reports = Vec::new();
    for case in fixture["cases"].as_array().context("cases")? {
        let q = compile(&case["query"], case["window"].as_i64().unwrap())?;
        let es: Vec<CanonicalEvent> = serde_json::from_value(case["events"].clone())?;
                                                                                        
        let err = plan(&q, &dummy).expect_err("DeFi cannot be authenticated by NFT manifest").to_string();
        ensure!(err.contains("unsupported event type"), "unexpected gate {err}");
        let mut renamed = q.clone();
        renamed.query_id = "unregistered-runtime-query".into();
        ensure!(plan(&renamed,&dummy).is_err(), "query-name-dependent admission");
        let start = Instant::now();
        let truth = crate::reference(&es, &q)?;
        let binding_ids = |xs: &[Answer]| -> Result<BTreeSet<Vec<i64>>> {
            xs.iter().map(|a| a.events.iter().map(|id| {
                Ok(id.strip_prefix("defi:").context("event id")?.parse::<i64>()?)
            }).collect::<Result<Vec<_>>>()).collect()
        };
        let sql: BTreeSet<Vec<i64>> = serde_json::from_value(case["expected"].clone())?;
        ensure!(binding_ids(&truth)? == sql, "SQL/Generic oracle mismatch {}", q.query_id);
        let sealed = columns::seal(&es, "trusted-local-sample-not-full-epoch".into())?;
        let wire = columns::packet(&sealed, &q, truth.clone())?;
        let (ts, claims) = columns::authenticate(&sealed.manifest, &q, &wire)?;
        let rs = columns::rows(&ts)?;
        let types = sealed.manifest.types.keys().cloned().collect();
        let eng = Engine::new(&rs, &q, &types)?;
        let (replay, _) = eng.replay()?;
        let (count, _) = eng.count()?;
        eng.sound_unique(&claims)?;
        ensure!(binding_ids(&replay)? == sql, "engine replay mismatch");
        ensure!(count == num_bigint::BigUint::from(sql.len()), "count mismatch");
        if !claims.is_empty() {
            let mut duplicate = claims.clone();
            duplicate.push(claims[0].clone());
            ensure!(eng.sound_unique(&duplicate).is_err(), "duplicate not rejected");
            let mut omitted = claims.clone();
            omitted.pop();
            ensure!(num_bigint::BigUint::from(omitted.len()) != count, "omission count seal failed");
        }
        let mut bad = wire.clone();
        let mid = bad.len()/2;
        bad[mid] ^= 1;
        ensure!(columns::authenticate(&sealed.manifest, &q, &bad).is_err(), "tampered package accepted");
        reports.push(json!({"label":case["label"],"window":case["window"],"sample_events":es.len(),"answers":sql.len(),
            "false_positive":0,"false_negative":0,"generic_full_input_auth":"passed on sample",
            "scc_status":"rejected: incompatible committed schema","scc_error":err,"sample_package_bytes":wire.len(),"functional_test_seconds":start.elapsed().as_secs_f64(),
            "duplicate_rejected":true,"omission_count_mismatch":true,"tamper_rejected":true}));
    }
    ensure!(reports.len()==20,"expected S1-S5 and E1-E5 at 3 windows");
    fs::write(path.join("rust_results.json"),serde_json::to_vec_pretty(&json!({"test_scope":"real-event subsets only; not full epoch performance or deployed DeFi authentication", "configurations":reports}))?)?;
    Ok(())
}
