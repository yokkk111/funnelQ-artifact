                                                                       
use crate::{
    columns::{self, RowAnswer},
    engine::Engine,
    hybrid_key_audit::{data, Trade},
    hybrid_live_server,
    key_projection::KeyPlan,
    query::QuerySpec,
    temporal_demand::{local_domains, TemporalPlan},
};
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet, fs, io::Write, path::Path, process::Command, sync::Arc, time::Instant,
};

fn write(p: &Path, b: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)?
        .write_all(b)?;
    Ok(())
}
pub(crate) fn oracle(root: &Path, qnum: usize, trades: &[Trade], ans: &[RowAnswer]) -> Result<String> {
    let bytes = fs::read(root.join(format!(
        "FunnelQ_ScopeFactor_Feasibility/results/q{qnum}_reference.json"
    )))?;
    let v: Value = serde_json::from_slice(&bytes)?;
    let mut expected = serde_json::from_value::<Vec<Vec<u64>>>(v["answers"].clone())?;
    let mut got = ans
        .iter()
        .map(|a| a.rows.iter().map(|r| trades[r.1].order).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    got.sort();
    expected.sort();
    ensure!(
        got == expected,
        "complete independent answer set mismatch Q{qnum}"
    );
    Ok(columns::digest(&bytes))
}
pub(crate) struct Eval {
    pub answers: Vec<RowAnswer>,
    pub ids: Vec<usize>,
    pub fields: BTreeSet<String>,
    pub stages: Value,
}
pub(crate) fn evaluate(seal: &columns::Seal, q: &QuerySpec, temporal: bool) -> Result<Eval> {
    let t = Instant::now();
    let p = KeyPlan::compile(q)?;
    let base = &seal.tables[&p.event_type];
    let mut d = p.execute_domains(&base.columns, base.n)?;
    let key_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let stats = if temporal {
        Some(TemporalPlan::compile(q)?.apply(&p, &base.columns, &mut d)?)
    } else {
        None
    };
    let temporal_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let ids = Arc::new((0..base.n).filter(|&i| d.union[i]).collect::<Vec<_>>());
    let view = columns::project_table(&p.event_type, base, ids.clone())?;
    let rows = columns::rows(&view)?;
    let atom = if temporal {
        Some(local_domains(q, &d, &ids)?)
    } else {
        None
    };
    let types = BTreeSet::from([p.event_type.clone()]);
    let engine =
        Engine::with_verified_domains(&rows, q, &types, Some(&d.projections), atom.as_ref())?;
    let compile_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let answers = engine.replay_rows()?.0;
    let exact_s = t.elapsed().as_secs_f64();
    let stages = json!({"key_filter_s":key_s,"temporal_filter_s":temporal_s,"exact_compile_s":compile_s,
        "enumeration_s":exact_s,"temporal_stats":stats,"projection_layouts":d.projections.projections.len(),
        "reused_projection_indexes":engine.reused_projection_indexes()});
    let mut fields = p.fields;
    if temporal {
        fields.extend(["order_seq".into(), "block_ts".into()]);
    }
    Ok(Eval {
        answers,
        ids: (*ids).clone(),
        fields,
        stages,
    })
}
pub fn run(args: &[String]) -> Result<()> {
    ensure!(args.len() == 5, "temporal-compare ROOT NEW_OUTPUT ROUNDS");
    let root = Path::new(&args[2]);
    let out = Path::new(&args[3]);
    let rounds: usize = args[4].parse()?;
    ensure!((1..=8).contains(&rounds), "round count");
    fs::create_dir(out)?;
    let t = Instant::now();
    let trades = data(root)?;
    let (events, source) = crate::events(root, None)?;
    let seal = columns::seal(&events, source.clone())?;
    drop(events);
                                                                                  
    let base = &seal.tables["NFTTrade"];
    ensure!(base.n == trades.len(), "source length");
    for (i, r) in trades.iter().enumerate() {
        for (f, v) in [
            ("order_seq", r.order),
            ("block_ts", r.time),
            ("move_from", r.src as u64),
            ("move_to", r.dst as u64),
            ("nft", r.nft as u64),
            ("collection_contract", r.collection as u64),
        ] {
            ensure!(
                base.columns[f].get(i) == Some(&crate::model::Scalar::Int(i64::try_from(v)?)),
                "source row/field alignment"
            );
        }
        ensure!(
            base.columns["tx_hash"].get(i)
                == Some(&crate::model::Scalar::Text(format!("tx:{}", r.tx))),
            "tx alignment"
        );
        ensure!(
            base.columns["event_id"].get(i)
                == Some(&crate::model::Scalar::Text(format!("order:{}", r.order))),
            "identity alignment"
        );
    }
    let tree = hybrid_live_server::tree(&trades)?;
    let offline_s = t.elapsed().as_secs_f64();
    crate::save(&out.join("trusted_manifest.json"), &seal.manifest)?;
    let hash = tree.last().unwrap()[0]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    crate::save(
        &out.join("trusted_row_root.json"),
        &json!({"nrows":trades.len(),"block_rows":16,"root":hash,"height":tree.len()-1}),
    )?;
    let mut paths = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|p| p.map(|p| p.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    ensure!(paths.len() == 5, "formal five queries");
    let queries = paths
        .iter()
        .map(|p| crate::load_query(p))
        .collect::<Result<Vec<_>>>()?;
    let mut validation = vec![];
    for (i, q) in queries.iter().enumerate() {
        let t = Instant::now();
        let rs = columns::rows(&seal.tables)?;
        let eng = Engine::new(&rs, q, &BTreeSet::from(["NFTTrade".into()]))?;
        let answers = eng.replay_rows()?.0;
        let live_s = t.elapsed().as_secs_f64();
        let ref_hash = oracle(root, i + 1, &trades, &answers)?;
        validation.push(json!({"query":i+1,"source_sha256":source,"query_sha256":columns::digest(&serde_json::to_vec(q)?),
            "saved_oracle_sha256":ref_hash,"live_full_domain_generic_s":live_s,"answers":answers.len(),"full_binding_set_equal":true}));
        eprintln!(
            "TEMPORAL full-domain reference Q{} answers={} equal",
            i + 1,
            answers.len()
        );
    }
    crate::save(&out.join("validation.json"), &validation)?;
    let mut records = vec![];
    for rep in 0..=rounds {
        let order = if rep % 2 == 0 {
            (0..5).collect::<Vec<_>>()
        } else {
            (0..5).rev().collect()
        };
        for i in order {
            let q = &queries[i];
            for temporal in if (i + rep) % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let backend = if temporal { "temporal" } else { "hybrid" };
                let dir = out.join(format!("q{}_r{rep}_{backend}", i + 1));
                fs::create_dir(&dir)?;
                let t = Instant::now();
                let ev = evaluate(&seal, q, temporal)?;
                let query_s = t.elapsed().as_secs_f64();
                oracle(root, i + 1, &trades, &ev.answers)?;
                let t = Instant::now();
                let (wire, opened, proof) = hybrid_live_server::packet_version(
                    &trades,
                    &seal,
                    &tree,
                    q,
                    &ev.ids,
                    &ev.fields,
                    &ev.answers,
                    if temporal {
                        "hybrid-temporal-audit/v1"
                    } else {
                        "hybrid-row-audit/v1"
                    },
                )?;
                let packet_s = t.elapsed().as_secs_f64();
                write(&dir.join("response.bin"), &wire)?;
                crate::save(&dir.join("query.json"), q)?;
                let mut modes = serde_json::Map::new();
                for mode in if rep % 2 == 0 {
                    ["count", "replay"]
                } else {
                    ["replay", "count"]
                } {
                    let result = dir.join(format!("{mode}.json"));
                    let t = Instant::now();
                    let run = Command::new(std::env::current_exe()?)
                        .arg(if temporal {
                            "temporal-client"
                        } else {
                            "hybrid-client"
                        })
                        .arg(&paths[i])
                        .arg(dir.join("query.json"))
                        .arg(out.join("trusted_manifest.json"))
                        .arg(out.join("trusted_row_root.json"))
                        .arg(dir.join("response.bin"))
                        .arg(&result)
                        .arg(mode)
                        .output()?;
                    let wall = t.elapsed().as_secs_f64();
                    ensure!(
                        run.status.success(),
                        "{backend}/{mode} failed: {}",
                        String::from_utf8_lossy(&run.stderr)
                    );
                    let details: Value = serde_json::from_slice(&fs::read(result)?)?;
                    ensure!(
                        details["verified"] == true
                            && details["answers"].as_u64() == Some(ev.answers.len() as u64),
                        "client result"
                    );
                    modes.insert(mode.into(),json!({"client_wall_s":wall,"compute_e2e_s":query_s+packet_s+wall,"details":details}));
                }
                let record = json!({"query":i+1,"backend":backend,"round":rep,"warmup":rep==0,"FP":0,"FN":0,
                    "answers":ev.answers.len(),"selected_rows":ev.ids.len(),"opened_rows":opened,"block_rows":16,
                    "wire_bytes":wire.len(),"proof_bytes":proof,"server_query_s":query_s,"server_packet_s":packet_s,
                    "server_stages":ev.stages,"modes":modes});
                eprintln!("TEMPORAL Q{} {backend} r{rep} count={:.3}s wire={:.3}MiB candidates={} FP=FN=0",i+1,
                    record["modes"]["count"]["compute_e2e_s"].as_f64().unwrap(),wire.len() as f64/1048576.,ev.ids.len());
                crate::save(&dir.join("record.json"), &record)?;
                records.push(record);
            }
        }
    }
    crate::save(
        &out.join("summary.json"),
        &json!({"version":"temporal-demand-paired/v1","input_rows":trades.len(),
        "source_sha256":source,"offline_build_s":offline_s,"rounds":rounds,"records":records,
        "network_included":false,"on_chain_deployed":false,"trusted_sealer":true,"fresh_full_domain_reference":true,
        "timing":"interleaved same binary/CPU; isolated count/replay; server query + packet + client process wall; network/offline/server-write excluded"}),
    )?;
    Ok(())
}
