#![allow(dead_code)]
mod columns;
mod columnview_compare;
mod engine;
mod hybrid_key_audit;
mod hybrid_live_server;
mod hybrid_packet_client;
#[path = "../../Q1CertifiedCountTreeSimulator/src/plain_index.rs"]
mod index;
mod key_projection;
#[cfg(not(test))]
mod legacy_count;
mod memory;
#[path = "../../FunnelQ_Residual_Obligation/src/model.rs"]
mod model;
#[path = "../../FunnelQ_Residual_Obligation/src/oracle.rs"]
mod oracle;
mod prefix_structure;
mod progressive_fields;
mod semantic_core;
#[path = "../../FunnelQ_Residual_Obligation/src/query.rs"]
mod query;
mod temporal_compare;
mod temporal_demand;
mod evaluator {
    use crate::model::Scalar;
    use std::collections::{BTreeMap, BTreeSet};
    #[derive(Default)]
    pub struct EvaluationRequest {
        pub allowed_event_ids: Option<BTreeSet<String>>,
        pub fixed_atoms: BTreeMap<String, String>,
    }
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub struct MatchRecord {
        pub query_id: String,
        pub branch_id: String,
        pub event_bindings: BTreeMap<String, String>,
        pub variable_bindings: BTreeMap<String, Scalar>,
    }
}
use anyhow::{ensure, Context, Result};
use columns::*;
use engine::Engine;
use model::*;
use query::*;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Write},
    path::Path,
    time::Instant,
};
fn save(p: &Path, v: &impl serde::Serialize) -> Result<()> {
    let f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(p)?;
    serde_json::to_writer_pretty(f, v)?;
    Ok(())
}
fn reference(es: &[CanonicalEvent], q: &QuerySpec) -> Result<Vec<Answer>> {
    let ix = index::QueryIndependentIndex::build_plain(es.to_vec())?;
    let mut out = BTreeSet::new();
    let requests = match &q.scope {
        ScopeSpec::Episode { anchor_atom, .. } => es
            .iter()
            .map(|e| evaluator::EvaluationRequest {
                allowed_event_ids: None,
                fixed_atoms: BTreeMap::from([(anchor_atom.clone(), e.event_id.clone())]),
            })
            .collect(),
        _ => vec![evaluator::EvaluationRequest::default()],
    };
    for req in requests {
        for m in oracle::brute_force_query(&ix, q, &req)? {
            let b = q
                .branches
                .iter()
                .find(|b| b.id == m.branch_id)
                .context("branch")?;
            out.insert(Answer {
                branch: m.branch_id,
                events: b
                    .atoms
                    .iter()
                    .map(|a| m.event_bindings[&a.id].clone())
                    .collect(),
            });
        }
    }
    Ok(out.into_iter().collect())
}
fn load_query(p: &Path) -> Result<QuerySpec> {
    let v: Value = serde_json::from_slice(&fs::read(p)?)?;
    let mut q = compile_query_document(&v)?;
    q.scope = ScopeSpec::Episode {
        anchor_atom: q.branches[0].atoms[0].id.clone(),
        window_seconds: v["full_binding_plan"]["delta_seconds"]
            .as_i64()
            .context("window")?,
    };
    Ok(q)
}
fn events(root: &Path, limit: Option<usize>) -> Result<(Vec<CanonicalEvent>, String)> {
    let bytes=fs::read(root.join("FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin"))?;
    ensure!(bytes.len() % 37 == 0, "input framing");
    let sha = digest(&bytes);
    let mut es = vec![];
    for r in bytes.chunks_exact(37) {
        if r[36] != 1 {
            continue;
        }
        let u64at = |o| u64::from_le_bytes(r[o..o + 8].try_into().unwrap());
        let u32at = |o| u32::from_le_bytes(r[o..o + 4].try_into().unwrap());
        let order = i64::try_from(u64at(0))?;
        let ts = i64::try_from(u64at(8))?;
        es.push(CanonicalEvent::new(
            format!("order:{order}"),
            "NFTTrade",
            order,
            ts,
            format!("tx:{}", u32at(32)),
            BTreeMap::from([
                ("move_from".into(), (u32at(16) as i64).into()),
                ("move_to".into(), (u32at(20) as i64).into()),
                ("nft".into(), (u32at(24) as i64).into()),
                ("collection_contract".into(), (u32at(28) as i64).into()),
            ]),
        ));
        if limit.is_some_and(|n| es.len() >= n) {
            break;
        }
    }
    if limit.is_none() {
        ensure!(es.len() == 1_123_403, "full trade domain regression")
    };
    Ok((es, sha))
}
fn compress(b: &[u8]) -> Result<Vec<u8>> {
    let mut z = flate2::write::ZlibEncoder::new(vec![], flate2::Compression::fast());
    z.write_all(b)?;
    Ok(z.finish()?)
}
fn decompress(b: &[u8]) -> Result<Vec<u8>> {
    let mut out = vec![];
    flate2::read::ZlibDecoder::new(b)
        .take(1_000_000_001)
        .read_to_end(&mut out)?;
    ensure!(out.len() <= 1_000_000_000, "decode resource cap");
    Ok(out)
}
fn benchmark(
    seal: &Seal,
    q: &QuerySpec,
    truth: &[Answer],
    rounds: usize,
    export: &Path,
) -> Result<Value> {
    let mut runs = vec![];
    let types = seal.manifest.types.keys().cloned().collect::<BTreeSet<_>>();
    for rep in 0..=rounds {
        let t = Instant::now();
        let server_rows = rows(&seal.tables)?;
        let server = Engine::new(&server_rows, q, &types)?;
        let (answers, _) = server.replay_rows()?;
        let server_query = t.elapsed().as_secs_f64();
        ensure!(
            server.canonical_answers(&answers)? == truth,
            "server/reference FULL SET mismatch"
        );
        let t = Instant::now();
        let raw = packet_rows(seal, q, answers)?;
        let wire = compress(&raw)?;
        let assemble = t.elapsed().as_secs_f64();
        let mut modes = BTreeMap::new();
        for count in if rep % 2 == 0 {
            [true, false]
        } else {
            [false, true]
        } {
            let t = Instant::now();
            let decoded = decompress(&wire)?;
            let decode = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let (tables, claims) = authenticate(&seal.manifest, q, &decoded)?;
            let auth = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let rs = rows(&tables)?;
            let engine = Engine::new(&rs, q, &types)?;
            let compile = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let (stat, sound) = if count {
                let (n, stat) = engine.count()?;
                let counting = t.elapsed().as_secs_f64();
                let t = Instant::now();
                engine.sound_unique(&claims)?;
                ensure!(
                    n == num_bigint::BigUint::from(claims.len()),
                    "cardinality seal"
                );
                let sound = t.elapsed().as_secs_f64();
                (json!({"evaluation_seconds":counting,"stats":stat}), sound)
            } else {
                let (actual, stat) = engine.replay_rows()?;
                let mut claims = claims.clone();
                claims.sort();
                ensure!(
                    claims == actual,
                    "full binding replay comparison (also rejects duplicates)"
                );
                (
                    json!({"evaluation_seconds":t.elapsed().as_secs_f64(),"stats":stat}),
                    0.,
                )
            };
            let eval = stat["evaluation_seconds"].as_f64().unwrap();
            modes.insert(if count{"count"}else{"replay"},json!({"decompress_s":decode,"authenticate_and_column_decode_s":auth,"compile_index_s":compile,"evaluation":stat,"sound_unique_s":sound,"client_s":decode+auth+compile+eval+sound,"e2e_no_network_s":server_query+assemble+decode+auth+compile+eval+sound}));
        }
        runs.push(json!({"round":rep,"warmup":rep==0,"process_peak_rss_bytes":memory::peak_bytes(),"server_query_including_compile_s":server_query,"response_assembly_compression_s":assemble,"wire_bytes":wire.len(),"uncompressed_packet_bytes":raw.len(),"modes":modes}));
                                                                             
                                                                              
        if rep == 0 {
            fs::create_dir(export)?;
            save(&export.join("trusted_manifest.json"), &seal.manifest)?;
            save(&export.join("query.json"), q)?;
            fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(export.join("response.bin"))?
                .write_all(&wire)?;
        }
        eprintln!("{} round {} {}", q.query_id, rep, runs.last().unwrap());
    }
    Ok(
        json!({"query":q,"expected_answers":truth.len(),"false_positive":0,"false_negative":0,"same_wire_for_both_modes":true,"runs":runs}),
    )
}
fn isolated_client(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 5,
        "usage: exe client BUNDLE_DIR count|replay NEW_RESULT_FILE"
    );
    let dir = Path::new(&args[2]);
    ensure!(args[3] == "count" || args[3] == "replay", "client mode");
                                                                                  
    let manifest: Manifest = serde_json::from_slice(&fs::read(dir.join("trusted_manifest.json"))?)?;
    let q: QuerySpec = serde_json::from_slice(&fs::read(dir.join("query.json"))?)?;
    let t = Instant::now();
    let wire = fs::read(dir.join("response.bin"))?;
    let disk = t.elapsed().as_secs_f64();
    let start = Instant::now();
    let raw = decompress(&wire)?;
    let (tables, claims) = authenticate(&manifest, &q, &raw)?;
    let auth = start.elapsed().as_secs_f64();
    let t = Instant::now();
    let rs = rows(&tables)?;
    let e = Engine::new(&rs, &q, &manifest.types.keys().cloned().collect())?;
    let compile = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let (stats, sound, eval) = if args[3] == "count" {
        let (n, stats) = e.count()?;
        let eval = t.elapsed().as_secs_f64();
        let t = Instant::now();
        e.sound_unique(&claims)?;
        ensure!(
            n == num_bigint::BigUint::from(claims.len()),
            "cardinality seal"
        );
        (stats, t.elapsed().as_secs_f64(), eval)
    } else {
        let (actual, stats) = e.replay_rows()?;
        let mut expected = claims.clone();
        expected.sort();
        ensure!(actual == expected, "complete replay comparison");
        (stats, 0., t.elapsed().as_secs_f64())
    };
    let result = json!({"mode":args[3],"query_id":q.query_id,"answers":claims.len(),"verified":true,"disk_read_s":disk,"auth_decode_s":auth,"compile_s":compile,"eval_s":eval,"sound_unique_s":sound,"client_s":start.elapsed().as_secs_f64(),"peak_rss_bytes":memory::peak_bytes(),"peak_scope":"isolated client process including wire and decoded packet","wire_bytes":wire.len(),"stats":stats});
    save(Path::new(&args[4]), &result)?;
    eprintln!("ISOLATED {}", result);
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.get(1).is_some_and(|s| s.starts_with("scc-ablation")) {
        return semantic_core::ablation::dispatch(&args);
    }
    if args.get(1).is_some_and(|s| s == "semantic-core") {
        return semantic_core::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "semantic-client") {
        return semantic_core::client(&args);
    }
    if args.get(1).is_some_and(|s| s == "semantic-extra") {
        return semantic_core::extra(&args);
    }
    if args.get(1).is_some_and(|s| s == "progressive-fields") {
        return progressive_fields::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "progressive-client") {
        return progressive_fields::client(&args);
    }
    if args.get(1).is_some_and(|s| s == "prefix-structure") {
        return prefix_structure::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "prefix-client") {
        return prefix_structure::client(&args);
    }
    if args.get(1).is_some_and(|s| s == "hybrid-live") {
        return hybrid_live_server::run(&args);
    }
    if args
        .get(1)
        .is_some_and(|s| s == "hybrid-client" || s == "temporal-client")
    {
        return hybrid_packet_client::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "temporal-compare") {
        return temporal_compare::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "columnview-compare") {
        return columnview_compare::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "subset-exact") {
        return hybrid_key_audit::verify_subset(&args);
    }
    if args
        .get(1)
        .is_some_and(|s| s == "key-audit" || s == "key-audit-exact")
    {
        return hybrid_key_audit::run(&args);
    }
    if args.get(1).is_some_and(|s| s == "client") {
        return isolated_client(&args);
    }
    ensure!(
        args.len() >= 3,
        "usage: exe ROOT NEW_OUTPUT_DIR [full|sample] [rounds]"
    );
    let root = Path::new(&args[1]);
    let out = Path::new(&args[2]);
    fs::create_dir(out)?;
    let validation = args.get(3).is_some_and(|s| s == "validate");
    let full = validation || args.get(3).is_some_and(|s| s == "full");
    let rounds = args.get(4).map(|s| s.parse()).transpose()?.unwrap_or(1);
    let (es, sha) = events(root, if full { None } else { Some(100) })?;
    let n = es.len();
                                                                                        
    let t = Instant::now();
    let seal = seal(&es, sha)?;
    let sealing = t.elapsed().as_secs_f64();
    save(&out.join("experimental_manifest.json"), &seal.manifest)?;
    let mut files = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|p| p.map(|p| p.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.sort();
    let mut results = vec![];
    for (i, f) in files.iter().enumerate() {
        let q = load_query(f)?;
        let truth = if full {
            let v: Value = serde_json::from_slice(&fs::read(root.join(format!(
                "FunnelQ_ScopeFactor_Feasibility/results/q{}_reference.json",
                i + 1
            )))?)?;
            let mut a = v["answers"]
                .as_array()
                .context("reference")?
                .iter()
                .map(|r| {
                    Ok(Answer {
                        branch: q.branches[0].id.clone(),
                        events: r
                            .as_array()
                            .context("reference tuple")?
                            .iter()
                            .map(|id| Ok(format!("order:{}", id.as_u64().context("reference ID")?)))
                            .collect::<Result<_>>()?,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            a.sort();
            a
        } else {
            reference(&es, &q)?
        };
        #[cfg(not(test))]
        if validation {
            let t = Instant::now();
            let types = seal.manifest.types.keys().cloned().collect();
            let old = legacy_count::ClientCount::with_authenticated_blockers(&es, &q, &types)?;
            let bindings = old.enumerate_indexed(2_000_000_000)?;
            let mut old_answers = bindings
                .into_iter()
                .map(|(branch, ids)| Answer {
                    branch,
                    events: ids.into_iter().map(|j| es[j].event_id.clone()).collect(),
                })
                .collect::<Vec<_>>();
            old_answers.sort();
            ensure!(
                old_answers == truth,
                "live original Generic differs from saved reference"
            );
            let rs = rows(&seal.tables)?;
            let new = Engine::new(&rs, &q, &types)?;
            let (actual, _) = new.replay()?;
            ensure!(actual == old_answers, "new/full original Generic mismatch");
            let (n, stats) = new.count()?;
            ensure!(
                n == num_bigint::BigUint::from(actual.len()),
                "count mismatch"
            );
            let r = json!({"query_id":q.query_id,"answers":actual.len(),"live_original_generic_full_set_equal":true,"FP":0,"FN":0,"exact_count":n.to_string(),"stats":stats,"validation_seconds":t.elapsed().as_secs_f64(),"reference_sha256":digest(&fs::read(root.join(format!("FunnelQ_ScopeFactor_Feasibility/results/q{}_reference.json",i+1)))?)});
            save(&out.join(format!("q{}.json", i + 1)), &r)?;
            eprintln!("VALIDATED {}", q.query_id);
            results.push(r);
            continue;
        }
        let r = benchmark(
            &seal,
            &q,
            &truth,
            rounds,
            &out.join(format!("q{}_client", i + 1)),
        )?;
        save(&out.join(format!("q{}.json", i + 1)), &r)?;
        results.push(r);
    }
    save(
        &out.join("summary.json"),
        &json!({"version":"complete-column-prefix-count/v2","warmup_rounds_per_query":1,"measured_rounds":rounds,"process_peak_rss_bytes":memory::peak_bytes(),"peak_scope":"entire in-process sealer/server/client harness; not isolated client memory","full_three_month_NFTTrade":full,"input_rows":n,"epoch_build_s":sealing,"epoch_column_bytes":seal.blobs.values().map(Vec::len).sum::<usize>(),"trusted_sealer":true,"on_chain_deployed":false,"network_time_included":false,"backend":"CPU typed columns / SHA256; no GPU or polynomial proving","results":results}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests;
