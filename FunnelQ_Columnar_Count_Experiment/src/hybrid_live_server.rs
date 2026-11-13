                                                                          
                                                                              
                                                                            
use crate::{
    columns::{self, Column, RowAnswer},
    engine::Engine,
    hybrid_key_audit::{data, key, Pattern, Trade},
    hybrid_packet_client::{hash_leaf, hash_node},
    key_projection::KeyPlan,
    model::{CanonicalEvent, Scalar},
};
use anyhow::{ensure, Context, Result};
use flate2::{write::ZlibEncoder, Compression};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Instant,
};

const BLOCK: usize = 16;
fn raw(r: &Trade) -> Vec<u8> {
    let mut out = Vec::with_capacity(37);
    out.extend(r.order.to_le_bytes());
    out.extend(r.time.to_le_bytes());
    for v in [r.src, r.dst, r.nft, r.collection, r.tx] {
        out.extend(v.to_le_bytes());
    }
    out.push(1);
    out
}
pub(crate) fn tree(rows: &[Trade]) -> Result<Vec<Vec<[u8; 32]>>> {
    let count = rows.len().div_ceil(BLOCK);
    let cap = count.next_power_of_two();
    let mut leaves = Vec::with_capacity(cap);
    for (i, chunk) in rows.chunks(BLOCK).enumerate() {
        let mut bytes = Vec::with_capacity(chunk.len() * 37);
        for r in chunk {
            bytes.extend(raw(r));
        }
        leaves.push(hash_leaf(BLOCK, i, &bytes)?);
    }
    for i in count..cap {
        let mut h = Sha256::new();
        h.update(b"EMPTY");
        h.update(u32::try_from(BLOCK)?.to_le_bytes());
        h.update(u32::try_from(i)?.to_le_bytes());
        leaves.push(h.finalize().into());
    }
    let mut levels = vec![leaves];
    while levels.last().unwrap().len() > 1 {
        let previous = levels.last().unwrap();
        levels.push(
            previous
                .chunks_exact(2)
                .map(|p| hash_node(&p[0], &p[1]))
                .collect(),
        );
    }
    Ok(levels)
}
fn merkle_proof(levels: &[Vec<[u8; 32]>], blocks: &[usize]) -> Vec<u8> {
    let mut known = blocks.iter().copied().collect::<BTreeSet<_>>();
    let mut out = Vec::new();
    for level in &levels[..levels.len() - 1] {
        for &i in &known {
            if !known.contains(&(i ^ 1)) {
                out.extend(level[i ^ 1]);
            }
        }
        known = known.iter().map(|i| i / 2).collect();
    }
    out
}
fn put(out: &mut Vec<u8>, n: usize) {
    out.extend((n as u64).to_le_bytes())
}
fn var(out: &mut Vec<u8>, mut n: usize) {
    while n >= 128 {
        out.push((n as u8) | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn event(r: &Trade) -> Result<CanonicalEvent> {
    let order = i64::try_from(r.order)?;
    let time = i64::try_from(r.time)?;
    Ok(CanonicalEvent::new(
        format!("order:{order}"),
        "NFTTrade",
        order,
        time,
        format!("tx:{}", r.tx),
        BTreeMap::from([
            ("move_from".into(), (r.src as i64).into()),
            ("move_to".into(), (r.dst as i64).into()),
            ("nft".into(), (r.nft as i64).into()),
            ("collection_contract".into(), (r.collection as i64).into()),
        ]),
    ))
}
fn field(r: &Trade, name: &str) -> Result<Scalar> {
    Ok(match name {
        "collection_contract" => Scalar::Int(r.collection as i64),
        "move_from" => Scalar::Int(r.src as i64),
        "move_to" => Scalar::Int(r.dst as i64),
        "nft" => Scalar::Int(r.nft as i64),
        "order_seq" => Scalar::Int(i64::try_from(r.order)?),
        "block_ts" => Scalar::Int(i64::try_from(r.time)?),
        "tx_hash" => Scalar::Text(format!("tx:{}", r.tx)),
        _ => anyhow::bail!("unsupported payload field {name}"),
    })
}
fn selected(rows: &[Trade], doc: &Value) -> Result<(Vec<usize>, BTreeSet<String>)> {
    let branches = doc["full_binding_plan"]["branches"]
        .as_array()
        .context("branches")?;
    ensure!(branches.len() == 1, "one-branch experiment");
    ensure!(
        branches[0]["constraints"]
            .as_array()
            .context("constraints")?
            .iter()
            .all(|c| !c["type"]
                .as_str()
                .unwrap_or("")
                .to_ascii_lowercase()
                .replace(['_', '-'], "")
                .starts_with("noevent")),
        "NoEvent blocker domains are outside this experiment"
    );
    let atoms = branches[0]["event_patterns"].as_array().context("atoms")?;
    let pats = atoms
        .iter()
        .map(|a| {
            ensure!(a["event_type"] == "NFTTrade", "one-type experiment");
            let mut p = Pattern::new();
            for f in ["src", "dst", "nft", "collection"] {
                if let Some(v) = a[f].as_str() {
                    if v.starts_with('$') {
                        p.insert(f.into(), v.into());
                    }
                }
            }
            Ok(p)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut keep = vec![vec![true; rows.len()]; pats.len()];
    let mut fields = BTreeSet::new();
    for i in 0..pats.len() {
        for j in i + 1..pats.len() {
            let l = pats[i].values().cloned().collect::<BTreeSet<_>>();
            let r = pats[j].values().cloned().collect::<BTreeSet<_>>();
            let common = l.intersection(&r).cloned().collect::<Vec<_>>();
            if common.is_empty() {
                continue;
            }
            for p in [&pats[i], &pats[j]] {
                for (f, v) in p {
                    if common.contains(v) {
                        fields.insert(
                            match f.as_str() {
                                "src" => "move_from",
                                "dst" => "move_to",
                                "nft" => "nft",
                                "collection" => "collection_contract",
                                _ => unreachable!(),
                            }
                            .to_string(),
                        );
                    }
                }
            }
            let mut left = HashSet::with_capacity(rows.len());
            let mut right = HashSet::with_capacity(rows.len());
            for row in rows {
                left.insert(key(row, &pats[i], &common)?);
                right.insert(key(row, &pats[j], &common)?);
            }
            for (at, row) in rows.iter().enumerate() {
                keep[i][at] &= right.contains(&key(row, &pats[i], &common)?);
                keep[j][at] &= left.contains(&key(row, &pats[j], &common)?);
            }
        }
    }
    Ok((
        (0..rows.len())
            .filter(|&i| keep.iter().any(|p| p[i]))
            .collect(),
        fields,
    ))
}
pub(crate) fn packet(
    rows: &[Trade],
    seal: &columns::Seal,
    levels: &[Vec<[u8; 32]>],
    q: &crate::query::QuerySpec,
    selected_ids: &[usize],
    full_fields: &BTreeSet<String>,
    answers: &[RowAnswer],
) -> Result<(Vec<u8>, usize, usize)> {
    packet_version(
        rows,
        seal,
        levels,
        q,
        selected_ids,
        full_fields,
        answers,
        "hybrid-row-audit/v1",
    )
}
pub(crate) fn packet_version(
    rows: &[Trade],
    seal: &columns::Seal,
    levels: &[Vec<[u8; 32]>],
    q: &crate::query::QuerySpec,
    selected_ids: &[usize],
    full_fields: &BTreeSet<String>,
    answers: &[RowAnswer],
    version: &str,
) -> Result<(Vec<u8>, usize, usize)> {
    let opened = selected_ids
        .iter()
        .map(|id| id / BLOCK)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let opened_rows = opened
        .iter()
        .flat_map(|&b| b * BLOCK..((b + 1) * BLOCK).min(rows.len()))
        .collect::<Vec<_>>();
    let all = [
        "collection_contract",
        "move_from",
        "move_to",
        "nft",
        "order_seq",
        "block_ts",
        "tx_hash",
    ]
    .into_iter()
    .map(str::to_string)
    .collect::<BTreeSet<_>>();
    let payload = all.difference(full_fields).cloned().collect::<Vec<_>>();
    let proof = merkle_proof(levels, &opened);
    let root = levels.last().unwrap()[0]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let meta = json!({"version":version,
        "epoch":columns::digest(&serde_json::to_vec(&seal.manifest)?),
        "query":columns::digest(&serde_json::to_vec(q)?),
        "answers":answers,"full_fields":full_fields,"opened_fields":payload,
        "block_rows":BLOCK,"row_root":root});
    let head = serde_json::to_vec(&meta)?;
    let mut raw = Vec::new();
    put(&mut raw, head.len());
    raw.extend(head);
    for f in full_fields {
        let b = &seal.blobs[&("NFTTrade".to_string(), f.clone())];
        put(&mut raw, b.len());
        raw.extend(b);
    }
    put(&mut raw, opened.len());
    let mut previous = 0;
    for &index in &opened {
        var(&mut raw, index - previous);
        previous = index;
    }
    for f in &payload {
        let c = Column::from_values(
            opened_rows
                .iter()
                .map(|&i| Some(field(&rows[i], f).unwrap())),
        );
        let b = c.encode();
        put(&mut raw, b.len());
        raw.extend(b);
    }
    put(&mut raw, proof.len());
    raw.extend(&proof);
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&raw)?;
    Ok((encoder.finish()?, opened_rows.len(), proof.len()))
}
fn write_new(p: &Path, bytes: &[u8]) -> Result<()> {
    let mut f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)?;
    f.write_all(bytes)?;
    Ok(())
}
pub fn run(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 5,
        "usage: exe hybrid-live ROOT NEW_OUTPUT_DIR ROUNDS"
    );
    let root = Path::new(&args[2]);
    let out = PathBuf::from(&args[3]);
    let rounds: usize = args[4].parse()?;
    ensure!((1..=8).contains(&rounds), "rounds");
    fs::create_dir(&out)?;
    let offline = Instant::now();
    let rows = data(root)?;
    let (es, source_sha) = crate::events(root, None)?;
    let full = columns::seal(&es, source_sha)?;
    let levels = tree(&rows)?;
    let offline_s = offline.elapsed().as_secs_f64();
    write_new(
        &out.join("trusted_manifest.json"),
        &serde_json::to_vec_pretty(&full.manifest)?,
    )?;
    let root_hex = levels.last().unwrap()[0]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    write_new(
        &out.join("trusted_row_root.json"),
        &serde_json::to_vec_pretty(&json!({"version":"compact-NFTTrade-row-root/v1",
            "nrows":rows.len(),"block_rows":BLOCK,"root":root_hex,"height":levels.len()-1}))?,
    )?;
    let mut paths = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|x| x.map(|v| v.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    ensure!(paths.len() == 5, "selected5 workload");
    let mut measurements = Vec::new();
    for (qi, query_path) in paths.iter().enumerate() {
        let q = crate::load_query(query_path)?;
        let query_file = out.join(format!("q{}_compiled.json", qi + 1));
        write_new(&query_file, &serde_json::to_vec_pretty(&q)?)?;
        for rep in 0..=rounds {
            let t = Instant::now();
            let plan = KeyPlan::compile(&q)?;
            let (mask, projections) = plan.execute(&full.tables["NFTTrade"].columns, rows.len())?;
            let selected_ids = Arc::new((0..rows.len()).filter(|&i| mask[i]).collect::<Vec<_>>());
            let fields = plan.fields;
            let tables =
                columns::project_table("NFTTrade", &full.tables["NFTTrade"], selected_ids.clone())?;
            let rowset = columns::rows(&tables)?;
            let engine = Engine::with_projections(
                &rowset,
                &q,
                &BTreeSet::from(["NFTTrade".to_string()]),
                Some(&projections),
            )?;
            let (answers, _) = engine.replay_rows()?;
            let server_query_s = t.elapsed().as_secs_f64();
            let root_claims = answers;
            if rep == 0 {
                let oracle: Value = serde_json::from_slice(&fs::read(root.join(format!(
                    "FunnelQ_ScopeFactor_Feasibility/results/q{}_reference.json",
                    qi + 1
                )))?)?;
                let mut expected = oracle["answers"]
                    .as_array()
                    .context("reference answers")?
                    .iter()
                    .map(|answer| {
                        answer
                            .as_array()
                            .context("reference binding")?
                            .iter()
                            .map(|v| v.as_u64().context("reference order"))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                expected.sort();
                let mut observed = root_claims
                    .iter()
                    .map(|answer| answer.rows.iter().map(|r| rows[r.1].order).collect())
                    .collect::<Vec<Vec<u64>>>();
                observed.sort();
                ensure!(
                    observed == expected,
                    "full-data Q{} oracle mismatch",
                    qi + 1
                );
                eprintln!("HYBRID_LIVE_ORACLE q{} exact bindings match", qi + 1);
            }
            let t = Instant::now();
            let (wire, opened_rows, proof_bytes) = packet(
                &rows,
                &full,
                &levels,
                &q,
                &selected_ids,
                &fields,
                &root_claims,
            )?;
            let assemble_s = t.elapsed().as_secs_f64();
            let stem = format!("q{}_round{}", qi + 1, rep);
            let packet_path = out.join(format!("{stem}_packet.bin"));
            write_new(&packet_path, &wire)?;
            let client_result = out.join(format!("{stem}_client.json"));
            let t = Instant::now();
            let run = Command::new(std::env::current_exe()?)
                .arg("hybrid-client")
                .arg(query_path)
                .arg(&query_file)
                .arg(out.join("trusted_manifest.json"))
                .arg(out.join("trusted_row_root.json"))
                .arg(&packet_path)
                .arg(&client_result)
                .output()?;
            let client_wall_s = t.elapsed().as_secs_f64();
            ensure!(
                run.status.success(),
                "client rejected generated packet: {}",
                String::from_utf8_lossy(&run.stderr)
            );
            let client: Value = serde_json::from_slice(&fs::read(&client_result)?)?;
            ensure!(client["verified"] == true, "client verification");
            let item = json!({"query":qi+1,"round":rep,"warmup":rep==0,
                "answers":root_claims.len(),"candidate_rows":selected_ids.len(),"opened_rows":opened_rows,
                "wire_bytes":wire.len(),"proof_bytes":proof_bytes,
                "client_process_peak_rss_bytes":client["client_process_peak_rss_bytes"],
                "server_live_query_s":server_query_s,"server_packet_generation_s":assemble_s,
                "client_process_wall_s":client_wall_s,
                "compute_e2e_count_s":server_query_s+assemble_s+client_wall_s-
                    client["exact_replay_s"].as_f64().context("replay time")?-
                    client["replay_compare_s"].as_f64().context("compare time")?,
                "compute_e2e_replay_s":server_query_s+assemble_s+client_wall_s-
                    client["exact_count_s"].as_f64().context("count time")?-
                    client["sound_unique_s"].as_f64().context("sound time")?});
            eprintln!("HYBRID_LIVE {}", item);
            measurements.push(item);
        }
    }
    write_new(
        &out.join("summary.json"),
        &serde_json::to_vec_pretty(&json!({"version":"hybrid-live-v1","full_rows":rows.len(),
            "offline_seal_and_row_tree_s":offline_s,"rounds":rounds,"records":measurements,
            "scope":"full three-month NFTTrade; Q1-Q5 only; no deployed chain root; no network"}))?,
    )?;
    Ok(())
}
