                                                                              
use crate::{
    columns::{self, RowAnswer},
    engine::Engine,
    hybrid_key_audit::{data, Trade},
    hybrid_live_server,
    key_projection::KeyPlan,
    query::QuerySpec,
};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
    time::Instant,
};

fn write(p: &Path, b: &[u8]) -> Result<()> {
    let mut f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)?;
    f.write_all(b)?;
    Ok(())
}
fn evaluate(
    full: &columns::Seal,
    q: &QuerySpec,
    hybrid: bool,
) -> Result<(Vec<RowAnswer>, Vec<usize>, BTreeSet<String>, usize, usize)> {
    let types = full.manifest.types.keys().cloned().collect();
    if hybrid {
        let plan = KeyPlan::compile(q)?;
        let base = &full.tables[&plan.event_type];
        let (mask, cache) = plan.execute(&base.columns, base.n)?;
        let ids = Arc::new((0..base.n).filter(|&i| mask[i]).collect::<Vec<_>>());
        let view = columns::project_table(&plan.event_type, base, ids.clone())?;
        let rows = columns::rows(&view)?;
        let engine = Engine::with_projections(&rows, q, &types, Some(&cache))?;
        let ans = engine.replay_rows()?.0;
        Ok((
            ans,
            (*ids).clone(),
            plan.fields,
            cache.projections.len(),
            engine.reused_projection_indexes(),
        ))
    } else {
        let rows = columns::rows(&full.tables)?;
        let engine = Engine::new(&rows, q, &types)?;
        Ok((engine.replay_rows()?.0, vec![], BTreeSet::new(), 0, 0))
    }
}
fn run_one(
    root: &Path,
    out: &Path,
    rows: &[Trade],
    full: &columns::Seal,
    tree: &[Vec<[u8; 32]>],
    query_path: &Path,
    q: &QuerySpec,
    qi: usize,
    rep: usize,
    hybrid: bool,
) -> Result<Value> {
    let name = if hybrid { "hybrid" } else { "full_columns" };
    let dir = out.join(format!("q{qi}_r{rep}_{name}"));
    fs::create_dir(&dir)?;
    let t = Instant::now();
    let (claims, ids, fields, layouts, reused) = evaluate(full, q, hybrid)?;
    let query_s = t.elapsed().as_secs_f64();
                                                                         
    let oracle: Value = serde_json::from_slice(&fs::read(root.join(format!(
        "FunnelQ_ScopeFactor_Feasibility/results/q{qi}_reference.json"
    )))?)?;
    let mut expected = serde_json::from_value::<Vec<Vec<u64>>>(oracle["answers"].clone())?;
    let mut got = claims
        .iter()
        .map(|a| a.rows.iter().map(|r| rows[r.1].order).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    expected.sort();
    got.sort();
    ensure!(expected == got, "full oracle mismatch Q{qi} {name}");
    let t = Instant::now();
    let (wire, opened, proof) = if hybrid {
        hybrid_live_server::packet(rows, full, tree, q, &ids, &fields, &claims)?
    } else {
        (
            crate::compress(&columns::packet_rows(full, q, claims.clone())?)?,
            rows.len(),
            0,
        )
    };
    let packet_s = t.elapsed().as_secs_f64();
    write(&dir.join("response.bin"), &wire)?;
    crate::save(&dir.join("query.json"), q)?;
    crate::save(&dir.join("trusted_manifest.json"), &full.manifest)?;
    let mut modes = serde_json::Map::new();
    for mode in if rep % 2 == 0 {
        ["count", "replay"]
    } else {
        ["replay", "count"]
    } {
        let result = dir.join(format!("{mode}.json"));
        let mut cmd = Command::new(std::env::current_exe()?);
        if hybrid {
            cmd.arg("hybrid-client")
                .arg(query_path)
                .arg(dir.join("query.json"))
                .arg(out.join("trusted_manifest.json"))
                .arg(out.join("trusted_row_root.json"))
                .arg(dir.join("response.bin"))
                .arg(&result)
                .arg(mode);
        } else {
            cmd.arg("client").arg(&dir).arg(mode).arg(&result);
        }
        let t = Instant::now();
        let run = cmd.output()?;
        let wall = t.elapsed().as_secs_f64();
        ensure!(
            run.status.success(),
            "{name} {mode} rejected: {}",
            String::from_utf8_lossy(&run.stderr)
        );
        let details: Value = serde_json::from_slice(&fs::read(&result)?)?;
        ensure!(
            details["verified"] == true && details["answers"].as_u64() == Some(claims.len() as u64),
            "client result"
        );
        modes.insert(
            mode.into(),
            json!({"client_wall_s":wall,"compute_e2e_s":query_s+packet_s+wall,"details":details}),
        );
    }
    Ok(
        json!({"query":qi,"backend":name,"round":rep,"warmup":rep==0,"FP":0,"FN":0,
        "answers":claims.len(),"wire_bytes":wire.len(),"proof_bytes":proof,"selected_rows":if hybrid{ids.len()}else{rows.len()},
        "opened_rows":opened,"projection_layouts":layouts,"reused_projection_indexes":reused,
        "server_query_s":query_s,"server_packet_s":packet_s,"modes":modes}),
    )
}
pub fn run(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 5,
        "usage: exe columnview-compare ROOT NEW_OUTPUT_DIR ROUNDS"
    );
    let root = Path::new(&args[2]);
    let out = PathBuf::from(&args[3]);
    let rounds: usize = args[4].parse()?;
    ensure!((1..=8).contains(&rounds), "rounds");
    fs::create_dir(&out)?;
    let t = Instant::now();
    let rows = data(root)?;
    let (events, source) = crate::events(root, None)?;
    let full = columns::seal(&events, source)?;
    let tree = hybrid_live_server::tree(&rows)?;
    let offline = t.elapsed().as_secs_f64();
    drop(events);
    crate::save(&out.join("trusted_manifest.json"), &full.manifest)?;
    let hash = tree.last().context("tree")?[0]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    crate::save(
        &out.join("trusted_row_root.json"),
        &json!({"version":"compact-NFTTrade-row-root/v1",
        "nrows":rows.len(),"block_rows":16,"root":hash,"height":tree.len()-1}),
    )?;
    let mut paths = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|e| e.map(|v| v.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    let queries = paths
        .iter()
        .map(|p| crate::load_query(p))
        .collect::<Result<Vec<_>>>()?;
    let mut records = vec![];
    for rep in 0..=rounds {
        let order = if rep % 2 == 0 {
            (0..paths.len()).collect::<Vec<_>>()
        } else {
            (0..paths.len()).rev().collect()
        };
        for i in order {
            for hybrid in if (rep + i) % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                let row = run_one(
                    root,
                    &out,
                    &rows,
                    &full,
                    &tree,
                    &paths[i],
                    &queries[i],
                    i + 1,
                    rep,
                    hybrid,
                )?;
                eprintln!(
                    "PAIRED Q{} {} r{} count={:.3}s replay={:.3}s wire={} oracle=equal",
                    i + 1,
                    row["backend"],
                    rep,
                    row["modes"]["count"]["compute_e2e_s"].as_f64().unwrap(),
                    row["modes"]["replay"]["compute_e2e_s"].as_f64().unwrap(),
                    row["wire_bytes"]
                );
                records.push(row);
            }
        }
    }
    crate::save(
        &out.join("summary.json"),
        &json!({"version":"columnview-paired/v1","full_rows":rows.len(),
        "offline_seal_and_tree_s":offline,"rounds":rounds,"records":records,
        "network_included":false,"on_chain_deployed":false,"isolated_modes":true,
        "timing":"same binary, interleaved backends, isolated count/replay; client disk read and process lifecycle included; packet disk write and offline seal excluded"}),
    )?;
    Ok(())
}
