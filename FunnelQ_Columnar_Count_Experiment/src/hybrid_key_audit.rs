                                                                         
                                                                             
use crate::{columns, engine::Engine, model::CanonicalEvent};
use anyhow::{ensure, Context, Result};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::Path,
    time::Instant,
};

#[derive(Clone, Copy)]
pub(crate) struct Trade {
    pub(crate) order: u64,
    pub(crate) time: u64,
    pub(crate) tx: u32,
    pub(crate) src: u32,
    pub(crate) dst: u32,
    pub(crate) nft: u32,
    pub(crate) collection: u32,
}
impl Trade {
    pub(crate) fn get(&self, field: &str) -> Result<u32> {
        Ok(match field {
            "src" => self.src,
            "dst" => self.dst,
            "nft" => self.nft,
            "collection" => self.collection,
            _ => anyhow::bail!("unsupported structural key in this NFT audit: {field}"),
        })
    }
}
fn read_u64(b: &[u8]) -> u64 {
    u64::from_le_bytes(b.try_into().unwrap())
}
fn read_u32(b: &[u8]) -> u32 {
    u32::from_le_bytes(b.try_into().unwrap())
}
pub(crate) fn data(root: &Path) -> Result<Vec<Trade>> {
    let p=root.join("FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin");
    let b = fs::read(p)?;
    ensure!(b.len() % 37 == 0, "framing");
    let mut rows = Vec::new();
    for x in b.chunks_exact(37) {
        if x[36] != 1 {
            continue;
        }
        rows.push(Trade {
            order: read_u64(&x[0..8]),
            time: read_u64(&x[8..16]),
            tx: read_u32(&x[32..36]),
            src: read_u32(&x[16..20]),
            dst: read_u32(&x[20..24]),
            nft: read_u32(&x[24..28]),
            collection: read_u32(&x[28..32]),
        });
    }
    rows.sort_unstable_by_key(|r| r.order);
    ensure!(
        rows.len() == 1_123_403 && rows.windows(2).all(|w| w[0].order < w[1].order),
        "complete unique NFTTrade order"
    );
    Ok(rows)
}
pub(crate) type Pattern = BTreeMap<String, String>;
pub(crate) fn key(row: &Trade, pat: &Pattern, vars: &[String]) -> Result<[u32; 4]> {
    ensure!(vars.len() <= 4, "four-field audit key capacity");
    let mut out = [0u32; 4];
    for (i, v) in vars.iter().enumerate() {
        let f = pat
            .iter()
            .find(|(_, x)| *x == v)
            .context("shared variable missing")?
            .0;
        out[i] = row.get(f)?;
    }
    Ok(out)
}
pub fn run(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 4,
        "usage: exe key-audit[-exact] ROOT NEW_RESULT_JSON"
    );
    let exact = args[1] == "key-audit-exact";
    let root = Path::new(&args[2]);
    let t = Instant::now();
    let rows = data(root)?;
    let load_s = t.elapsed().as_secs_f64();
    let mut paths = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|x| x.map(|x| x.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    ensure!(paths.len() == 5, "five workload queries");
    let mut result = vec![];
    for (qi, path) in paths.iter().enumerate() {
        let doc: Value = serde_json::from_slice(&fs::read(path)?)?;
        let branches = doc["full_binding_plan"]["branches"]
            .as_array()
            .context("branches")?;
        ensure!(branches.len() == 1, "this audit expects one branch");
        let atoms = branches[0]["event_patterns"].as_array().context("atoms")?;
        let pats = atoms
            .iter()
            .map(|a| {
                ensure!(
                    a["event_type"] == "NFTTrade",
                    "this source contains NFTTrade only"
                );
                let mut p = Pattern::new();
                for field in ["src", "dst", "nft", "collection"] {
                    if let Some(v) = a[field].as_str() {
                        if v.starts_with('$') {
                            p.insert(field.into(), v.into());
                        }
                    }
                }
                Ok(p)
            })
            .collect::<Result<Vec<_>>>()?;
        let t = Instant::now();
        let mut keep = vec![vec![true; rows.len()]; pats.len()];
        let mut pair_count = 0;
        for i in 0..pats.len() {
            for j in i + 1..pats.len() {
                let l = pats[i].values().cloned().collect::<BTreeSet<_>>();
                let r = pats[j].values().cloned().collect::<BTreeSet<_>>();
                let shared = l.intersection(&r).cloned().collect::<Vec<_>>();
                if shared.is_empty() {
                    continue;
                }
                pair_count += 1;
                let mut left = HashSet::with_capacity(rows.len());
                let mut right = HashSet::with_capacity(rows.len());
                for row in &rows {
                    left.insert(key(row, &pats[i], &shared)?);
                    right.insert(key(row, &pats[j], &shared)?);
                }
                for (n, row) in rows.iter().enumerate() {
                    keep[i][n] &= right.contains(&key(row, &pats[i], &shared)?);
                    keep[j][n] &= left.contains(&key(row, &pats[j], &shared)?);
                }
            }
        }
        let elapsed = t.elapsed().as_secs_f64();
        let mut union = vec![false; rows.len()];
        for mask in &keep {
            for (n, v) in mask.iter().enumerate() {
                union[n] |= *v
            }
        }
        let reference: Value = serde_json::from_slice(&fs::read(root.join(format!(
            "FunnelQ_ScopeFactor_Feasibility/results/q{}_reference.json",
            qi + 1
        )))?)?;
        for ans in reference["answers"]
            .as_array()
            .context("reference answers")?
        {
            for (a, id) in ans.as_array().context("answer tuple")?.iter().enumerate() {
                let needle = id.as_u64().context("numeric order ID")?;
                let n = rows
                    .binary_search_by_key(&needle, |r| r.order)
                    .map_err(|_| anyhow::anyhow!("answer ID absent"))?;
                ensure!(
                    keep[a][n],
                    "unsafe key pruning at query {} atom {a}",
                    qi + 1
                );
            }
        }
        let exact_audit = if exact {
            let t = Instant::now();
            let subset = rows
                .iter()
                .enumerate()
                .filter(|(i, _)| union[*i])
                .map(|(_, r)| {
                    CanonicalEvent::new(
                        format!("order:{}", r.order),
                        "NFTTrade",
                        r.order as i64,
                        r.time as i64,
                        format!("tx:{}", r.tx),
                        BTreeMap::from([
                            ("move_from".into(), (r.src as i64).into()),
                            ("move_to".into(), (r.dst as i64).into()),
                            ("nft".into(), (r.nft as i64).into()),
                            ("collection_contract".into(), (r.collection as i64).into()),
                        ]),
                    )
                })
                .collect::<Vec<_>>();
            let seal = columns::seal(&subset, "independent-hybrid-feasibility".into())?;
            let prepare_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let rowset = columns::rows(&seal.tables)?;
            let query = crate::load_query(path)?;
            let types = BTreeSet::from(["NFTTrade".to_string()]);
            let engine = Engine::new(&rowset, &query, &types)?;
            let compile_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let (count, count_stats) = engine.count()?;
            let count_s = t.elapsed().as_secs_f64();
            let t = Instant::now();
            let (replayed, replay_stats) = engine.replay_rows()?;
            let replay_s = t.elapsed().as_secs_f64();
            ensure!(
                count == num_bigint::BigUint::from(replayed.len()),
                "count/replay mismatch"
            );
            let mut got = engine
                .canonical_answers(&replayed)?
                .into_iter()
                .map(|a| {
                    a.events
                        .into_iter()
                        .map(|e| {
                            e.strip_prefix("order:")
                                .context("event ID prefix")?
                                .parse::<u64>()
                                .map_err(Into::into)
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            got.sort();
            let mut expected = reference["answers"]
                .as_array()
                .context("reference answers")?
                .iter()
                .map(|a| {
                    a.as_array()
                        .context("answer tuple")?
                        .iter()
                        .map(|v| v.as_u64().context("numeric order ID"))
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            expected.sort();
            ensure!(got == expected, "full answer set mismatch");
            json!({"prepare_subset_and_seal_seconds":prepare_s,"compile_index_seconds":compile_s,
                "client_exact_count_seconds":count_s,"client_exact_replay_seconds":replay_s,
                "exact_count":count.to_string(),"full_answer_set_equal":true,
                "count_stats":count_stats,"replay_stats":replay_stats})
        } else {
            json!(null)
        };
        let item = json!({"query":qi+1,"key_filter_seconds":elapsed,"pair_checks":pair_count,
            "atom_candidate_rows":keep.iter().map(|m|m.iter().filter(|x|**x).count()).collect::<Vec<_>>(),
            "survivor_union_rows":union.iter().filter(|x|**x).count(),"answer_preservation":true,
            "exact_subset_audit":exact_audit});
        eprintln!("KEY_AUDIT {}", item);
        result.push(item);
    }
    let output = json!({"source_rows":rows.len(),"load_and_sort_seconds":load_s,"results":result,
        "scope":"one safe pass of complete composite equality keys; no time/order/disequality pruning; no query-specific branch"});
    let f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args[3])?;
    serde_json::to_writer_pretty(f, &output)?;
    Ok(())
}

                                                                              
                                                                             
pub fn verify_subset(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 6,
        "usage: exe subset-exact QUERY_JSON VERIFIED_SUBSET_BIN PACKET_CLAIMS_JSON NEW_RESULT_JSON"
    );
    let t = Instant::now();
    let bytes = fs::read(&args[3])?;
    ensure!(bytes.len() % 37 == 0, "subset framing");
    let mut subset = Vec::with_capacity(bytes.len() / 37);
    for x in bytes.chunks_exact(37) {
        ensure!(x[36] == 1, "subset type");
        let order = read_u64(&x[0..8]);
        subset.push(CanonicalEvent::new(
            format!("order:{order}"),
            "NFTTrade",
            order as i64,
            read_u64(&x[8..16]) as i64,
            format!("tx:{}", read_u32(&x[32..36])),
            BTreeMap::from([
                ("move_from".into(), (read_u32(&x[16..20]) as i64).into()),
                ("move_to".into(), (read_u32(&x[20..24]) as i64).into()),
                ("nft".into(), (read_u32(&x[24..28]) as i64).into()),
                (
                    "collection_contract".into(),
                    (read_u32(&x[28..32]) as i64).into(),
                ),
            ]),
        ));
    }
    ensure!(
        subset.windows(2).all(|w| w[0].order_seq < w[1].order_seq),
        "subset order"
    );
    let parse_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let seal = columns::seal(&subset, "verified-hybrid-packet".into())?;
    let seal_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let q = crate::load_query(Path::new(&args[2]))?;
    let rows = columns::rows(&seal.tables)?;
    let engine = Engine::new(&rows, &q, &BTreeSet::from(["NFTTrade".to_string()]))?;
    let compile_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let (n, count_stats) = engine.count()?;
    let count_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let (actual, replay_stats) = engine.replay_rows()?;
    let replay_s = t.elapsed().as_secs_f64();
    let mut got = engine
        .canonical_answers(&actual)?
        .into_iter()
        .map(|a| {
            a.events
                .into_iter()
                .map(|id| {
                    id.strip_prefix("order:")
                        .context("event ID")?
                        .parse::<u64>()
                        .map_err(Into::into)
                })
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    got.sort();
    let mut claimed: Vec<Vec<u64>> = serde_json::from_slice(&fs::read(&args[4])?)?;
    ensure!(
        claimed.iter().all(|x| x.len() == q.branches[0].atoms.len()),
        "claim width"
    );
    claimed.sort();
    ensure!(claimed.windows(2).all(|w| w[0] != w[1]), "duplicate claim");
    ensure!(
        n == num_bigint::BigUint::from(claimed.len()),
        "count mismatch"
    );
    ensure!(got == claimed, "complete answer set mismatch");
    let output = json!({"verified":true,"subset_rows":subset.len(),"answers":claimed.len(),
        "parse_subset_seconds":parse_s,"subset_column_build_seconds":seal_s,"compile_seconds":compile_s,
        "exact_count_seconds":count_s,"exact_replay_seconds":replay_s,"count_stats":count_stats,
        "replay_stats":replay_stats});
    let f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args[5])?;
    serde_json::to_writer_pretty(f, &output)?;
    eprintln!("SUBSET_EXACT {}", output);
    Ok(())
}
