                                                                              
                                                                            
                                                                          
use crate::{
    columns::{self, Column, Manifest, RowAnswer},
    engine::Engine,
    key_projection::KeyPlan,
    model::Scalar,
    query::Constraint,
};
use anyhow::{ensure, Context, Result};
use flate2::read::ZlibDecoder;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    io::Read,
    path::Path,
    sync::Arc,
    time::Instant,
};

fn read_u64(raw: &[u8], pos: &mut usize) -> Result<usize> {
    ensure!(*pos + 8 <= raw.len(), "packet length truncated");
    let n = u64::from_le_bytes(raw[*pos..*pos + 8].try_into()?);
    *pos += 8;
    Ok(usize::try_from(n)?)
}
fn take<'a>(raw: &'a [u8], pos: &mut usize, n: usize) -> Result<&'a [u8]> {
    ensure!(
        n <= raw.len().saturating_sub(*pos),
        "packet field truncated"
    );
    let out = &raw[*pos..*pos + n];
    *pos += n;
    Ok(out)
}
fn var(raw: &[u8], pos: &mut usize) -> Result<usize> {
    let mut n = 0u64;
    for shift in (0..=63).step_by(7) {
        let x = *take(raw, pos, 1)?.first().unwrap();
        ensure!(shift < 63 || x <= 1, "varint overflow");
        n |= ((x & 127) as u64) << shift;
        if x < 128 {
            return Ok(usize::try_from(n)?);
        }
    }
    anyhow::bail!("invalid varint")
}
pub(crate) fn field_hash(manifest: &Manifest, f: &str, blob: &[u8]) -> Result<String> {
    let h = &manifest.types["NFTTrade"];
    let mut sha = Sha256::new();
    sha.update(serde_json::to_vec(&(
        "typed-column/v1",
        &manifest.source_digest,
        "NFTTrade",
        &h.layout,
        h.n,
        f,
        blob.len(),
    ))?);
    sha.update(blob);
    Ok(format!("{:x}", sha.finalize()))
}
pub(crate) fn hash_leaf(block: usize, index: usize, raw: &[u8]) -> Result<[u8; 32]> {
    let mut h = Sha256::new();
    h.update(b"R");
    h.update(u32::try_from(block)?.to_le_bytes());
    h.update(u32::try_from(index)?.to_le_bytes());
    h.update(raw);
    Ok(h.finalize().into())
}
pub(crate) fn hash_node(l: &[u8; 32], r: &[u8; 32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"N");
    h.update(l);
    h.update(r);
    h.finalize().into()
}
fn verify_merkle(
    opened: &[usize],
    raw: &[u8],
    block: usize,
    n: usize,
    proof: &[u8],
    height: usize,
    trusted_root: &str,
) -> Result<()> {
    ensure!(proof.len() % 32 == 0, "proof hash framing");
    let mut active = BTreeMap::<usize, [u8; 32]>::new();
    let mut cursor = 0;
    for &i in opened {
        let count = (n - i * block).min(block);
        let size = count * 37;
        active.insert(i, hash_leaf(block, i, take(raw, &mut cursor, size)?)?);
    }
    ensure!(cursor == raw.len(), "opened row count");
    verify_merkle_leaves(active, proof, height, trusted_root)
}
fn verify_merkle_leaves(
    mut active: BTreeMap<usize, [u8; 32]>,
    proof: &[u8],
    height: usize,
    trusted_root: &str,
) -> Result<()> {
    ensure!(proof.len() % 32 == 0, "proof hash framing");
    if active.is_empty() {
        ensure!(proof.is_empty(), "empty-domain proof");
        return Ok(());
    }
    let mut p = 0;
    for _ in 0..height {
        let mut next = BTreeMap::new();
        for (&i, value) in &active {
            let sib = if let Some(x) = active.get(&(i ^ 1)) {
                *x
            } else {
                let b = take(proof, &mut p, 32)?;
                b.try_into()?
            };
            let parent = if i % 2 == 0 {
                hash_node(value, &sib)
            } else {
                hash_node(&sib, value)
            };
            if let Some(old) = next.insert(i / 2, parent) {
                ensure!(old == parent, "inconsistent proof parent")
            }
        }
        active = next;
    }
    ensure!(p == proof.len() && active.len() == 1, "proof length");
    ensure!(
        format!("{:x}", hex_digest(&active[&0])) == trusted_root,
        "row root mismatch"
    );
    Ok(())
}
fn hex_digest(b: &[u8; 32]) -> impl std::fmt::LowerHex + '_ {
    struct H<'a>(&'a [u8; 32]);
    impl std::fmt::LowerHex for H<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            for v in self.0 {
                write!(f, "{v:02x}")?
            }
            Ok(())
        }
    }
    H(b)
}
fn field_value(cols: &BTreeMap<String, Arc<Column>>, field: &str, index: usize) -> Result<u32> {
    let f = match field {
        "src" => "move_from",
        "dst" => "move_to",
        "nft" => "nft",
        "collection" => "collection_contract",
        _ => anyhow::bail!("unknown key field"),
    };
    match cols
        .get(f)
        .context("missing complete key field")?
        .get(index)
    {
        Some(Scalar::Int(x)) => Ok(u32::try_from(*x)?),
        _ => anyhow::bail!("noninteger structural field"),
    }
}
type Pattern = BTreeMap<String, String>;
fn key(
    cols: &BTreeMap<String, Arc<Column>>,
    pat: &Pattern,
    vars: &[String],
    row: usize,
) -> Result<[u32; 4]> {
    ensure!(vars.len() <= 4, "structural key width");
    let mut out = [0; 4];
    for (i, v) in vars.iter().enumerate() {
        let field = pat
            .iter()
            .find(|(_, x)| *x == v)
            .context("unbound shared variable")?
            .0;
        out[i] = field_value(cols, field, row)?;
    }
    Ok(out)
}
fn selected(
    full: &BTreeMap<String, Arc<Column>>,
    query: &Value,
    n: usize,
) -> Result<(Vec<bool>, BTreeSet<String>)> {
    let branches = query["full_binding_plan"]["branches"]
        .as_array()
        .context("branches")?;
    ensure!(branches.len() == 1, "one branch audit");
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
            ensure!(a["event_type"] == "NFTTrade", "unsupported event type");
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
    let mut keep = vec![vec![true; n]; pats.len()];
    let mut fields = BTreeSet::new();
    for i in 0..pats.len() {
        for j in i + 1..pats.len() {
            let l = pats[i].values().cloned().collect::<BTreeSet<_>>();
            let r = pats[j].values().cloned().collect::<BTreeSet<_>>();
            let shared = l.intersection(&r).cloned().collect::<Vec<_>>();
            if shared.is_empty() {
                continue;
            }
            for p in [&pats[i], &pats[j]] {
                for (f, v) in p {
                    if shared.contains(v) {
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
            let mut left = HashSet::with_capacity(n);
            let mut right = HashSet::with_capacity(n);
            for row in 0..n {
                left.insert(key(full, &pats[i], &shared, row)?);
                right.insert(key(full, &pats[j], &shared, row)?);
            }
            for row in 0..n {
                keep[i][row] &= right.contains(&key(full, &pats[i], &shared, row)?);
                keep[j][row] &= left.contains(&key(full, &pats[j], &shared, row)?);
            }
        }
    }
    let mut union = vec![false; n];
    for m in &keep {
        for (i, x) in m.iter().enumerate() {
            union[i] |= *x;
        }
    }
    Ok((union, fields))
}
fn integer(col: &Column, index: usize) -> Result<i64> {
    match col.get(index) {
        Some(Scalar::Int(v)) => Ok(*v),
        _ => anyhow::bail!("invalid integer column"),
    }
}
fn tx(col: &Column, index: usize) -> Result<u32> {
    match col.get(index) {
        Some(Scalar::Text(s)) => Ok(s.strip_prefix("tx:").context("tx prefix")?.parse()?),
        _ => anyhow::bail!("invalid tx column"),
    }
}
fn value(
    full: &BTreeMap<String, Arc<Column>>,
    sparse: &BTreeMap<String, Arc<Column>>,
    field: &str,
    global: usize,
    local: usize,
) -> Result<i64> {
    if let Some(c) = full.get(field) {
        integer(c, global)
    } else {
        integer(sparse.get(field).context("missing sparse field")?, local)
    }
}
fn tx_value(
    full: &BTreeMap<String, Arc<Column>>,
    sparse: &BTreeMap<String, Arc<Column>>,
    global: usize,
    local: usize,
) -> Result<u32> {
    if let Some(c) = full.get("tx_hash") {
        tx(c, global)
    } else {
        tx(sparse.get("tx_hash").context("missing sparse tx")?, local)
    }
}
pub fn run(args: &[String]) -> Result<()> {
    ensure!(args.len()==8 || args.len()==9,"usage: exe hybrid-client QUERY_DOC COMPILED_QUERY TRUSTED_MANIFEST TRUSTED_ROW_ROOT PACKET NEW_RESULT_JSON [count|replay|both]");
    let mode = args.get(8).map(String::as_str).unwrap_or("both");
    let temporal = args[1] == "temporal-client";
    ensure!(["count", "replay", "both"].contains(&mode), "client mode");
    let start = Instant::now();
    let doc: Value = serde_json::from_slice(&fs::read(&args[2])?)?;
    let compiled: crate::query::QuerySpec = serde_json::from_slice(&fs::read(&args[3])?)?;
    ensure!(
        compiled == crate::load_query(Path::new(&args[2]))?,
        "QuerySpec/document mismatch"
    );
    ensure!(
        compiled
            .branches
            .iter()
            .flat_map(|branch| &branch.constraints)
            .all(|constraint| !matches!(constraint, Constraint::NoEvent { .. })),
        "NoEvent blocker domains are outside this experiment"
    );
    let manifest: Manifest = serde_json::from_slice(&fs::read(&args[4])?)?;
    let root: Value = serde_json::from_slice(&fs::read(&args[5])?)?;
    let mut zip = ZlibDecoder::new(fs::File::open(&args[6])?);
    let mut raw = Vec::new();
    zip.read_to_end(&mut raw)?;
    let mut p = 0;
    let hlen = read_u64(&raw, &mut p)?;
    let head: Value = serde_json::from_slice(take(&raw, &mut p, hlen)?)?;
    ensure!(
        head["version"]
            == if temporal {
                "hybrid-temporal-audit/v1"
            } else {
                "hybrid-row-audit/v1"
            },
        "packet version"
    );
    ensure!(
        head["epoch"] == columns::digest(&serde_json::to_vec(&manifest)?)
            && head["query"] == columns::digest(&serde_json::to_vec(&compiled)?),
        "epoch/query binding"
    );
    ensure!(
        head["row_root"] == root["root"] && head["block_rows"] == root["block_rows"],
        "row root binding"
    );
    let n = root["nrows"].as_u64().context("nrows")? as usize;
    let block = root["block_rows"].as_u64().context("block_rows")? as usize;
    let height = root["height"].as_u64().context("height")? as usize;
    ensure!(block > 0 && n == manifest.types["NFTTrade"].n, "row layout");
    let full_fields = head["full_fields"].as_array().context("full fields")?;
    let mut full = BTreeMap::new();
    for f in full_fields {
        let f = f.as_str().context("full field name")?;
        let size = read_u64(&raw, &mut p)?;
        let blob = take(&raw, &mut p, size)?;
        ensure!(
            field_hash(&manifest, f, blob)? == manifest.types["NFTTrade"].fields[f],
            "full column authentication"
        );
        ensure!(
            full.insert(f.to_string(), Arc::new(Column::decode(blob, n)?))
                .is_none(),
            "duplicate full field"
        );
    }
    let nopen = read_u64(&raw, &mut p)?;
    let mut indices = Vec::with_capacity(nopen);
    let mut last = 0usize;
    for i in 0..nopen {
        last = last
            .checked_add(var(&raw, &mut p)?)
            .context("index overflow")?;
        if i > 0 {
            ensure!(last > indices[i - 1], "duplicate opened block")
        }
        indices.push(last);
    }
    ensure!(
        indices.last().is_none_or(|&i| i < (n + block - 1) / block),
        "opened bounds"
    );
    let opened_count = indices
        .iter()
        .map(|&i| (n - i * block).min(block))
        .sum::<usize>();
    let sparse_fields = head["opened_fields"].as_array().context("opened fields")?;
    let mut sparse = BTreeMap::new();
    for f in sparse_fields {
        let f = f.as_str().context("sparse field name")?;
        let size = read_u64(&raw, &mut p)?;
        let blob = take(&raw, &mut p, size)?;
        ensure!(
            sparse
                .insert(f.to_string(), Arc::new(Column::decode(blob, opened_count)?))
                .is_none(),
            "duplicate sparse field"
        );
    }
    let proof_len = read_u64(&raw, &mut p)?;
    let proof = take(&raw, &mut p, proof_len)?;
    ensure!(p == raw.len(), "trailing packet bytes");
    let fields = [
        "collection_contract",
        "move_from",
        "move_to",
        "nft",
        "order_seq",
        "block_ts",
        "tx_hash",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    ensure!(
        full.keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            .is_disjoint(&sparse.keys().map(String::as_str).collect())
            && full
                .keys()
                .chain(sparse.keys())
                .map(String::as_str)
                .collect::<BTreeSet<_>>()
                == fields,
        "complete field envelope"
    );
    let decoded_s = start.elapsed().as_secs_f64();
    let t = Instant::now();
    let plan = KeyPlan::compile(&compiled)?;
    let mut domains = plan.execute_domains(&full, n)?;
    let key_filter_s = t.elapsed().as_secs_f64();
    let temporal_t = Instant::now();
    let temporal_stats = if temporal {
        Some(
            crate::temporal_demand::TemporalPlan::compile(&compiled)?.apply(
                &plan,
                &full,
                &mut domains,
            )?,
        )
    } else {
        None
    };
    let temporal_filter_s = temporal_t.elapsed().as_secs_f64();
    let wanted = &domains.union;
    let projections = &domains.projections;
    let mut key_fields = plan.fields.clone();
    if temporal {
        key_fields.extend(["block_ts".to_string(), "order_seq".to_string()]);
    }
    ensure!(
        key_fields == full.keys().cloned().collect(),
        "full structural key envelope"
    );
    let expected = (0..n)
        .filter(|&i| wanted[i])
        .map(|i| i / block)
        .collect::<BTreeSet<_>>();
    ensure!(
        expected.into_iter().collect::<Vec<_>>() == indices,
        "incomplete opened candidate coverage"
    );
    let filter_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let mut opened_position = 0;
    let mut selected_ids = Vec::new();
    let mut selected_positions = Vec::new();
    let mut selected_orders = Vec::new();
    let mut leaves = BTreeMap::new();
    for &b in &indices {
        let mut block_bytes = Vec::with_capacity(block * 37);
        for row in b * block..((b + 1) * block).min(n) {
            let at = opened_position;
            opened_position += 1;
            let order = value(&full, &sparse, "order_seq", row, at)?;
            let time = value(&full, &sparse, "block_ts", row, at)?;
            let src = u32::try_from(value(&full, &sparse, "move_from", row, at)?)?;
            let dst = u32::try_from(value(&full, &sparse, "move_to", row, at)?)?;
            let nft = u32::try_from(value(&full, &sparse, "nft", row, at)?)?;
            let coll = u32::try_from(value(&full, &sparse, "collection_contract", row, at)?)?;
            let tx = tx_value(&full, &sparse, row, at)?;
            block_bytes.extend(u64::try_from(order)?.to_le_bytes());
            block_bytes.extend(u64::try_from(time)?.to_le_bytes());
            for v in [src, dst, nft, coll, tx] {
                block_bytes.extend(v.to_le_bytes());
            }
            block_bytes.push(1);
            if wanted[row] {
                selected_ids.push(row);
                selected_positions.push(at);
                selected_orders.push(order);
            }
        }
        leaves.insert(b, hash_leaf(block, b, &block_bytes)?);
    }
    verify_merkle_leaves(
        leaves,
        proof,
        height,
        root["root"].as_str().context("root hex")?,
    )?;
    let row_verify_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let claims: Vec<RowAnswer> = serde_json::from_value(head["answers"].clone())?;
    let mut claimed = claims.clone();
    let branch = doc["full_binding_plan"]["branches"][0]["branch_id"]
        .as_str()
        .context("branch")?;
    for answer in &claims {
        ensure!(answer.branch == branch, "wrong branch");
        ensure!(
            answer.rows.len() == compiled.branches[0].atoms.len(),
            "claim width"
        );
        for r in &answer.rows {
            ensure!(
                r.0 == "NFTTrade" && r.1 < n && wanted[r.1],
                "answer row outside authenticated domain"
            );
        }
    }
    claimed.sort();
    ensure!(claimed.windows(2).all(|w| w[0] != w[1]), "duplicate answer");
    let claim_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let selected_ids = Arc::new(selected_ids);
    let selected_positions = Arc::new(selected_positions);
    let mut view = full
        .iter()
        .map(|(f, c)| Ok((f.clone(), Arc::new(c.project(selected_ids.clone())?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    for (f, c) in &sparse {
        view.insert(f.clone(), Arc::new(c.project(selected_positions.clone())?));
    }
    view.insert(
        "event_id".into(),
        Arc::new(Column::from_values(
            selected_orders
                .iter()
                .map(|o| Some(Scalar::Text(format!("order:{o}")))),
        )),
    );
    view.insert(
        "event_type".into(),
        Arc::new(Column::from_values(
            selected_orders
                .iter()
                .map(|_| Some(Scalar::Text("NFTTrade".into()))),
        )),
    );
    let tables = columns::sparse_view("NFTTrade", view, selected_ids.clone())?;
    let rowset = columns::rows(&tables)?;
    let atomic_domains = if temporal {
        Some(crate::temporal_demand::local_domains(
            &compiled,
            &domains,
            &selected_ids,
        )?)
    } else {
        None
    };
    let engine = Engine::with_verified_domains(
        &rowset,
        &compiled,
        &BTreeSet::from(["NFTTrade".to_string()]),
        Some(projections),
        atomic_domains.as_ref(),
    )?;
    let compile_s = t.elapsed().as_secs_f64();
    let common_s = start.elapsed().as_secs_f64();
    let (count_s, sound_s) = if mode != "replay" {
        let t = Instant::now();
        let (count, _) = engine.count()?;
        let count_s = t.elapsed().as_secs_f64();
        let t = Instant::now();
        engine.sound_unique(&claims)?;
        ensure!(
            count == num_bigint::BigUint::from(claimed.len()),
            "cardinality seal mismatch"
        );
        (count_s, t.elapsed().as_secs_f64())
    } else {
        (0., 0.)
    };
    let (replay_s, compare_s) = if mode != "count" {
        let t = Instant::now();
        let (mut replayed, _) = engine.replay_rows()?;
        let replay_s = t.elapsed().as_secs_f64();
        let t = Instant::now();
        replayed.sort();
        ensure!(replayed == claimed, "binding set mismatch");
        (replay_s, t.elapsed().as_secs_f64())
    } else {
        (0., 0.)
    };
    let result = json!({"verified":true,"mode":mode,"client_s":start.elapsed().as_secs_f64(),"candidate_rows":selected_ids.len(),"opened_rows":opened_count,
        "projection_layouts":projections.projections.len(),"reused_projection_indexes":engine.reused_projection_indexes(),
        "answers":claimed.len(),"wire_bytes":fs::metadata(&args[6])?.len(),
        "packet_decode_and_complete_column_auth_s":decoded_s,"complete_key_filter_s":filter_s,
        "key_filter_only_s":key_filter_s,"temporal_filter_s":temporal_filter_s,"temporal_stats":temporal_stats,
        "row_reconstruction_and_proof_s":row_verify_s,"claim_parse_unique_s":claim_s,
        "subset_compile_s":compile_s,"exact_count_s":count_s,"sound_unique_s":sound_s,
        "exact_replay_s":replay_s,"replay_compare_s":compare_s,
        "client_count_mode_s":common_s+count_s+sound_s,
        "client_replay_mode_s":common_s+replay_s+compare_s,
        "client_e2e_count_and_replay_s":start.elapsed().as_secs_f64(),
        "client_process_peak_rss_bytes":crate::memory::peak_bytes()});
    let output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args[7])?;
    serde_json::to_writer_pretty(output, &result)?;
    eprintln!("HYBRID_CLIENT {}", result);
    Ok(())
}
