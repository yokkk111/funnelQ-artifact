use anyhow::{Context, Result};
use clap::Parser;
use rayon::prelude::*;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long = "query-json")]
    query_json: PathBuf,
    #[arg(long)]
    outdir: PathBuf,
    #[arg(long, default_value_t = 8)]
    threads: usize,
    #[arg(long, default_value_t = 0)]
    max_results: usize,
    #[arg(long, default_value_t = 1)]
    warmup_runs: usize,
    #[arg(long = "phase-marker")]
    phase_marker: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum OneOrMany {
    One(String),
    Many(Vec<String>),
}
impl OneOrMany {
    fn values(&self) -> Vec<String> {
        match self {
            Self::One(x) => vec![x.clone()],
            Self::Many(xs) => xs.clone(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct QueryDocument {
    full_binding_plan: FullPlan,
}
#[derive(Debug, Clone, Deserialize)]
struct FullPlan {
    query_id: String,
    result_semantics: String,
    #[serde(default = "default_delta")]
    delta_seconds: i64,
    branches: Vec<Branch>,
}
fn default_delta() -> i64 { 86400 }

#[derive(Debug, Clone, Deserialize)]
struct Branch {
    branch_id: String,
    event_patterns: Vec<Pattern>,
    #[serde(default)]
    constraints: Vec<Value>,
}
#[derive(Debug, Clone, Deserialize)]
struct Pattern {
    id: String,
    event_type: OneOrMany,
    #[serde(default)]
    src: Option<String>,
    #[serde(default)]
    dst: Option<String>,
    #[serde(default)]
    nft: Option<String>,
    #[serde(default)]
    collection: Option<String>,
}

#[derive(Debug, Clone)]
struct Edge {
    edge_id: i64,
    event_id: String,
    event_type: String,
    collection_id: i64,
    nft_id: i64,
    src_id: i64,
    dst_id: i64,
    block_ts: i64,
    order_seq: i64,
    tx_hash: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoleKind { Address, Nft, Collection }
#[derive(Debug, Clone)]
struct Bound {
    kind: RoleKind,
    id: i64,
    text: String,
}

#[derive(Default)]
struct Dictionaries {
    address: HashMap<i64, String>,
    nft: HashMap<i64, String>,
    collection: HashMap<i64, String>,
}
impl Dictionaries {
    fn text(&self, kind: RoleKind, id: i64) -> String {
        match kind {
            RoleKind::Address => self.address.get(&id),
            RoleKind::Nft => self.nft.get(&id),
            RoleKind::Collection => self.collection.get(&id),
        }.cloned().unwrap_or_default()
    }
}

struct Index {
    edges: Vec<Edge>,
    by_collection: HashMap<i64, Vec<usize>>,
    by_src: HashMap<i64, Vec<usize>>,
    by_dst: HashMap<i64, Vec<usize>>,
    by_nft: HashMap<i64, Vec<usize>>,
    dictionaries: Dictionaries,
}

fn load_dict(conn: &Connection, table: &str, id_col: &str) -> Result<HashMap<i64, String>> {
    let sql = format!("SELECT {}, value_text FROM {}", id_col, table);
    let mut stmt = conn.prepare(&sql)?;
    let mut out = HashMap::new();
    for row in stmt.query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)))? {
        let (id, text) = row?;
        out.insert(id, text);
    }
    Ok(out)
}

fn load_index(path: &PathBuf) -> Result<Index> {
    let conn = Connection::open(path).with_context(|| format!("open {}", path.display()))?;
    let dictionaries = Dictionaries {
        address: load_dict(&conn, "hg_address_dict", "address_id")?,
        nft: load_dict(&conn, "hg_nft_dict", "nft_id")?,
        collection: load_dict(&conn, "hg_collection_dict", "collection_id")?,
    };
    let mut stmt = conn.prepare(
        "SELECT edge_id,event_id,event_type,collection_id,nft_id,src_id,dst_id,block_ts,order_seq,tx_hash \
         FROM hg_hyperedges ORDER BY order_seq,event_id"
    )?;
    let rows = stmt.query_map([], |r| Ok(Edge {
        edge_id: r.get(0)?, event_id: r.get(1)?, event_type: r.get(2)?, collection_id: r.get(3)?,
        nft_id: r.get(4)?, src_id: r.get(5)?, dst_id: r.get(6)?, block_ts: r.get(7)?, order_seq: r.get(8)?,
        tx_hash: r.get(9)?,
    }))?;
    let mut edges = Vec::new();
    for row in rows { edges.push(row?); }
    let mut by_collection: HashMap<i64, Vec<usize>> = HashMap::new();
    let mut by_src: HashMap<i64, Vec<usize>> = HashMap::new();
    let mut by_dst: HashMap<i64, Vec<usize>> = HashMap::new();
    let mut by_nft: HashMap<i64, Vec<usize>> = HashMap::new();
    for (i, e) in edges.iter().enumerate() {
        by_collection.entry(e.collection_id).or_default().push(i);
        by_src.entry(e.src_id).or_default().push(i);
        by_dst.entry(e.dst_id).or_default().push(i);
        by_nft.entry(e.nft_id).or_default().push(i);
    }
    Ok(Index { edges, by_collection, by_src, by_dst, by_nft, dictionaries })
}

fn is_var(raw: &str) -> bool { raw.starts_with('$') && raw.len() > 1 }
fn vname(raw: &str) -> &str { raw.strip_prefix('$').unwrap_or(raw) }

fn role_value(index: &Index, edge: &Edge, role: &str) -> Bound {
    match role {
        "src" => Bound { kind: RoleKind::Address, id: edge.src_id, text: index.dictionaries.text(RoleKind::Address, edge.src_id) },
        "dst" => Bound { kind: RoleKind::Address, id: edge.dst_id, text: index.dictionaries.text(RoleKind::Address, edge.dst_id) },
        "nft" => Bound { kind: RoleKind::Nft, id: edge.nft_id, text: index.dictionaries.text(RoleKind::Nft, edge.nft_id) },
        "collection" => Bound { kind: RoleKind::Collection, id: edge.collection_id, text: index.dictionaries.text(RoleKind::Collection, edge.collection_id) },
        _ => Bound { kind: RoleKind::Address, id: 0, text: String::new() },
    }
}

fn pattern_roles(p: &Pattern) -> Vec<(&'static str, &String)> {
    let mut out = Vec::new();
    if let Some(x) = &p.src { out.push(("src", x)); }
    if let Some(x) = &p.dst { out.push(("dst", x)); }
    if let Some(x) = &p.nft { out.push(("nft", x)); }
    if let Some(x) = &p.collection { out.push(("collection", x)); }
    out
}

fn bind_pattern(index: &Index, p: &Pattern, edge: &Edge, bindings: &HashMap<String, Bound>) -> Option<HashMap<String, Bound>> {
    if !p.event_type.values().iter().any(|x| x == &edge.event_type) { return None; }
    let mut out = bindings.clone();
    for (role, raw) in pattern_roles(p) {
        let actual = role_value(index, edge, role);
        if actual.id == 0 || actual.text.is_empty() { return None; }
        if is_var(raw) {
            let name = vname(raw).to_string();
            if let Some(old) = out.get(&name) {
                if old.kind != actual.kind || old.id != actual.id { return None; }
            } else { out.insert(name, actual); }
        } else if raw != &actual.text { return None; }
    }
    Some(out)
}

fn resolve_text(raw: &Value, bindings: &HashMap<String, Bound>) -> Option<String> {
    match raw {
        Value::String(s) if is_var(s) => bindings.get(vname(s)).map(|x| x.text.clone()),
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        _ => Some(raw.to_string()),
    }
}

fn ctype(c: &Value) -> &str { c.get("type").and_then(Value::as_str).unwrap_or("") }
fn string_array(c: &Value, key: &str) -> Vec<String> {
    c.get(key).and_then(Value::as_array).map(|a| a.iter().filter_map(Value::as_str).map(str::to_string).collect()).unwrap_or_default()
}

fn constraints_ok(index: &Index, branch: &Branch, assigned: &HashMap<String, usize>, bindings: &HashMap<String, Bound>, closure: &[usize], final_check: bool) -> bool {
    let mut ids = HashSet::new();
    for idx in assigned.values() { if !ids.insert(index.edges[*idx].event_id.clone()) { return false; } }
    for c in &branch.constraints {
        match ctype(c) {
            "order" => {
                let seq = string_array(c, "sequence");
                for pair in seq.windows(2) {
                    match (assigned.get(&pair[0]), assigned.get(&pair[1])) {
                        (Some(a), Some(b)) if index.edges[*b].order_seq <= index.edges[*a].order_seq => return false,
                        (None, _) | (_, None) if final_check => return false,
                        _ => {}
                    }
                }
            }
            "strict_time_order" => {
                let seq = string_array(c, "sequence");
                for pair in seq.windows(2) {
                    match (assigned.get(&pair[0]), assigned.get(&pair[1])) {
                        (Some(a), Some(b)) if index.edges[*b].block_ts <= index.edges[*a].block_ts => return false,
                        (None, _) | (_, None) if final_check => return false,
                        _ => {}
                    }
                }
            }
            "within" => {
                let a = c.get("earlier").and_then(Value::as_str).unwrap_or("");
                let b = c.get("later").and_then(Value::as_str).unwrap_or("");
                match (assigned.get(a), assigned.get(b)) {
                    (Some(x), Some(y)) => {
                        let ex = &index.edges[*x]; let ey = &index.edges[*y];
                        if ey.order_seq <= ex.order_seq || ey.block_ts > ex.block_ts + c.get("delta_seconds").and_then(Value::as_i64).unwrap_or(86400) { return false; }
                    }
                    (None, _) | (_, None) if final_check => return false,
                    _ => {}
                }
            }
            "not_equal" | "less_than" => {
                let l = c.get("left").and_then(|x| resolve_text(x, bindings));
                let r = c.get("right").and_then(|x| resolve_text(x, bindings));
                match (l, r) {
                    (Some(a), Some(b)) if ctype(c) == "not_equal" && a == b => return false,
                    (Some(a), Some(b)) if ctype(c) == "less_than" && a >= b => return false,
                    (None, _) | (_, None) if final_check => return false,
                    _ => {}
                }
            }
            "min_distinct" | "distinct_count_between" if final_check => {
                let vals = c.get("values").and_then(Value::as_array).cloned().unwrap_or_default();
                let distinct: HashSet<String> = vals.iter().filter_map(|v| resolve_text(v, bindings)).filter(|v| !v.is_empty()).collect();
                let n = distinct.len() as i64;
                if ctype(c) == "min_distinct" && n < c.get("min_count").and_then(Value::as_i64).unwrap_or(0) { return false; }
                if ctype(c) == "distinct_count_between" {
                    if n < c.get("min_count").and_then(Value::as_i64).unwrap_or(0) || n > c.get("max_count").and_then(Value::as_i64).unwrap_or(i64::MAX) { return false; }
                }
            }
            "distinct_event_ids" => {
                let names = string_array(c, "events");
                let mut seen = HashSet::new(); let mut ready = 0usize;
                for name in &names { if let Some(i) = assigned.get(name) { ready += 1; if !seen.insert(index.edges[*i].event_id.clone()) { return false; } } }
                if final_check && ready != names.len() { return false; }
            }
            "distinct_transactions" => {
                let names = string_array(c, "events");
                let mut seen = HashSet::new(); let mut ready = 0usize;
                for name in &names {
                    if let Some(i) = assigned.get(name) {
                        ready += 1;
                        let tx = &index.edges[*i].tx_hash;
                        if tx.is_empty() || !seen.insert(tx.clone()) { return false; }
                    }
                }
                if final_check && ready != names.len() { return false; }
            }
            "no_event" if final_check => {
                let after = c.get("after").and_then(Value::as_str).unwrap_or("root");
                let Some(anchor_idx) = assigned.get(after) else { return false; };
                let anchor = &index.edges[*anchor_idx];
                let types: HashSet<String> = string_array(c, "event_type").into_iter().collect();
                let src = c.get("src").and_then(|x| resolve_text(x, bindings));
                let dst = c.get("dst").and_then(|x| resolve_text(x, bindings));
                let nft = c.get("nft").and_then(|x| resolve_text(x, bindings));
                let coll = c.get("collection").and_then(|x| resolve_text(x, bindings));
                let end_ts = anchor.block_ts + c.get("within_seconds").and_then(Value::as_i64).unwrap_or(86400);
                for i in closure {
                    let e = &index.edges[*i];
                    if e.order_seq <= anchor.order_seq || e.block_ts > end_ts { continue; }
                    if !types.is_empty() && !types.contains(&e.event_type) { continue; }
                    if src.as_ref().is_some_and(|v| *v != index.dictionaries.text(RoleKind::Address, e.src_id)) { continue; }
                    if dst.as_ref().is_some_and(|v| *v != index.dictionaries.text(RoleKind::Address, e.dst_id)) { continue; }
                    if nft.as_ref().is_some_and(|v| *v != index.dictionaries.text(RoleKind::Nft, e.nft_id)) { continue; }
                    if coll.as_ref().is_some_and(|v| *v != index.dictionaries.text(RoleKind::Collection, e.collection_id)) { continue; }
                    return false;
                }
            }
            "min_distinct" | "distinct_count_between" | "no_event" => {}
            other => panic!("unsupported constraint {other}"),
        }
    }
    true
}

fn closure_indices(index: &Index, root: &Edge, delta: i64) -> Vec<usize> {
    index.by_collection.get(&root.collection_id).map(|rows| rows.iter().copied().filter(|i| {
        let e = &index.edges[*i]; e.order_seq >= root.order_seq && e.block_ts <= root.block_ts + delta
    }).collect()).unwrap_or_default()
}

fn candidate_indices(index: &Index, pattern: &Pattern, bindings: &HashMap<String, Bound>, closure: &[usize]) -> Vec<usize> {
    let mut sources: Vec<&Vec<usize>> = Vec::new();
    for (role, raw) in pattern_roles(pattern) {
        if !is_var(raw) { continue; }
        let Some(bound) = bindings.get(vname(raw)) else { continue; };
        match role {
            "src" => if let Some(x) = index.by_src.get(&bound.id) { sources.push(x); },
            "dst" => if let Some(x) = index.by_dst.get(&bound.id) { sources.push(x); },
            "nft" => if let Some(x) = index.by_nft.get(&bound.id) { sources.push(x); },
            "collection" => if let Some(x) = index.by_collection.get(&bound.id) { sources.push(x); },
            _ => {}
        }
    }
    if sources.is_empty() { return closure.to_vec(); }
    let base = sources.into_iter().min_by_key(|x| x.len()).unwrap();
    let closure_set: HashSet<usize> = closure.iter().copied().collect();
    base.iter().copied().filter(|i| closure_set.contains(i)).collect()
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalRow {
    query_id: String,
    branch_id: String,
    closure_id: i64,
    anchor_order: i64,
    instance_key: String,
    event_bindings_json: String,
    event_orders_json: String,
    variable_bindings_json: String,
}

fn canonical_row(index: &Index, query_id: &str, branch: &Branch, assigned: &HashMap<String, usize>, bindings: &HashMap<String, Bound>) -> CanonicalRow {
    let mut events = BTreeMap::new();
    let mut orders = BTreeMap::new();
    for p in &branch.event_patterns {
        let e = &index.edges[*assigned.get(&p.id).unwrap()];
        events.insert(p.id.clone(), e.event_id.clone());
        orders.insert(p.id.clone(), e.order_seq);
    }
    let variables: BTreeMap<String, String> = bindings.iter().map(|(k, v)| (k.clone(), v.text.clone())).collect();
    let payload = json!({"branch_id": branch.branch_id, "events": events, "query_id": query_id, "variables": variables});
    let raw = serde_json::to_string(&payload).unwrap();
    let key = hex::encode(Sha256::digest(raw.as_bytes()));
    let anchor_order = *orders.get("root").unwrap();
    CanonicalRow {
        query_id: query_id.to_string(), branch_id: branch.branch_id.clone(), closure_id: anchor_order + 1, anchor_order,
        instance_key: key,
        event_bindings_json: serde_json::to_string(&events).unwrap(),
        event_orders_json: serde_json::to_string(&orders).unwrap(),
        variable_bindings_json: serde_json::to_string(&variables).unwrap(),
    }
}

fn match_branch(index: &Index, plan: &FullPlan, branch: &Branch, root_idx: usize) -> Vec<CanonicalRow> {
    let root = &index.edges[root_idx];
    let Some(initial) = bind_pattern(index, &branch.event_patterns[0], root, &HashMap::new()) else { return Vec::new(); };
    let closure = closure_indices(index, root, plan.delta_seconds);
    let mut assigned = HashMap::new(); assigned.insert("root".to_string(), root_idx);
    let mut out = Vec::new();
    fn dfs(index: &Index, plan: &FullPlan, branch: &Branch, closure: &[usize], pos: usize, assigned: &mut HashMap<String, usize>, bindings: &HashMap<String, Bound>, out: &mut Vec<CanonicalRow>) {
        if pos == branch.event_patterns.len() {
            if constraints_ok(index, branch, assigned, bindings, closure, true) { out.push(canonical_row(index, &plan.query_id, branch, assigned, bindings)); }
            return;
        }
        let p = &branch.event_patterns[pos];
        let used: HashSet<String> = assigned.values().map(|i| index.edges[*i].event_id.clone()).collect();
        for i in candidate_indices(index, p, bindings, closure) {
            if used.contains(&index.edges[i].event_id) { continue; }
            let Some(next) = bind_pattern(index, p, &index.edges[i], bindings) else { continue; };
            assigned.insert(p.id.clone(), i);
            if constraints_ok(index, branch, assigned, &next, closure, false) { dfs(index, plan, branch, closure, pos + 1, assigned, &next, out); }
            assigned.remove(&p.id);
        }
    }
    if constraints_ok(index, branch, &assigned, &initial, &closure, false) { dfs(index, plan, branch, &closure, 1, &mut assigned, &initial, &mut out); }
    out
}

fn execute_query(
    index: &Index,
    plan: &FullPlan,
    max_results: usize,
) -> (Vec<CanonicalRow>, usize, String, f64, f64) {
    let tmatch = Instant::now();
    let branches = &plan.branches;
    let per_root: Vec<Vec<CanonicalRow>> = (0..index.edges.len()).into_par_iter().map(|root_idx| {
        let mut rows = Vec::new();
        for branch in branches { rows.extend(match_branch(index, plan, branch, root_idx)); }
        rows
    }).collect();
    let mut rows: Vec<CanonicalRow> = per_root.into_iter().flatten().collect();
    let structural_match_s = tmatch.elapsed().as_secs_f64();
    let tcanon = Instant::now();
    rows.sort_by(|a, b| a.instance_key.cmp(&b.instance_key));
    let before = rows.len();
    rows.dedup_by(|a, b| a.instance_key == b.instance_key);
    let duplicate_count = before - rows.len();
    if max_results > 0 && rows.len() > max_results { rows.truncate(max_results); }
    let mut digest = Sha256::new();
    for r in &rows { digest.update(r.instance_key.as_bytes()); digest.update(b"\n"); }
    let result_digest = hex::encode(digest.finalize());
    let result_canonicalize_s = tcanon.elapsed().as_secs_f64();
    (rows, duplicate_count, result_digest, structural_match_s, result_canonicalize_s)
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.outdir)?;
    rayon::ThreadPoolBuilder::new().num_threads(args.threads.max(1)).build_global().ok();
    let doc: QueryDocument = serde_json::from_str(&fs::read_to_string(&args.query_json)?)?;
    if doc.full_binding_plan.result_semantics != "full_event_binding_v1" { anyhow::bail!("unsupported result semantics"); }
    let all0 = Instant::now();
    let tload = Instant::now();
    let index = load_index(&args.db)?;
    let load_s = tload.elapsed().as_secs_f64();
    let twarm = Instant::now();
    for _ in 0..args.warmup_runs {
        let _ = execute_query(&index, &doc.full_binding_plan, args.max_results);
    }
    let warmup_s = twarm.elapsed().as_secs_f64();
    if let Some(marker) = &args.phase_marker {
        if let Some(parent) = marker.parent() { fs::create_dir_all(parent)?; }
        fs::write(marker, b"query\n")?;
    }
    let (rows, duplicate_count, result_digest, structural_match_s, result_canonicalize_s) =
        execute_query(&index, &doc.full_binding_plan, args.max_results);
    if let Some(marker) = &args.phase_marker {
        if marker.exists() { fs::remove_file(marker)?; }
    }
    let tcsv = Instant::now();
    let csv_path = args.outdir.join("instances.csv");
    let mut writer = csv::Writer::from_path(&csv_path)?;
    for row in &rows { writer.serialize(row)?; }
    writer.flush()?;
    let csv_write_s = tcsv.elapsed().as_secs_f64();
    let report = json!({
        "system": "HGMatch",
        "engine": "Rust match-by-hyperedge integer postings with Rayon",
        "query": doc.full_binding_plan.query_id,
        "result_semantics": doc.full_binding_plan.result_semantics,
        "result_count": rows.len(),
        "duplicate_count": duplicate_count,
        "result_digest_sha256": result_digest,
        "instances_csv": csv_path,
        "threads_requested": args.threads,
        "threads_effective": args.threads.max(1),
        "hyperedge_count": index.edges.len(),
        "timing": {
            "store_open_and_hypergraph_load_s": load_s,
            "warmup_runs": args.warmup_runs,
            "warmup_s": warmup_s,
            "structural_expansion_and_exact_check_s": structural_match_s,
            "result_canonicalize_s": result_canonicalize_s,
            "csv_write_s": csv_write_s,
            "query_latency_s": structural_match_s + result_canonicalize_s,
            "wall_s": all0.elapsed().as_secs_f64(),
            "runtime_load_excluded_from_query_latency": true,
            "same_runtime_warmup": true,
            "offline_index_build_excluded": true,
            "result_file_write_excluded_from_query_latency": true
        }
    });
    fs::write(args.outdir.join("execution_report.json"), serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
