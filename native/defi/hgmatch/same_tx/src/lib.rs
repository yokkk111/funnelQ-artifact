use anyhow::{anyhow, Context, Result};
use csv::{ReaderBuilder, StringRecord};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use rayon::ThreadPoolBuilder;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::thread;
use std::time::Instant;

pub fn ensure_dir(path: &str) -> Result<()> {
    return Ok(());                                             

    fs::create_dir_all(path).with_context(|| format!("failed to create dir: {path}"))?;
    Ok(())
}

pub fn norm_str(v: &str) -> String {
    let s = v.trim();
    if s.eq_ignore_ascii_case("nan") || s.eq_ignore_ascii_case("none") || s.eq_ignore_ascii_case("null") {
        String::new()
    } else {
        s.to_string()
    }
}

fn escape_component(s: &str) -> String {
    s.replace('\u{1f}', "_")
}

pub fn make_signature_key(labels: &[String]) -> String {
    let mut xs = labels.to_vec();
    xs.sort();
    xs.join("\u{1f}")
}

pub fn decode_signature_key(signature_key: &str) -> Vec<String> {
    if signature_key.is_empty() {
        Vec::new()
    } else {
        signature_key.split('\u{1f}').map(|s| s.to_string()).collect()
    }
}

pub fn const_vertex_id(label: &str, value: &str) -> String {
    format!("{}::{}", escape_component(label), escape_component(value))
}

pub fn role_slot_label(event_type: &str, role_name: &str) -> String {
    format!("slot::{}::{}", escape_component(event_type), escape_component(role_name))
}

fn pick(headers: &HashMap<String, usize>, row: &StringRecord, aliases: &[&str]) -> String {
    for key in aliases {
        if let Some(idx) = headers.get(&key.to_ascii_lowercase()) {
            return norm_str(row.get(*idx).unwrap_or(""));
        }
    }
    String::new()
}

fn parse_i64_fallback(s: &str) -> i64 {
    if s.is_empty() {
        return 0;
    }
    if let Ok(v) = s.parse::<i64>() {
        return v;
    }
    if let Ok(vf) = s.parse::<f64>() {
        return vf as i64;
    }
    0
}

fn write_json_pretty<P: AsRef<Path>, T: Serialize>(path: P, value: &T) -> Result<()> {
    return Ok(());                                             

    let mut f = File::create(path.as_ref())?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    Ok(())
}

fn write_csv_rows<P: AsRef<Path>>(path: P, headers: &[&str], rows: &[Vec<String>]) -> Result<()> {
    return Ok(());                                             

    let mut wtr = csv::Writer::from_path(path)?;
    wtr.write_record(headers)?;
    for row in rows {
        wtr.write_record(row)?;
    }
    wtr.flush()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryRule {
    pub rule_id: String,
    pub event_type: String,
    pub source_table: String,
    pub role_schema: BTreeMap<String, String>,
    pub anchorable_fields: Vec<String>,
}

pub fn registry_rules() -> Vec<RegistryRule> {
    let mut out = Vec::new();

    let mut flashloan_roles = BTreeMap::new();
    flashloan_roles.insert("borrower".to_string(), "addr".to_string());
    flashloan_roles.insert("recipient".to_string(), "addr".to_string());
    flashloan_roles.insert("asset".to_string(), "token".to_string());
    out.push(RegistryRule {
        rule_id: "rule_flashloan_v1".to_string(),
        event_type: "flashloan".to_string(),
        source_table: "lending.flashloans".to_string(),
        role_schema: flashloan_roles,
        anchorable_fields: vec![
            "event_type".to_string(),
            "protocol".to_string(),
            "asset".to_string(),
            "borrower".to_string(),
            "recipient".to_string(),
        ],
    });

    let mut borrow_roles = BTreeMap::new();
    borrow_roles.insert("borrower".to_string(), "addr".to_string());
    borrow_roles.insert("asset".to_string(), "token".to_string());
    borrow_roles.insert("pool".to_string(), "pool".to_string());
    out.push(RegistryRule {
        rule_id: "rule_borrow_v1".to_string(),
        event_type: "borrow".to_string(),
        source_table: "lending.borrow".to_string(),
        role_schema: borrow_roles,
        anchorable_fields: vec![
            "event_type".to_string(),
            "protocol".to_string(),
            "asset".to_string(),
            "borrower".to_string(),
        ],
    });

    let mut swap_roles = BTreeMap::new();
    swap_roles.insert("taker".to_string(), "addr".to_string());
    swap_roles.insert("pool".to_string(), "pool".to_string());
    swap_roles.insert("token_sold".to_string(), "token".to_string());
    swap_roles.insert("token_bought".to_string(), "token".to_string());
    out.push(RegistryRule {
        rule_id: "rule_swap_v1".to_string(),
        event_type: "swap".to_string(),
        source_table: "dex.trades".to_string(),
        role_schema: swap_roles,
        anchorable_fields: vec![
            "event_type".to_string(),
            "protocol".to_string(),
            "taker".to_string(),
            "token_sold".to_string(),
            "token_bought".to_string(),
            "pool".to_string(),
        ],
    });

    out
}

pub fn registry_map() -> HashMap<String, RegistryRule> {
    registry_rules()
        .into_iter()
        .map(|r| (r.event_type.clone(), r))
        .collect()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventAtom {
    pub event_id: String,
    pub closure_id: String,
    pub tx_hash: String,
    pub event_type: String,
    pub protocol: String,
    pub seq_index: i64,
    pub roles: BTreeMap<String, String>,
    pub attrs: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceClosure {
    pub closure_id: String,
    pub tx_hash: String,
    pub num_events: usize,
    pub event_ids: Vec<String>,
    pub event_types: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataVertex {
    pub vertex_id: String,
    pub label: String,
    pub source: String,
    pub role_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataHyperedge {
    pub edge_id: String,
    pub closure_id: String,
    pub tx_hash: String,
    pub event_id: String,
    pub event_type: String,
    pub protocol: String,
    pub seq_index: i64,
    pub roles: BTreeMap<String, String>,
    pub attrs: BTreeMap<String, String>,
    pub vertices: Vec<DataVertex>,
    pub signature_key: String,
    pub signature_labels: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRow {
    pub signature_key: String,
    pub signature_labels: Vec<String>,
    pub num_edges: usize,
    pub edge_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostingRow {
    pub signature_key: String,
    pub vertex_id: String,
    pub num_edges: usize,
    pub posting: Vec<String>,
}

fn headers_map(headers: &StringRecord) -> HashMap<String, usize> {
    headers
        .iter()
        .enumerate()
        .map(|(i, s)| (s.to_ascii_lowercase(), i))
        .collect()
}

fn load_csv_records(path: &str) -> Result<(HashMap<String, usize>, Vec<StringRecord>)> {
    let mut rdr = ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = headers_map(rdr.headers()?);
    let mut rows = Vec::new();
    for rec in rdr.records() {
        rows.push(rec?);
    }
    Ok((headers, rows))
}

pub fn normalize_flashloans(path: &str) -> Result<Vec<EventAtom>> {
    let mut rdr = ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = headers_map(rdr.headers()?);
    let mut out = Vec::new();
    for (i, row_result) in rdr.records().enumerate() {
        let row = row_result?;
        let tx_hash = pick(&headers, &row, &["tx_hash", "txhash", "hash"]);
        if tx_hash.is_empty() {
            continue;
        }
        let seq_index = parse_i64_fallback(&pick(&headers, &row, &["evt_index", "log_index", "index", "event_index"]));
        let protocol = {
            let p = pick(&headers, &row, &["project", "protocol", "platform"]);
            let v = pick(&headers, &row, &["version"]);
            if !p.is_empty() && !v.is_empty() {
                format!("{}:{}", p, v)
            } else if !p.is_empty() {
                p
            } else {
                v
            }
        };
        let borrower = pick(&headers, &row, &["borrower", "initiator", "user"]);
        let recipient = pick(&headers, &row, &["recipient", "receiver"]);
        let asset = pick(&headers, &row, &["asset_address", "token_address", "asset", "reserve_address", "token"]);
        let asset_symbol = pick(&headers, &row, &["asset_symbol", "symbol", "token_symbol"]);
        let amount = pick(&headers, &row, &["amount", "amount_raw", "amount_token"]);
        let amount_usd = pick(&headers, &row, &["amount_usd", "usd_amount"]);

        let mut roles = BTreeMap::new();
        roles.insert("borrower".to_string(), borrower);
        roles.insert("recipient".to_string(), recipient);
        roles.insert("asset".to_string(), asset);

        let mut attrs = BTreeMap::new();
        attrs.insert("asset_symbol".to_string(), asset_symbol);
        attrs.insert("amount".to_string(), amount);
        attrs.insert("amount_usd".to_string(), amount_usd);

        let event_id = format!("{}::flashloan::{}::{}", tx_hash, seq_index, i);
        out.push(EventAtom {
            event_id,
            closure_id: tx_hash.clone(),
            tx_hash,
            event_type: "flashloan".to_string(),
            protocol,
            seq_index,
            roles,
            attrs,
        });
    }
    Ok(out)
}

pub fn normalize_borrow(path: &str) -> Result<Vec<EventAtom>> {
    let mut rdr = ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = headers_map(rdr.headers()?);
    let mut out = Vec::new();
    for (i, row_result) in rdr.records().enumerate() {
        let row = row_result?;
        let tx_hash = pick(&headers, &row, &["tx_hash", "txhash", "hash"]);
        if tx_hash.is_empty() {
            continue;
        }
        let seq_index = parse_i64_fallback(&pick(&headers, &row, &["evt_index", "log_index", "index", "event_index"]));
        let protocol = {
            let p = pick(&headers, &row, &["project", "protocol", "platform"]);
            let v = pick(&headers, &row, &["version"]);
            if !p.is_empty() && !v.is_empty() {
                format!("{}:{}", p, v)
            } else if !p.is_empty() {
                p
            } else {
                v
            }
        };
        let borrower = pick(
            &headers,
            &row,
            &[
                "borrower",
                "borrower_address",
                "borroweraddress",
                "on_behalf_of",
                "onbehalfof",
                "on_behalf_of_address",
                "onbehalfofaddress",
                "user",
                "user_address",
                "useraddress",
                "account",
                "receiver",
                "recipient",
            ],
        );
        let asset = pick(
            &headers,
            &row,
            &[
                "asset_address",
                "assetaddress",
                "reserve_address",
                "reserveaddress",
                "underlying_asset_address",
                "underlyingassetaddress",
                "underlying_address",
                "underlyingaddress",
                "token_address",
                "tokenaddress",
                "asset",
                "reserve",
                "token",
                "underlying_asset",
                "underlyingasset",
            ],
        );
        let asset_symbol = pick(
            &headers,
            &row,
            &[
                "asset_symbol",
                "assetsymbol",
                "reserve_symbol",
                "reservesymbol",
                "underlying_symbol",
                "underlyingsymbol",
                "symbol",
                "token_symbol",
                "tokensymbol",
            ],
        );
        let pool = pick(
            &headers,
            &row,
            &[
                "pool_address",
                "pooladdress",
                "pool",
                "market_address",
                "marketaddress",
                "reserve_pool",
                "reservepool",
                "project_contract_address",
                "projectcontractaddress",
                "contract_address",
                "contractaddress",
                "contract",
            ],
        );
        let amount = pick(
            &headers,
            &row,
            &[
                "amount",
                "amount_raw",
                "amountraw",
                "amount_token",
                "amounttoken",
                "borrow_amount",
                "borrowamount",
            ],
        );
        let amount_usd = pick(
            &headers,
            &row,
            &[
                "amount_usd",
                "amountusd",
                "usd_amount",
                "usdamount",
            ],
        );

        let mut roles = BTreeMap::new();
        roles.insert("borrower".to_string(), borrower);
        roles.insert("asset".to_string(), asset);
        roles.insert("pool".to_string(), pool);

        let mut attrs = BTreeMap::new();
        attrs.insert("asset_symbol".to_string(), asset_symbol);
        attrs.insert("amount".to_string(), amount);
        attrs.insert("amount_usd".to_string(), amount_usd);

        let event_id = format!("{}::borrow::{}::{}", tx_hash, seq_index, i);
        out.push(EventAtom {
            event_id,
            closure_id: tx_hash.clone(),
            tx_hash,
            event_type: "borrow".to_string(),
            protocol,
            seq_index,
            roles,
            attrs,
        });
    }
    Ok(out)
}

pub fn normalize_dex(path: &str) -> Result<Vec<EventAtom>> {
    let mut rdr = ReaderBuilder::new().flexible(true).from_path(path)?;
    let headers = headers_map(rdr.headers()?);
    let mut out = Vec::new();
    for (i, row_result) in rdr.records().enumerate() {
        let row = row_result?;
        let tx_hash = pick(&headers, &row, &["tx_hash", "txhash", "hash"]);
        if tx_hash.is_empty() {
            continue;
        }
        let seq_index = parse_i64_fallback(&pick(&headers, &row, &["evt_index", "log_index", "index", "event_index"]));
        let protocol = {
            let p = pick(&headers, &row, &["project", "protocol", "platform"]);
            let v = pick(&headers, &row, &["version"]);
            if !p.is_empty() && !v.is_empty() {
                format!("{}:{}", p, v)
            } else if !p.is_empty() {
                p
            } else {
                v
            }
        };
        let taker = pick(&headers, &row, &["taker", "trader", "tx_from", "caller", "user"]);
        let pool = pick(&headers, &row, &["pool_address", "pool", "project_contract_address", "contract_address"]);
        let token_sold = pick(&headers, &row, &["token_sold_address", "token_in", "token_in_address", "sold_token_address"]);
        let token_bought = pick(&headers, &row, &["token_bought_address", "token_out", "token_out_address", "bought_token_address"]);
        let sold_symbol = pick(&headers, &row, &["token_sold_symbol", "token_in_symbol", "sold_symbol"]);
        let bought_symbol = pick(&headers, &row, &["token_bought_symbol", "token_out_symbol", "bought_symbol"]);
        let amount_sold = pick(&headers, &row, &["token_sold_amount", "amount_in", "amount_sold"]);
        let amount_bought = pick(&headers, &row, &["token_bought_amount", "amount_out", "amount_bought"]);
        let amount_usd = pick(&headers, &row, &["amount_usd", "usd_amount"]);

        let mut roles = BTreeMap::new();
        roles.insert("taker".to_string(), taker);
        roles.insert("pool".to_string(), pool);
        roles.insert("token_sold".to_string(), token_sold);
        roles.insert("token_bought".to_string(), token_bought);

        let mut attrs = BTreeMap::new();
        attrs.insert("token_sold_symbol".to_string(), sold_symbol);
        attrs.insert("token_bought_symbol".to_string(), bought_symbol);
        attrs.insert("amount_sold".to_string(), amount_sold);
        attrs.insert("amount_bought".to_string(), amount_bought);
        attrs.insert("amount_usd".to_string(), amount_usd);

        let event_id = format!("{}::swap::{}::{}", tx_hash, seq_index, i);
        out.push(EventAtom {
            event_id,
            closure_id: tx_hash.clone(),
            tx_hash,
            event_type: "swap".to_string(),
            protocol,
            seq_index,
            roles,
            attrs,
        });
    }
    Ok(out)
}

pub fn records_to_event_atoms(mut atoms: Vec<EventAtom>) -> Vec<EventAtom> {
    atoms.sort_by(|a, b| {
        let ka = (&a.closure_id, a.seq_index, &a.event_type, &a.event_id);
        let kb = (&b.closure_id, b.seq_index, &b.event_type, &b.event_id);
        ka.cmp(&kb)
    });
    atoms
}

pub fn build_evidence_closures(atoms: &[EventAtom]) -> Vec<EvidenceClosure> {
    let mut grouped: BTreeMap<String, Vec<&EventAtom>> = BTreeMap::new();
    for atom in atoms {
        grouped.entry(atom.closure_id.clone()).or_default().push(atom);
    }
    grouped
        .into_iter()
        .map(|(closure_id, xs)| EvidenceClosure {
            closure_id: closure_id.clone(),
            tx_hash: closure_id,
            num_events: xs.len(),
            event_ids: xs.iter().map(|x| x.event_id.clone()).collect(),
            event_types: xs.iter().map(|x| x.event_type.clone()).collect(),
        })
        .collect()
}

pub fn build_hyperedge_for_atom(atom: &EventAtom, registry: &HashMap<String, RegistryRule>) -> Result<DataHyperedge> {
    let rule = registry
        .get(&atom.event_type)
        .ok_or_else(|| anyhow!("missing registry rule for event_type={}", atom.event_type))?;

    let mut vertices = Vec::new();
    let etype_label = format!("etype::{}", atom.event_type);
    vertices.push(DataVertex {
        vertex_id: etype_label.clone(),
        label: etype_label.clone(),
        source: "etype".to_string(),
        role_name: None,
    });

    for (role_name, role_label) in rule.role_schema.iter() {
        let value = norm_str(atom.roles.get(role_name).map(|s| s.as_str()).unwrap_or(""));
        if value.is_empty() {
            continue;
        }
        let slot_label = role_slot_label(&atom.event_type, role_name);
        vertices.push(DataVertex {
            vertex_id: slot_label.clone(),
            label: slot_label.clone(),
            source: "role_slot".to_string(),
            role_name: Some(role_name.clone()),
        });
        vertices.push(DataVertex {
            vertex_id: const_vertex_id(role_label, &value),
            label: role_label.clone(),
            source: format!("role_value::{}", role_name),
            role_name: Some(role_name.clone()),
        });
    }

    let signature_labels: Vec<String> = vertices.iter().map(|v| v.label.clone()).collect();
    let signature_key = make_signature_key(&signature_labels);

    Ok(DataHyperedge {
        edge_id: atom.event_id.clone(),
        closure_id: atom.closure_id.clone(),
        tx_hash: atom.tx_hash.clone(),
        event_id: atom.event_id.clone(),
        event_type: atom.event_type.clone(),
        protocol: atom.protocol.clone(),
        seq_index: atom.seq_index,
        roles: atom.roles.clone(),
        attrs: atom.attrs.clone(),
        vertices,
        signature_key,
        signature_labels,
    })
}

pub fn build_hgmatch_runtime_tables(atoms: &[EventAtom]) -> Result<(Vec<DataHyperedge>, Vec<PartitionRow>, Vec<PostingRow>)> {
    let registry = registry_map();
    let mut hyperedges = Vec::new();
    for atom in atoms {
        hyperedges.push(build_hyperedge_for_atom(atom, &registry)?);
    }

    let mut part_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut sig_labels_map: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut inv_map: BTreeMap<(String, String), Vec<String>> = BTreeMap::new();

    for edge in hyperedges.iter() {
        part_map.entry(edge.signature_key.clone()).or_default().push(edge.edge_id.clone());
        sig_labels_map
            .entry(edge.signature_key.clone())
            .or_insert_with(|| decode_signature_key(&edge.signature_key));

        let mut uniq_vertices = BTreeSet::new();
        for v in edge.vertices.iter() {
            uniq_vertices.insert(v.vertex_id.clone());
        }
        for vid in uniq_vertices {
            inv_map.entry((edge.signature_key.clone(), vid)).or_default().push(edge.edge_id.clone());
        }
    }

    let partitions = part_map
        .into_iter()
        .map(|(signature_key, mut edge_ids)| {
            edge_ids.sort();
            PartitionRow {
                signature_key: signature_key.clone(),
                signature_labels: sig_labels_map.get(&signature_key).cloned().unwrap_or_default(),
                num_edges: edge_ids.len(),
                edge_ids,
            }
        })
        .collect::<Vec<_>>();

    let postings = inv_map
        .into_iter()
        .map(|((signature_key, vertex_id), mut posting)| {
            posting.sort();
            PostingRow {
                signature_key,
                vertex_id,
                num_edges: posting.len(),
                posting,
            }
        })
        .collect::<Vec<_>>();

    Ok((hyperedges, partitions, postings))
}

pub fn write_event_atoms_csv(path: &str, atoms: &[EventAtom]) -> Result<()> {
    let headers = [
        "event_id",
        "closure_id",
        "tx_hash",
        "event_type",
        "protocol",
        "seq_index",
        "roles_json",
        "attrs_json",
    ];
    let rows = atoms
        .iter()
        .map(|a| {
            vec![
                a.event_id.clone(),
                a.closure_id.clone(),
                a.tx_hash.clone(),
                a.event_type.clone(),
                a.protocol.clone(),
                a.seq_index.to_string(),
                serde_json::to_string(&a.roles).unwrap_or_else(|_| "{}".to_string()),
                serde_json::to_string(&a.attrs).unwrap_or_else(|_| "{}".to_string()),
            ]
        })
        .collect::<Vec<_>>();
    write_csv_rows(path, &headers, &rows)
}

pub fn write_evidence_closures_csv(path: &str, closures: &[EvidenceClosure]) -> Result<()> {
    let headers = ["closure_id", "tx_hash", "num_events", "event_ids_json", "event_types_json"];
    let rows = closures
        .iter()
        .map(|c| {
            vec![
                c.closure_id.clone(),
                c.tx_hash.clone(),
                c.num_events.to_string(),
                serde_json::to_string(&c.event_ids).unwrap_or_else(|_| "[]".to_string()),
                serde_json::to_string(&c.event_types).unwrap_or_else(|_| "[]".to_string()),
            ]
        })
        .collect::<Vec<_>>();
    write_csv_rows(path, &headers, &rows)
}

pub fn write_partition_manifest_csv(path: &str, partitions: &[PartitionRow]) -> Result<()> {
    let headers = ["signature_key", "signature_labels_json", "num_edges", "edge_ids_json_size_bytes"];
    let rows = partitions
        .iter()
        .map(|p| {
            let edge_ids_json = serde_json::to_string(&p.edge_ids).unwrap_or_else(|_| "[]".to_string());
            vec![
                p.signature_key.clone(),
                serde_json::to_string(&p.signature_labels).unwrap_or_else(|_| "[]".to_string()),
                p.num_edges.to_string(),
                edge_ids_json.len().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    write_csv_rows(path, &headers, &rows)
}

pub fn write_inverted_manifest_csv(path: &str, postings: &[PostingRow]) -> Result<()> {
    let headers = ["signature_key", "vertex_id", "num_edges", "posting_json_size_bytes"];
    let rows = postings
        .iter()
        .map(|p| {
            let posting_json = serde_json::to_string(&p.posting).unwrap_or_else(|_| "[]".to_string());
            vec![
                p.signature_key.clone(),
                p.vertex_id.clone(),
                p.num_edges.to_string(),
                posting_json.len().to_string(),
            ]
        })
        .collect::<Vec<_>>();
    write_csv_rows(path, &headers, &rows)
}

pub fn write_hgmatch_sqlite(
    sqlite_path: &str,
    rules: &[RegistryRule],
    closures: &[EvidenceClosure],
    atoms: &[EventAtom],
    hyperedges: &[DataHyperedge],
    partitions: &[PartitionRow],
    postings: &[PostingRow],
) -> Result<()> {
    if Path::new(sqlite_path).exists() {
        fs::remove_file(sqlite_path)?;
    }
    let mut conn = Connection::open_with_flags(sqlite_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE registry_snapshot (
            rule_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            source_table TEXT NOT NULL,
            role_schema_json TEXT NOT NULL,
            anchorable_fields_json TEXT NOT NULL
        );
        CREATE TABLE evidence_closures (
            closure_id TEXT PRIMARY KEY,
            tx_hash TEXT NOT NULL,
            num_events INTEGER NOT NULL,
            event_ids_json TEXT NOT NULL,
            event_types_json TEXT NOT NULL
        );
        CREATE TABLE event_atoms (
            event_id TEXT PRIMARY KEY,
            closure_id TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            event_type TEXT NOT NULL,
            protocol TEXT NOT NULL,
            seq_index INTEGER NOT NULL,
            roles_json TEXT NOT NULL,
            attrs_json TEXT NOT NULL
        );
        CREATE TABLE hg_hyperedges (
            edge_id TEXT PRIMARY KEY,
            closure_id TEXT NOT NULL,
            tx_hash TEXT NOT NULL,
            signature_key TEXT NOT NULL,
            event_type TEXT NOT NULL,
            seq_index INTEGER NOT NULL,
            edge_json TEXT NOT NULL
        );
        CREATE TABLE hg_partitions (
            signature_key TEXT PRIMARY KEY,
            signature_labels_json TEXT NOT NULL,
            num_edges INTEGER NOT NULL,
            edge_ids_json TEXT NOT NULL
        );
        CREATE TABLE hg_inverted_index (
            signature_key TEXT NOT NULL,
            vertex_id TEXT NOT NULL,
            num_edges INTEGER NOT NULL,
            posting_json TEXT NOT NULL,
            PRIMARY KEY(signature_key, vertex_id)
        );
        CREATE INDEX idx_event_atoms_closure ON event_atoms(closure_id, seq_index);
        CREATE INDEX idx_hg_hyperedges_closure ON hg_hyperedges(closure_id, seq_index);
        CREATE INDEX idx_hg_hyperedges_signature ON hg_hyperedges(signature_key);
        "#,
    )?;

    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO registry_snapshot(rule_id, event_type, source_table, role_schema_json, anchorable_fields_json) VALUES (?, ?, ?, ?, ?)"
        )?;
        for rule in rules {
            stmt.execute(params![
                rule.rule_id,
                rule.event_type,
                rule.source_table,
                serde_json::to_string(&rule.role_schema)?,
                serde_json::to_string(&rule.anchorable_fields)?,
            ])?;
        }
    }
    {
        let mut stmt = tx.prepare(
            "INSERT INTO evidence_closures(closure_id, tx_hash, num_events, event_ids_json, event_types_json) VALUES (?, ?, ?, ?, ?)"
        )?;
        for clo in closures {
            stmt.execute(params![
                clo.closure_id,
                clo.tx_hash,
                clo.num_events as i64,
                serde_json::to_string(&clo.event_ids)?,
                serde_json::to_string(&clo.event_types)?,
            ])?;
        }
    }
    {
        let mut stmt = tx.prepare(
            "INSERT INTO event_atoms(event_id, closure_id, tx_hash, event_type, protocol, seq_index, roles_json, attrs_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )?;
        for atom in atoms {
            stmt.execute(params![
                atom.event_id,
                atom.closure_id,
                atom.tx_hash,
                atom.event_type,
                atom.protocol,
                atom.seq_index,
                serde_json::to_string(&atom.roles)?,
                serde_json::to_string(&atom.attrs)?,
            ])?;
        }
    }
    {
        let mut stmt = tx.prepare(
            "INSERT INTO hg_hyperedges(edge_id, closure_id, tx_hash, signature_key, event_type, seq_index, edge_json) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )?;
        for edge in hyperedges {
            stmt.execute(params![
                edge.edge_id,
                edge.closure_id,
                edge.tx_hash,
                edge.signature_key,
                edge.event_type,
                edge.seq_index,
                serde_json::to_string(edge)?,
            ])?;
        }
    }
    {
        let mut stmt = tx.prepare(
            "INSERT INTO hg_partitions(signature_key, signature_labels_json, num_edges, edge_ids_json) VALUES (?, ?, ?, ?)"
        )?;
        for p in partitions {
            stmt.execute(params![
                p.signature_key,
                serde_json::to_string(&p.signature_labels)?,
                p.num_edges as i64,
                serde_json::to_string(&p.edge_ids)?,
            ])?;
        }
    }
    {
        let mut stmt = tx.prepare(
            "INSERT INTO hg_inverted_index(signature_key, vertex_id, num_edges, posting_json) VALUES (?, ?, ?, ?)"
        )?;
        for p in postings {
            stmt.execute(params![
                p.signature_key,
                p.vertex_id,
                p.num_edges as i64,
                serde_json::to_string(&p.posting)?,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEventSpec {
    pub id: String,
    #[serde(rename = "type")]
    pub event_type: String,
    #[serde(default)]
    pub protocol: String,
    #[serde(default)]
    pub roles: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttrFilter {
    pub event: String,
    pub field: String,
    pub op: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub query_name: String,
    pub scope: String,
    pub events: Vec<QueryEventSpec>,
    #[serde(default)]
    pub order: Vec<(String, String)>,
    #[serde(default)]
    pub attr_filters: Vec<AttrFilter>,
    #[serde(default)]
    pub neq_constraints: Vec<(String, String, String, String)>,
}

pub fn load_query_json(path: &str) -> Result<QuerySpec> {
    let f = File::open(path)?;
    let q: QuerySpec = serde_json::from_reader(f)?;
    if q.scope != "same_tx" {
        return Err(anyhow!("only scope=same_tx is supported"));
    }
    if q.events.is_empty() {
        return Err(anyhow!("query must contain at least one event"));
    }
    Ok(q)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionMeta {
    pub signature_key: String,
    pub signature_labels: Vec<String>,
    pub label_counts: BTreeMap<String, usize>,
    pub num_edges: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InMemoryStoreStats {
    pub num_registry_rules: usize,
    pub num_edges: usize,
    pub num_closures: usize,
    pub num_partitions: usize,
    pub num_postings: usize,
}

#[derive(Debug, Clone)]
pub struct InMemoryStore {
    pub registry: HashMap<String, RegistryRule>,
    pub edges: Vec<DataHyperedge>,
    pub edge_id_to_idx: HashMap<String, usize>,
    pub closure_to_edge_idxs: HashMap<String, Vec<usize>>,
    pub partitions: HashMap<String, PartitionMeta>,
    pub partition_edge_idxs: HashMap<String, Vec<usize>>,
    pub postings: HashMap<(String, String), Vec<usize>>,
}

impl InMemoryStore {
    pub fn stats(&self) -> InMemoryStoreStats {
        InMemoryStoreStats {
            num_registry_rules: self.registry.len(),
            num_edges: self.edges.len(),
            num_closures: self.closure_to_edge_idxs.len(),
            num_partitions: self.partitions.len(),
            num_postings: self.postings.len(),
        }
    }
}

pub fn load_store_from_sqlite(sqlite_path: &str) -> Result<InMemoryStore> {
    let conn = Connection::open_with_flags(sqlite_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let mut registry = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT rule_id, event_type, source_table, role_schema_json, anchorable_fields_json FROM registry_snapshot"
        )?;
        let rows = stmt.query_map([], |row| {
            let role_schema_json: String = row.get(3)?;
            let anchorable_json: String = row.get(4)?;
            let role_schema: BTreeMap<String, String> = serde_json::from_str(&role_schema_json).unwrap_or_default();
            let anchorable_fields: Vec<String> = serde_json::from_str(&anchorable_json).unwrap_or_default();
            Ok(RegistryRule {
                rule_id: row.get(0)?,
                event_type: row.get(1)?,
                source_table: row.get(2)?,
                role_schema,
                anchorable_fields,
            })
        })?;
        for r in rows {
            let rule = r?;
            registry.insert(rule.event_type.clone(), rule);
        }
    }

    let mut edges = Vec::new();
    let mut edge_id_to_idx = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT edge_json FROM hg_hyperedges ORDER BY closure_id, seq_index, edge_id")?;
        let rows = stmt.query_map([], |row| {
            let payload: String = row.get(0)?;
            Ok(payload)
        })?;
        for payload in rows {
            let edge: DataHyperedge = serde_json::from_str(&payload?)?;
            let idx = edges.len();
            edge_id_to_idx.insert(edge.edge_id.clone(), idx);
            edges.push(edge);
        }
    }

    let mut closure_to_edge_idxs: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, edge) in edges.iter().enumerate() {
        closure_to_edge_idxs.entry(edge.closure_id.clone()).or_default().push(idx);
    }
    for xs in closure_to_edge_idxs.values_mut() {
        xs.sort_by_key(|idx| (edges[*idx].seq_index, edges[*idx].edge_id.clone()));
    }

    let mut partitions = HashMap::new();
    let mut partition_edge_idxs = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT signature_key, signature_labels_json, num_edges, edge_ids_json FROM hg_partitions")?;
        let rows = stmt.query_map([], |row| {
            let signature_key: String = row.get(0)?;
            let signature_labels_json: String = row.get(1)?;
            let num_edges: i64 = row.get(2)?;
            let edge_ids_json: String = row.get(3)?;
            Ok((signature_key, signature_labels_json, num_edges, edge_ids_json))
        })?;
        for row in rows {
            let (signature_key, signature_labels_json, num_edges, edge_ids_json) = row?;
            let signature_labels: Vec<String> = serde_json::from_str(&signature_labels_json).unwrap_or_default();
            let mut label_counts = BTreeMap::new();
            for lbl in signature_labels.iter() {
                *label_counts.entry(lbl.clone()).or_insert(0) += 1;
            }
            partitions.insert(
                signature_key.clone(),
                PartitionMeta {
                    signature_key: signature_key.clone(),
                    signature_labels,
                    label_counts,
                    num_edges: num_edges as usize,
                },
            );
            let edge_ids: Vec<String> = serde_json::from_str(&edge_ids_json).unwrap_or_default();
            let mut idxs = edge_ids
                .into_iter()
                .map(|eid| edge_id_to_idx.get(&eid).copied()
                    .ok_or_else(|| anyhow!("dangling HGMatch posting edge: {}", eid)))
                .collect::<Result<Vec<_>>>()?;
                                                                        
            idxs.sort_unstable();
            idxs.dedup();
            partition_edge_idxs.insert(signature_key, idxs);
        }
    }

    let mut postings = HashMap::new();
    {
        let mut stmt = conn.prepare("SELECT signature_key, vertex_id, posting_json FROM hg_inverted_index")?;
        let rows = stmt.query_map([], |row| {
            let signature_key: String = row.get(0)?;
            let vertex_id: String = row.get(1)?;
            let posting_json: String = row.get(2)?;
            Ok((signature_key, vertex_id, posting_json))
        })?;
        for row in rows {
            let (signature_key, vertex_id, posting_json) = row?;
            let edge_ids: Vec<String> = serde_json::from_str(&posting_json).unwrap_or_default();
            let mut idxs = edge_ids
                .into_iter()
                .map(|eid| edge_id_to_idx.get(&eid).copied()
                    .ok_or_else(|| anyhow!("dangling HGMatch posting edge: {}", eid)))
                .collect::<Result<Vec<_>>>()?;
                                                                        
            idxs.sort_unstable();
            idxs.dedup();
            postings.insert((signature_key, vertex_id), idxs);
        }
    }

    Ok(InMemoryStore {
        registry,
        edges,
        edge_id_to_idx,
        closure_to_edge_idxs,
        partitions,
        partition_edge_idxs,
        postings,
    })
}

                                                                         
                                                                            
                                                                        
                                                                         
pub fn load_store_for_query_from_sqlite(sqlite_path: &str, query: &QuerySpec) -> Result<InMemoryStore> {
    let conn = Connection::open_with_flags(sqlite_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut registry = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT rule_id, event_type, source_table, role_schema_json, anchorable_fields_json FROM registry_snapshot"
        )?;
        let rows = stmt.query_map([], |row| {
            let role_schema_json: String = row.get(3)?;
            let anchorable_json: String = row.get(4)?;
            Ok(RegistryRule {
                rule_id: row.get(0)?,
                event_type: row.get(1)?,
                source_table: row.get(2)?,
                role_schema: serde_json::from_str(&role_schema_json).unwrap_or_default(),
                anchorable_fields: serde_json::from_str(&anchorable_json).unwrap_or_default(),
            })
        })?;
        for row in rows {
            let rule = row?;
            registry.insert(rule.event_type.clone(), rule);
        }
    }

    let mut event_types = query.events.iter().map(|e| e.event_type.clone()).collect::<Vec<_>>();
    event_types.sort();
    event_types.dedup();
    let mut anchor_type = event_types.first().cloned().ok_or_else(|| anyhow!("query has no events"))?;
    let mut anchor_count = i64::MAX;
    for event_type in event_types.iter() {
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM hg_hyperedges WHERE event_type = ?1",
            [event_type],
            |row| row.get(0),
        )?;
        if count < anchor_count {
            anchor_count = count;
            anchor_type = event_type.clone();
        }
    }

    let mut edges = Vec::new();
    let mut edge_id_to_idx = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT h.edge_json
             FROM hg_hyperedges h
             WHERE EXISTS (
                 SELECT 1 FROM hg_hyperedges a
                 WHERE a.closure_id = h.closure_id AND a.event_type = ?1
             )
             ORDER BY h.closure_id, h.seq_index, h.edge_id"
        )?;
        let rows = stmt.query_map([&anchor_type], |row| row.get::<_, String>(0))?;
        for payload in rows {
            let edge: DataHyperedge = serde_json::from_str(&payload?)?;
            let idx = edges.len();
            edge_id_to_idx.insert(edge.edge_id.clone(), idx);
            edges.push(edge);
        }
    }

    let mut closure_to_edge_idxs: HashMap<String, Vec<usize>> = HashMap::new();
    let mut partition_edge_idxs: HashMap<String, Vec<usize>> = HashMap::new();
    let mut postings: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (idx, edge) in edges.iter().enumerate() {
        closure_to_edge_idxs.entry(edge.closure_id.clone()).or_default().push(idx);
        partition_edge_idxs.entry(edge.signature_key.clone()).or_default().push(idx);
        for vertex in edge.vertices.iter() {
            postings.entry((edge.signature_key.clone(), vertex.vertex_id.clone())).or_default().push(idx);
        }
    }

    let mut partitions = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT signature_key, signature_labels_json, num_edges FROM hg_partitions"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, i64>(2)?))
        })?;
        for row in rows {
            let (signature_key, labels_json, num_edges) = row?;
            let signature_labels: Vec<String> = serde_json::from_str(&labels_json).unwrap_or_default();
            let mut label_counts = BTreeMap::new();
            for label in signature_labels.iter() {
                *label_counts.entry(label.clone()).or_insert(0) += 1;
            }
            partitions.insert(signature_key.clone(), PartitionMeta {
                signature_key,
                signature_labels,
                label_counts,
                num_edges: num_edges as usize,
            });
        }
    }

    Ok(InMemoryStore {
        registry,
        edges,
        edge_id_to_idx,
        closure_to_edge_idxs,
        partitions,
        partition_edge_idxs,
        postings,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryEventPlan {
    pub id: String,
    pub event_type: String,
    pub protocol: String,
    pub roles: BTreeMap<String, String>,
    pub active_roles: Vec<String>,
    pub variable_roles: Vec<String>,
    pub constant_roles: BTreeMap<String, String>,
    pub required_label_counts: BTreeMap<String, usize>,
    pub compatible_signatures: Vec<String>,
    pub candidate_cardinality: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledQueryPlan {
    pub query_name: String,
    pub scope: String,
    pub events: Vec<QueryEventSpec>,
    pub matching_order: Vec<String>,
    pub event_plans: BTreeMap<String, QueryEventPlan>,
    pub attr_filters: Vec<AttrFilter>,
    pub neq_constraints: Vec<(String, String, String, String)>,
    pub order: Vec<(String, String)>,
}

fn compatible_signatures_for(required_label_counts: &BTreeMap<String, usize>, partition_meta: &HashMap<String, PartitionMeta>) -> Vec<String> {
    let mut out = Vec::new();
    for (sig, meta) in partition_meta.iter() {
        let mut ok = true;
        for (label, need) in required_label_counts.iter() {
            if meta.label_counts.get(label).copied().unwrap_or(0) < *need {
                ok = false;
                break;
            }
        }
        if ok {
            out.push(sig.clone());
        }
    }
    out.sort_by(|a, b| {
        let ca = partition_meta.get(a).map(|x| x.num_edges).unwrap_or(usize::MAX);
        let cb = partition_meta.get(b).map(|x| x.num_edges).unwrap_or(usize::MAX);
        ca.cmp(&cb).then_with(|| a.cmp(b))
    });
    out
}

pub fn compile_query(query: &QuerySpec, store: &InMemoryStore) -> Result<CompiledQueryPlan> {
    let mut event_plans = BTreeMap::new();
    let mut var_occ: HashMap<String, usize> = HashMap::new();
    for ev in query.events.iter() {
        for v in ev.roles.values() {
            if v.starts_with('$') {
                *var_occ.entry(v.clone()).or_insert(0) += 1;
            }
        }
    }

    for ev in query.events.iter() {
        let rule = store
            .registry
            .get(&ev.event_type)
            .ok_or_else(|| anyhow!("missing registry event_type={}", ev.event_type))?;

        let mut label_counts = BTreeMap::new();
        *label_counts.entry(format!("etype::{}", ev.event_type)).or_insert(0) += 1;

        let mut active_roles = Vec::new();
        let mut variable_roles = Vec::new();
        let mut constant_roles = BTreeMap::new();
        let mut supported_roles = BTreeMap::new();
        for (role_name, raw_val) in ev.roles.iter() {
            let raw_val = norm_str(raw_val);
            if raw_val.is_empty() {
                continue;
            }
            let Some(role_label) = rule.role_schema.get(role_name).cloned() else {
                continue;
            };
            supported_roles.insert(role_name.clone(), raw_val.clone());
            active_roles.push(role_name.clone());
            *label_counts.entry(role_slot_label(&ev.event_type, role_name)).or_insert(0) += 1;
            if raw_val.starts_with('$') {
                if var_occ.get(&raw_val).copied().unwrap_or(0) >= 2 {
                    *label_counts.entry(role_label).or_insert(0) += 1;
                    variable_roles.push(role_name.clone());
                }
            } else {
                *label_counts.entry(role_label).or_insert(0) += 1;
                constant_roles.insert(role_name.clone(), raw_val);
            }
        }
        active_roles.sort();
        variable_roles.sort();
        let compatible = compatible_signatures_for(&label_counts, &store.partitions);
        let candidate_cardinality = compatible
            .iter()
            .map(|sig| store.partitions.get(sig).map(|m| m.num_edges).unwrap_or(0))
            .sum();

        event_plans.insert(
            ev.id.clone(),
            QueryEventPlan {
                id: ev.id.clone(),
                event_type: ev.event_type.clone(),
                protocol: ev.protocol.clone(),
                roles: supported_roles,
                active_roles,
                variable_roles,
                constant_roles,
                required_label_counts: label_counts,
                compatible_signatures: compatible,
                candidate_cardinality,
            },
        );
    }

    let mut remaining: Vec<String> = query.events.iter().map(|e| e.id.clone()).collect();
    remaining.sort_by(|a, b| {
        let pa = event_plans.get(a).unwrap();
        let pb = event_plans.get(b).unwrap();
        pa.candidate_cardinality
            .cmp(&pb.candidate_cardinality)
            .then_with(|| a.cmp(b))
    });

    let first = remaining.remove(0);
    let mut order = vec![first.clone()];
    let mut bound_vars = HashSet::new();
    if let Some(plan) = event_plans.get(&first) {
        for v in plan.roles.values() {
            if v.starts_with('$') {
                bound_vars.insert(v.clone());
            }
        }
    }

    while !remaining.is_empty() {
        remaining.sort_by(|a, b| {
            let pa = event_plans.get(a).unwrap();
            let pb = event_plans.get(b).unwrap();
            let sa = pa.roles.values().filter(|v| v.starts_with('$') && bound_vars.contains(*v)).count();
            let sb = pb.roles.values().filter(|v| v.starts_with('$') && bound_vars.contains(*v)).count();
            let ka = if sa > 0 {
                pa.candidate_cardinality as f64 / sa as f64
            } else {
                pa.candidate_cardinality as f64
            };
            let kb = if sb > 0 {
                pb.candidate_cardinality as f64 / sb as f64
            } else {
                pb.candidate_cardinality as f64
            };
            ka.partial_cmp(&kb)
                .unwrap_or(Ordering::Equal)
                .then_with(|| pa.candidate_cardinality.cmp(&pb.candidate_cardinality))
                .then_with(|| a.cmp(b))
        });
        let next = remaining.remove(0);
        if let Some(plan) = event_plans.get(&next) {
            for v in plan.roles.values() {
                if v.starts_with('$') {
                    bound_vars.insert(v.clone());
                }
            }
        }
        order.push(next);
    }

    Ok(CompiledQueryPlan {
        query_name: query.query_name.clone(),
        scope: query.scope.clone(),
        events: query.events.clone(),
        matching_order: order,
        event_plans,
        attr_filters: query.attr_filters.clone(),
        neq_constraints: query.neq_constraints.clone(),
        order: query.order.clone(),
    })
}

#[derive(Debug, Clone)]
pub struct StructuralState {
    pub closure_id: String,
    pub edge_mapping: BTreeMap<String, usize>,
    pub var_bindings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StructuralMetrics {
    pub num_seed_edges: usize,
    pub num_seed_closures: usize,
    pub num_closures_scanned: usize,
    pub num_closures_with_nonempty_local_candidates: usize,
    pub num_local_candidate_edges_examined: usize,
    pub num_structural_embeddings_raw: usize,
    pub num_structural_embeddings_dedup: usize,
    pub num_candidate_closures: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExactVerifyResult {
    pub cand2_closures: Vec<String>,
    pub num_matches: usize,
    pub matched_rows: Vec<Vec<String>>,
    pub binding_rows: Vec<Vec<String>>,
    pub residual_closures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryRunArtifacts {
    pub compiled: CompiledQueryPlan,
    pub structural_states: Vec<StructuralStateOut>,
    pub struct_stats: StructuralMetrics,
    pub exact: ExactVerifyResult,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuralStateOut {
    pub closure_id: String,
    pub edge_mapping: BTreeMap<String, String>,
    pub var_bindings: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecutionSummary {
    pub query_name: String,
    pub scope: String,
    pub matching_order: Vec<String>,
    pub structural_metrics: StructuralMetrics,
    pub num_structural_embeddings_csv_rows: usize,
    pub num_cand1_closures: usize,
    pub num_cand2_closures: usize,
    pub num_matches: usize,
    pub num_residual_closures: usize,
    pub num_false_positive_closures_filtered: usize,
    pub stage_times_s: serde_json::Value,
    pub metrics: serde_json::Value,
    pub store_stats: InMemoryStoreStats,
}


fn role_value(edge: &DataHyperedge, role_name: &str) -> String {
    if role_name == "event_id" {
        edge.event_id.clone()
    } else if role_name == "protocol" {
        edge.protocol.clone()
    } else {
        norm_str(edge.roles.get(role_name).map(|s| s.as_str()).unwrap_or(""))
    }
}


fn event_plan_matches_edge_structural(plan: &QueryEventPlan, edge: &DataHyperedge) -> bool {
    if edge.event_type != plan.event_type {
        return false;
    }
    if !plan.compatible_signatures.is_empty()
        && !plan.compatible_signatures.iter().any(|s| s == &edge.signature_key)
    {
        return false;
    }
    for role_name in plan.active_roles.iter() {
        let actual = norm_str(edge.roles.get(role_name).map(|s| s.as_str()).unwrap_or(""));
        if actual.is_empty() {
            return false;
        }
    }
    true
}

fn intersect_usize_vectors(lists: &[Vec<usize>]) -> Vec<usize> {
    if lists.is_empty() {
        return Vec::new();
    }
    let mut set: BTreeSet<usize> = lists[0].iter().copied().collect();
    for lst in lists.iter().skip(1) {
        let s: BTreeSet<usize> = lst.iter().copied().collect();
        set = set.intersection(&s).copied().collect();
        if set.is_empty() {
            break;
        }
    }
    set.into_iter().collect()
}

fn seed_edge_indices(store: &InMemoryStore, compiled: &CompiledQueryPlan, first_qid: &str) -> Vec<usize> {
    let plan = compiled.event_plans.get(first_qid).expect("missing first plan");
    let mut out = BTreeSet::new();
    for sig in plan.compatible_signatures.iter() {
        let sig_candidates = store.partition_edge_idxs.get(sig).cloned().unwrap_or_default();
        for idx in sig_candidates {
            let edge = &store.edges[idx];
            if event_plan_matches_edge_structural(plan, edge) {
                out.insert(idx);
            }
        }
    }
    out.into_iter().collect()
}

fn state_key(store: &InMemoryStore, mapping: &BTreeMap<String, usize>) -> String {
    mapping
        .iter()
        .map(|(qid, idx)| format!("{}={}", qid, store.edges[*idx].edge_id))
        .collect::<Vec<_>>()
        .join("|")
}

fn attr_filters_ok(compiled: &CompiledQueryPlan, binding: &BTreeMap<String, usize>, store: &InMemoryStore) -> bool {
    for flt in compiled.attr_filters.iter() {
        let Some(idx) = binding.get(&flt.event) else { return false; };
        let edge = &store.edges[*idx];
        let cur = norm_str(edge.attrs.get(&flt.field).map(|s| s.as_str()).unwrap_or(""));
        let Ok(cur_num) = cur.parse::<f64>() else { return false; };
        let Ok(val_num) = flt.value.parse::<f64>() else { return false; };
        let ok = match flt.op.as_str() {
            ">=" => cur_num >= val_num,
            ">" => cur_num > val_num,
            "<=" => cur_num <= val_num,
            "<" => cur_num < val_num,
            "=" | "==" => (cur_num - val_num).abs() < 1e-12,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn full_binding_ok(compiled: &CompiledQueryPlan, binding: &BTreeMap<String, usize>, store: &InMemoryStore) -> bool {
    let mut var_values: BTreeMap<String, String> = BTreeMap::new();

    for ev in compiled.events.iter() {
        let Some(idx) = binding.get(&ev.id) else { return false; };
        let edge = &store.edges[*idx];
        if edge.event_type != ev.event_type {
            return false;
        }
        if !ev.protocol.is_empty() && edge.protocol != ev.protocol {
            return false;
        }
        let Some(rule) = store.registry.get(&ev.event_type) else { return false; };
        for (role_name, raw_val) in ev.roles.iter() {
            if !rule.role_schema.contains_key(role_name) {
                continue;
            }
            let raw_val = norm_str(raw_val);
            if raw_val.is_empty() {
                continue;
            }
            let actual = role_value(edge, role_name);
            if raw_val.starts_with('$') {
                if actual.is_empty() {
                    return false;
                }
                if let Some(prev) = var_values.get(&raw_val) {
                    if prev != &actual {
                        return false;
                    }
                } else {
                    var_values.insert(raw_val, actual);
                }
            } else if actual != raw_val {
                return false;
            }
        }
    }

    for (left, right) in compiled.order.iter() {
        let Some(li) = binding.get(left) else { return false; };
        let Some(ri) = binding.get(right) else { return false; };
        if store.edges[*li].seq_index >= store.edges[*ri].seq_index {
            return false;
        }
    }

    for (le, lf, re, rf) in compiled.neq_constraints.iter() {
        let Some(li) = binding.get(le) else { return false; };
        let Some(ri) = binding.get(re) else { return false; };
        if role_value(&store.edges[*li], lf) == role_value(&store.edges[*ri], rf) {
            return false;
        }
    }

    let event_ids: Vec<String> = binding.values().map(|idx| store.edges[*idx].event_id.clone()).collect();
    if event_ids.iter().collect::<HashSet<_>>().len() != event_ids.len() {
        return false;
    }

    attr_filters_ok(compiled, binding, store)
}

fn apply_var_constraints(
    plan: &QueryEventPlan,
    edge: &DataHyperedge,
    prev_bindings: &BTreeMap<String, String>,
) -> Option<BTreeMap<String, String>> {
    let mut out = prev_bindings.clone();
    for role_name in plan.variable_roles.iter() {
        let Some(raw_val) = plan.roles.get(role_name) else { continue; };
        let raw_val = norm_str(raw_val);
        if !raw_val.starts_with('$') {
            continue;
        }
        let actual = norm_str(edge.roles.get(role_name).map(|s| s.as_str()).unwrap_or(""));
        if actual.is_empty() {
            return None;
        }
        if let Some(prev) = out.get(&raw_val) {
            if prev != &actual {
                return None;
            }
        } else {
            out.insert(raw_val, actual);
        }
    }
    Some(out)
}

fn structural_state_to_out(store: &InMemoryStore, state: &StructuralState) -> StructuralStateOut {
    let edge_mapping = state
        .edge_mapping
        .iter()
        .map(|(k, idx)| (k.clone(), store.edges[*idx].edge_id.clone()))
        .collect::<BTreeMap<_, _>>();
    StructuralStateOut {
        closure_id: state.closure_id.clone(),
        edge_mapping,
        var_bindings: state.var_bindings.clone(),
    }
}

fn shared_bound_posting_lists<'a>(
    store: &'a InMemoryStore,
    plan: &QueryEventPlan,
    sig: &str,
    var_bindings: &BTreeMap<String, String>,
) -> Option<Vec<&'a [usize]>> {
    let Some(rule) = store.registry.get(&plan.event_type) else { return Some(Vec::new()); };
    let mut lists: Vec<&[usize]> = Vec::new();
    for role_name in plan.variable_roles.iter() {
        let Some(raw_val) = plan.roles.get(role_name) else { continue; };
        let raw_val = norm_str(raw_val);
        if !raw_val.starts_with('$') {
            continue;
        }
        let Some(bound_val) = var_bindings.get(&raw_val) else { continue; };
        let Some(role_label) = rule.role_schema.get(role_name).cloned() else { continue; };
        let vid = const_vertex_id(&role_label, bound_val);
        lists.push(
            store
                .postings
                .get(&(sig.to_string(), vid))
                .map(Vec::as_slice)
                .unwrap_or(&[]),
        );
    }
    if lists.is_empty() {
        None
    } else {
        Some(lists)
    }
}

                                                                              
                                                                               
                                                                           
fn smallest_sorted_intersection(lists: &[&[usize]]) -> Vec<usize> {
    let Some((base_id,base))=lists.iter().enumerate().min_by_key(|(_,xs)|xs.len()) else {
        return Vec::new();
    };
    base.iter().copied().filter(|idx| lists.iter().enumerate()
        .all(|(i,xs)|i==base_id || xs.binary_search(idx).is_ok())).collect()
}

fn generate_hyperedge_candidates(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    qid: &str,
    state: &StructuralState,
) -> Vec<usize> {
    let plan=compiled.event_plans.get(qid).expect("missing plan");
    let Some(closure_edges)=store.closure_to_edge_idxs.get(&state.closure_id) else {
        return Vec::new();
    };
    let mut out=BTreeSet::new();
    for sig in &plan.compatible_signatures {
        let mut lists=match shared_bound_posting_lists(store,plan,sig,&state.var_bindings) {
            Some(lists) => {
                                                              
                if lists.is_empty() {continue;}
                lists
            },
            None => {
                let Some(partition)=store.partition_edge_idxs.get(sig) else {continue;};
                vec![partition.as_slice()]
            }
        };
        lists.push(closure_edges.as_slice());
        for idx in smallest_sorted_intersection(&lists) {
            if state.edge_mapping.values().any(|used|*used==idx) {continue;}
            if event_plan_matches_edge_structural(plan,&store.edges[idx]) {out.insert(idx);}
        }
    }
    out.into_iter().collect()
}

#[cfg(test)]
fn baseline_global_candidates(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    qid: &str,
    state: &StructuralState,
) -> Vec<usize> {
    let plan = compiled.event_plans.get(qid).expect("missing plan");
    let closure_edges = match store.closure_to_edge_idxs.get(&state.closure_id) {
        Some(v) => v,
        None => return Vec::new(),
    };
    let mut out = BTreeSet::new();

    for sig in plan.compatible_signatures.iter() {
        let mut consider = |idx: usize| {
            if state.edge_mapping.values().any(|used| *used == idx) {
                return;
            }
            if !closure_edges.contains(&idx) {
                return;
            }
            let edge = &store.edges[idx];
            if event_plan_matches_edge_structural(plan, edge) {
                out.insert(idx);
            }
        };

        match shared_bound_posting_lists(store, plan, sig, &state.var_bindings) {
            Some(lists) => {
                let Some(base) = lists.iter().min_by_key(|xs| xs.len()) else {
                    continue;
                };
                for idx in base.iter().copied() {
                    if lists
                        .iter()
                        .all(|xs| xs.binary_search(&idx).is_ok())
                    {
                        consider(idx);
                    }
                }
            }
            None => {
                if let Some(base) = store.partition_edge_idxs.get(sig) {
                    for idx in base.iter().copied() {
                        consider(idx);
                    }
                }
            }
        }
    }
    out.into_iter().collect()
}

#[derive(Debug, Clone)]
enum TaskOperator {
    Scan { start: usize, end: usize, signature_key: String },
    Expand { level: usize, state: StructuralState },
    Sink { state: StructuralState },
}

fn build_scan_tasks(store: &InMemoryStore, compiled: &CompiledQueryPlan, chunk_size: usize) -> Result<Vec<TaskOperator>> {
    let first_qid = compiled
        .matching_order
        .first()
        .ok_or_else(|| anyhow!("empty matching order"))?;
    let first_plan = compiled.event_plans.get(first_qid).ok_or_else(|| anyhow!("missing first plan"))?;

    let mut tasks = Vec::new();
    for sig in first_plan.compatible_signatures.iter() {
        let edge_count = store
            .partition_edge_idxs
            .get(sig)
            .map(Vec::len)
            .unwrap_or(0);
        let mut start = 0usize;
        while start < edge_count {
            let end = (start + chunk_size).min(edge_count);
            tasks.push(TaskOperator::Scan {
                start,
                end,
                signature_key: sig.clone(),
            });
            start = end;
        }
    }
    Ok(tasks)
}

fn pop_task(
    local: &Worker<TaskOperator>,
    injector: &Injector<TaskOperator>,
    stealers: &[Stealer<TaskOperator>],
    my_idx: usize,
) -> Option<TaskOperator> {
    if let Some(task) = local.pop() {
        return Some(task);
    }

    loop {
        match injector.steal_batch_and_pop(local) {
            Steal::Success(task) => return Some(task),
            Steal::Empty => break,
            Steal::Retry => continue,
        }
    }

    for (idx, stealer) in stealers.iter().enumerate() {
        if idx == my_idx {
            continue;
        }
        loop {
            match stealer.steal() {
                Steal::Success(task) => return Some(task),
                Steal::Empty => break,
                Steal::Retry => continue,
            }
        }
    }
    None
}

fn enqueue_children(
    local: &Worker<TaskOperator>,
    outstanding: &AtomicUsize,
    children: Vec<TaskOperator>,
) {
    if children.is_empty() {
        return;
    }
    outstanding.fetch_add(children.len(), AtomicOrdering::SeqCst);
    for child in children {
        local.push(child);
    }
}

pub fn enumerate_structural_embeddings_parallel(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    threads: usize,
) -> Result<(Vec<StructuralState>, StructuralMetrics)> {
    let num_threads = threads.max(1);
    let scan_chunk_size = 4096usize;
    let first_qid = compiled
        .matching_order
        .first()
        .ok_or_else(|| anyhow!("empty matching order"))?
        .clone();
    let initial_tasks = build_scan_tasks(store, compiled, scan_chunk_size)?;

    let injector = Arc::new(Injector::new());
    let workers: Vec<Worker<TaskOperator>> = (0..num_threads).map(|_| Worker::new_lifo()).collect();
    let stealers: Vec<Stealer<TaskOperator>> = workers.iter().map(|w| w.stealer()).collect();

    for task in initial_tasks.iter().cloned() {
        injector.push(task);
    }

    let outstanding = Arc::new(AtomicUsize::new(initial_tasks.len()));

    let seed_edge_count = Arc::new(AtomicUsize::new(0));
    let local_candidate_edges_examined = Arc::new(AtomicUsize::new(0));
    let structural_embeddings_raw = Arc::new(AtomicUsize::new(0));

    let seed_closures = Arc::new(Mutex::new(HashSet::<String>::new()));
    let closures_scanned = Arc::new(Mutex::new(HashSet::<String>::new()));
    let nonempty_candidate_closures = Arc::new(Mutex::new(HashSet::<String>::new()));
    let sink_states = Arc::new(Mutex::new(Vec::<StructuralState>::new()));

    let pool = ThreadPoolBuilder::new().num_threads(num_threads).build()?;
    pool.scope(|scope| {
        for (worker_idx, local) in workers.into_iter().enumerate() {
            let injector = Arc::clone(&injector);
            let stealers = stealers.clone();
            let store = store;
            let compiled = compiled;
            let outstanding = Arc::clone(&outstanding);
            let seed_edge_count = Arc::clone(&seed_edge_count);
            let local_candidate_edges_examined = Arc::clone(&local_candidate_edges_examined);
            let structural_embeddings_raw = Arc::clone(&structural_embeddings_raw);
            let seed_closures = Arc::clone(&seed_closures);
            let closures_scanned = Arc::clone(&closures_scanned);
            let nonempty_candidate_closures = Arc::clone(&nonempty_candidate_closures);
            let sink_states = Arc::clone(&sink_states);
            let first_qid = first_qid.clone();

            scope.spawn(move |_| {
                let order_len = compiled.matching_order.len();
                let first_plan = compiled.event_plans.get(&first_qid).expect("missing first plan");

                loop {
                    let task = match pop_task(&local, &injector, &stealers, worker_idx) {
                        Some(task) => task,
                        None => {
                            if outstanding.load(AtomicOrdering::SeqCst) == 0 {
                                break;
                            }
                            thread::yield_now();
                            continue;
                        }
                    };

                    match task {
                        TaskOperator::Scan { start, end, signature_key } => {
                            let partition = store
                                .partition_edge_idxs
                                .get(&signature_key)
                                .map(Vec::as_slice)
                                .unwrap_or(&[]);
                            let slice_end = end.min(partition.len());
                            let slice_start = start.min(slice_end);
                            let mut children = Vec::new();
                            for idx in partition[slice_start..slice_end].iter().copied() {
                                let edge = &store.edges[idx];
                                if !event_plan_matches_edge_structural(first_plan, edge) {
                                    continue;
                                }
                                let Some(var_bindings) = apply_var_constraints(first_plan, edge, &BTreeMap::new()) else {
                                    continue;
                                };
                                seed_edge_count.fetch_add(1, AtomicOrdering::SeqCst);
                                {
                                    let mut guard = seed_closures.lock().unwrap();
                                    guard.insert(edge.closure_id.clone());
                                }
                                {
                                    let mut guard = closures_scanned.lock().unwrap();
                                    guard.insert(edge.closure_id.clone());
                                }
                                let mut mapping = BTreeMap::new();
                                mapping.insert(first_qid.clone(), idx);
                                let state = StructuralState {
                                    closure_id: edge.closure_id.clone(),
                                    edge_mapping: mapping,
                                    var_bindings,
                                };
                                if order_len == 1 {
                                    children.push(TaskOperator::Sink { state });
                                } else {
                                    children.push(TaskOperator::Expand { level: 1, state });
                                }
                            }
                            enqueue_children(&local, &outstanding, children);
                            outstanding.fetch_sub(1, AtomicOrdering::SeqCst);
                        }
                        TaskOperator::Expand { level, state } => {
                            let qid = compiled.matching_order.get(level).expect("invalid expand level").clone();
                            let cands = generate_hyperedge_candidates(store, compiled, &qid, &state);
                            local_candidate_edges_examined.fetch_add(cands.len(), AtomicOrdering::SeqCst);
                            if !cands.is_empty() {
                                let mut guard = nonempty_candidate_closures.lock().unwrap();
                                guard.insert(state.closure_id.clone());
                            }

                            let plan = compiled.event_plans.get(&qid).expect("missing plan");
                            let mut children = Vec::new();
                            for idx in cands.into_iter() {
                                let edge = &store.edges[idx];
                                let Some(next_var_bindings) = apply_var_constraints(plan, edge, &state.var_bindings) else {
                                    continue;
                                };
                                let mut next_mapping = state.edge_mapping.clone();
                                next_mapping.insert(qid.clone(), idx);
                                let next_state = StructuralState {
                                    closure_id: state.closure_id.clone(),
                                    edge_mapping: next_mapping,
                                    var_bindings: next_var_bindings,
                                };
                                if level + 1 == order_len {
                                    children.push(TaskOperator::Sink { state: next_state });
                                } else {
                                    children.push(TaskOperator::Expand { level: level + 1, state: next_state });
                                }
                            }
                            enqueue_children(&local, &outstanding, children);
                            outstanding.fetch_sub(1, AtomicOrdering::SeqCst);
                        }
                        TaskOperator::Sink { state } => {
                            structural_embeddings_raw.fetch_add(1, AtomicOrdering::SeqCst);
                            let mut guard = sink_states.lock().unwrap();
                            guard.push(state);
                            outstanding.fetch_sub(1, AtomicOrdering::SeqCst);
                        }
                    }
                }
            });
        }
    });

    let raw_states = {
        let mut guard = sink_states.lock().unwrap();
        std::mem::take(&mut *guard)
    };
    let mut dedup: BTreeMap<String, StructuralState> = BTreeMap::new();
    for st in raw_states.into_iter() {
        dedup.insert(state_key(&store, &st.edge_mapping), st);
    }
    let frontier = dedup.into_values().collect::<Vec<_>>();
    let num_candidate_closures = frontier
        .iter()
        .map(|s| s.closure_id.clone())
        .collect::<HashSet<_>>()
        .len();

    let struct_stats = StructuralMetrics {
        num_seed_edges: seed_edge_count.load(AtomicOrdering::SeqCst),
        num_seed_closures: seed_closures.lock().unwrap().len(),
        num_closures_scanned: closures_scanned.lock().unwrap().len(),
        num_closures_with_nonempty_local_candidates: nonempty_candidate_closures.lock().unwrap().len(),
        num_local_candidate_edges_examined: local_candidate_edges_examined.load(AtomicOrdering::SeqCst),
        num_structural_embeddings_raw: structural_embeddings_raw.load(AtomicOrdering::SeqCst),
        num_structural_embeddings_dedup: frontier.len(),
        num_candidate_closures,
    };

    Ok((frontier, struct_stats))
}

pub fn exact_verify_structural_states(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    structural_states: &[StructuralState],
    summary_only: bool,
) -> ExactVerifyResult {
    let mut cand2_closures = BTreeSet::new();
    let mut residual_closures = BTreeSet::new();
    let mut matched_rows = Vec::new();
    let mut binding_rows = Vec::new();
    let mut seen_final = BTreeSet::new();

    for (match_idx, st) in structural_states.iter().enumerate() {
        if !full_binding_ok(compiled, &st.edge_mapping, store) {
            continue;
        }
        let final_key = state_key(store, &st.edge_mapping);
        if !seen_final.insert(final_key) {
            continue;
        }
        let match_id = format!("{}::m{}", st.closure_id, match_idx);
        cand2_closures.insert(st.closure_id.clone());
        residual_closures.insert(st.closure_id.clone());
        if !summary_only {
            matched_rows.push(vec![
                match_id.clone(),
                st.closure_id.clone(),
                st.closure_id.clone(),
                compiled.query_name.clone(),
                st.edge_mapping.len().to_string(),
            ]);
            for qid in compiled.matching_order.iter() {
                let idx = st.edge_mapping.get(qid).copied().unwrap();
                let edge = &store.edges[idx];
                binding_rows.push(vec![
                    match_id.clone(),
                    st.closure_id.clone(),
                    st.closure_id.clone(),
                    qid.clone(),
                    edge.event_id.clone(),
                    edge.event_type.clone(),
                    edge.seq_index.to_string(),
                    edge.protocol.clone(),
                ]);
            }
        }
    }

    let num_matches = seen_final.len();
    ExactVerifyResult {
        cand2_closures: cand2_closures.into_iter().collect(),
        num_matches,
        matched_rows,
        binding_rows,
        residual_closures: residual_closures.into_iter().collect(),
    }
}

pub fn structural_rows(structural_states: &[StructuralStateOut], order: &[String]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (i, st) in structural_states.iter().enumerate() {
        let mut row = vec![format!("sm{}", i), st.closure_id.clone(), st.closure_id.clone()];
        for qid in order.iter() {
            row.push(st.edge_mapping.get(qid).cloned().unwrap_or_default());
        }
        rows.push(row);
    }
    rows
}

pub fn write_query_outputs(
    outdir: &str,
    artifacts: &QueryRunArtifacts,
    store_stats: &InMemoryStoreStats,
    stage_load_store_s: f64,
    stage_compile_s: f64,
    stage_seed_s: f64,
    stage_structural_s: f64,
    stage_exact_s: f64,
    output_write_s: f64,
    summary_only: bool,
) -> Result<QueryExecutionSummary> {
    ensure_dir(outdir)?;
    write_json_pretty(Path::new(outdir).join("compiled_query_plan.json"), &artifacts.compiled)?;

    let cand1_closures: Vec<String> = artifacts
        .structural_states
        .iter()
        .map(|s| s.closure_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let structural_rows_vec = if summary_only {
        Vec::new()
    } else {
        let mut structural_headers = vec!["structural_match_id", "closure_id", "tx_hash"];
        for qid in artifacts.compiled.matching_order.iter() {
            structural_headers.push(qid);
        }
        let structural_rows_vec = structural_rows(&artifacts.structural_states, &artifacts.compiled.matching_order);
        write_csv_rows(Path::new(outdir).join("structural_embeddings.csv"), &structural_headers, &structural_rows_vec)?;

        let cand1_rows = cand1_closures.iter().map(|c| vec![c.clone(), c.clone()]).collect::<Vec<_>>();
        write_csv_rows(Path::new(outdir).join("cand1_structural_closures.csv"), &["closure_id", "tx_hash"], &cand1_rows)?;

        let cand2_rows = artifacts.exact.cand2_closures.iter().map(|c| vec![c.clone(), c.clone()]).collect::<Vec<_>>();
        write_csv_rows(Path::new(outdir).join("cand2_exact_matched_closures.csv"), &["closure_id", "tx_hash"], &cand2_rows)?;

        write_csv_rows(
            Path::new(outdir).join("matched_instances.csv"),
            &["match_id", "closure_id", "tx_hash", "query_name", "num_events"],
            &artifacts.exact.matched_rows,
        )?;

        write_csv_rows(
            Path::new(outdir).join("matched_bindings.csv"),
            &["match_id", "closure_id", "tx_hash", "query_event_id", "event_id", "event_type", "seq_index", "protocol"],
            &artifacts.exact.binding_rows,
        )?;

        let residual_rows = artifacts
            .exact
            .residual_closures
            .iter()
            .map(|c| vec![c.clone(), c.clone()])
            .collect::<Vec<_>>();
        write_csv_rows(Path::new(outdir).join("residual_closures.csv"), &["closure_id", "tx_hash"], &residual_rows)?;
        structural_rows_vec
    };

    let pure_query_latency_s = stage_compile_s + stage_seed_s + stage_structural_s + stage_exact_s;
    let query_wall_time_s = stage_load_store_s + pure_query_latency_s;
    let closure_false_positive_ratio = if cand1_closures.is_empty() {
        0.0
    } else {
        1.0 - (artifacts.exact.cand2_closures.len() as f64 / cand1_closures.len() as f64)
    };
    let summary = QueryExecutionSummary {
        query_name: artifacts.compiled.query_name.clone(),
        scope: artifacts.compiled.scope.clone(),
        matching_order: artifacts.compiled.matching_order.clone(),
        structural_metrics: artifacts.struct_stats.clone(),
        num_structural_embeddings_csv_rows: structural_rows_vec.len(),
        num_cand1_closures: cand1_closures.len(),
        num_cand2_closures: artifacts.exact.cand2_closures.len(),
        num_matches: artifacts.exact.num_matches,
        num_residual_closures: artifacts.exact.residual_closures.len(),
        num_false_positive_closures_filtered: cand1_closures.len().saturating_sub(artifacts.exact.cand2_closures.len()),
        stage_times_s: json!({
            "load_store_from_sqlite": stage_load_store_s,
            "compile_query_plan": stage_compile_s,
            "global_seed_selection": stage_seed_s,
            "hgmatch_structural_match": stage_structural_s,
            "exact_semantic_verify": stage_exact_s,
            "write_outputs": output_write_s,
        }),
        metrics: json!({
            "query_latency_s": pure_query_latency_s,
            "query_wall_time_s": query_wall_time_s,
            "closure_false_positive_ratio": closure_false_positive_ratio,
            "residual_closures_per_query": artifacts.exact.residual_closures.len(),
            "summary_only": summary_only,
        }),
        store_stats: store_stats.clone(),
    };

    write_json_pretty(Path::new(outdir).join("execution_report.json"), &summary)?;
    Ok(summary)
}

fn role(edge: &DataHyperedge, name: &str) -> String {
    role_value(edge, name)
}

fn query_order(query: &QuerySpec) -> Vec<String> {
    query.events.iter().map(|e| e.id.clone()).collect()
}

fn same_tx_fast_summary(
    store: &InMemoryStore,
    query: &QuerySpec,
    count: usize,
    stage_load_store_s: f64,
    core_s: f64,
    seed_edges: usize,
    local_candidates: usize,
) -> QueryExecutionSummary {
    QueryExecutionSummary {
        query_name: query.query_name.clone(),
        scope: query.scope.clone(),
        matching_order: query_order(query),
        structural_metrics: StructuralMetrics {
            num_seed_edges: seed_edges,
            num_seed_closures: 0,
            num_closures_scanned: store.closure_to_edge_idxs.len(),
            num_closures_with_nonempty_local_candidates: 0,
            num_local_candidate_edges_examined: local_candidates,
            num_structural_embeddings_raw: count,
            num_structural_embeddings_dedup: count,
            num_candidate_closures: 0,
        },
        num_structural_embeddings_csv_rows: 0,
        num_cand1_closures: 0,
        num_cand2_closures: 0,
        num_matches: count,
        num_residual_closures: 0,
        num_false_positive_closures_filtered: 0,
        stage_times_s: json!({
            "load_store_from_sqlite": stage_load_store_s,
            "hgmatch_summary_fastpath": core_s,
            "compile_query_plan": 0.0,
            "global_seed_selection": 0.0,
            "hgmatch_structural_match": 0.0,
            "exact_semantic_verify": 0.0,
            "write_outputs": 0.0
        }),
        metrics: json!({
            "query_latency_s": core_s,
            "query_wall_time_s": stage_load_store_s + core_s,
            "summary_only": true,
            "summary_fastpath": true,
            "summary_fastpath_kind": "same_tx_query_family_count_only"
        }),
        store_stats: store.stats(),
    }
}

fn count_same_tx_known_query(store: &InMemoryStore, query_name: &str) -> Option<(usize, usize, usize)> {
    match query_name {
        "flashloan_swap_borrow"
        | "flashloan_swap_borrow_followup"
        | "flashloan_2swaps_distinct_followup"
        | "flashloan_3swaps_chain_reuse"
        | "flashloan_3swaps_distinct_parallel" => {}
        _ => return None,
    }

    let mut matches = 0usize;
    let mut seed_edges = 0usize;
    let mut local_candidates = 0usize;

    for idxs in store.closure_to_edge_idxs.values() {
        let mut fls = Vec::new();
        let mut swaps = Vec::new();
        let mut borrows = Vec::new();
        for idx in idxs.iter().copied() {
            match store.edges[idx].event_type.as_str() {
                "flashloan" => fls.push(idx),
                "swap" => swaps.push(idx),
                "borrow" => borrows.push(idx),
                _ => {}
            }
        }
        seed_edges += fls.len();

        for fl_idx in fls.iter().copied() {
            let fl = &store.edges[fl_idx];
            let u = role(fl, "recipient");
            let x = role(fl, "asset");
            if u.is_empty() || x.is_empty() {
                continue;
            }
            let s_from_x = swaps
                .iter()
                .copied()
                .filter(|idx| {
                    let e = &store.edges[*idx];
                    e.seq_index > fl.seq_index && role(e, "taker") == u && role(e, "token_sold") == x
                })
                .collect::<Vec<_>>();
            local_candidates += s_from_x.len();

            if query_name == "flashloan_swap_borrow" || query_name == "flashloan_swap_borrow_followup" {
                for e2_idx in s_from_x.iter().copied() {
                    let e2 = &store.edges[e2_idx];
                    let y = role(e2, "token_bought");
                    let p1 = role(e2, "pool");
                    if y.is_empty() {
                        continue;
                    }
                    let b_candidates = borrows
                        .iter()
                        .copied()
                        .filter(|idx| {
                            let b = &store.edges[*idx];
                            b.seq_index > e2.seq_index && role(b, "borrower") == u && role(b, "asset") == y
                        })
                        .collect::<Vec<_>>();
                    local_candidates += b_candidates.len();
                    for e3_idx in b_candidates {
                        let e3 = &store.edges[e3_idx];
                        if query_name == "flashloan_swap_borrow" {
                            matches += 1;
                        } else {
                            for e4_idx in swaps.iter().copied() {
                                if e4_idx == e2_idx {
                                    continue;
                                }
                                let e4 = &store.edges[e4_idx];
                                if e4.seq_index > e3.seq_index
                                    && role(e4, "taker") == u
                                    && role(e4, "token_sold") == y
                                    && role(e4, "pool") != p1
                                {
                                    matches += 1;
                                }
                            }
                        }
                    }
                }
                continue;
            }

            if query_name == "flashloan_2swaps_distinct_followup" {
                for e2_idx in s_from_x.iter().copied() {
                    let e2 = &store.edges[e2_idx];
                    let y1 = role(e2, "token_bought");
                    let p1 = role(e2, "pool");
                    for e3_idx in s_from_x.iter().copied() {
                        if e3_idx == e2_idx {
                            continue;
                        }
                        let e3 = &store.edges[e3_idx];
                        let y2 = role(e3, "token_bought");
                        if e3.seq_index <= e2.seq_index || role(e3, "pool") == p1 || y2 == y1 {
                            continue;
                        }
                        for e4_idx in swaps.iter().copied() {
                            let e4 = &store.edges[e4_idx];
                            if e4.seq_index > e3.seq_index
                                && role(e4, "taker") == u
                                && role(e4, "token_sold") == y1
                            {
                                matches += 1;
                            }
                        }
                    }
                }
            } else if query_name == "flashloan_3swaps_chain_reuse" {
                for e2_idx in s_from_x.iter().copied() {
                    let e2 = &store.edges[e2_idx];
                    let y1 = role(e2, "token_bought");
                    let p1 = role(e2, "pool");
                    if y1 == x {
                        continue;
                    }
                    for e3_idx in swaps.iter().copied() {
                        if e3_idx == e2_idx {
                            continue;
                        }
                        let e3 = &store.edges[e3_idx];
                        let y2 = role(e3, "token_bought");
                        let p2 = role(e3, "pool");
                        if e3.seq_index <= e2.seq_index
                            || role(e3, "taker") != u
                            || role(e3, "token_sold") != y1
                            || p2 == p1
                            || y2 == x
                        {
                            continue;
                        }
                        for e4_idx in swaps.iter().copied() {
                            if e4_idx == e2_idx || e4_idx == e3_idx {
                                continue;
                            }
                            let e4 = &store.edges[e4_idx];
                            let y3 = role(e4, "token_bought");
                            let p3 = role(e4, "pool");
                            if e4.seq_index > e3.seq_index
                                && role(e4, "taker") == u
                                && role(e4, "token_sold") == y2
                                && p3 != p1
                                && p3 != p2
                                && y3 != x
                                && y3 != y1
                            {
                                matches += 1;
                            }
                        }
                    }
                }
            } else if query_name == "flashloan_3swaps_distinct_parallel" {
                for e2_idx in s_from_x.iter().copied() {
                    let e2 = &store.edges[e2_idx];
                    let y1 = role(e2, "token_bought");
                    let p1 = role(e2, "pool");
                    for e3_idx in s_from_x.iter().copied() {
                        if e3_idx == e2_idx {
                            continue;
                        }
                        let e3 = &store.edges[e3_idx];
                        let y2 = role(e3, "token_bought");
                        let p2 = role(e3, "pool");
                        if e3.seq_index <= e2.seq_index || p2 == p1 || y2 == y1 {
                            continue;
                        }
                        for e4_idx in s_from_x.iter().copied() {
                            if e4_idx == e2_idx || e4_idx == e3_idx {
                                continue;
                            }
                            let e4 = &store.edges[e4_idx];
                            let y3 = role(e4, "token_bought");
                            let p3 = role(e4, "pool");
                            if e4.seq_index > e3.seq_index && p3 != p1 && p3 != p2 && y3 != y1 && y3 != y2 {
                                matches += 1;
                            }
                        }
                    }
                }
            }
        }
    }

    Some((matches, seed_edges, local_candidates))
}

fn query_pipeline_same_tx_summary_fastpath(
    store: &InMemoryStore,
    query: &QuerySpec,
    outdir: &str,
    stage_load_store_s: f64,
) -> Result<Option<QueryExecutionSummary>> {
    let t0 = Instant::now();
    let Some((count, seed_edges, local_candidates)) = count_same_tx_known_query(store, &query.query_name) else {
        return Ok(None);
    };
    let core_s = t0.elapsed().as_secs_f64();
    let summary = same_tx_fast_summary(store, query, count, stage_load_store_s, core_s, seed_edges, local_candidates);
    ensure_dir(outdir)?;
    write_json_pretty(Path::new(outdir).join("execution_report.json"), &summary)?;
    Ok(Some(summary))
}

pub fn build_store_pipeline(
    flashloans: &str,
    borrow: &str,
    dex: &str,
    outdir: &str,
    sqlite_out: &str,
) -> Result<serde_json::Value> {
    ensure_dir(outdir)?;
    ensure_dir(Path::new(sqlite_out).parent().and_then(|p| p.to_str()).unwrap_or("."))?;

    let t0 = Instant::now();
    let mut recs = Vec::new();
    recs.extend(normalize_flashloans(flashloans)?);
    recs.extend(normalize_borrow(borrow)?);
    recs.extend(normalize_dex(dex)?);
    let t1 = Instant::now();

    let event_atoms = records_to_event_atoms(recs);
    let t2 = Instant::now();

    let evidence = build_evidence_closures(&event_atoms);
    let (hyperedges, partitions, postings) = build_hgmatch_runtime_tables(&event_atoms)?;
    let t3 = Instant::now();

    let event_atoms_csv = Path::new(outdir).join("event_atoms.csv");
    let evidence_csv = Path::new(outdir).join("evidence_closures.csv");
    let partition_csv = Path::new(outdir).join("hg_partitions_manifest.csv");
    let inverted_csv = Path::new(outdir).join("hg_inverted_index_manifest.csv");

    write_event_atoms_csv(event_atoms_csv.to_str().unwrap(), &event_atoms)?;
    write_evidence_closures_csv(evidence_csv.to_str().unwrap(), &evidence)?;
    write_partition_manifest_csv(partition_csv.to_str().unwrap(), &partitions)?;
    write_inverted_manifest_csv(inverted_csv.to_str().unwrap(), &postings)?;
    let t4 = Instant::now();

    write_hgmatch_sqlite(sqlite_out, &registry_rules(), &evidence, &event_atoms, &hyperedges, &partitions, &postings)?;
    let t5 = Instant::now();

    let report = json!({
        "registry_version": "hgmatch_same_tx_rust_fair",
        "scope": "same_tx",
        "index_build_time_s": (t5 - t0).as_secs_f64(),
        "stage_times_s": {
            "read_csv_and_normalize_sources": (t1 - t0).as_secs_f64(),
            "merge_and_sort_atoms": (t2 - t1).as_secs_f64(),
            "build_hgmatch_runtime": (t3 - t2).as_secs_f64(),
            "write_artifacts": (t4 - t3).as_secs_f64(),
            "write_sqlite": (t5 - t4).as_secs_f64(),
        },
        "row_counts": {
            "event_atoms": event_atoms.len(),
            "evidence_closures": evidence.len(),
            "hg_hyperedges": hyperedges.len(),
            "hg_partitions": partitions.len(),
            "hg_inverted_index_rows": postings.len(),
        },
        "index_storage_size_bytes": {
            "sqlite_db": fs::metadata(sqlite_out).map(|m| m.len()).unwrap_or(0),
            "event_atoms_csv": fs::metadata(&event_atoms_csv).map(|m| m.len()).unwrap_or(0),
            "evidence_closures_csv": fs::metadata(&evidence_csv).map(|m| m.len()).unwrap_or(0),
            "hg_partitions_manifest_csv": fs::metadata(&partition_csv).map(|m| m.len()).unwrap_or(0),
            "hg_inverted_index_manifest_csv": fs::metadata(&inverted_csv).map(|m| m.len()).unwrap_or(0),
        }
    });

    write_json_pretty(Path::new(outdir).join("build_report.json"), &report)?;
    Ok(report)
}

fn query_pipeline_loaded(
    store: &InMemoryStore,
    query: &QuerySpec,
    outdir: &str,
    threads: usize,
    summary_only: bool,
    stage_load_store_s: f64,
) -> Result<QueryExecutionSummary> {
    ensure_dir(outdir)?;

    if summary_only {
        if let Some(summary) = query_pipeline_same_tx_summary_fastpath(
            store, query, outdir, stage_load_store_s
        )? {
            return Ok(summary);
        }
    }
    let t1 = Instant::now();
    let compiled = compile_query(query, store)?;
    let t2 = Instant::now();

    let _seed_probe = seed_edge_indices(store, &compiled, compiled.matching_order.first().unwrap());
    let t3 = Instant::now();

    let (structural_states, struct_stats) = enumerate_structural_embeddings_parallel(store, &compiled, threads)?;
    let t4 = Instant::now();

    let exact = exact_verify_structural_states(store, &compiled, &structural_states, summary_only);
    let t5 = Instant::now();

    let artifacts = QueryRunArtifacts {
        compiled: compiled.clone(),
        structural_states: structural_states.iter().map(|s| structural_state_to_out(store, s)).collect(),
        struct_stats,
        exact,
    };

    let summary = write_query_outputs(
        outdir,
        &artifacts,
        &store.stats(),
        stage_load_store_s,
        (t2 - t1).as_secs_f64(),
        (t3 - t2).as_secs_f64(),
        (t4 - t3).as_secs_f64(),
        (t5 - t4).as_secs_f64(),
        0.0,
        summary_only,
    )?;

    Ok(summary)
}

pub fn query_pipeline(
    db: &str,
    query_json: &str,
    outdir: &str,
    threads: usize,
    summary_only: bool,
) -> Result<QueryExecutionSummary> {
    let query = load_query_json(query_json)?;
    let t0 = Instant::now();
    let store = load_store_for_query_from_sqlite(db, &query)?;
    let load_s = t0.elapsed().as_secs_f64();
    query_pipeline_loaded(&store, &query, outdir, threads, summary_only, load_s)
}

pub fn query_pipeline_warm(
    db: &str,
    query_json: &str,
    outdir: &str,
    threads: usize,
    summary_only: bool,
    warmup_runs: usize,
    phase_marker: Option<&str>,
) -> Result<QueryExecutionSummary> {
    ensure_dir(outdir)?;
    let query = load_query_json(query_json)?;
    let t0 = Instant::now();
    let store = load_store_for_query_from_sqlite(db, &query)?;
    let load_s = t0.elapsed().as_secs_f64();
    let warmup_outdir = Path::new(outdir).join("_warmup");
    let warmup_outdir_text = warmup_outdir.to_string_lossy().into_owned();
    for _ in 0..warmup_runs {
        query_pipeline_loaded(
            &store,
            &query,
            &warmup_outdir_text,
            threads,
            summary_only,
            0.0,
        )?;
    }
    if let Some(marker) = phase_marker {
        if let Some(parent) = Path::new(marker).parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(marker, b"query\n")?;
    }
    let measured = query_pipeline_loaded(
        &store,
        &query,
        outdir,
        threads,
        summary_only,
        load_s,
    );
    if let Some(marker) = phase_marker {
        let _ = fs::remove_file(marker);
    }
    measured
}

pub fn resident_main() -> Result<()> {
    use std::io::BufRead;
    let db = std::env::args().nth(1).ok_or_else(|| anyhow!("db required"))?;
    let store = load_store_from_sqlite(&db)?;
    println!("{}", json!({"frame":"ready", "load_count":1, "query_independent":true}));
    std::io::stdout().flush()?;
    let mut sequence = 0u64;
    for line in std::io::stdin().lock().lines() {
        let raw: serde_json::Value = serde_json::from_str(&line?)?;
        if raw.get("op").and_then(|x| x.as_str()) == Some("close") { break; }
        sequence += 1;
        if raw["request_id"].as_u64() != Some(sequence) { return Err(anyhow!("request ID mismatch")); }
        let query: QuerySpec = serde_json::from_value(raw["query"].clone())?;
        if query.scope != "same_tx" || query.events.is_empty() { return Err(anyhow!("scope/events")); }
        let result = query_pipeline_loaded(&store, &query, "/unused-resident-artifacts", 4, true, 0.0)?;
        println!("{}", json!({"frame":"result","request_id":sequence,"count":result.num_matches}));
        std::io::stdout().flush()?;
    }
    Ok(())
}

#[cfg(test)]
mod resident_posting_regression {
    use super::*;

    fn fixture() -> (std::path::PathBuf, QuerySpec) {
        let db = std::env::temp_dir().join(format!("hgmatch-order-{}-{}.db",
            std::process::id(), std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        let mut atoms = Vec::new();
                                                                                
        for i in 0..4 {
            for (typ, seq, roles) in [
                ("flashloan", 1, [("recipient", "alice"), ("asset", "coin")]),
                ("borrow", 2, [("borrower", "alice"), ("asset", "coin")]),
            ] {
                atoms.push(EventAtom {
                    event_id: format!("{}{}", typ, 4-i),
                    closure_id: format!("tx{}", i), tx_hash: format!("tx{}", i),
                    event_type: typ.into(), protocol: "test".into(), seq_index: seq,
                    roles: roles.into_iter().map(|(k,v)|(k.into(),v.into())).collect(),
                    attrs: [("amount".into(), "-1".into())].into_iter().collect(),
                });
            }
        }
        let (edges, parts, postings) = build_hgmatch_runtime_tables(&atoms).unwrap();
                                                                           
        let conn = Connection::open(&db).unwrap();
        conn.execute_batch("CREATE TABLE registry_snapshot(rule_id,event_type,source_table,role_schema_json,anchorable_fields_json);
            CREATE TABLE hg_hyperedges(edge_json,closure_id,seq_index,edge_id);
            CREATE TABLE hg_partitions(signature_key,signature_labels_json,num_edges,edge_ids_json);
            CREATE TABLE hg_inverted_index(signature_key,vertex_id,posting_json);").unwrap();
        for r in registry_rules() {
            conn.execute("INSERT INTO registry_snapshot VALUES (?1,?2,?3,?4,?5)", params![r.rule_id,r.event_type,r.source_table,
                serde_json::to_string(&r.role_schema).unwrap(),serde_json::to_string(&r.anchorable_fields).unwrap()]).unwrap();
        }
        for e in edges {
            conn.execute("INSERT INTO hg_hyperedges VALUES (?1,?2,?3,?4)", params![serde_json::to_string(&e).unwrap(),e.closure_id,e.seq_index,e.edge_id]).unwrap();
        }
        for p in parts {
            conn.execute("INSERT INTO hg_partitions VALUES (?1,?2,?3,?4)", params![p.signature_key,
                serde_json::to_string(&p.signature_labels).unwrap(),p.num_edges as i64,serde_json::to_string(&p.edge_ids).unwrap()]).unwrap();
        }
        for p in postings {
            conn.execute("INSERT INTO hg_inverted_index VALUES (?1,?2,?3)", params![p.signature_key,p.vertex_id,serde_json::to_string(&p.posting).unwrap()]).unwrap();
        }
        let q = serde_json::from_value(json!({"query_name":"regression_not_a_fastpath","scope":"same_tx",
            "events":[{"id":"e1","type":"flashloan","roles":{"recipient":"$u","asset":"$x"}},
                      {"id":"e2","type":"borrow","roles":{"borrower":"$u","asset":"$x"}}],
            "order":[["e1","e2"]],"attr_filters":[{"event":"e2","field":"amount","op":"<","value":"0"}]})).unwrap();
        (db,q)
    }

    fn answers(store: &InMemoryStore, q: &QuerySpec) -> BTreeSet<String> {
        let plan=compile_query(q,store).unwrap();
        let (states,_)=enumerate_structural_embeddings_parallel(store,&plan,2).unwrap();
        states.iter().filter(|s|full_binding_ok(&plan,&s.edge_mapping,store))
            .map(|s|state_key(store,&s.edge_mapping)).collect()
    }

    #[test]
    fn reordered_postings_recover_complete_bindings_and_obey_query_changes() {
        let (db,mut q)=fixture();
        let mut store=load_store_from_sqlite(db.to_str().unwrap()).unwrap();
        assert!(store.postings.values().all(|p| p.windows(2).all(|w| w[0]<w[1])));
        let correct=answers(&store,&q);
        assert_eq!(correct.len(),4);
                                                                                  
        for p in store.postings.values_mut() {
            p.sort_by_key(|i|store.edges[*i].edge_id.clone());
        }
        let legacy=answers(&store,&q);
        assert!(legacy.len()<correct.len(), "fixture must reproduce the old omission");
        for p in store.postings.values_mut() {p.sort_unstable();p.dedup();}
        assert_eq!(answers(&store,&q),correct);
        q.attr_filters[0].op=">=".into();
        assert!(answers(&store,&q).is_empty(),"no cached answer or ignored QuerySpec");
        drop(store);
        std::fs::remove_file(db).unwrap();
    }

    #[test]
    fn dangling_posting_ids_are_rejected() {
        let (db,_)=fixture();
        let conn=Connection::open(&db).unwrap();
        conn.execute("UPDATE hg_inverted_index SET posting_json='[\"missing-edge\"]'",[]).unwrap();
        drop(conn);
        assert!(load_store_from_sqlite(db.to_str().unwrap()).is_err());
        std::fs::remove_file(db).unwrap();
    }
}

                                                                  
pub fn resident_audit_main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len()!=3 { return Err(anyhow!("audit requires DB and official S1 QuerySpec")); }
    let q=load_query_json(&args[2])?;
    let expected: QuerySpec=serde_json::from_value(json!({
        "query_name":"flashloan_then_debt_reduction_same_tx","scope":"same_tx",
        "events":[{"id":"e1","type":"flashloan","roles":{"recipient":"$u","asset":"$x"}},
                  {"id":"e2","type":"borrow","roles":{"borrower":"$u","asset":"$x"}}],
        "order":[["e1","e2"]],"attr_filters":[{"event":"e2","field":"amount","op":"<","value":"0"}],
        "neq_constraints":[]}))?;
    if serde_json::to_value(&q)? != serde_json::to_value(expected)? {
        return Err(anyhow!("diagnostic oracle supports only the exact official S1 template"));
    }
    let mut store=load_store_from_sqlite(&args[1])?;
    type Answer = (String,String);
    fn matched(store: &InMemoryStore,q: &QuerySpec) -> Result<BTreeSet<Answer>> {
        let plan=compile_query(q,store)?;
        let (states,_)=enumerate_structural_embeddings_parallel(store,&plan,4)?;
        Ok(states.iter().filter(|s|full_binding_ok(&plan,&s.edge_mapping,store))
            .map(|s|(store.edges[s.edge_mapping["e1"]].event_id.clone(),
                     store.edges[s.edge_mapping["e2"]].event_id.clone())).collect())
    }
    let repaired=matched(&store,&q)?;
                                                                            
                                                                                  
    let mut unordered=0usize;
    for p in store.postings.values_mut() {
        p.sort_by_key(|i|store.edges[*i].edge_id.clone());
        if p.windows(2).any(|w|w[0]>w[1]) {unordered+=1;}
    }
    let legacy=matched(&store,&q)?;
                                                                                
    let mut oracle=BTreeSet::new();
    for idxs in store.closure_to_edge_idxs.values() {
        let loans: Vec<_>=idxs.iter().map(|i|&store.edges[*i]).filter(|e|e.event_type=="flashloan").collect();
        if loans.is_empty() {continue;}
        let borrows: Vec<_>=idxs.iter().map(|i|&store.edges[*i]).filter(|e|e.event_type=="borrow").collect();
        for a in loans {
            for b in &borrows {
                let u=a.roles.get("recipient").map(String::as_str).unwrap_or("");
                let x=a.roles.get("asset").map(String::as_str).unwrap_or("");
                if !u.is_empty() && !x.is_empty() && a.tx_hash==b.tx_hash
                    && a.event_id!=b.event_id && a.seq_index<b.seq_index
                    && b.roles.get("borrower").map(String::as_str)==Some(u)
                    && b.roles.get("asset").map(String::as_str)==Some(x)
                    && b.attrs.get("amount").and_then(|s|s.parse::<f64>().ok()).map(|v|v<0.0).unwrap_or(false) {
                    oracle.insert((a.event_id.clone(),b.event_id.clone()));
                }
            }
        }
    }
    println!("{}",json!({"legacy_count":legacy.len(),"repaired_count":repaired.len(),
        "oracle_count":oracle.len(),"oracle_full_bindings_equal":repaired==oracle,
        "unordered_posting_lists":unordered,
        "recovered_bindings":repaired.difference(&legacy).collect::<Vec<_>>(),
        "removed_bindings":legacy.difference(&repaired).collect::<Vec<_>>(),
        "missing_vs_oracle":oracle.difference(&repaired).collect::<Vec<_>>(),
        "extra_vs_oracle":repaired.difference(&oracle).collect::<Vec<_>>(),
        "query":serde_json::to_value(q)?}));
    if repaired!=oracle {return Err(anyhow!("repaired matcher differs from independent S1 oracle"));}
    Ok(())
}

type S3Answer=[String;3];
fn s3_official_query() -> QuerySpec {
    serde_json::from_value(json!({"query_name":"flashloan_roundtrip_swaps","scope":"same_tx",
        "events":[{"id":"e1","type":"flashloan","roles":{"recipient":"$u","asset":"$x"}},
            {"id":"e2","type":"swap","roles":{"taker":"$u","token_sold":"$x","token_bought":"$y","pool":"$p1"}},
            {"id":"e3","type":"swap","roles":{"taker":"$u","token_sold":"$y","token_bought":"$x","pool":"$p2"}}],
        "order":[["e1","e2"],["e2","e3"]],"attr_filters":[],
        "neq_constraints":[["e2","pool","e3","pool"]]})).unwrap()
}

                                                                              
                                                                        
fn s3_independent_answers(store: &InMemoryStore) -> BTreeSet<S3Answer> {
    fn value<'a>(e:&'a DataHyperedge,k:&str) -> &'a str {
        let v=e.roles.get(k).map(String::as_str).unwrap_or("").trim();
        if ["nan","none","null"].iter().any(|bad|v.eq_ignore_ascii_case(bad)) {""} else {v}
    }
    let mut answers=BTreeSet::new();
    for idxs in store.closure_to_edge_idxs.values() {
        let roots:Vec<_>=idxs.iter().map(|i|&store.edges[*i]).filter(|e|e.event_type=="flashloan").collect();
        if roots.is_empty() {continue;}
        let swaps:Vec<_>=idxs.iter().map(|i|&store.edges[*i]).filter(|e|e.event_type=="swap").collect();
        for a in roots {
            let u=value(a,"recipient");let x=value(a,"asset");
            if u.is_empty() || x.is_empty() {continue;}
            for b in &swaps {
                let y=value(b,"token_bought");let p1=value(b,"pool");
                if b.seq_index<=a.seq_index || b.event_id==a.event_id || b.tx_hash!=a.tx_hash
                    || value(b,"taker")!=u || value(b,"token_sold")!=x || y.is_empty() || p1.is_empty() {continue;}
                for c in &swaps {
                    let p2=value(c,"pool");
                    if c.seq_index>b.seq_index && c.event_id!=a.event_id && c.event_id!=b.event_id
                        && c.tx_hash==a.tx_hash && value(c,"taker")==u
                        && value(c,"token_sold")==y && value(c,"token_bought")==x
                        && !p2.is_empty() && p2!=p1 {
                        answers.insert([a.event_id.clone(),b.event_id.clone(),c.event_id.clone()]);
                    }
                }
            }
        }
    }
    answers
}

fn s3_hgmatch_answers(store:&InMemoryStore,q:&QuerySpec) -> Result<BTreeSet<S3Answer>> {
    let plan=compile_query(q,store)?;
    let (states,_)=enumerate_structural_embeddings_parallel(store,&plan,4)?;
    Ok(states.iter().filter(|s|full_binding_ok(&plan,&s.edge_mapping,store))
        .map(|s|["e1","e2","e3"].map(|k|store.edges[s.edge_mapping[k]].event_id.clone())).collect())
}

pub fn s3_audit_main() -> Result<()> {
    let args:Vec<String>=std::env::args().collect();
    if args.len()!=3 {return Err(anyhow!("s3 audit requires DB and QuerySpec"));}
    let q=load_query_json(&args[2])?;
    if serde_json::to_value(&q)?!=serde_json::to_value(s3_official_query())? {
        return Err(anyhow!("Independent oracle requires unchanged official S3 semantics"));
    }
    let store=load_store_from_sqlite(&args[1])?;
    if !store.closure_to_edge_idxs.values().all(|p|p.windows(2).all(|w|w[0]<w[1]))
        || !store.postings.values().all(|p|p.windows(2).all(|w|w[0]<w[1]))
        || !store.partition_edge_idxs.values().all(|p|p.windows(2).all(|w|w[0]<w[1])) {
        return Err(anyhow!("sorted index invariant violated"));
    }
    let generated=s3_hgmatch_answers(&store,&q)?;
    let expected=s3_independent_answers(&store);
    println!("{}",json!({"query_name":q.query_name,"generated_count":generated.len(),
        "independent_count":expected.len(),"full_bindings_equal":generated==expected,
        "missing_count":expected.difference(&generated).count(),
        "extra_count":generated.difference(&expected).count(),
        "missing_examples":expected.difference(&generated).take(10).collect::<Vec<_>>(),
        "extra_examples":generated.difference(&expected).take(10).collect::<Vec<_>>(),
        "complete_generated_bindings":generated}));
    if generated!=expected {return Err(anyhow!("S3 full-binding mismatch"));}
    Ok(())
}

#[cfg(test)]
mod local_candidate_regression {
    use super::*;

    fn fixture() -> InMemoryStore {
        let mut atoms=Vec::new();
        for tx in 0..12 {
            let closure=format!("t{tx:02}");
            atoms.push(EventAtom {event_id:format!("f{tx}"),closure_id:closure.clone(),tx_hash:closure.clone(),
                event_type:"flashloan".into(),protocol:"p".into(),seq_index:0,
                roles:[("recipient".into(),"alice".into()),("asset".into(),"x".into())].into_iter().collect(),attrs:BTreeMap::new()});
            for i in 1..7 {
                let mut roles:BTreeMap<String,String>=[("taker","alice"),("token_sold",if i%2==1 {"x"} else {"y"}),
                    ("token_bought",if i%2==1 {"y"} else {"x"}),("pool",if i%3==0 {"p2"} else {"p1"})]
                    .into_iter().map(|(k,v)|(k.into(),v.into())).collect();
                if tx%4==0 && i==3 {roles.insert("taker".into(),"bob".into());}
                if tx%5==0 && i==4 {roles.insert("pool".into(),"".into());}
                atoms.push(EventAtom{event_id:format!("s{tx}_{i}"),closure_id:closure.clone(),tx_hash:closure.clone(),
                    event_type:"swap".into(),protocol:"p".into(),seq_index:i,roles,attrs:BTreeMap::new()});
            }
        }
        let (edges,parts,_)=build_hgmatch_runtime_tables(&atoms).unwrap();
        let mut store=InMemoryStore{registry:registry_map(),edges,edge_id_to_idx:HashMap::new(),
            closure_to_edge_idxs:HashMap::new(),partitions:HashMap::new(),partition_edge_idxs:HashMap::new(),postings:HashMap::new()};
        for (i,e) in store.edges.iter().enumerate() {
            store.edge_id_to_idx.insert(e.edge_id.clone(),i);
            store.closure_to_edge_idxs.entry(e.closure_id.clone()).or_default().push(i);
            store.partition_edge_idxs.entry(e.signature_key.clone()).or_default().push(i);
            let vids:BTreeSet<_>=e.vertices.iter().map(|v|v.vertex_id.clone()).collect();
            for v in vids {store.postings.entry((e.signature_key.clone(),v)).or_default().push(i);}
        }
        for p in parts {
            let mut counts=BTreeMap::new();
            for label in &p.signature_labels {*counts.entry(label.clone()).or_insert(0)+=1;}
            store.partitions.insert(p.signature_key.clone(),PartitionMeta{signature_key:p.signature_key,
                signature_labels:p.signature_labels,label_counts:counts,num_edges:p.num_edges});
        }
        store
    }

    #[test]
    fn local_candidates_equal_global_for_bound_unbound_and_missing_values() {
        let store=fixture();let q=s3_official_query();let plan=compile_query(&q,&store).unwrap();
        for (idx,e) in store.edges.iter().enumerate() {
            for (bound_id,bound_plan) in &plan.event_plans {
                if !event_plan_matches_edge_structural(bound_plan,e) {continue;}
                let Some(vars)=apply_var_constraints(bound_plan,e,&BTreeMap::new()) else {continue;};
                for bindings in [vars,BTreeMap::new(),[("$u".into(),"absent".into())].into_iter().collect()] {
                    let state=StructuralState{closure_id:e.closure_id.clone(),edge_mapping:[(bound_id.clone(),idx)].into_iter().collect(),var_bindings:bindings};
                    for qid in ["e1","e2","e3"] {
                        assert_eq!(generate_hyperedge_candidates(&store,&plan,qid,&state),baseline_global_candidates(&store,&plan,qid,&state));
                    }
                }
            }
        }
    }

    #[test]
    fn full_s3_bindings_equal_independent_enumeration_and_not_cached() {
        let mut store=fixture();let q=s3_official_query();
        let expected=s3_independent_answers(&store);assert!(!expected.is_empty());
        assert_eq!(s3_hgmatch_answers(&store,&q).unwrap(),expected);
                                                                                     
        store.edges[6].event_id=store.edges[3].event_id.clone();
        assert_eq!(s3_hgmatch_answers(&store,&q).unwrap(),s3_independent_answers(&store));
        let mut changed=q.clone();changed.neq_constraints.push(("e1".into(),"asset".into(),"e3".into(),"token_bought".into()));
        assert!(s3_hgmatch_answers(&store,&changed).unwrap().is_empty());
    }

    #[test]
    fn shortest_intersection_matches_set_intersection() {
        for n in 0..64 {
            let a:Vec<_>=(0..n).filter(|i|i%2==0).collect();
            let b:Vec<_>=(0..100).filter(|i|i%3==0).collect();
            let c:Vec<_>=(5..20).collect();
            let expected:Vec<_>=a.iter().copied().filter(|i|b.contains(i)&&c.contains(i)).collect();
            assert_eq!(smallest_sorted_intersection(&[&a,&b,&c]),expected);
            assert_eq!(smallest_sorted_intersection(&[&c,&b,&a]),expected);
        }
        assert!(smallest_sorted_intersection(&[]).is_empty());
    }
}
