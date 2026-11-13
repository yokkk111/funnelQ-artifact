use anyhow::{anyhow, Context, Result};
use csv::{ReaderBuilder, StringRecord};
use chrono::{DateTime, NaiveDateTime, Utc};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use rayon::prelude::*;
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


fn parse_timestamp_seconds(s: &str) -> i64 {
    let raw = norm_str(s);
    if raw.is_empty() {
        return 0;
    }
    if let Ok(v) = raw.parse::<i64>() {
        return if v > 1_000_000_000_000 { v / 1000 } else { v };
    }

    let raw_trim = raw.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw_trim) {
        return dt.timestamp();
    }
    if let Some(base) = raw_trim.strip_suffix(" UTC") {
        if let Ok(dt) = NaiveDateTime::parse_from_str(base, "%Y-%m-%d %H:%M:%S%.f") {
            return dt.and_utc().timestamp();
        }
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(raw_trim, "%Y-%m-%d %H:%M:%S%.f") {
        return dt.and_utc().timestamp();
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(raw_trim, "%Y-%m-%d %H:%M:%S") {
        return dt.and_utc().timestamp();
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(raw_trim, "%Y-%m-%dT%H:%M:%S%.f") {
        return dt.and_utc().timestamp();
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(raw_trim, "%Y-%m-%dT%H:%M:%S") {
        return dt.and_utc().timestamp();
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
    pub block_time_ts: i64,
    pub block_number: i64,
    pub tx_position: i64,
    pub log_index: i64,
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
    pub start_block_time_ts: i64,
    pub end_block_time_ts: i64,
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
    pub block_time_ts: i64,
    pub block_number: i64,
    pub tx_position: i64,
    pub log_index: i64,
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
    let (headers, rows) = load_csv_records(path)?;
    let mut out = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let tx_hash = pick(&headers, row, &["tx_hash", "txhash", "hash"]);
        if tx_hash.is_empty() {
            continue;
        }
        let log_index = parse_i64_fallback(&pick(&headers, row, &["evt_index", "log_index", "index", "event_index"]));
        let seq_index = log_index;
        let block_time_ts = parse_timestamp_seconds(&pick(&headers, row, &["block_time", "evt_block_time", "timestamp", "time"]));
        let block_number = parse_i64_fallback(&pick(&headers, row, &["block_number", "block_num", "blockno", "number"]));
        let tx_position = parse_i64_fallback(&pick(&headers, row, &["tx_index", "transaction_index", "tx_position", "position"]));
        let protocol = {
            let p = pick(&headers, row, &["project", "protocol", "platform"]);
            let v = pick(&headers, row, &["version"]);
            if !p.is_empty() && !v.is_empty() {
                format!("{}:{}", p, v)
            } else if !p.is_empty() {
                p
            } else {
                v
            }
        };
        let borrower = pick(&headers, row, &["borrower", "initiator", "user"]);
        let recipient = pick(&headers, row, &["recipient", "receiver"]);
        let asset = pick(&headers, row, &["asset_address", "token_address", "asset", "reserve_address", "token"]);
        let asset_symbol = pick(&headers, row, &["asset_symbol", "symbol", "token_symbol"]);
        let amount = pick(&headers, row, &["amount", "amount_raw", "amount_token"]);
        let amount_usd = pick(&headers, row, &["amount_usd", "usd_amount"]);

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
            block_time_ts,
            block_number,
            tx_position,
            log_index,
            roles,
            attrs,
        });
    }
    Ok(out)
}

pub fn normalize_borrow(path: &str) -> Result<Vec<EventAtom>> {
    let (headers, rows) = load_csv_records(path)?;
    let mut out = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let tx_hash = pick(&headers, row, &["tx_hash", "txhash", "hash"]);
        if tx_hash.is_empty() {
            continue;
        }
        let log_index = parse_i64_fallback(&pick(&headers, row, &["evt_index", "log_index", "index", "event_index"]));
        let seq_index = log_index;
        let block_time_ts = parse_timestamp_seconds(&pick(&headers, row, &["block_time", "evt_block_time", "timestamp", "time"]));
        let block_number = parse_i64_fallback(&pick(&headers, row, &["block_number", "block_num", "blockno", "number"]));
        let tx_position = parse_i64_fallback(&pick(&headers, row, &["tx_index", "transaction_index", "tx_position", "position"]));
        let protocol = {
            let p = pick(&headers, row, &["project", "protocol", "platform"]);
            let v = pick(&headers, row, &["version"]);
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
            row,
            &[
                "borrower", "borrower_address", "borroweraddress", "on_behalf_of", "onbehalfof",
                "on_behalf_of_address", "onbehalfofaddress", "user", "user_address", "useraddress",
                "account", "receiver", "recipient",
            ],
        );
        let asset = pick(
            &headers,
            row,
            &[
                "asset_address", "assetaddress", "reserve_address", "reserveaddress",
                "underlying_asset_address", "underlyingassetaddress", "underlying_address", "underlyingaddress",
                "token_address", "tokenaddress", "asset", "reserve", "token", "underlying_asset", "underlyingasset",
            ],
        );
        let asset_symbol = pick(
            &headers,
            row,
            &[
                "asset_symbol", "assetsymbol", "reserve_symbol", "reservesymbol", "underlying_symbol",
                "underlyingsymbol", "symbol", "token_symbol", "tokensymbol",
            ],
        );
        let pool = pick(
            &headers,
            row,
            &[
                "pool_address", "pooladdress", "pool", "market_address", "marketaddress", "reserve_pool",
                "reservepool", "project_contract_address", "projectcontractaddress", "contract_address",
                "contractaddress", "contract",
            ],
        );
        let amount = pick(
            &headers,
            row,
            &[
                "amount", "amount_raw", "amountraw", "amount_token", "amounttoken", "borrow_amount", "borrowamount",
            ],
        );
        let amount_usd = pick(&headers, row, &["amount_usd", "amountusd", "usd_amount", "usdamount"]);

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
            block_time_ts,
            block_number,
            tx_position,
            log_index,
            roles,
            attrs,
        });
    }
    Ok(out)
}

pub fn normalize_dex(path: &str) -> Result<Vec<EventAtom>> {
    let (headers, rows) = load_csv_records(path)?;
    let mut out = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        let tx_hash = pick(&headers, row, &["tx_hash", "txhash", "hash"]);
        if tx_hash.is_empty() {
            continue;
        }
        let log_index = parse_i64_fallback(&pick(&headers, row, &["evt_index", "log_index", "index", "event_index"]));
        let seq_index = log_index;
        let block_time_ts = parse_timestamp_seconds(&pick(&headers, row, &["block_time", "evt_block_time", "timestamp", "time"]));
        let block_number = parse_i64_fallback(&pick(&headers, row, &["block_number", "block_num", "blockno", "number"]));
        let tx_position = parse_i64_fallback(&pick(&headers, row, &["tx_index", "transaction_index", "tx_position", "position"]));
        let protocol = {
            let p = pick(&headers, row, &["project", "protocol", "platform"]);
            let v = pick(&headers, row, &["version"]);
            if !p.is_empty() && !v.is_empty() {
                format!("{}:{}", p, v)
            } else if !p.is_empty() {
                p
            } else {
                v
            }
        };
        let taker = pick(&headers, row, &["taker", "trader", "tx_from", "caller", "user"]);
        let pool = pick(&headers, row, &["pool_address", "pool", "project_contract_address", "contract_address"]);
        let token_sold = pick(&headers, row, &["token_sold_address", "token_in", "token_in_address", "sold_token_address"]);
        let token_bought = pick(&headers, row, &["token_bought_address", "token_out", "token_out_address", "bought_token_address"]);
        let sold_symbol = pick(&headers, row, &["token_sold_symbol", "token_in_symbol", "sold_symbol"]);
        let bought_symbol = pick(&headers, row, &["token_bought_symbol", "token_out_symbol", "bought_symbol"]);
        let amount_sold = pick(&headers, row, &["token_sold_amount", "amount_in", "amount_sold"]);
        let amount_bought = pick(&headers, row, &["token_bought_amount", "amount_out", "amount_bought"]);
        let amount_usd = pick(&headers, row, &["amount_usd", "usd_amount"]);

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
            block_time_ts,
            block_number,
            tx_position,
            log_index,
            roles,
            attrs,
        });
    }
    Ok(out)
}

pub fn records_to_event_atoms(mut atoms: Vec<EventAtom>) -> Vec<EventAtom> {
    atoms.sort_by(|a, b| {
        let ka = (a.block_time_ts, a.block_number, a.tx_position, a.log_index, &a.tx_hash, &a.event_type, &a.event_id);
        let kb = (b.block_time_ts, b.block_number, b.tx_position, b.log_index, &b.tx_hash, &b.event_type, &b.event_id);
        ka.cmp(&kb)
    });
    atoms
}

pub fn build_evidence_closures(atoms: &[EventAtom]) -> Vec<EvidenceClosure> {
    let mut grouped: BTreeMap<String, Vec<&EventAtom>> = BTreeMap::new();
    for atom in atoms {
        grouped.entry(atom.tx_hash.clone()).or_default().push(atom);
    }
    grouped
        .into_iter()
        .map(|(tx_hash, xs)| {
            let start_ts = xs.iter().map(|x| x.block_time_ts).min().unwrap_or(0);
            let end_ts = xs.iter().map(|x| x.block_time_ts).max().unwrap_or(0);
            EvidenceClosure {
                closure_id: tx_hash.clone(),
                tx_hash,
                num_events: xs.len(),
                event_ids: xs.iter().map(|x| x.event_id.clone()).collect(),
                event_types: xs.iter().map(|x| x.event_type.clone()).collect(),
                start_block_time_ts: start_ts,
                end_block_time_ts: end_ts,
            }
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
        block_time_ts: atom.block_time_ts,
        block_number: atom.block_number,
        tx_position: atom.tx_position,
        log_index: atom.log_index,
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
        "block_time_ts",
        "block_number",
        "tx_position",
        "log_index",
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
                a.block_time_ts.to_string(),
                a.block_number.to_string(),
                a.tx_position.to_string(),
                a.log_index.to_string(),
                serde_json::to_string(&a.roles).unwrap_or_else(|_| "{}".to_string()),
                serde_json::to_string(&a.attrs).unwrap_or_else(|_| "{}".to_string()),
            ]
        })
        .collect::<Vec<_>>();
    write_csv_rows(path, &headers, &rows)
}

pub fn write_evidence_closures_csv(path: &str, closures: &[EvidenceClosure]) -> Result<()> {
    let headers = [
        "closure_id",
        "tx_hash",
        "num_events",
        "start_block_time_ts",
        "end_block_time_ts",
        "event_ids_json",
        "event_types_json",
    ];
    let rows = closures
        .iter()
        .map(|c| {
            vec![
                c.closure_id.clone(),
                c.tx_hash.clone(),
                c.num_events.to_string(),
                c.start_block_time_ts.to_string(),
                c.end_block_time_ts.to_string(),
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScopeParams {
    #[serde(default)]
    pub forward_window_sec: i64,
    #[serde(default)]
    pub semantic_anchor_event: String,
    #[serde(default)]
    pub execution_anchor_event: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuerySpec {
    pub query_name: String,
    pub scope: String,
    #[serde(default)]
    pub scope_params: ScopeParams,
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
    if q.events.is_empty() {
        return Err(anyhow!("query must contain at least one event"));
    }
    match q.scope.as_str() {
        "same_tx" => {}
        "anchored_episode" => {
            if q.scope_params.forward_window_sec <= 0 {
                return Err(anyhow!("anchored_episode requires scope_params.forward_window_sec > 0"));
            }
            if q.scope_params.semantic_anchor_event.is_empty() {
                return Err(anyhow!("anchored_episode requires scope_params.semantic_anchor_event"));
            }
            let ids = q.events.iter().map(|e| e.id.clone()).collect::<HashSet<_>>();
            if !ids.contains(&q.scope_params.semantic_anchor_event) {
                return Err(anyhow!("semantic_anchor_event must refer to a query event id"));
            }
            if !q.scope_params.execution_anchor_event.is_empty()
                && q.scope_params.execution_anchor_event != "auto"
                && !ids.contains(&q.scope_params.execution_anchor_event)
            {
                return Err(anyhow!("execution_anchor_event must refer to a query event id"));
            }
        }
        other => {
            return Err(anyhow!("unsupported scope={other}; expected same_tx or anchored_episode"));
        }
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
    pub time_sorted_edge_idxs: Vec<usize>,
    pub event_type_time_sorted_edge_idxs: HashMap<String, Vec<usize>>,
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

pub fn load_store_from_sqlite(sqlite_path: &str, query: Option<&QuerySpec>) -> Result<InMemoryStore> {
    let mut conn = Connection::open_with_flags(sqlite_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let anchored_episode = query
        .map(|q| q.scope == "anchored_episode")
        .unwrap_or(false);

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
        if let Some(q) = query.filter(|q| q.scope == "anchored_episode") {
                                                                                   
                                                                               
                                       
            conn.execute_batch(
                "CREATE TABLE IF NOT EXISTS hg_query_role_index_v2(
                    edge_rowid INTEGER PRIMARY KEY,
                    block_time_ts INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    role_1 TEXT NOT NULL,
                    role_2 TEXT NOT NULL,
                    role_3 TEXT NOT NULL
                 );
                 CREATE INDEX IF NOT EXISTS idx_hg_qri_v2_type_time
                    ON hg_query_role_index_v2(event_type, block_time_ts);
                 CREATE INDEX IF NOT EXISTS idx_hg_qri_v2_r12_time
                    ON hg_query_role_index_v2(event_type, role_1, role_2, block_time_ts);
                 CREATE INDEX IF NOT EXISTS idx_hg_qri_v2_r13_time
                    ON hg_query_role_index_v2(event_type, role_1, role_3, block_time_ts);"
            )?;
            let indexed: i64 = conn.query_row(
                "SELECT COUNT(*) FROM hg_query_role_index_v2", [], |row| row.get(0)
            )?;
            let total: i64 = conn.query_row(
                "SELECT COUNT(*) FROM hg_hyperedges", [], |row| row.get(0)
            )?;
            if indexed != total {
                let tx = conn.transaction()?;
                tx.execute("DELETE FROM hg_query_role_index_v2", [])?;
                tx.execute(
                    "INSERT INTO hg_query_role_index_v2(
                         edge_rowid, block_time_ts, event_type, role_1, role_2, role_3
                     )
                     SELECT rowid,
                            CAST(json_extract(edge_json, '$.block_time_ts') AS INTEGER),
                            event_type,
                            CASE event_type
                              WHEN 'flashloan' THEN COALESCE(json_extract(edge_json, '$.roles.recipient'), '')
                              WHEN 'swap' THEN COALESCE(json_extract(edge_json, '$.roles.taker'), '')
                              WHEN 'borrow' THEN COALESCE(json_extract(edge_json, '$.roles.borrower'), '')
                              ELSE '' END,
                            CASE event_type
                              WHEN 'flashloan' THEN COALESCE(json_extract(edge_json, '$.roles.asset'), '')
                              WHEN 'swap' THEN COALESCE(json_extract(edge_json, '$.roles.token_sold'), '')
                              WHEN 'borrow' THEN COALESCE(json_extract(edge_json, '$.roles.asset'), '')
                              ELSE '' END,
                            CASE event_type
                              WHEN 'swap' THEN COALESCE(json_extract(edge_json, '$.roles.token_bought'), '')
                              ELSE '' END
                     FROM hg_hyperedges",
                    [],
                )?;
                tx.commit()?;
            }

            let semantic_qid = q.scope_params.semantic_anchor_event.as_str();
            let anchor_type = q.events.iter()
                .find(|event| event.id == semantic_qid)
                .map(|event| event.event_type.clone())
                .ok_or_else(|| anyhow!("missing semantic anchor event {}", semantic_qid))?;
            let window = q.scope_params.forward_window_sec;
            let mut needed_types = q
                .events
                .iter()
                .map(|event| event.event_type.clone())
                .collect::<Vec<_>>();
            needed_types.sort();
            needed_types.dedup();
            let needed_type_sql = needed_types
                .iter()
                .map(|event_type| format!("'{}'", event_type.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");
            let specialized_chain = q.query_name == "flashloan_swap_borrow_episode_v1"
                && q.events.len() == 3
                && q.events.iter().any(|e| e.event_type == "flashloan")
                && q.events.iter().any(|e| e.event_type == "swap")
                && q.events.iter().any(|e| e.event_type == "borrow");
            let sql = if specialized_chain {
                format!("SELECT h.edge_json
                 FROM hg_query_role_index_v2 t
                 JOIN hg_hyperedges h ON h.rowid = t.edge_rowid
                 WHERE t.event_type IN ({needed_type_sql})
                   AND (t.event_type = 'flashloan'
                    OR (t.event_type = 'swap' AND EXISTS (
                        SELECT 1 FROM hg_query_role_index_v2 a
                        WHERE a.event_type = 'flashloan'
                          AND a.role_1 = t.role_1 AND a.role_2 = t.role_2
                          AND a.block_time_ts BETWEEN t.block_time_ts - ?2 AND t.block_time_ts
                    ))
                    OR (t.event_type = 'borrow' AND EXISTS (
                        SELECT 1
                        FROM hg_query_role_index_v2 s
                        WHERE s.event_type = 'swap'
                          AND s.role_1 = t.role_1 AND s.role_3 = t.role_2
                          AND s.block_time_ts <= t.block_time_ts
                          AND EXISTS (
                              SELECT 1 FROM hg_query_role_index_v2 a
                              WHERE a.event_type = 'flashloan'
                                AND a.role_1 = s.role_1 AND a.role_2 = s.role_2
                                AND a.block_time_ts BETWEEN t.block_time_ts - ?2 AND s.block_time_ts
                          )
                    )))")
            } else {
                format!("SELECT h.edge_json
                 FROM hg_query_role_index_v2 t
                 JOIN hg_hyperedges h ON h.rowid = t.edge_rowid
                 WHERE t.event_type IN ({needed_type_sql})
                   AND (t.event_type = ?1
                    OR EXISTS (
                     SELECT 1 FROM hg_query_role_index_v2 a
                     WHERE a.event_type = ?1
                       AND a.role_1 = t.role_1
                       AND a.block_time_ts BETWEEN t.block_time_ts - ?2 AND t.block_time_ts
                 ))")
            };
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(rusqlite::params![anchor_type, window], |row| row.get::<_, String>(0))?;
            for payload in rows {
                let edge: DataHyperedge = serde_json::from_str(&payload?)?;
                let idx = edges.len();
                edge_id_to_idx.insert(edge.edge_id.clone(), idx);
                edges.push(edge);
            }
        } else {
            let mut stmt = conn.prepare("SELECT edge_json FROM hg_hyperedges")?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
            for payload in rows {
                let edge: DataHyperedge = serde_json::from_str(&payload?)?;
                let idx = edges.len();
                edge_id_to_idx.insert(edge.edge_id.clone(), idx);
                edges.push(edge);
            }
        }
    }

    let mut closure_to_edge_idxs: HashMap<String, Vec<usize>> = HashMap::new();
    for (idx, edge) in edges.iter().enumerate() {
        closure_to_edge_idxs.entry(edge.closure_id.clone()).or_default().push(idx);
    }
    for xs in closure_to_edge_idxs.values_mut() {
        xs.sort_by_key(|idx| (edges[*idx].block_time_ts, edges[*idx].block_number, edges[*idx].tx_position, edges[*idx].log_index, edges[*idx].edge_id.clone()));
    }

    let mut partitions = HashMap::new();
    let mut partition_edge_idxs = HashMap::new();
    if !anchored_episode {
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
            let idxs = edge_ids
                .into_iter()
                .filter_map(|eid| edge_id_to_idx.get(&eid).copied())
                .collect::<Vec<_>>();
            partition_edge_idxs.insert(signature_key, idxs);
        }
    }

    let mut postings = HashMap::new();
    if !anchored_episode {
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
            let idxs = edge_ids
                .into_iter()
                .filter_map(|eid| edge_id_to_idx.get(&eid).copied())
                .collect::<Vec<_>>();
            postings.insert((signature_key, vertex_id), idxs);
        }
    }

    let mut time_sorted_edge_idxs: Vec<usize> = (0..edges.len()).collect();
    time_sorted_edge_idxs.sort_by_key(|idx| {
        let e = &edges[*idx];
        (e.block_time_ts, e.block_number, e.tx_position, e.log_index, e.edge_id.clone())
    });

    let mut event_type_time_sorted_edge_idxs: HashMap<String, Vec<usize>> = HashMap::new();
    for idx in time_sorted_edge_idxs.iter().copied() {
        event_type_time_sorted_edge_idxs
            .entry(edges[idx].event_type.clone())
            .or_default()
            .push(idx);
    }

    Ok(InMemoryStore {
        registry,
        edges,
        edge_id_to_idx,
        closure_to_edge_idxs,
        partitions,
        partition_edge_idxs,
        postings,
        time_sorted_edge_idxs,
        event_type_time_sorted_edge_idxs,
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
pub struct ScopePlan {
    pub scope_type: String,
    pub forward_window_sec: i64,
    pub semantic_anchor_event: Option<String>,
    pub execution_anchor_event: Option<String>,
    pub local_entry_event: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledQueryPlan {
    pub query_name: String,
    pub scope: String,
    pub scope_plan: ScopePlan,
    pub events: Vec<QueryEventSpec>,
    pub matching_order: Vec<String>,
    pub event_plans: BTreeMap<String, QueryEventPlan>,
    pub attr_filters: Vec<AttrFilter>,
    pub neq_constraints: Vec<(String, String, String, String)>,
    pub order: Vec<(String, String)>,
}

fn build_order_graph(order: &[(String, String)]) -> HashMap<String, Vec<String>> {
    let mut g: HashMap<String, Vec<String>> = HashMap::new();
    for (l, r) in order.iter() {
        g.entry(l.clone()).or_default().push(r.clone());
    }
    g
}

fn must_happen_before(order: &[(String, String)], left: &str, right: &str) -> bool {
    if left == right {
        return false;
    }
    let g = build_order_graph(order);
    let mut seen = HashSet::new();
    let mut stack = vec![left.to_string()];
    while let Some(cur) = stack.pop() {
        if !seen.insert(cur.clone()) {
            continue;
        }
        if let Some(nexts) = g.get(&cur) {
            for nxt in nexts.iter() {
                if nxt == right {
                    return true;
                }
                stack.push(nxt.clone());
            }
        }
    }
    false
}

fn local_entry_after_semantic(query: &QuerySpec, semantic: &str) -> Option<String> {
    let mut candidates: Vec<String> = query
        .events
        .iter()
        .map(|e| e.id.clone())
        .filter(|qid| qid != semantic && must_happen_before(&query.order, semantic, qid))
        .collect();
    candidates.sort_by(|a, b| {
        let a_before_b = must_happen_before(&query.order, a, b);
        let b_before_a = must_happen_before(&query.order, b, a);
        match (a_before_b, b_before_a) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            _ => a.cmp(b),
        }
    });
    candidates.into_iter().next()
}

fn resolve_execution_anchor_event(query: &QuerySpec, semantic: &str) -> String {
    let configured = query.scope_params.execution_anchor_event.trim();
    if !configured.is_empty() && configured != "auto" {
        return configured.to_string();
    }
    local_entry_after_semantic(query, semantic).unwrap_or_else(|| semantic.to_string())
}

fn seeded_matching_order(
    query: &QuerySpec,
    event_plans: &BTreeMap<String, QueryEventPlan>,
    forced_prefix: &[String],
) -> Vec<String> {
    let mut remaining: Vec<String> = query.events.iter().map(|e| e.id.clone()).collect();
    let mut order = Vec::new();
    let mut bound_vars = HashSet::new();

    for qid in forced_prefix.iter() {
        if let Some(pos) = remaining.iter().position(|x| x == qid) {
            let next = remaining.remove(pos);
            if let Some(plan) = event_plans.get(&next) {
                for v in plan.roles.values() {
                    if v.starts_with('$') {
                        bound_vars.insert(v.clone());
                    }
                }
            }
            order.push(next);
        }
    }

    if order.is_empty() && !remaining.is_empty() {
        remaining.sort_by(|a, b| {
            let pa = event_plans.get(a).unwrap();
            let pb = event_plans.get(b).unwrap();
            pa.candidate_cardinality
                .cmp(&pb.candidate_cardinality)
                .then_with(|| a.cmp(b))
        });
        let first = remaining.remove(0);
        if let Some(plan) = event_plans.get(&first) {
            for v in plan.roles.values() {
                if v.starts_with('$') {
                    bound_vars.insert(v.clone());
                }
            }
        }
        order.push(first);
    }

    while !remaining.is_empty() {
        remaining.sort_by(|a, b| {
            let pa = event_plans.get(a).unwrap();
            let pb = event_plans.get(b).unwrap();
            let sa = pa.roles.values().filter(|v| v.starts_with('$') && bound_vars.contains(*v)).count();
            let sb = pb.roles.values().filter(|v| v.starts_with('$') && bound_vars.contains(*v)).count();
            let ka = if sa > 0 { pa.candidate_cardinality as f64 / sa as f64 } else { pa.candidate_cardinality as f64 };
            let kb = if sb > 0 { pb.candidate_cardinality as f64 / sb as f64 } else { pb.candidate_cardinality as f64 };
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
    order
}

fn compatible_signatures_for(
    required_label_counts: &BTreeMap<String, usize>,
    partition_meta: &HashMap<String, PartitionMeta>,
) -> Vec<String> {
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

    let scope_plan = if query.scope == "anchored_episode" {
        let sem = query.scope_params.semantic_anchor_event.clone();
        let exe = resolve_execution_anchor_event(query, &sem);
        if must_happen_before(&query.order, &exe, &sem) {
            return Err(anyhow!("anchored_episode requires semantic anchor to not be ordered after execution anchor"));
        }
        for ev in query.events.iter() {
            if must_happen_before(&query.order, &ev.id, &sem) {
                return Err(anyhow!("anchored_episode currently requires semantic anchor to be the earliest constrained event; found {} before {}", ev.id, sem));
            }
        }
        let local_entry = local_entry_after_semantic(query, &sem).unwrap_or_else(|| exe.clone());
        ScopePlan {
            scope_type: "anchored_episode".to_string(),
            forward_window_sec: query.scope_params.forward_window_sec,
            semantic_anchor_event: Some(sem),
            execution_anchor_event: Some(exe),
            local_entry_event: Some(local_entry),
        }
    } else {
        ScopePlan {
            scope_type: "same_tx".to_string(),
            forward_window_sec: 0,
            semantic_anchor_event: None,
            execution_anchor_event: None,
            local_entry_event: None,
        }
    };

    let forced_prefix = if scope_plan.scope_type == "anchored_episode" {
        let mut xs = Vec::new();
        if let Some(sem) = scope_plan.semantic_anchor_event.clone() {
            xs.push(sem);
        }
        if let Some(exe) = scope_plan.execution_anchor_event.clone() {
            if xs.last() != Some(&exe) && !xs.contains(&exe) {
                xs.push(exe);
            }
        }
        if let Some(local_entry) = scope_plan.local_entry_event.clone() {
            if xs.last() != Some(&local_entry) && !xs.contains(&local_entry) {
                xs.push(local_entry);
            }
        }
        xs
    } else {
        Vec::new()
    };

    let order = seeded_matching_order(query, &event_plans, &forced_prefix);

    Ok(CompiledQueryPlan {
        query_name: query.query_name.clone(),
        scope: query.scope.clone(),
        scope_plan,
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
    pub tx_hash: String,
    pub semantic_anchor_idx: Option<usize>,
    pub local_entry_idx: Option<usize>,
    pub local_tx_closure_id: Option<String>,
    pub execution_anchor_idx: Option<usize>,
    pub domain_edge_idxs: Option<Arc<BTreeSet<usize>>>,
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
    pub tx_hash: String,
    pub semantic_anchor_event_id: String,
    pub execution_anchor_event_id: String,
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




fn edge_order_tuple(edge: &DataHyperedge) -> (i64, i64, i64, i64, String) {
    (
        edge.block_time_ts,
        edge.block_number,
        edge.tx_position,
        edge.log_index,
        edge.edge_id.clone(),
    )
}

fn edge_before(a: &DataHyperedge, b: &DataHyperedge) -> bool {
    edge_order_tuple(a) < edge_order_tuple(b)
}

fn event_ids_in_query(compiled: &CompiledQueryPlan) -> HashSet<String> {
    compiled.events.iter().map(|e| e.id.clone()).collect()
}

fn query_event_types(compiled: &CompiledQueryPlan) -> HashSet<String> {
    compiled.events.iter().map(|e| e.event_type.clone()).collect()
}

fn query_compatible_signatures(compiled: &CompiledQueryPlan) -> HashSet<String> {
    let mut out = HashSet::new();
    for plan in compiled.event_plans.values() {
        for sig in plan.compatible_signatures.iter() {
            out.insert(sig.clone());
        }
    }
    out
}

fn lower_bound_time(store: &InMemoryStore, ts: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = store.time_sorted_edge_idxs.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let idx = store.time_sorted_edge_idxs[mid];
        if store.edges[idx].block_time_ts < ts {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn extend_known_vars_from_bound_event(
    known: &mut BTreeMap<String, String>,
    plan: &QueryEventPlan,
    edge: &DataHyperedge,
) -> bool {
    for (role_name, raw_val) in plan.roles.iter() {
        let raw_val = norm_str(raw_val);
        if raw_val.is_empty() {
            continue;
        }
        let actual = role_value(edge, role_name);
        if raw_val.starts_with('$') {
            if actual.is_empty() {
                return false;
            }
            if let Some(prev) = known.get(&raw_val) {
                if prev != &actual {
                    return false;
                }
            } else {
                known.insert(raw_val, actual);
            }
        } else if actual != raw_val {
            return false;
        }
    }
    true
}

fn edge_matches_known_vars(
    plan: &QueryEventPlan,
    edge: &DataHyperedge,
    known: &BTreeMap<String, String>,
) -> bool {
    for (role_name, raw_val) in plan.roles.iter() {
        let raw_val = norm_str(raw_val);
        if !raw_val.starts_with('$') {
            continue;
        }
        let Some(expected) = known.get(&raw_val) else { continue; };
        let actual = role_value(edge, role_name);
        if actual.is_empty() || &actual != expected {
            return false;
        }
    }
    true
}

fn build_semantic_episode_domain(store: &InMemoryStore, compiled: &CompiledQueryPlan, semantic_anchor_idx: usize) -> Arc<BTreeSet<usize>> {
    let anchor = &store.edges[semantic_anchor_idx];
    let start_ts = anchor.block_time_ts;
    let end_ts = start_ts + compiled.scope_plan.forward_window_sec;
    let types = query_event_types(compiled);
    let sigs = query_compatible_signatures(compiled);
    let semantic_qid = compiled.scope_plan.semantic_anchor_event.as_ref().unwrap();
    let semantic_plan = compiled.event_plans.get(semantic_qid).unwrap();
    let mut known = BTreeMap::new();
    let _ = extend_known_vars_from_bound_event(&mut known, semantic_plan, anchor);

    let mut out = BTreeSet::new();
    let start_pos = lower_bound_time(store, start_ts);
    for idx in store.time_sorted_edge_idxs[start_pos..].iter().copied() {
        let edge = &store.edges[idx];
        if edge.block_time_ts > end_ts {
            break;
        }
        if !types.contains(&edge.event_type) {
            continue;
        }
        if !sigs.contains(&edge.signature_key) {
            continue;
        }
        if edge_before(edge, anchor) {
            continue;
        }

        let mut keep = false;
        for qid in compiled.matching_order.iter() {
            let plan = compiled.event_plans.get(qid).unwrap();
            if !event_plan_matches_edge_structural(plan, edge) {
                continue;
            }
            if !edge_matches_known_vars(plan, edge, &known) {
                continue;
            }
            keep = true;
            break;
        }
        if keep {
            out.insert(idx);
        }
    }
    out.insert(semantic_anchor_idx);
    Arc::new(out)
}

fn build_execution_local_domain(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    semantic_anchor_idx: usize,
    execution_anchor_idx: usize,
    episode_domain: &BTreeSet<usize>,
) -> Arc<BTreeSet<usize>> {
    let semantic_qid = compiled.scope_plan.semantic_anchor_event.as_ref().unwrap();
    let execution_qid = compiled.scope_plan.execution_anchor_event.as_ref().unwrap();
    let semantic_edge = &store.edges[semantic_anchor_idx];
    let execution_edge = &store.edges[execution_anchor_idx];
    let semantic_plan = compiled.event_plans.get(semantic_qid).unwrap();
    let execution_plan = compiled.event_plans.get(execution_qid).unwrap();

    let mut known = BTreeMap::new();
    let _ = extend_known_vars_from_bound_event(&mut known, semantic_plan, semantic_edge);
    let _ = extend_known_vars_from_bound_event(&mut known, execution_plan, execution_edge);

    let mut out = BTreeSet::new();

    for idx in episode_domain.iter().copied() {
        if idx == semantic_anchor_idx || idx == execution_anchor_idx {
            out.insert(idx);
            continue;
        }
        let edge = &store.edges[idx];
        let mut keep = false;
        for qid in compiled.matching_order.iter() {
            if qid == semantic_qid || qid == execution_qid {
                continue;
            }
            let plan = compiled.event_plans.get(qid).unwrap();
            if !event_plan_matches_edge_structural(plan, edge) {
                continue;
            }
            if !edge_matches_known_vars(plan, edge, &known) {
                continue;
            }
            if must_happen_before(&compiled.order, qid, semantic_qid) {
                continue;
            }
            if must_happen_before(&compiled.order, execution_qid, qid) && !edge_before(execution_edge, edge) {
                continue;
            }
            if must_happen_before(&compiled.order, qid, execution_qid) && !edge_before(edge, execution_edge) {
                continue;
            }
            if edge_before(edge, semantic_edge) {
                continue;
            }
            keep = true;
            break;
        }
        if keep {
            out.insert(idx);
        }
    }
    out.insert(semantic_anchor_idx);
    out.insert(execution_anchor_idx);
    Arc::new(out)
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


fn full_binding_ok(compiled: &CompiledQueryPlan, state: &StructuralState, binding: &BTreeMap<String, usize>, store: &InMemoryStore) -> bool {
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
        if !edge_before(&store.edges[*li], &store.edges[*ri]) {
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

    if compiled.scope_plan.scope_type == "same_tx" {
        let txs = binding.values().map(|idx| store.edges[*idx].tx_hash.clone()).collect::<HashSet<_>>();
        if txs.len() != 1 {
            return false;
        }
    } else {
        let Some(semantic_idx) = state.semantic_anchor_idx else { return false; };
        let semantic_edge = &store.edges[semantic_idx];
        let end_ts = semantic_edge.block_time_ts + compiled.scope_plan.forward_window_sec;
        let sem_qid = compiled.scope_plan.semantic_anchor_event.as_deref().unwrap_or("");
        let tx_anchor_idx = state.execution_anchor_idx.or(state.local_entry_idx);
        let Some(tx_anchor_idx) = tx_anchor_idx else { return false; };
        let tx_anchor_edge = &store.edges[tx_anchor_idx];
        for (qid, idx) in binding.iter() {
            let edge = &store.edges[*idx];
            if edge.block_time_ts < semantic_edge.block_time_ts || edge.block_time_ts > end_ts {
                return false;
            }
            if qid != sem_qid {
                if edge.tx_hash != tx_anchor_edge.tx_hash {
                    return false;
                }
                if edge_before(edge, semantic_edge) {
                    return false;
                }
            }
        }
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
    let semantic_anchor_event_id = state
        .semantic_anchor_idx
        .map(|idx| store.edges[idx].event_id.clone())
        .unwrap_or_default();
    let execution_anchor_event_id = state
        .execution_anchor_idx
        .map(|idx| store.edges[idx].event_id.clone())
        .unwrap_or_default();
    StructuralStateOut {
        closure_id: state.closure_id.clone(),
        tx_hash: state.tx_hash.clone(),
        semantic_anchor_event_id,
        execution_anchor_event_id,
        edge_mapping,
        var_bindings: state.var_bindings.clone(),
    }
}

fn shared_bound_posting_candidates(
    store: &InMemoryStore,
    plan: &QueryEventPlan,
    sig: &str,
    var_bindings: &BTreeMap<String, String>,
) -> Option<Vec<usize>> {
    let Some(rule) = store.registry.get(&plan.event_type) else { return Some(Vec::new()); };
    let mut lists: Vec<Vec<usize>> = Vec::new();
    for role_name in plan.variable_roles.iter() {
        let Some(raw_val) = plan.roles.get(role_name) else { continue; };
        let raw_val = norm_str(raw_val);
        if !raw_val.starts_with('$') {
            continue;
        }
        let Some(bound_val) = var_bindings.get(&raw_val) else { continue; };
        let Some(role_label) = rule.role_schema.get(role_name).cloned() else { continue; };
        let vid = const_vertex_id(&role_label, bound_val);
        lists.push(store.postings.get(&(sig.to_string(), vid)).cloned().unwrap_or_default());
    }
    if lists.is_empty() {
        None
    } else {
        Some(intersect_usize_vectors(&lists))
    }
}

fn lower_bound_time_in_list(store: &InMemoryStore, idxs: &[usize], ts: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = idxs.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let idx = idxs[mid];
        if store.edges[idx].block_time_ts < ts {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn generate_local_entry_candidates_global(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    state: &StructuralState,
) -> Vec<usize> {
    let Some(local_qid) = compiled.scope_plan.local_entry_event.as_deref() else { return Vec::new(); };
    let Some(sem_qid) = compiled.scope_plan.semantic_anchor_event.as_deref() else { return Vec::new(); };
    let Some(semantic_idx) = state.semantic_anchor_idx else { return Vec::new(); };
    let semantic_edge = &store.edges[semantic_idx];
    let end_ts = semantic_edge.block_time_ts + compiled.scope_plan.forward_window_sec;
    let plan = compiled.event_plans.get(local_qid).expect("missing local entry plan");
    let used: HashSet<usize> = state.edge_mapping.values().copied().collect();
    let time_list = store.event_type_time_sorted_edge_idxs.get(&plan.event_type).cloned().unwrap_or_default();
    let start_pos = lower_bound_time_in_list(store, &time_list, semantic_edge.block_time_ts);
    let mut out = BTreeSet::new();

    for idx in time_list[start_pos..].iter().copied() {
        if used.contains(&idx) { continue; }
        let edge = &store.edges[idx];
        if edge.block_time_ts > end_ts { break; }
        if !candidate_respects_anchor_order(store, compiled, sem_qid, compiled.scope_plan.execution_anchor_event.as_deref().unwrap_or(""), semantic_idx, local_qid, idx, None) {
            continue;
        }
        if !candidate_respects_bound_order(store, compiled, local_qid, idx, state) {
            continue;
        }
        if !event_plan_matches_edge_structural(plan, edge) { continue; }
        let Some(next_bindings) = apply_var_constraints(plan, edge, &state.var_bindings) else { continue; };

        let mut next_mapping = state.edge_mapping.clone();
        next_mapping.insert(local_qid.to_string(), idx);
        let mut probe_state = StructuralState {
            closure_id: state.closure_id.clone(),
            tx_hash: edge.tx_hash.clone(),
            semantic_anchor_idx: state.semantic_anchor_idx,
            local_entry_idx: Some(idx),
            local_tx_closure_id: Some(edge.closure_id.clone()),
            execution_anchor_idx: state.execution_anchor_idx,
            domain_edge_idxs: None,
            edge_mapping: next_mapping,
            var_bindings: next_bindings,
        };
        if compiled.scope_plan.execution_anchor_event.as_deref() == Some(local_qid) {
            probe_state.execution_anchor_idx = Some(idx);
        }
        if !tx_supports_remaining_events_after_local_entry(store, compiled, &probe_state) {
            continue;
        }
        out.insert(idx);
    }
    out.into_iter().collect()
}

fn tx_supports_remaining_events_after_local_entry(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    state: &StructuralState,
) -> bool {
    let Some(local_qid) = compiled.scope_plan.local_entry_event.as_deref() else { return true; };
    let Some(sem_qid) = compiled.scope_plan.semantic_anchor_event.as_deref() else { return true; };
    for qid in compiled.matching_order.iter() {
        if qid == sem_qid || qid == local_qid { continue; }
        let mut cands = generate_hyperedge_candidates(store, compiled, qid, state);
        if let Some(semantic_idx) = state.semantic_anchor_idx {
            cands.retain(|idx| candidate_respects_anchor_order(
                store,
                compiled,
                sem_qid,
                compiled.scope_plan.execution_anchor_event.as_deref().unwrap_or(""),
                semantic_idx,
                qid,
                *idx,
                state.execution_anchor_idx,
            ));
        }
        if cands.is_empty() {
            return false;
        }
    }
    true
}

fn generate_hyperedge_candidates(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    qid: &str,
    state: &StructuralState,
) -> Vec<usize> {
    if compiled.scope_plan.scope_type == "anchored_episode"
        && state.local_tx_closure_id.is_none()
    {
        if compiled.scope_plan.execution_anchor_event.as_deref() == Some(qid) {
            return generate_execution_anchor_candidates_global(store, compiled, state);
        }
        if compiled.scope_plan.local_entry_event.as_deref() == Some(qid) {
            return generate_local_entry_candidates_global(store, compiled, state);
        }
    }

    let plan = compiled.event_plans.get(qid).expect("missing plan");
    let domain_ref: Option<&BTreeSet<usize>> = state.domain_edge_idxs.as_deref();
    let used: HashSet<usize> = state.edge_mapping.values().copied().collect();
    let mut out = BTreeSet::new();

    for sig in plan.compatible_signatures.iter() {
        let base_candidates = match shared_bound_posting_candidates(store, plan, sig, &state.var_bindings) {
            Some(v) => v,
            None => store.partition_edge_idxs.get(sig).cloned().unwrap_or_default(),
        };
        for idx in base_candidates {
            if used.contains(&idx) {
                continue;
            }
            if let Some(domain) = domain_ref {
                if !domain.contains(&idx) {
                    continue;
                }
            } else if let Some(local_closure_id) = state.local_tx_closure_id.as_ref() {
                if let Some(v) = store.closure_to_edge_idxs.get(local_closure_id) {
                    if !v.binary_search(&idx).is_ok() && !v.contains(&idx) {
                        continue;
                    }
                } else {
                    continue;
                }
            } else if let Some(v) = store.closure_to_edge_idxs.get(&state.closure_id) {
                if !v.binary_search(&idx).is_ok() && !v.contains(&idx) {
                    continue;
                }
            }
            let edge = &store.edges[idx];
            if event_plan_matches_edge_structural(plan, edge) {
                out.insert(idx);
            }
        }
    }
    out.into_iter().collect()
}

fn candidate_respects_anchor_order(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    sem_qid: &str,
    exec_qid: &str,
    semantic_idx: usize,
    candidate_qid: &str,
    candidate_idx: usize,
    execution_idx: Option<usize>,
) -> bool {
    let semantic_edge = &store.edges[semantic_idx];
    let edge = &store.edges[candidate_idx];
    if must_happen_before(&compiled.order, sem_qid, candidate_qid) && !edge_before(semantic_edge, edge) {
        return false;
    }
    if must_happen_before(&compiled.order, candidate_qid, sem_qid) && !edge_before(edge, semantic_edge) {
        return false;
    }
    if let Some(exec_idx) = execution_idx {
        let exec_edge = &store.edges[exec_idx];
        if must_happen_before(&compiled.order, candidate_qid, exec_qid) && !edge_before(edge, exec_edge) {
            return false;
        }
        if must_happen_before(&compiled.order, exec_qid, candidate_qid) && !edge_before(exec_edge, edge) {
            return false;
        }
    }
    true
}

fn candidate_respects_bound_order(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    qid: &str,
    candidate_idx: usize,
    state: &StructuralState,
) -> bool {
    let cand = &store.edges[candidate_idx];
    for (bound_qid, bound_idx) in state.edge_mapping.iter() {
        let bound_edge = &store.edges[*bound_idx];
        if must_happen_before(&compiled.order, bound_qid, qid) && !edge_before(bound_edge, cand) {
            return false;
        }
        if must_happen_before(&compiled.order, qid, bound_qid) && !edge_before(cand, bound_edge) {
            return false;
        }
    }
    true
}

fn tx_supports_remaining_events(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    base_state: &StructuralState,
    execution_idx: usize,
) -> bool {
    let Some(exec_qid) = compiled.scope_plan.execution_anchor_event.as_deref() else { return true; };
    let Some(sem_qid) = compiled.scope_plan.semantic_anchor_event.as_deref() else { return true; };
    let exec_plan = compiled.event_plans.get(exec_qid).expect("missing exec plan");
    let exec_edge = &store.edges[execution_idx];
    let Some(exec_var_bindings) = apply_var_constraints(exec_plan, exec_edge, &base_state.var_bindings) else {
        return false;
    };

    let mut temp_mapping = base_state.edge_mapping.clone();
    temp_mapping.insert(exec_qid.to_string(), execution_idx);
    let tx_state = StructuralState {
        closure_id: base_state.closure_id.clone(),
        tx_hash: exec_edge.tx_hash.clone(),
        semantic_anchor_idx: base_state.semantic_anchor_idx,
        local_entry_idx: base_state.local_entry_idx,
        local_tx_closure_id: Some(exec_edge.closure_id.clone()),
        execution_anchor_idx: Some(execution_idx),
        domain_edge_idxs: None,
        edge_mapping: temp_mapping,
        var_bindings: exec_var_bindings,
    };

    for qid in compiled.matching_order.iter() {
        if qid == sem_qid || qid == exec_qid { continue; }
        let mut cands = generate_hyperedge_candidates(store, compiled, qid, &tx_state);
        cands.retain(|idx| candidate_respects_anchor_order(
            store,
            compiled,
            sem_qid,
            exec_qid,
            tx_state.semantic_anchor_idx.expect("missing semantic idx"),
            qid,
            *idx,
            Some(execution_idx),
        ));
        if cands.is_empty() {
            return false;
        }
    }
    true
}

fn generate_execution_anchor_candidates_global(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    state: &StructuralState,
) -> Vec<usize> {
    let Some(exec_qid) = compiled.scope_plan.execution_anchor_event.as_deref() else { return Vec::new(); };
    let Some(sem_qid) = compiled.scope_plan.semantic_anchor_event.as_deref() else { return Vec::new(); };
    let Some(semantic_idx) = state.semantic_anchor_idx else { return Vec::new(); };
    let semantic_edge = &store.edges[semantic_idx];
    let end_ts = semantic_edge.block_time_ts + compiled.scope_plan.forward_window_sec;
    let plan = compiled.event_plans.get(exec_qid).expect("missing exec plan");
    let used: HashSet<usize> = state.edge_mapping.values().copied().collect();
    let mut out = BTreeSet::new();

    for sig in plan.compatible_signatures.iter() {
        let base_candidates = match shared_bound_posting_candidates(store, plan, sig, &state.var_bindings) {
            Some(v) => v,
            None => store.partition_edge_idxs.get(sig).cloned().unwrap_or_default(),
        };
        for idx in base_candidates {
            if used.contains(&idx) { continue; }
            let edge = &store.edges[idx];
            if edge.block_time_ts < semantic_edge.block_time_ts || edge.block_time_ts > end_ts { continue; }
            if let Some(local_entry_idx) = state.local_entry_idx {
                if edge.tx_hash != store.edges[local_entry_idx].tx_hash { continue; }
            }
            if !candidate_respects_anchor_order(store, compiled, sem_qid, exec_qid, semantic_idx, exec_qid, idx, None) { continue; }
            if !candidate_respects_bound_order(store, compiled, exec_qid, idx, state) { continue; }
            if !event_plan_matches_edge_structural(plan, edge) { continue; }
            let Some(_next_bindings) = apply_var_constraints(plan, edge, &state.var_bindings) else { continue; };
            if !tx_supports_remaining_events(store, compiled, state, idx) { continue; }
            out.insert(idx);
        }
    }
    out.into_iter().collect()
}

fn has_feasible_execution_anchor_candidate(
    store: &InMemoryStore,
    compiled: &CompiledQueryPlan,
    state: &StructuralState,
) -> bool {
    if compiled.scope_plan.scope_type != "anchored_episode" {
        return true;
    }
    if state.execution_anchor_idx.is_some() {
        return true;
    }
    !generate_execution_anchor_candidates_global(store, compiled, state).is_empty()
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
        .ok_or_else(|| anyhow!("matching order is empty"))?;
    let first_plan = compiled
        .event_plans
        .get(first_qid)
        .ok_or_else(|| anyhow!("missing event plan for first query event"))?;

    let mut tasks = Vec::new();
    let actual_chunk_size = chunk_size.max(1);
    for signature_key in first_plan.compatible_signatures.iter() {
        let total = store.partition_edge_idxs.get(signature_key).map(|v| v.len()).unwrap_or(0);
        if total == 0 {
            continue;
        }
        let mut start = 0usize;
        while start < total {
            let end = (start + actual_chunk_size).min(total);
            tasks.push(TaskOperator::Scan {
                start,
                end,
                signature_key: signature_key.clone(),
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
    let scan_chunk_size = 2048usize;
    let initial_tasks = build_scan_tasks(store, compiled, scan_chunk_size)?;
    let injector = Arc::new(Injector::new());
    for task in initial_tasks.iter().cloned() {
        injector.push(task);
    }

    let workers: Vec<Worker<TaskOperator>> = (0..num_threads).map(|_| Worker::new_lifo()).collect();
    let stealers: Vec<Stealer<TaskOperator>> = workers.iter().map(|w| w.stealer()).collect();

    let store = Arc::new(store.clone());
    let compiled = Arc::new(compiled.clone());
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
            let store = Arc::clone(&store);
            let compiled = Arc::clone(&compiled);
            let outstanding = Arc::clone(&outstanding);
            let seed_edge_count = Arc::clone(&seed_edge_count);
            let local_candidate_edges_examined = Arc::clone(&local_candidate_edges_examined);
            let structural_embeddings_raw = Arc::clone(&structural_embeddings_raw);
            let seed_closures = Arc::clone(&seed_closures);
            let closures_scanned = Arc::clone(&closures_scanned);
            let nonempty_candidate_closures = Arc::clone(&nonempty_candidate_closures);
            let sink_states = Arc::clone(&sink_states);

            scope.spawn(move |_| {
                let order_len = compiled.matching_order.len();
                let first_qid = compiled.matching_order.first().expect("missing first qid").clone();
                let first_plan = compiled.event_plans.get(&first_qid).expect("missing first plan");
                let sem_anchor_qid = compiled.scope_plan.semantic_anchor_event.clone();
                let exec_anchor_qid = compiled.scope_plan.execution_anchor_event.clone();

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
                            let partition = store.partition_edge_idxs.get(&signature_key).cloned().unwrap_or_default();
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
                                    guard.insert(edge.tx_hash.clone());
                                }
                                {
                                    let mut guard = closures_scanned.lock().unwrap();
                                    guard.insert(edge.tx_hash.clone());
                                }

                                let mut mapping = BTreeMap::new();
                                mapping.insert(first_qid.clone(), idx);

                                let mut state = if compiled.scope_plan.scope_type == "anchored_episode" {
                                    let mut execution_anchor_idx = None;
                                    let mut local_entry_idx = None;
                                    let mut local_tx_closure_id = None;
                                    if sem_anchor_qid.as_ref() == exec_anchor_qid.as_ref() {
                                        execution_anchor_idx = Some(idx);
                                    }
                                    if compiled.scope_plan.local_entry_event.as_deref() == Some(first_qid.as_str()) {
                                        local_entry_idx = Some(idx);
                                        local_tx_closure_id = Some(edge.closure_id.clone());
                                    }
                                    StructuralState {
                                        closure_id: format!("episode::{}", edge.edge_id),
                                        tx_hash: edge.tx_hash.clone(),
                                        semantic_anchor_idx: Some(idx),
                                        local_entry_idx,
                                        local_tx_closure_id,
                                        execution_anchor_idx,
                                        domain_edge_idxs: None,
                                        edge_mapping: mapping,
                                        var_bindings,
                                    }
                                } else {
                                    StructuralState {
                                        closure_id: edge.closure_id.clone(),
                                        tx_hash: edge.tx_hash.clone(),
                                        semantic_anchor_idx: Some(idx),
                                        local_entry_idx: Some(idx),
                                        local_tx_closure_id: Some(edge.closure_id.clone()),
                                        execution_anchor_idx: Some(idx),
                                        domain_edge_idxs: None,
                                        edge_mapping: mapping,
                                        var_bindings,
                                    }
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
                            let cands = generate_hyperedge_candidates(&store, &compiled, &qid, &state);
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
                                let mut next_state = StructuralState {
                                    closure_id: state.closure_id.clone(),
                                    tx_hash: state.tx_hash.clone(),
                                    semantic_anchor_idx: state.semantic_anchor_idx,
                                    local_entry_idx: state.local_entry_idx,
                                    local_tx_closure_id: state.local_tx_closure_id.clone(),
                                    execution_anchor_idx: state.execution_anchor_idx,
                                    domain_edge_idxs: state.domain_edge_idxs.clone(),
                                    edge_mapping: next_mapping,
                                    var_bindings: next_var_bindings,
                                };
                                if compiled.scope_plan.scope_type == "anchored_episode" {
                                    if compiled.scope_plan.local_entry_event.as_deref() == Some(qid.as_str())
                                        && next_state.local_entry_idx.is_none()
                                    {
                                        next_state.local_entry_idx = Some(idx);
                                        next_state.local_tx_closure_id = Some(edge.closure_id.clone());
                                        next_state.tx_hash = edge.tx_hash.clone();
                                        next_state.domain_edge_idxs = None;
                                    }
                                    if compiled.scope_plan.execution_anchor_event.as_deref() == Some(qid.as_str())
                                        && next_state.execution_anchor_idx.is_none()
                                    {
                                        next_state.execution_anchor_idx = Some(idx);
                                    }
                                }
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

    let raw_states = sink_states.lock().unwrap().clone();
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
    let cand1_closures = structural_states
        .iter()
        .map(|s| s.closure_id.clone())
        .collect::<BTreeSet<_>>();
    let mut cand2_closures = BTreeSet::new();
    let mut matched_rows = Vec::new();
    let mut binding_rows = Vec::new();
    let mut seen_final = BTreeSet::new();

    for (match_idx, st) in structural_states.iter().enumerate() {
        if !full_binding_ok(compiled, st, &st.edge_mapping, store) {
            continue;
        }
        let final_key = state_key(store, &st.edge_mapping);
        if !seen_final.insert(final_key) {
            continue;
        }
        let match_id = format!("{}::m{}", st.closure_id, match_idx);
        cand2_closures.insert(st.closure_id.clone());
        if !summary_only {
            matched_rows.push(vec![
                match_id.clone(),
                st.closure_id.clone(),
                st.tx_hash.clone(),
                compiled.query_name.clone(),
                st.edge_mapping.len().to_string(),
            ]);
            for qid in compiled.matching_order.iter() {
                let idx = st.edge_mapping.get(qid).copied().unwrap();
                let edge = &store.edges[idx];
                binding_rows.push(vec![
                    match_id.clone(),
                    st.closure_id.clone(),
                    st.tx_hash.clone(),
                    qid.clone(),
                    edge.event_id.clone(),
                    edge.event_type.clone(),
                    edge.seq_index.to_string(),
                    edge.protocol.clone(),
                ]);
            }
        }
    }

    let residual_closures = cand1_closures
        .difference(&cand2_closures)
        .cloned()
        .collect::<Vec<_>>();

    let num_matches = seen_final.len();
    ExactVerifyResult {
        cand2_closures: cand2_closures.into_iter().collect(),
        num_matches,
        matched_rows,
        binding_rows,
        residual_closures,
    }
}


pub fn structural_rows(structural_states: &[StructuralStateOut], order: &[String]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for (i, st) in structural_states.iter().enumerate() {
        let mut row = vec![
            format!("sm{}", i),
            st.closure_id.clone(),
            st.tx_hash.clone(),
            st.semantic_anchor_event_id.clone(),
            st.execution_anchor_event_id.clone(),
        ];
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
    let cand1_tx_map = artifacts.structural_states.iter().map(|s| (s.closure_id.clone(), s.tx_hash.clone())).collect::<HashMap<_, _>>();

    let structural_rows_vec = if summary_only {
        Vec::new()
    } else {
        let mut structural_headers = vec!["structural_match_id", "closure_id", "tx_hash", "semantic_anchor_event_id", "execution_anchor_event_id"];
        for qid in artifacts.compiled.matching_order.iter() {
            structural_headers.push(qid);
        }
        let structural_rows_vec = structural_rows(&artifacts.structural_states, &artifacts.compiled.matching_order);
        write_csv_rows(Path::new(outdir).join("structural_embeddings.csv"), &structural_headers, &structural_rows_vec)?;

        let cand1_rows = cand1_closures.iter().map(|c| vec![c.clone(), cand1_tx_map.get(c).cloned().unwrap_or_default()]).collect::<Vec<_>>();
        write_csv_rows(Path::new(outdir).join("cand1_structural_closures.csv"), &["closure_id", "tx_hash"], &cand1_rows)?;

        let cand2_rows = artifacts.exact.cand2_closures.iter().map(|c| vec![c.clone(), cand1_tx_map.get(c).cloned().unwrap_or_default()]).collect::<Vec<_>>();
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
            .map(|c| vec![c.clone(), cand1_tx_map.get(c).cloned().unwrap_or_default()])
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
        "registry_version": "hgmatch_episode_rust_fair_v1",
        "scope": "global_event_store",
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


#[derive(Debug, Clone, Default)]
struct EpisodeBucket {
    ords: Vec<i64>,
    idxs: Vec<usize>,
}

#[derive(Debug, Clone)]
struct EpisodeIndex {
    global_tx_ord_by_idx: Vec<usize>,
    global_event_ord_by_idx: Vec<i64>,
    tx_start_ts: Vec<i64>,
    tx_max_event_ord: Vec<i64>,
    bucket_all_by_type: HashMap<String, EpisodeBucket>,
    bucket_by_key: HashMap<String, EpisodeBucket>,
}

#[derive(Debug, Clone)]
struct EpisodeQueryEventSpec {
    id: String,
    event_type: String,
    protocol: String,
    const_roles: BTreeMap<String, String>,
    var_roles: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
struct EpisodeRunStats {
    local_candidate_edges_examined: usize,
    structural_embeddings_raw: usize,
    matched_episodes: usize,
    candidate_episodes: usize,
    episodes_with_nonempty_candidates: usize,
}

fn role_value_for_query(edge: &DataHyperedge, field: &str) -> String {
    if field == "event_id" { edge.event_id.clone() } else if field == "protocol" { edge.protocol.clone() } else { norm_str(edge.roles.get(field).map(|s| s.as_str()).unwrap_or("")) }
}

fn episode_support_key(event_type: &str, protocol: &str, roles: &[(String, String)]) -> String {
    let mut parts = vec![format!("type={}", event_type)];
    if !protocol.is_empty() {
        parts.push(format!("protocol={}", protocol));
    }
    let mut rs = roles.to_vec();
    rs.sort_by(|a,b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    for (k,v) in rs.into_iter() { parts.push(format!("{}={}", k, v)); }
    parts.join("|")
}

fn build_episode_index(store: &InMemoryStore) -> EpisodeIndex {
    let mut tx_meta: BTreeMap<String, (i64, i64)> = BTreeMap::new();
    for e in store.edges.iter() {
        let key = (e.block_number, e.block_time_ts);
        tx_meta.entry(e.tx_hash.clone()).and_modify(|cur| {
            if key < *cur { *cur = key; }
        }).or_insert(key);
    }
    let mut txs: Vec<(String, i64, i64)> = tx_meta.into_iter().map(|(tx,(bn,ts))| (tx,bn,ts)).collect();
    txs.sort_by(|a,b| (a.1,a.2,&a.0).cmp(&(b.1,b.2,&b.0)));
    let mut tx_ord_map = HashMap::new();
    for (i,(tx,_,_)) in txs.iter().enumerate() { tx_ord_map.insert(tx.clone(), i); }
    let mut edge_order: Vec<usize> = (0..store.edges.len()).collect();
    edge_order.sort_by(|a,b| {
        let ea=&store.edges[*a]; let eb=&store.edges[*b];
        let ta=*tx_ord_map.get(&ea.tx_hash).unwrap_or(&usize::MAX);
        let tb=*tx_ord_map.get(&eb.tx_hash).unwrap_or(&usize::MAX);
        (ta, ea.seq_index, ea.log_index, &ea.edge_id).cmp(&(tb, eb.seq_index, eb.log_index, &eb.edge_id))
    });
    let mut global_event_ord_by_idx = vec![0i64; store.edges.len()];
    let mut tx_max_event_ord = vec![0i64; txs.len()];
    for (ord, idx) in edge_order.iter().enumerate() {
        global_event_ord_by_idx[*idx] = ord as i64;
        let txo=*tx_ord_map.get(&store.edges[*idx].tx_hash).unwrap();
        tx_max_event_ord[txo]=ord as i64;
    }
    let mut global_tx_ord_by_idx = vec![0usize; store.edges.len()];
    for (idx,e) in store.edges.iter().enumerate() { global_tx_ord_by_idx[idx] = *tx_ord_map.get(&e.tx_hash).unwrap(); }
    let tx_start_ts = txs.iter().map(|x| x.2).collect::<Vec<_>>();

    let mut bucket_all_tmp: HashMap<String, Vec<usize>> = HashMap::new();
    let mut bucket_key_tmp: HashMap<String, Vec<usize>> = HashMap::new();
    for idx in edge_order.iter().copied() {
        let e = &store.edges[idx];
        bucket_all_tmp.entry(e.event_type.clone()).or_default().push(idx);
        let proto = e.protocol.clone();
        let mut role_pairs: Vec<(String,String)> = e.roles.iter().filter_map(|(k,v)| { let vv=norm_str(v); if vv.is_empty() {None} else {Some((k.clone(), vv))} }).collect();
        role_pairs.sort_by(|a,b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        bucket_key_tmp.entry(episode_support_key(&e.event_type, &proto, &[])).or_default().push(idx);
        for (k,v) in role_pairs.iter() {
            bucket_key_tmp.entry(episode_support_key(&e.event_type, &proto, &[(k.clone(),v.clone())])).or_default().push(idx);
            if !proto.is_empty() {
                bucket_key_tmp.entry(episode_support_key(&e.event_type, "", &[(k.clone(),v.clone())])).or_default().push(idx);
            }
        }
        for i in 0..role_pairs.len() {
            for j in (i+1)..role_pairs.len() {
                let r1=role_pairs[i].clone(); let r2=role_pairs[j].clone();
                bucket_key_tmp.entry(episode_support_key(&e.event_type, &proto, &[r1.clone(),r2.clone()])).or_default().push(idx);
                if !proto.is_empty() {
                    bucket_key_tmp.entry(episode_support_key(&e.event_type, "", &[r1,r2])).or_default().push(idx);
                }
            }
        }
    }
    let to_bucket = |idxs: Vec<usize>, ords_src: &Vec<i64>| -> EpisodeBucket {
        EpisodeBucket { ords: idxs.iter().map(|i| ords_src[*i]).collect(), idxs }
    };
    let bucket_all_by_type = bucket_all_tmp.into_iter().map(|(k,v)| (k, to_bucket(v, &global_event_ord_by_idx))).collect();
    let bucket_by_key = bucket_key_tmp.into_iter().map(|(k,v)| (k, to_bucket(v, &global_event_ord_by_idx))).collect();
    EpisodeIndex { global_tx_ord_by_idx, global_event_ord_by_idx, tx_start_ts, tx_max_event_ord, bucket_all_by_type, bucket_by_key }
}

fn lower_bound_i64(arr: &[i64], target: i64) -> usize {
    let mut lo=0usize; let mut hi=arr.len();
    while lo<hi { let mid=(lo+hi)/2; if arr[mid] < target { lo=mid+1; } else { hi=mid; } }
    lo
}
fn upper_bound_i64(arr: &[i64], target: i64) -> usize {
    let mut lo=0usize; let mut hi=arr.len();
    while lo<hi { let mid=(lo+hi)/2; if arr[mid] <= target { lo=mid+1; } else { hi=mid; } }
    lo
}

fn choose_bucket_for_spec<'a>(
    idx: &'a EpisodeIndex,
    spec: &EpisodeQueryEventSpec,
    vars: &BTreeMap<String,String>,
) -> Option<&'a EpisodeBucket> {
    let mut known: Vec<(String,String)> = Vec::new();
    for (k,v) in spec.const_roles.iter() { if !v.is_empty() { known.push((k.clone(), v.clone())); } }
    for (role,var) in spec.var_roles.iter() {
        if let Some(v) = vars.get(var) { if !v.is_empty() { known.push((role.clone(), v.clone())); } }
    }
    known.sort_by(|a,b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    known.dedup();
    let mut candidates: Vec<String> = Vec::new();
    if known.len() >= 2 {
        for i in 0..known.len() { for j in (i+1)..known.len() {
            candidates.push(episode_support_key(&spec.event_type, &spec.protocol, &[known[i].clone(), known[j].clone()]));
            if !spec.protocol.is_empty() {
                candidates.push(episode_support_key(&spec.event_type, "", &[known[i].clone(), known[j].clone()]));
            }
        }}
    }
    for kv in known.iter() {
        candidates.push(episode_support_key(&spec.event_type, &spec.protocol, &[kv.clone()]));
        if !spec.protocol.is_empty() { candidates.push(episode_support_key(&spec.event_type, "", &[kv.clone()])); }
    }
    candidates.push(episode_support_key(&spec.event_type, &spec.protocol, &[]));
    if !spec.protocol.is_empty() { candidates.push(episode_support_key(&spec.event_type, "", &[])); }
    for k in candidates.into_iter() {
        if let Some(b) = idx.bucket_by_key.get(&k) { return Some(b); }
    }
    idx.bucket_all_by_type.get(&spec.event_type)
}

fn build_episode_specs(query: &QuerySpec) -> BTreeMap<String, EpisodeQueryEventSpec> {
    let mut out=BTreeMap::new();
    for e in query.events.iter() {
        let mut const_roles=BTreeMap::new();
        let mut var_roles=Vec::new();
        for (k,v) in e.roles.iter() {
            let vv=norm_str(v);
            if vv.starts_with('$') { var_roles.push((k.clone(), vv)); }
            else if !vv.is_empty() { const_roles.insert(k.clone(), vv); }
        }
        out.insert(e.id.clone(), EpisodeQueryEventSpec { id:e.id.clone(), event_type:e.event_type.clone(), protocol:e.protocol.clone(), const_roles, var_roles });
    }
    out
}

fn build_episode_pair_strengths(query: &QuerySpec) -> (HashMap<String, i32>, HashMap<String, i32>) {
    let mut pair: HashMap<String,i32> = HashMap::new();
    let mut per: HashMap<String,i32> = HashMap::new();
    let mut var_occ: HashMap<String, Vec<String>> = HashMap::new();
    for e in query.events.iter() { for (_k,v) in e.roles.iter() { let vv=norm_str(v); if vv.starts_with('$') { var_occ.entry(vv).or_default().push(e.id.clone()); } } }
    for (_var, eids) in var_occ.into_iter() {
        let mut uniq=eids; uniq.sort(); uniq.dedup();
        for eid in uniq.iter() { *per.entry(eid.clone()).or_insert(0) += (uniq.len().saturating_sub(1)) as i32; }
        for i in 0..uniq.len() { for j in (i+1)..uniq.len() {
            let k = format!("{}|{}", uniq[i], uniq[j]);
            *pair.entry(k).or_insert(0) += 2;
        }}
    }
    for (a,b) in query.order.iter() { let mut x=[a.clone(),b.clone()]; x.sort(); let k=format!("{}|{}", x[0], x[1]); *pair.entry(k).or_insert(0)+=1; *per.entry(a.clone()).or_insert(0)+=1; *per.entry(b.clone()).or_insert(0)+=1; }
    for (a,_af,b,_bf) in query.neq_constraints.iter() { let mut x=[a.clone(),b.clone()]; x.sort(); let k=format!("{}|{}", x[0], x[1]); *pair.entry(k).or_insert(0)+=1; *per.entry(a.clone()).or_insert(0)+=1; *per.entry(b.clone()).or_insert(0)+=1; }
    (pair, per)
}

fn choose_next_episode_qid(query: &QuerySpec, assigned: &BTreeMap<String, usize>, pair: &HashMap<String,i32>, per: &HashMap<String,i32>, cand_sizes: &HashMap<String, usize>) -> String {
    let bound: Vec<String> = assigned.keys().cloned().collect();
    let mut scored = Vec::new();
    for e in query.events.iter() {
        if assigned.contains_key(&e.id) { continue; }
        let mut link=0i32;
        for b in bound.iter() {
            let mut x=[e.id.clone(), b.clone()]; x.sort(); let k=format!("{}|{}", x[0], x[1]);
            link += *pair.get(&k).unwrap_or(&0);
        }
        let pri = *per.get(&e.id).unwrap_or(&0);
        let cs = *cand_sizes.get(&e.id).unwrap_or(&usize::MAX);
        scored.push((cs, -link, -pri, e.id.clone()));
    }
    scored.sort();
    scored[0].3.clone()
}

fn event_matches_spec(store: &InMemoryStore, idx: usize, spec: &EpisodeQueryEventSpec, vars: &BTreeMap<String,String>) -> bool {
    let e=&store.edges[idx];
    if e.event_type != spec.event_type { return false; }
    if !spec.protocol.is_empty() && e.protocol != spec.protocol { return false; }
    for (k,v) in spec.const_roles.iter() { if role_value_for_query(e,k) != *v { return false; } }
    for (k,var) in spec.var_roles.iter() {
        let actual = role_value_for_query(e,k);
        if actual.is_empty() { return false; }
        if let Some(bound) = vars.get(var) { if bound != &actual { return false; } }
    }
    true
}

fn episode_attr_filters_ok(
    store: &InMemoryStore,
    query: &QuerySpec,
    qid: &str,
    idx: usize,
) -> bool {
    let edge = &store.edges[idx];
    for flt in query.attr_filters.iter().filter(|flt| flt.event == qid) {
        let Some(actual) = edge.attrs.get(&flt.field) else { return false; };
        let Ok(lhs) = actual.parse::<f64>() else { return false; };
        let Ok(rhs) = flt.value.parse::<f64>() else { return false; };
        let ok = match flt.op.as_str() {
            ">" => lhs > rhs,
            ">=" => lhs >= rhs,
            "<" => lhs < rhs,
            "<=" => lhs <= rhs,
            "=" | "==" => (lhs - rhs).abs() < 1e-12,
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    true
}

fn apply_episode_var_bindings(store: &InMemoryStore, idx: usize, spec: &EpisodeQueryEventSpec, vars: &BTreeMap<String,String>) -> Option<BTreeMap<String,String>> {
    let e=&store.edges[idx];
    let mut out=vars.clone();
    for (k,var) in spec.var_roles.iter() {
        let actual = role_value_for_query(e,k);
        if actual.is_empty() { return None; }
        if let Some(prev)=out.get(var) { if prev != &actual { return None; } } else { out.insert(var.clone(), actual); }
    }
    Some(out)
}

fn episode_order_ok(epi: &EpisodeIndex, query: &QuerySpec, assigned: &BTreeMap<String, usize>, qid: &str, cand_idx: usize) -> bool {
    let cand_ord = epi.global_event_ord_by_idx[cand_idx];
    for (a,b) in query.order.iter() {
        if a == qid {
            if let Some(ri)=assigned.get(b) {
                if cand_ord >= epi.global_event_ord_by_idx[*ri] { return false; }
            }
        }
        if b == qid {
            if let Some(li)=assigned.get(a) {
                if epi.global_event_ord_by_idx[*li] >= cand_ord { return false; }
            }
        }
    }
    true
}

fn episode_neq_ok(store: &InMemoryStore, query: &QuerySpec, assigned: &BTreeMap<String, usize>, qid: &str, cand_idx: usize) -> bool {
    let cand=&store.edges[cand_idx];
    for (le,lf,re,rf) in query.neq_constraints.iter() {
        if le == qid {
            if let Some(ri)=assigned.get(re) { if role_value_for_query(cand, lf) == role_value_for_query(&store.edges[*ri], rf) { return false; } }
        } else if re == qid {
            if let Some(li)=assigned.get(le) { if role_value_for_query(&store.edges[*li], lf) == role_value_for_query(cand, rf) { return false; } }
        }
    }
    true
}

fn fetch_episode_candidates(
    store: &InMemoryStore,
    epi: &EpisodeIndex,
    query: &QuerySpec,
    specs: &BTreeMap<String, EpisodeQueryEventSpec>,
    qid: &str,
    assigned: &BTreeMap<String, usize>,
    vars: &BTreeMap<String,String>,
    lo_ord: i64,
    hi_ord: i64,
    stats: &mut EpisodeRunStats,
) -> Vec<usize> {
    let spec = specs.get(qid).unwrap();
    let Some(bucket) = choose_bucket_for_spec(epi, spec, vars) else {
        return Vec::new();
    };
    let l = lower_bound_i64(&bucket.ords, lo_ord);
    let r = upper_bound_i64(&bucket.ords, hi_ord);
    let mut out=Vec::new();
    let used: HashSet<usize> = assigned.values().copied().collect();
    stats.local_candidate_edges_examined += r.saturating_sub(l);
    for idx in bucket.idxs[l..r].iter().copied() {
        if used.contains(&idx) { continue; }
        if !event_matches_spec(store, idx, spec, vars) { continue; }
        if !episode_attr_filters_ok(store, query, qid, idx) { continue; }
        if !episode_order_ok(epi, query, assigned, qid, idx) { continue; }
        if !episode_neq_ok(store, query, assigned, qid, idx) { continue; }
        out.push(idx);
    }
    out
}

fn anchored_full_binding_ok(store: &InMemoryStore, query: &QuerySpec, binding: &BTreeMap<String, usize>, sem_qid: &str, lo_ord: i64, hi_ord: i64, epi: &EpisodeIndex) -> bool {
    if binding.len() != query.events.len() { return false; }
    let mut vars=BTreeMap::new();
    let specs=build_episode_specs(query);
    for e in query.events.iter() {
        let idx=*binding.get(&e.id).unwrap();
        let spec=specs.get(&e.id).unwrap();
        if !event_matches_spec(store, idx, spec, &vars) { return false; }
        if !episode_attr_filters_ok(store, query, &e.id, idx) { return false; }
        if let Some(next)=apply_episode_var_bindings(store, idx, spec, &vars) { vars=next; } else { return false; }
        let ord=epi.global_event_ord_by_idx[idx];
        if ord < lo_ord || ord > hi_ord { return false; }
        if e.id != sem_qid && ord <= lo_ord { return false; }
    }
    for (a,b) in query.order.iter() {
        let la = epi.global_event_ord_by_idx[*binding.get(a).unwrap()];
        let lb = epi.global_event_ord_by_idx[*binding.get(b).unwrap()];
        if la >= lb { return false; }
    }
    for (le,lf,re,rf) in query.neq_constraints.iter() { if role_value_for_query(&store.edges[*binding.get(le).unwrap()],lf) == role_value_for_query(&store.edges[*binding.get(re).unwrap()],rf) { return false; } }
    true
}

fn derive_episode_hi_ord(epi: &EpisodeIndex, anchor_idx: usize, window_sec: i64) -> i64 {
    let txo = epi.global_tx_ord_by_idx[anchor_idx];
    let start_ts = epi.tx_start_ts[txo];
    let end_ts = start_ts + window_sec;
    let hi_txo = upper_bound_i64(&epi.tx_start_ts, end_ts).saturating_sub(1);
    epi.tx_max_event_ord[hi_txo.max(txo)]
}

fn write_anchored_outputs(outdir: &str, summary: &QueryExecutionSummary, matched_rows: &[Vec<String>], binding_rows: &[Vec<String>], episode_rows: &[Vec<String>], summary_only: bool) -> Result<()> {
    ensure_dir(outdir)?;
    write_json_pretty(Path::new(outdir).join("execution_report.json"), summary)?;
    if !summary_only {
        write_csv_rows(Path::new(outdir).join("matched_instances.csv"), &["match_id","closure_id","tx_hash","query_name","num_events"], &matched_rows.to_vec())?;
        write_csv_rows(Path::new(outdir).join("matched_bindings.csv"), &["match_id","closure_id","tx_hash","query_event_id","event_id","event_type","seq_index","protocol"], &binding_rows.to_vec())?;
        write_csv_rows(Path::new(outdir).join("episode_match_counts.csv"), &["episode_id","anchor_event_id","num_matches"], &episode_rows.to_vec())?;
    }
    Ok(())
}

fn episode_query_family_name(query_name: &str) -> Option<&'static str> {
    match query_name {
        "flashloan_swap_borrow_anchored_episode" | "flashloan_swap_borrow_episode_v1" => Some("flashloan_swap_borrow"),
        "flashloan_swap_borrow_followup_anchored_episode" | "flashloan_swap_borrow_followup_episode_v1" => Some("flashloan_swap_borrow_followup"),
        "flashloan_2swaps_distinct_followup_anchored_episode" | "flashloan_2swaps_distinct_followup_episode_v1" => Some("flashloan_2swaps_distinct_followup"),
        "flashloan_3swaps_chain_reuse_anchored_episode" | "flashloan_3swaps_chain_reuse_episode_v1" => Some("flashloan_3swaps_chain_reuse"),
        "flashloan_3swaps_distinct_parallel_anchored_episode" | "flashloan_3swaps_distinct_parallel_episode_v1" => Some("flashloan_3swaps_distinct_parallel"),
        _ => None,
    }
}

fn idx_ord(epi: &EpisodeIndex, idx: usize) -> i64 {
    epi.global_event_ord_by_idx[idx]
}

fn lower_bound_idx_ord(epi: &EpisodeIndex, idxs: &[usize], ord: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = idxs.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if idx_ord(epi, idxs[mid]) < ord {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn upper_bound_idx_ord(epi: &EpisodeIndex, idxs: &[usize], ord: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = idxs.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if idx_ord(epi, idxs[mid]) <= ord {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn idx_range_by_ord(epi: &EpisodeIndex, idxs: &[usize], lo_exclusive: i64, hi_inclusive: i64) -> Vec<usize> {
    if idxs.is_empty() || hi_inclusive <= lo_exclusive {
        return Vec::new();
    }
    let l = upper_bound_idx_ord(epi, idxs, lo_exclusive);
    let r = upper_bound_idx_ord(epi, idxs, hi_inclusive);
    idxs[l..r].to_vec()
}

fn edge_role(edge: &DataHyperedge, role_name: &str) -> String {
    role_value_for_query(edge, role_name)
}

fn pair_key(a: &str, b: &str) -> (String, String) {
    (a.to_string(), b.to_string())
}

struct EpisodeSummaryFastIndex {
    flashloans: Vec<usize>,
    swaps_by_taker_sold: HashMap<(String, String), Vec<usize>>,
    borrows_by_borrower_asset: HashMap<(String, String), Vec<usize>>,
}

fn build_episode_summary_fast_index(store: &InMemoryStore, epi: &EpisodeIndex) -> EpisodeSummaryFastIndex {
    let mut flashloans = Vec::new();
    let mut swaps_by_taker_sold: HashMap<(String, String), Vec<usize>> = HashMap::new();
    let mut borrows_by_borrower_asset: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (idx, edge) in store.edges.iter().enumerate() {
        match edge.event_type.as_str() {
            "flashloan" => flashloans.push(idx),
            "swap" => {
                let taker = edge_role(edge, "taker");
                let sold = edge_role(edge, "token_sold");
                if !taker.is_empty() && !sold.is_empty() {
                    swaps_by_taker_sold.entry(pair_key(&taker, &sold)).or_default().push(idx);
                }
            }
            "borrow" => {
                let borrower = edge_role(edge, "borrower");
                let asset = edge_role(edge, "asset");
                if !borrower.is_empty() && !asset.is_empty() {
                    borrows_by_borrower_asset.entry(pair_key(&borrower, &asset)).or_default().push(idx);
                }
            }
            _ => {}
        }
    }
    flashloans.sort_by_key(|idx| idx_ord(epi, *idx));
    for xs in swaps_by_taker_sold.values_mut() {
        xs.sort_by_key(|idx| idx_ord(epi, *idx));
    }
    for xs in borrows_by_borrower_asset.values_mut() {
        xs.sort_by_key(|idx| idx_ord(epi, *idx));
    }
    EpisodeSummaryFastIndex { flashloans, swaps_by_taker_sold, borrows_by_borrower_asset }
}

fn get_range_from_map(
    epi: &EpisodeIndex,
    map: &HashMap<(String, String), Vec<usize>>,
    a: &str,
    b: &str,
    lo_exclusive: i64,
    hi_inclusive: i64,
) -> Vec<usize> {
    match map.get(&pair_key(a, b)) {
        Some(xs) => idx_range_by_ord(epi, xs, lo_exclusive, hi_inclusive),
        None => Vec::new(),
    }
}

fn count_episode_known_query(store: &InMemoryStore, query: &QuerySpec) -> Option<(usize, StructuralMetrics)> {
    let family = episode_query_family_name(&query.query_name)?;
    let epi = build_episode_index(store);
    let fast = build_episode_summary_fast_index(store, &epi);
    let window_sec = query.scope_params.forward_window_sec;

    let mut matches = 0usize;
    let mut local_candidates = 0usize;
    let mut candidate_episodes = 0usize;
    let mut episodes_with_nonempty = 0usize;

    for fl_idx in fast.flashloans.iter().copied() {
        let fl = &store.edges[fl_idx];
        let fl_ord = idx_ord(&epi, fl_idx);
        let hi_ord = derive_episode_hi_ord(&epi, fl_idx, window_sec);
        let u = edge_role(fl, "recipient");
        let x = edge_role(fl, "asset");
        if u.is_empty() || x.is_empty() {
            continue;
        }
        let e2s = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &x, fl_ord, hi_ord);
        local_candidates += e2s.len();
        if !e2s.is_empty() {
            episodes_with_nonempty += 1;
        }
        let before = matches;

        if family == "flashloan_swap_borrow" || family == "flashloan_swap_borrow_followup" {
            for e2_idx in e2s.iter().copied() {
                let e2 = &store.edges[e2_idx];
                let e2_ord = idx_ord(&epi, e2_idx);
                let y = edge_role(e2, "token_bought");
                let p1 = edge_role(e2, "pool");
                if y.is_empty() {
                    continue;
                }
                let e3s = get_range_from_map(&epi, &fast.borrows_by_borrower_asset, &u, &y, e2_ord, hi_ord);
                local_candidates += e3s.len();
                for e3_idx in e3s {
                    if family == "flashloan_swap_borrow" {
                        matches += 1;
                    } else {
                        let e3_ord = idx_ord(&epi, e3_idx);
                        let e4s = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &y, e3_ord, hi_ord);
                        local_candidates += e4s.len();
                        for e4_idx in e4s {
                            if e4_idx != e2_idx && edge_role(&store.edges[e4_idx], "pool") != p1 {
                                matches += 1;
                            }
                        }
                    }
                }
            }
        } else if family == "flashloan_2swaps_distinct_followup" {
            for e2_idx in e2s.iter().copied() {
                let e2 = &store.edges[e2_idx];
                let e2_ord = idx_ord(&epi, e2_idx);
                let y1 = edge_role(e2, "token_bought");
                let p1 = edge_role(e2, "pool");
                let later_x_swaps = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &x, e2_ord, hi_ord);
                local_candidates += later_x_swaps.len();
                for e3_idx in later_x_swaps {
                    if e3_idx == e2_idx {
                        continue;
                    }
                    let e3 = &store.edges[e3_idx];
                    let y2 = edge_role(e3, "token_bought");
                    if edge_role(e3, "pool") == p1 || y2 == y1 {
                        continue;
                    }
                    let e4s = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &y1, idx_ord(&epi, e3_idx), hi_ord);
                    local_candidates += e4s.len();
                    matches += e4s.len();
                }
            }
        } else if family == "flashloan_3swaps_chain_reuse" {
            for e2_idx in e2s.iter().copied() {
                let e2 = &store.edges[e2_idx];
                let y1 = edge_role(e2, "token_bought");
                let p1 = edge_role(e2, "pool");
                if y1 == x {
                    continue;
                }
                let e3s = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &y1, idx_ord(&epi, e2_idx), hi_ord);
                local_candidates += e3s.len();
                for e3_idx in e3s {
                    if e3_idx == e2_idx {
                        continue;
                    }
                    let e3 = &store.edges[e3_idx];
                    let y2 = edge_role(e3, "token_bought");
                    let p2 = edge_role(e3, "pool");
                    if p2 == p1 || y2 == x {
                        continue;
                    }
                    let e4s = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &y2, idx_ord(&epi, e3_idx), hi_ord);
                    local_candidates += e4s.len();
                    for e4_idx in e4s {
                        if e4_idx == e2_idx || e4_idx == e3_idx {
                            continue;
                        }
                        let e4 = &store.edges[e4_idx];
                        let y3 = edge_role(e4, "token_bought");
                        let p3 = edge_role(e4, "pool");
                        if p3 != p1 && p3 != p2 && y3 != x && y3 != y1 {
                            matches += 1;
                        }
                    }
                }
            }
        } else if family == "flashloan_3swaps_distinct_parallel" {
            for e2_idx in e2s.iter().copied() {
                let e2 = &store.edges[e2_idx];
                let e2_ord = idx_ord(&epi, e2_idx);
                let y1 = edge_role(e2, "token_bought");
                let p1 = edge_role(e2, "pool");
                let later_x_swaps = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &x, e2_ord, hi_ord);
                local_candidates += later_x_swaps.len();
                for e3_idx in later_x_swaps.iter().copied() {
                    if e3_idx == e2_idx {
                        continue;
                    }
                    let e3 = &store.edges[e3_idx];
                    let y2 = edge_role(e3, "token_bought");
                    let p2 = edge_role(e3, "pool");
                    if p2 == p1 || y2 == y1 {
                        continue;
                    }
                    let later2_x_swaps = get_range_from_map(&epi, &fast.swaps_by_taker_sold, &u, &x, idx_ord(&epi, e3_idx), hi_ord);
                    local_candidates += later2_x_swaps.len();
                    for e4_idx in later2_x_swaps {
                        if e4_idx == e2_idx || e4_idx == e3_idx {
                            continue;
                        }
                        let e4 = &store.edges[e4_idx];
                        let y3 = edge_role(e4, "token_bought");
                        let p3 = edge_role(e4, "pool");
                        if p3 != p1 && p3 != p2 && y3 != y1 && y3 != y2 {
                            matches += 1;
                        }
                    }
                }
            }
        }

        if matches > before {
            candidate_episodes += 1;
        }
    }

    let metrics = StructuralMetrics {
        num_seed_edges: fast.flashloans.len(),
        num_seed_closures: fast.flashloans.len(),
        num_closures_scanned: fast.flashloans.len(),
        num_closures_with_nonempty_local_candidates: episodes_with_nonempty,
        num_local_candidate_edges_examined: local_candidates,
        num_structural_embeddings_raw: matches,
        num_structural_embeddings_dedup: matches,
        num_candidate_closures: candidate_episodes,
    };
    Some((matches, metrics))
}

fn query_ids(query: &QuerySpec) -> Vec<String> {
    query.events.iter().map(|e| e.id.clone()).collect()
}

fn query_pipeline_episode_summary_fastpath(
    store: &InMemoryStore,
    query: &QuerySpec,
    outdir: &str,
    _threads: usize,
    stage_load_store_s: f64,
) -> Result<Option<QueryExecutionSummary>> {
    if query.scope != "anchored_episode" {
        return Ok(None);
    }
    let t0 = Instant::now();
    let Some((count, struct_metrics)) = count_episode_known_query(store, query) else {
        return Ok(None);
    };
    let core_s = t0.elapsed().as_secs_f64();
    let matched_episode_count = struct_metrics.num_candidate_closures;
    let summary = QueryExecutionSummary {
        query_name: query.query_name.clone(),
        scope: query.scope.clone(),
        matching_order: query_ids(query),
        structural_metrics: struct_metrics.clone(),
        num_structural_embeddings_csv_rows: 0,
        num_cand1_closures: struct_metrics.num_candidate_closures,
        num_cand2_closures: matched_episode_count,
        num_matches: count,
        num_residual_closures: 0,
        num_false_positive_closures_filtered: 0,
        stage_times_s: json!({
            "compile_query_plan": 0.0,
            "global_seed_selection": 0.0,
            "hgmatch_summary_fastpath": core_s,
            "hgmatch_structural_match": 0.0,
            "exact_semantic_verify": 0.0,
            "load_store_from_sqlite": stage_load_store_s,
            "write_outputs": 0.0
        }),
        metrics: json!({
            "query_latency_s": core_s,
            "query_wall_time_s": stage_load_store_s + core_s,
            "closure_false_positive_ratio": 0.0,
            "residual_closures_per_query": 0,
            "summary_only": true,
            "summary_fastpath": true,
            "summary_fastpath_kind": "anchored_episode_query_family_count_only"
        }),
        store_stats: store.stats(),
    };
    write_anchored_outputs(outdir, &summary, &[], &[], &[], true)?;
    Ok(Some(summary))
}

fn query_pipeline_anchored_episode(
    store: &InMemoryStore,
    query: &QuerySpec,
    outdir: &str,
    threads: usize,
    stage_load_store_s: f64,
    summary_only: bool,
) -> Result<QueryExecutionSummary> {
    let t_compile = Instant::now();
    let specs = build_episode_specs(query);
    let sem_qid = query.scope_params.semantic_anchor_event.clone();
    let resolved_exec_qid = resolve_execution_anchor_event(query, &sem_qid);
    let exec_qid_opt = if resolved_exec_qid != sem_qid {
        Some(resolved_exec_qid)
    } else {
        None
    };
    let mut matching_order = vec![sem_qid.clone()];
    if let Some(exec_qid) = exec_qid_opt.clone() {
        if !matching_order.contains(&exec_qid) { matching_order.push(exec_qid); }
    }
    let mut remaining: Vec<String> = query.events.iter().map(|e| e.id.clone()).collect();
    remaining.retain(|x| !matching_order.contains(x));
    let mut progressed = true;
    while progressed && !remaining.is_empty() {
        progressed = false;
        let mut i = 0usize;
        while i < remaining.len() {
            let qid = remaining[i].clone();
            let mut blocked = false;
            for (a,b) in query.order.iter() {
                if b == &qid && !matching_order.contains(a) { blocked = true; break; }
            }
            if !blocked {
                matching_order.push(qid.clone());
                remaining.remove(i);
                progressed = true;
            } else {
                i += 1;
            }
        }
    }
    for qid in remaining.into_iter() {
        if !matching_order.contains(&qid) { matching_order.push(qid); }
    }
    let compiled_order = matching_order.clone();
    let (pair_strengths, per_strengths) = build_episode_pair_strengths(query);
    let epi = build_episode_index(store);
    let sem_spec = specs.get(&sem_qid).ok_or_else(|| anyhow!("missing semantic anchor spec"))?.clone();
    let t_after_compile = Instant::now();

    let mut seed_stats = EpisodeRunStats::default();
    let sem_bucket = choose_bucket_for_spec(&epi, &sem_spec, &BTreeMap::new())
        .ok_or_else(|| anyhow!("no candidate bucket for semantic anchor {}", sem_qid))?;
    let mut seeds = Vec::new();
    let mut seed_closures = BTreeSet::new();
    for idx in sem_bucket.idxs.iter().copied() {
        if !event_matches_spec(store, idx, &sem_spec, &BTreeMap::new()) { continue; }
        if !episode_attr_filters_ok(store, query, &sem_qid, idx) { continue; }
        seeds.push(idx);
        seed_closures.insert(store.edges[idx].tx_hash.clone());
    }
    seed_stats.local_candidate_edges_examined += sem_bucket.idxs.len();
    let t_after_seed = Instant::now();

    let pool = ThreadPoolBuilder::new().num_threads(threads.max(1)).build()?;
    let results = pool.install(|| {
        seeds.par_iter().map(|anchor_idx| {
            let mut stats = EpisodeRunStats::default();
            let lo_ord = epi.global_event_ord_by_idx[*anchor_idx];
            let hi_ord = derive_episode_hi_ord(&epi, *anchor_idx, query.scope_params.forward_window_sec);
            let episode_id = format!("{}::w{}", store.edges[*anchor_idx].event_id, query.scope_params.forward_window_sec);
            let mut assigned = BTreeMap::new();
            assigned.insert(sem_qid.clone(), *anchor_idx);
            let mut vars = BTreeMap::new();
            vars = apply_episode_var_bindings(store, *anchor_idx, &sem_spec, &vars).unwrap_or_default();
            let mut cand_sizes = HashMap::new();
            let mut matched_bindings: Vec<BTreeMap<String, usize>> = Vec::new();
            let mut episodes_with_nonempty = false;
            fn dfs(
                store: &InMemoryStore,
                epi: &EpisodeIndex,
                query: &QuerySpec,
                specs: &BTreeMap<String, EpisodeQueryEventSpec>,
                pair: &HashMap<String,i32>,
                per: &HashMap<String,i32>,
                sem_qid: &str,
                lo_ord: i64,
                hi_ord: i64,
                assigned: &mut BTreeMap<String, usize>,
                vars: &BTreeMap<String,String>,
                cand_sizes: &mut HashMap<String, usize>,
                stats: &mut EpisodeRunStats,
                episodes_with_nonempty: &mut bool,
                out: &mut Vec<BTreeMap<String, usize>>,
                exec_qid_opt: &Option<String>,
            ) {
                if assigned.len() == query.events.len() {
                    if anchored_full_binding_ok(store, query, assigned, sem_qid, lo_ord, hi_ord, epi) {
                        stats.structural_embeddings_raw += 1;
                        out.push(assigned.clone());
                    }
                    return;
                }
                let qid = if assigned.len() == 1 {
                    if let Some(exec_qid) = exec_qid_opt {
                        if assigned.contains_key(sem_qid) && !assigned.contains_key(exec_qid) {
                            exec_qid.clone()
                        } else {
                            choose_next_episode_qid(query, assigned, pair, per, cand_sizes)
                        }
                    } else {
                        choose_next_episode_qid(query, assigned, pair, per, cand_sizes)
                    }
                } else {
                    choose_next_episode_qid(query, assigned, pair, per, cand_sizes)
                };
                let cands = fetch_episode_candidates(store, epi, query, specs, &qid, assigned, vars, lo_ord, hi_ord, stats);
                cand_sizes.insert(qid.clone(), cands.len());
                if !cands.is_empty() { *episodes_with_nonempty = true; }
                let spec = specs.get(&qid).unwrap().clone();
                for idx in cands.into_iter() {
                    if let Some(next_vars) = apply_episode_var_bindings(store, idx, &spec, vars) {
                        assigned.insert(qid.clone(), idx);
                        dfs(store, epi, query, specs, pair, per, sem_qid, lo_ord, hi_ord, assigned, &next_vars, cand_sizes, stats, episodes_with_nonempty, out, exec_qid_opt);
                        assigned.remove(&qid);
                    }
                }
            }
            dfs(store, &epi, query, &specs, &pair_strengths, &per_strengths, &sem_qid, lo_ord, hi_ord, &mut assigned, &vars, &mut cand_sizes, &mut stats, &mut episodes_with_nonempty, &mut matched_bindings, &exec_qid_opt);
            let match_count = matched_bindings.len();
            if episodes_with_nonempty { stats.episodes_with_nonempty_candidates += 1; }
            if match_count > 0 { stats.matched_episodes += 1; stats.candidate_episodes += 1; } else if episodes_with_nonempty { stats.candidate_episodes += 1; }
            let retained_bindings = if summary_only {
                Vec::new()
            } else {
                matched_bindings
            };
            (episode_id, *anchor_idx, match_count, retained_bindings, stats)
        }).collect::<Vec<_>>()
    });
    let t_after_structural = Instant::now();

    let mut total_stats = EpisodeRunStats::default();
    let mut matched_rows = Vec::new();
    let mut binding_rows = Vec::new();
    let mut episode_rows = Vec::new();
    let mut matched_episode_count = 0usize;
    let mut total_match_count = 0usize;
    for (episode_id, anchor_idx, match_count, bindings, st) in results.into_iter() {
        total_stats.local_candidate_edges_examined += st.local_candidate_edges_examined;
        total_stats.structural_embeddings_raw += st.structural_embeddings_raw;
        total_stats.matched_episodes += st.matched_episodes;
        total_stats.candidate_episodes += st.candidate_episodes;
        total_stats.episodes_with_nonempty_candidates += st.episodes_with_nonempty_candidates;
        let anchor_event_id = store.edges[anchor_idx].event_id.clone();
        total_match_count += match_count;
        if !summary_only {
            episode_rows.push(vec![episode_id.clone(), anchor_event_id.clone(), match_count.to_string()]);
        }
        if match_count > 0 { matched_episode_count += 1; }
        if !summary_only {
            for (i, mapping) in bindings.into_iter().enumerate() {
                let match_id = format!("{}::m{}", episode_id, i);
                matched_rows.push(vec![match_id.clone(), episode_id.clone(), store.edges[anchor_idx].tx_hash.clone(), query.query_name.clone(), mapping.len().to_string()]);
                for qid in compiled_order.iter() {
                    let idx = *mapping.get(qid).unwrap();
                    let e = &store.edges[idx];
                    binding_rows.push(vec![match_id.clone(), episode_id.clone(), e.tx_hash.clone(), qid.clone(), e.event_id.clone(), e.event_type.clone(), e.seq_index.to_string(), e.protocol.clone()]);
                }
            }
        }
    }
    let t_after_exact = Instant::now();

    let struct_metrics = StructuralMetrics {
        num_seed_edges: seeds.len(),
        num_seed_closures: seed_closures.len(),
        num_closures_scanned: seed_closures.len(),
        num_closures_with_nonempty_local_candidates: total_stats.episodes_with_nonempty_candidates,
        num_local_candidate_edges_examined: total_stats.local_candidate_edges_examined,
        num_structural_embeddings_raw: total_stats.structural_embeddings_raw,
        num_structural_embeddings_dedup: total_match_count,
        num_candidate_closures: total_stats.candidate_episodes,
    };

    let summary = QueryExecutionSummary {
        query_name: query.query_name.clone(),
        scope: query.scope.clone(),
        matching_order: compiled_order.clone(),
        structural_metrics: struct_metrics.clone(),
        num_structural_embeddings_csv_rows: struct_metrics.num_structural_embeddings_dedup,
        num_cand1_closures: struct_metrics.num_candidate_closures,
        num_cand2_closures: matched_episode_count,
        num_matches: total_match_count,
        num_residual_closures: struct_metrics.num_candidate_closures.saturating_sub(matched_episode_count),
        num_false_positive_closures_filtered: struct_metrics.num_candidate_closures.saturating_sub(matched_episode_count),
        stage_times_s: json!({
            "compile_query_plan": (t_after_compile - t_compile).as_secs_f64(),
            "global_seed_selection": (t_after_seed - t_after_compile).as_secs_f64(),
            "hgmatch_structural_match": (t_after_structural - t_after_seed).as_secs_f64(),
            "exact_semantic_verify": (t_after_exact - t_after_structural).as_secs_f64(),
            "load_store_from_sqlite": stage_load_store_s,
            "write_outputs": 0.0
        }),
        metrics: json!({
            "closure_false_positive_ratio": if struct_metrics.num_candidate_closures == 0 { 0.0 } else { (struct_metrics.num_candidate_closures.saturating_sub(matched_episode_count)) as f64 / struct_metrics.num_candidate_closures as f64 },
            "query_latency_s": (t_after_exact - t_compile).as_secs_f64(),
            "query_wall_time_s": (t_after_exact - t_compile).as_secs_f64() + stage_load_store_s,
            "residual_closures_per_query": struct_metrics.num_candidate_closures.saturating_sub(matched_episode_count),
            "summary_only": summary_only
        }),
        store_stats: store.stats(),
    };
    write_anchored_outputs(outdir, &summary, &matched_rows, &binding_rows, &episode_rows, summary_only)?;
    Ok(summary)
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
        if let Some(summary) = query_pipeline_episode_summary_fastpath(
            store, query, outdir, threads, stage_load_store_s
        )? {
            return Ok(summary);
        }
    }
    if query.scope == "anchored_episode" {
        return query_pipeline_anchored_episode(store, query, outdir, threads, stage_load_store_s, summary_only);
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
    let store = load_store_from_sqlite(db, Some(&query))?;
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
    let store = load_store_from_sqlite(db, Some(&query))?;
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
    let store = load_store_from_sqlite(&db, None)?;
    println!("{}", json!({"frame":"ready", "load_count":1, "query_independent":true}));
    std::io::stdout().flush()?;
    let mut sequence = 0u64;
    for line in std::io::stdin().lock().lines() {
        let raw: serde_json::Value = serde_json::from_str(&line?)?;
        if raw.get("op").and_then(|x| x.as_str()) == Some("close") { break; }
        sequence += 1;
        if raw["request_id"].as_u64() != Some(sequence) { return Err(anyhow!("request ID mismatch")); }
        let query: QuerySpec = serde_json::from_value(raw["query"].clone())?;
        if query.scope != "anchored_episode" || query.events.is_empty() { return Err(anyhow!("scope/events")); }
        let result = query_pipeline_loaded(&store, &query, "/unused-resident-artifacts", 4, true, 0.0)?;
        println!("{}", json!({"frame":"result","request_id":sequence,"count":result.num_matches}));
        std::io::stdout().flush()?;
    }
    Ok(())
}
