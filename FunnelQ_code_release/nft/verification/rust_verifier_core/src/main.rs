use anyhow::{bail, Context, Result};
use clap::Parser;
use csv::StringRecord;
use flate2::read::GzDecoder;
use rusqlite::{params, Connection, OptionalExtension, Row};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const PROTOCOL_VERSION: &str = "funnelq_nft_compact_v4_formal_vo_v3";
const COLUMNS: [&str; 8] = [
    "query_id",
    "branch_id",
    "closure_id",
    "anchor_order",
    "instance_key",
    "event_bindings_json",
    "event_orders_json",
    "variable_bindings_json",
];
const EVENT_FIELDS: [&str; 15] = [
    "order_seq",
    "event_id",
    "event_type_id",
    "event_type",
    "block_ts",
    "src_addr_id",
    "src_addr",
    "dst_addr_id",
    "dst_addr",
    "nft_id",
    "nft",
    "collection_id",
    "collection",
    "tx_id",
    "tx_hash",
];

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    package: PathBuf,
    #[arg(long)]
    query: PathBuf,
    #[arg(long)]
    instances: PathBuf,
    #[arg(long)]
    work_db: PathBuf,
    #[arg(long)]
    out_json: PathBuf,
    #[arg(long, default_value_t = 100_000)]
    progress_every: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    position: i64,
    closure_id: i64,
    anchor_order_seq: i64,
    start_ts: i64,
    end_ts: i64,
    end_order_seq: i64,
    collection_id: i64,
    transition_count: i64,
    addr_count: i64,
    nft_count: i64,
    nft_ge2_count: i64,
    b_out_event_count: i64,
    root_event_type_id: i64,
    l2_ba_nft_equal_min_delta: i64,
    l2_ba_nft_different_min_delta: i64,
    l3_bcca_all_equal_min_delta: i64,
    l3_bcca_all_distinct_min_delta: i64,
    l3_bcca_root_back_equal_min_delta: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClosureDescriptor {
    closure_position: i64,
    is_anchor: i64,
    closure_id: i64,
    anchor_order_seq: i64,
    start_ts: i64,
    end_ts: i64,
    end_order_seq: i64,
    collection_id: i64,
    transition_count: i64,
    addr_count: i64,
    nft_count: i64,
    nft_ge2_count: i64,
    b_out_event_count: i64,
    root_event_type_id: i64,
    l2_ba_nft_equal_min_delta: i64,
    l2_ba_nft_different_min_delta: i64,
    l3_bcca_all_equal_min_delta: i64,
    l3_bcca_all_distinct_min_delta: i64,
    l3_bcca_root_back_equal_min_delta: i64,
}

impl ClosureDescriptor {
    fn as_descriptor(&self, position: i64) -> Descriptor {
        Descriptor {
            position,
            closure_id: self.closure_id,
            anchor_order_seq: self.anchor_order_seq,
            start_ts: self.start_ts,
            end_ts: self.end_ts,
            end_order_seq: self.end_order_seq,
            collection_id: self.collection_id,
            transition_count: self.transition_count,
            addr_count: self.addr_count,
            nft_count: self.nft_count,
            nft_ge2_count: self.nft_ge2_count,
            b_out_event_count: self.b_out_event_count,
            root_event_type_id: self.root_event_type_id,
            l2_ba_nft_equal_min_delta: self.l2_ba_nft_equal_min_delta,
            l2_ba_nft_different_min_delta: self.l2_ba_nft_different_min_delta,
            l3_bcca_all_equal_min_delta: self.l3_bcca_all_equal_min_delta,
            l3_bcca_all_distinct_min_delta: self.l3_bcca_all_distinct_min_delta,
            l3_bcca_root_back_equal_min_delta: self.l3_bcca_root_back_equal_min_delta,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Aggregate {
    count: i64,
    start_position: i64,
    end_position: i64,
    max_transition_count: i64,
    max_addr_count: i64,
    max_nft_count: i64,
    min_transition_count: i64,
    min_addr_count: i64,
    min_nft_count: i64,
    min_l2_ba_nft_equal_min_delta: i64,
    min_l2_ba_nft_different_min_delta: i64,
    min_l3_bcca_all_equal_min_delta: i64,
    min_l3_bcca_all_distinct_min_delta: i64,
    min_l3_bcca_root_back_equal_min_delta: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CoverageRecord {
    level: i64,
    index: i64,
    aggregate: Aggregate,
    node_hash: String,
    kind: String,
    anchor_directory_node_hash: String,
    #[serde(default)]
    descriptor: Option<Descriptor>,
    #[serde(default)]
    left_hash: Option<String>,
    #[serde(default)]
    right_hash: Option<String>,
    #[serde(default)]
    duplicated: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ClosureRecord {
    descriptor: ClosureDescriptor,
    leaf_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Event {
    order_seq: i64,
    event_id: String,
    event_type_id: i64,
    event_type: String,
    block_ts: i64,
    src_addr_id: i64,
    src_addr: String,
    dst_addr_id: i64,
    dst_addr: String,
    nft_id: i64,
    nft: String,
    collection_id: i64,
    collection: String,
    tx_id: i64,
    tx_hash: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpenedEvent {
    position: i64,
    event: Event,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MerklePathItem {
    sibling: String,
    sibling_on_left: bool,
    duplicated: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FrontierItem {
    level: i64,
    index: i64,
    hash: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CollectionOpening {
    directory_position: i64,
    collection_id: i64,
    event_count: i64,
    global_start_position: i64,
    first_order_seq: i64,
    last_order_seq: i64,
    event_root: String,
    directory_leaf_hash: String,
    directory_path: Vec<MerklePathItem>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RangeRecord {
    range_id: i64,
    collection: CollectionOpening,
    left_position: i64,
    right_position: i64,
    events: Vec<OpenedEvent>,
    range_frontier: Vec<FrontierItem>,
    tx_left_position: i64,
    tx_right_position: i64,
    tx_range_frontier: Vec<FrontierItem>,
    tx_leaf_hashes: Vec<String>,
}

#[derive(Debug, Default, Serialize)]
struct Comparison {
    generated: i64,
    claimed: i64,
    missing: i64,
    extra: i64,
    changed: i64,
    equal: bool,
}

#[derive(Debug, Default)]
struct CoverageStats {
    variant: String,
    base_frontier: i64,
    base_anchors: i64,
    refined_frontier: i64,
    refined_anchors: i64,
    replay_frontier: i64,
    replay: i64,
    vocov_ms: f64,
    voneg_base_ms: f64,
    voneg_refined_ms: f64,
}

#[derive(Debug, Serialize)]
struct Report {
    status: &'static str,
    #[serde(rename = "verifierEngine")]
    verifier_engine: &'static str,
    variant: String,
    #[serde(rename = "cand0Count")]
    cand0_count: i64,
    #[serde(rename = "cand1Count")]
    cand1_count: i64,
    #[serde(rename = "replayCount")]
    replay_count: i64,
    #[serde(rename = "cand2Count")]
    cand2_count: Option<i64>,
    #[serde(rename = "resultCount")]
    result_count: i64,
    #[serde(rename = "generatedResultCount")]
    generated_result_count: i64,
    vocov_ms: f64,
    voneg_base_ms: f64,
    voneg_refined_ms: f64,
    vorep_ms: f64,
    exact_match_ms: f64,
    result_compare_ms: f64,
    #[serde(rename = "uniqueOpenedEventCount")]
    unique_opened_event_count: i64,
    #[serde(rename = "residualRangeCount")]
    residual_range_count: i64,
    #[serde(rename = "directSharedRangeReplay")]
    direct_shared_range_replay: bool,
    #[serde(rename = "temporaryReplayBytes")]
    temporary_replay_bytes: i64,
    #[serde(rename = "authenticatedCandidateDbBytes")]
    authenticated_candidate_db_bytes: u64,
    comparison: Comparison,
}

#[derive(Debug, Clone)]
struct Roots {
    summary_root: String,
    anchor_directory_root: String,
    closure_domain_root: String,
    event_stream_root: String,
    tx_binding_root: String,
    anchor_count: i64,
    closure_count: i64,
    collection_count: i64,
    event_count: i64,
}

fn compact(value: &Value) -> Result<String> {
    Ok(serde_json::to_string(value)?)
}

fn sorted_object(pairs: impl IntoIterator<Item = (String, Value)>) -> Value {
    let mut ordered = BTreeMap::<String, Value>::new();
    for (key, value) in pairs {
        ordered.insert(key, value);
    }
    Value::Object(ordered.into_iter().collect::<Map<String, Value>>())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_obj(domain: &str, value: &Value) -> Result<String> {
    let payload = compact(value)?;
    let mut digest = Sha256::new();
    digest.update(PROTOCOL_VERSION.as_bytes());
    digest.update([0u8]);
    digest.update(domain.as_bytes());
    digest.update([0u8]);
    digest.update((payload.as_bytes().len() as u64).to_be_bytes());
    digest.update(payload.as_bytes());
    Ok(format!("{:x}", digest.finalize()))
}

fn parent_hash(domain: &str, left: &str, right: &str) -> Result<String> {
    hash_obj(domain, &json!({"left": left, "right": right}))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn read_json(path: &Path) -> Result<Value> {
    Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

fn object_text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .with_context(|| format!("missing field {key}"))?
        .as_str()
        .with_context(|| format!("field {key} is not text"))
}

fn object_i64(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .with_context(|| format!("missing field {key}"))?
        .as_i64()
        .with_context(|| format!("field {key} is not integer"))
}

fn gzip_reader(path: &Path) -> Result<BufReader<GzDecoder<File>>> {
    Ok(BufReader::new(GzDecoder::new(File::open(path)?)))
}

fn gzip_has_records(path: &Path) -> Result<bool> {
    let mut line = String::new();
    Ok(gzip_reader(path)?.read_line(&mut line)? > 0 && !line.trim().is_empty())
}

fn progress(label: &str, current: u64, every: u64, started: Instant) {
    if every > 0 && current > 0 && current % every == 0 {
        eprintln!(
            "[{label}] processed={current} elapsed_s={:.1}",
            started.elapsed().as_secs_f64()
        );
    }
}

fn descriptor_value(descriptor: &Descriptor) -> Result<Value> {
    Ok(serde_json::to_value(descriptor)?)
}

fn closure_value(descriptor: &ClosureDescriptor) -> Result<Value> {
    Ok(serde_json::to_value(descriptor)?)
}

fn aggregate_value(aggregate: &Aggregate) -> Result<Value> {
    Ok(serde_json::to_value(aggregate)?)
}

fn leaf_aggregate(row: &Descriptor) -> Aggregate {
    Aggregate {
        count: 1,
        start_position: row.position,
        end_position: row.position,
        max_transition_count: row.transition_count,
        max_addr_count: row.addr_count,
        max_nft_count: row.nft_count,
        min_transition_count: row.transition_count,
        min_addr_count: row.addr_count,
        min_nft_count: row.nft_count,
        min_l2_ba_nft_equal_min_delta: row.l2_ba_nft_equal_min_delta,
        min_l2_ba_nft_different_min_delta: row.l2_ba_nft_different_min_delta,
        min_l3_bcca_all_equal_min_delta: row.l3_bcca_all_equal_min_delta,
        min_l3_bcca_all_distinct_min_delta: row.l3_bcca_all_distinct_min_delta,
        min_l3_bcca_root_back_equal_min_delta: row.l3_bcca_root_back_equal_min_delta,
    }
}

fn parent_aggregate(left: &Aggregate, right: &Aggregate) -> Aggregate {
    Aggregate {
        count: left.count + right.count,
        start_position: left.start_position,
        end_position: right.end_position,
        max_transition_count: left.max_transition_count.max(right.max_transition_count),
        max_addr_count: left.max_addr_count.max(right.max_addr_count),
        max_nft_count: left.max_nft_count.max(right.max_nft_count),
        min_transition_count: left.min_transition_count.min(right.min_transition_count),
        min_addr_count: left.min_addr_count.min(right.min_addr_count),
        min_nft_count: left.min_nft_count.min(right.min_nft_count),
        min_l2_ba_nft_equal_min_delta: left
            .min_l2_ba_nft_equal_min_delta
            .min(right.min_l2_ba_nft_equal_min_delta),
        min_l2_ba_nft_different_min_delta: left
            .min_l2_ba_nft_different_min_delta
            .min(right.min_l2_ba_nft_different_min_delta),
        min_l3_bcca_all_equal_min_delta: left
            .min_l3_bcca_all_equal_min_delta
            .min(right.min_l3_bcca_all_equal_min_delta),
        min_l3_bcca_all_distinct_min_delta: left
            .min_l3_bcca_all_distinct_min_delta
            .min(right.min_l3_bcca_all_distinct_min_delta),
        min_l3_bcca_root_back_equal_min_delta: left
            .min_l3_bcca_root_back_equal_min_delta
            .min(right.min_l3_bcca_root_back_equal_min_delta),
    }
}

fn anchor_leaf_hash(row: &Descriptor) -> Result<String> {
    hash_obj("anchor-descriptor-leaf-v1", &descriptor_value(row)?)
}

fn closure_leaf_hash(row: &ClosureDescriptor) -> Result<String> {
    hash_obj("closure-descriptor-leaf-v1", &closure_value(row)?)
}

fn anchor_directory_leaf_hash(closure_id: i64, anchor_order_seq: i64) -> Result<String> {
    hash_obj(
        "anchor-directory-leaf-v1",
        &json!({"closure_id": closure_id, "anchor_order_seq": anchor_order_seq}),
    )
}

fn anchor_node_hash(
    level: i64,
    index: i64,
    aggregate: &Aggregate,
    left: &str,
    right: &str,
) -> Result<String> {
    hash_obj(
        "anchor-summary-node-v1",
        &json!({
            "level": level,
            "index": index,
            "aggregate": aggregate_value(aggregate)?,
            "left": left,
            "right": right,
        }),
    )
}

fn event_leaf_hash(event: &Event) -> Result<String> {
    if event.event_id.is_empty()
        || event.event_type.is_empty()
        || event.src_addr.is_empty()
        || event.dst_addr.is_empty()
        || event.nft.is_empty()
        || event.collection.is_empty()
        || event.tx_hash.is_empty()
    {
        bail!("event leaf contains an empty semantic field");
    }
    let value = serde_json::to_value(event)?;
    let keys = value
        .as_object()
        .context("event leaf is not an object")?
        .keys()
        .map(String::as_str)
        .collect::<HashSet<_>>();
    if keys != EVENT_FIELDS.into_iter().collect::<HashSet<_>>() {
        bail!("event leaf fields are not canonical");
    }
    hash_obj("event-leaf-v2", &value)
}

fn tx_leaf_hash(event: &Event) -> Result<String> {
    hash_obj(
        "tx-binding-leaf-v1",
        &json!({
            "order_seq": event.order_seq,
            "tx_id": event.tx_id,
            "tx_hash": event.tx_hash.as_str(),
        }),
    )
}

fn collection_leaf_hash(collection: &CollectionOpening) -> Result<String> {
    hash_obj(
        "collection-directory-leaf-v2",
        &json!({
            "collection_id": collection.collection_id,
            "event_count": collection.event_count,
            "global_start_position": collection.global_start_position,
            "first_order": collection.first_order_seq,
            "last_order": collection.last_order_seq,
            "event_root": collection.event_root.as_str(),
        }),
    )
}

fn root_from_leaves(mut leaves: Vec<String>, domain: &str) -> Result<String> {
    if leaves.is_empty() {
        return hash_obj(domain, &json!({"empty": true}));
    }
    while leaves.len() > 1 {
        let mut next = Vec::with_capacity((leaves.len() + 1) / 2);
        let mut index = 0usize;
        while index < leaves.len() {
            let left = &leaves[index];
            let right = if index + 1 < leaves.len() {
                &leaves[index + 1]
            } else {
                left
            };
            next.push(parent_hash(domain, left, right)?);
            index += 2;
        }
        leaves = next;
    }
    Ok(leaves.remove(0))
}

fn anchor_roots_from_descriptors(descriptors: Vec<Descriptor>) -> Result<(String, String)> {
    if descriptors.is_empty() {
        bail!("derived Anchor subset is empty");
    }
    let mut summary = Vec::with_capacity(descriptors.len());
    let mut directory = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        summary.push((anchor_leaf_hash(&descriptor)?, leaf_aggregate(&descriptor)));
        directory.push(anchor_directory_leaf_hash(
            descriptor.closure_id,
            descriptor.anchor_order_seq,
        )?);
    }
    let mut level = 0i64;
    while summary.len() > 1 {
        let mut next = Vec::with_capacity((summary.len() + 1) / 2);
        let mut index = 0usize;
        while index < summary.len() {
            let (left_hash, left_aggregate) = &summary[index];
            let (right_hash, right_aggregate) = if index + 1 < summary.len() {
                &summary[index + 1]
            } else {
                &summary[index]
            };
            let aggregate = if index + 1 < summary.len() {
                parent_aggregate(left_aggregate, right_aggregate)
            } else {
                left_aggregate.clone()
            };
            next.push((
                anchor_node_hash(
                    level + 1,
                    (index / 2) as i64,
                    &aggregate,
                    left_hash,
                    right_hash,
                )?,
                aggregate,
            ));
            index += 2;
        }
        summary = next;
        level += 1;
    }
    Ok((
        summary.remove(0).0,
        root_from_leaves(directory, "anchor-directory-node-v1")?,
    ))
}

fn base_pass(row: &Descriptor, transcript: &Value) -> Result<bool> {
    let base = transcript.get("base").context("transcript base missing")?;
    Ok(row.transition_count >= object_i64(base, "minTransitionCount")?
        && row.addr_count >= object_i64(base, "minAddressCount")?
        && row.nft_count >= object_i64(base, "minNftCount")?)
}

fn refined_pass(row: &Descriptor, transcript: &Value) -> Result<bool> {
    let refined = transcript
        .get("refined")
        .context("transcript refined missing")?;
    let delta = object_i64(refined, "maxCompletionDeltaSeconds")?;
    let signatures = refined
        .get("genericSignatures")
        .and_then(Value::as_array)
        .context("genericSignatures missing")?;
    for signature in signatures {
        let value = match signature.as_str().context("signature is not text")? {
            "l2_ba_nft_equal_min_delta" => row.l2_ba_nft_equal_min_delta,
            "l2_ba_nft_different_min_delta" => row.l2_ba_nft_different_min_delta,
            "l3_bcca_all_equal_min_delta" => row.l3_bcca_all_equal_min_delta,
            "l3_bcca_all_distinct_min_delta" => row.l3_bcca_all_distinct_min_delta,
            "l3_bcca_root_back_equal_min_delta" => row.l3_bcca_root_back_equal_min_delta,
            other => bail!("unsupported refined signature {other}"),
        };
        if value <= delta {
            return Ok(true);
        }
    }
    Ok(false)
}

fn base_negative(aggregate: &Aggregate, transcript: &Value) -> Result<bool> {
    let base = transcript.get("base").context("transcript base missing")?;
    Ok(aggregate.max_transition_count < object_i64(base, "minTransitionCount")?
        || aggregate.max_addr_count < object_i64(base, "minAddressCount")?
        || aggregate.max_nft_count < object_i64(base, "minNftCount")?)
}

fn base_positive(aggregate: &Aggregate, transcript: &Value) -> Result<bool> {
    let base = transcript.get("base").context("transcript base missing")?;
    Ok(aggregate.min_transition_count >= object_i64(base, "minTransitionCount")?
        && aggregate.min_addr_count >= object_i64(base, "minAddressCount")?
        && aggregate.min_nft_count >= object_i64(base, "minNftCount")?)
}

fn aggregate_signature(aggregate: &Aggregate, name: &str) -> Result<i64> {
    Ok(match name {
        "l2_ba_nft_equal_min_delta" => aggregate.min_l2_ba_nft_equal_min_delta,
        "l2_ba_nft_different_min_delta" => aggregate.min_l2_ba_nft_different_min_delta,
        "l3_bcca_all_equal_min_delta" => aggregate.min_l3_bcca_all_equal_min_delta,
        "l3_bcca_all_distinct_min_delta" => aggregate.min_l3_bcca_all_distinct_min_delta,
        "l3_bcca_root_back_equal_min_delta" => {
            aggregate.min_l3_bcca_root_back_equal_min_delta
        }
        other => bail!("unsupported refined aggregate signature {other}"),
    })
}

fn refined_negative(aggregate: &Aggregate, transcript: &Value) -> Result<bool> {
    let refined = transcript
        .get("refined")
        .context("transcript refined missing")?;
    let delta = object_i64(refined, "maxCompletionDeltaSeconds")?;
    let signatures = refined
        .get("genericSignatures")
        .and_then(Value::as_array)
        .context("genericSignatures missing")?;
    for signature in signatures {
        if aggregate_signature(
            aggregate,
            signature.as_str().context("signature is not text")?,
        )? <= delta
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn create_candidate_db(path: &Path) -> Result<Connection> {
    if path.exists() {
        std::fs::remove_file(path)?;
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let connection = Connection::open(path)?;
    connection.execute_batch(
        "PRAGMA journal_mode=OFF;
         PRAGMA synchronous=OFF;
         PRAGMA temp_store=FILE;
         PRAGMA cache_size=-131072;
         CREATE TABLE candidates(
           position INTEGER NOT NULL,
           closure_id INTEGER NOT NULL UNIQUE,
           anchor_order_seq INTEGER PRIMARY KEY,
           start_ts INTEGER NOT NULL,
           end_ts INTEGER NOT NULL,
           end_order_seq INTEGER NOT NULL,
           collection_id INTEGER NOT NULL,
           transition_count INTEGER NOT NULL,
           addr_count INTEGER NOT NULL,
           nft_count INTEGER NOT NULL,
           nft_ge2_count INTEGER NOT NULL,
           b_out_event_count INTEGER NOT NULL,
           root_event_type_id INTEGER NOT NULL,
           l2_ba_nft_equal_min_delta INTEGER NOT NULL,
           l2_ba_nft_different_min_delta INTEGER NOT NULL,
           l3_bcca_all_equal_min_delta INTEGER NOT NULL,
           l3_bcca_all_distinct_min_delta INTEGER NOT NULL,
           l3_bcca_root_back_equal_min_delta INTEGER NOT NULL
         ) WITHOUT ROWID;
         CREATE INDEX idx_candidates_collection_order
           ON candidates(collection_id,anchor_order_seq);",
    )?;
    Ok(connection)
}

fn insert_candidate(statement: &mut rusqlite::Statement<'_>, row: &Descriptor) -> Result<()> {
    statement.execute(params![
        row.position,
        row.closure_id,
        row.anchor_order_seq,
        row.start_ts,
        row.end_ts,
        row.end_order_seq,
        row.collection_id,
        row.transition_count,
        row.addr_count,
        row.nft_count,
        row.nft_ge2_count,
        row.b_out_event_count,
        row.root_event_type_id,
        row.l2_ba_nft_equal_min_delta,
        row.l2_ba_nft_different_min_delta,
        row.l3_bcca_all_equal_min_delta,
        row.l3_bcca_all_distinct_min_delta,
        row.l3_bcca_root_back_equal_min_delta,
    ])?;
    Ok(())
}

fn descriptor_from_row(row: &Row<'_>) -> rusqlite::Result<Descriptor> {
    Ok(Descriptor {
        position: row.get(0)?,
        closure_id: row.get(1)?,
        anchor_order_seq: row.get(2)?,
        start_ts: row.get(3)?,
        end_ts: row.get(4)?,
        end_order_seq: row.get(5)?,
        collection_id: row.get(6)?,
        transition_count: row.get(7)?,
        addr_count: row.get(8)?,
        nft_count: row.get(9)?,
        nft_ge2_count: row.get(10)?,
        b_out_event_count: row.get(11)?,
        root_event_type_id: row.get(12)?,
        l2_ba_nft_equal_min_delta: row.get(13)?,
        l2_ba_nft_different_min_delta: row.get(14)?,
        l3_bcca_all_equal_min_delta: row.get(15)?,
        l3_bcca_all_distinct_min_delta: row.get(16)?,
        l3_bcca_root_back_equal_min_delta: row.get(17)?,
    })
}

fn solve_summary(
    level: i64,
    index: i64,
    start: i64,
    width: i64,
    leaf_count: i64,
    frontier: &HashMap<(i64, i64), (String, Aggregate)>,
) -> Result<(String, Aggregate)> {
    if let Some(existing) = frontier.get(&(level, index)) {
        return Ok(existing.clone());
    }
    if level == 0 {
        bail!("coverage frontier omits a leaf");
    }
    let left = solve_summary(
        level - 1,
        index * 2,
        start,
        width / 2,
        leaf_count,
        frontier,
    )?;
    let right_start = start + width / 2;
    let (right, aggregate) = if right_start >= leaf_count {
        (left.clone(), left.1.clone())
    } else {
        let right = solve_summary(
            level - 1,
            index * 2 + 1,
            right_start,
            width / 2,
            leaf_count,
            frontier,
        )?;
        let aggregate = parent_aggregate(&left.1, &right.1);
        (right, aggregate)
    };
    Ok((
        anchor_node_hash(level, index, &aggregate, &left.0, &right.0)?,
        aggregate,
    ))
}

fn solve_directory(
    level: i64,
    index: i64,
    start: i64,
    width: i64,
    leaf_count: i64,
    frontier: &HashMap<(i64, i64), String>,
) -> Result<String> {
    if let Some(existing) = frontier.get(&(level, index)) {
        return Ok(existing.clone());
    }
    if level == 0 {
        bail!("Anchor directory frontier omits a leaf");
    }
    let left = solve_directory(
        level - 1,
        index * 2,
        start,
        width / 2,
        leaf_count,
        frontier,
    )?;
    let right_start = start + width / 2;
    let right = if right_start >= leaf_count {
        left.clone()
    } else {
        solve_directory(
            level - 1,
            index * 2 + 1,
            right_start,
            width / 2,
            leaf_count,
            frontier,
        )?
    };
    parent_hash("anchor-directory-node-v1", &left, &right)
}

fn verify_coverage(
    package: &Path,
    transcript: &Value,
    roots: &Roots,
    candidate_db: &mut Connection,
    progress_every: u64,
) -> Result<CoverageStats> {
    let coverage_dir = package.join("coverage");
    let mut manifest = read_json(&coverage_dir.join("coverage.json"))?;
    let digest = object_text(&manifest, "coverageDigest")?.to_string();
    manifest
        .as_object_mut()
        .context("coverage manifest is not an object")?
        .remove("coverageDigest");
    if !is_sha256(&digest) || hash_obj("coverage-manifest-v3", &manifest)? != digest {
        bail!("coverage manifest digest mismatch");
    }
    let variant = object_text(&manifest, "variant")?.to_string();
    if !["full", "no_anchor", "no_refined", "no_pruning"].contains(&variant.as_str()) {
        bail!("unsupported verification ablation variant");
    }
    if object_text(&manifest, "coverageVersion")? != "complete_replay_frontier_v3" {
        bail!("unsupported coverage version");
    }
    if object_text(&manifest, "summaryRoot")? != roots.summary_root
        || object_text(&manifest, "anchorDirectoryRoot")? != roots.anchor_directory_root
        || object_text(&manifest, "closureDomainRoot")? != roots.closure_domain_root
    {
        bail!("coverage roots differ from Epoch roots");
    }
    if object_i64(&manifest, "closureCount")? != roots.closure_count
        || object_i64(&manifest, "anchorCount")? != roots.anchor_count
    {
        bail!("coverage domain counts differ from Epoch");
    }
    if manifest
        .get("candidateHintUsedAsProof")
        .and_then(Value::as_bool)
        != Some(false)
    {
        bail!("candidate hints cannot be proof inputs");
    }

    let declared_base_frontier = object_i64(&manifest, "baseNegativeFrontierCount")?;
    let declared_base_anchors = object_i64(&manifest, "baseNegativeAnchorCount")?;
    let declared_refined_frontier = object_i64(&manifest, "refinedNegativeFrontierCount")?;
    let declared_refined_anchors = object_i64(&manifest, "refinedNegativeAnchorCount")?;
    let declared_replay_frontier = object_i64(&manifest, "replayFrontierCount")?;
    let declared_replay = object_i64(&manifest, "replayCount")?;
    let declared_closure_records = object_i64(&manifest, "closureDomainRecordCount")?;

    let transaction = candidate_db.transaction()?;
    let mut insert = transaction.prepare(
        "INSERT INTO candidates VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    )?;
    let mut stats = CoverageStats {
        variant: variant.clone(),
        ..CoverageStats::default()
    };

    if variant == "full" || variant == "no_refined" {
        let leaf_count = object_i64(&manifest, "anchorCount")?;
        let height = object_i64(&manifest, "treeHeight")?;
        let mut width = leaf_count;
        let mut expected_height = 0i64;
        while width > 1 {
            expected_height += 1;
            width = (width + 1) / 2;
        }
        if leaf_count <= 0 || height != expected_height {
            bail!("coverage tree dimensions are not canonical");
        }
        let mut intervals = Vec::<(i64, i64)>::new();
        let mut frontier = HashMap::<(i64, i64), (String, Aggregate)>::new();
        let mut directory_frontier = HashMap::<(i64, i64), String>::new();
        let files = [
            ("negative_base.jsonl.gz", "BASE_NEGATIVE", "base"),
            ("negative_refined.jsonl.gz", "REFINED_NEGATIVE", "refined"),
            ("replay_authenticated.jsonl.gz", "REPLAY", "replay"),
        ];
        for (file_name, expected_kind, stage_name) in files {
            let stage_started = Instant::now();
            let mut reader = gzip_reader(&coverage_dir.join(file_name))?;
            let mut line = String::new();
            let mut record_count = 0u64;
            loop {
                line.clear();
                if reader.read_line(&mut line)? == 0 {
                    break;
                }
                if line.trim().is_empty() {
                    continue;
                }
                let record: CoverageRecord = serde_json::from_str(&line)?;
                record_count += 1;
                progress(
                    &format!("coverage:{variant}:{stage_name}"),
                    record_count,
                    progress_every,
                    stage_started,
                );
                let level = record.level;
                let index = record.index;
                if level < 0 || level > height || index < 0 {
                    bail!("coverage frontier coordinates are invalid");
                }
                let span = 1i64 << level;
                let canonical_start = index * span;
                if canonical_start >= leaf_count {
                    bail!("coverage frontier node lies outside Anchor tree");
                }
                let canonical_end = (canonical_start + span).min(leaf_count) - 1;
                if record.aggregate.start_position != canonical_start
                    || record.aggregate.end_position != canonical_end
                    || record.aggregate.count != canonical_end - canonical_start + 1
                {
                    bail!("coverage aggregate range differs from tree coordinates");
                }
                let descriptor = if level == 0 {
                    let descriptor = record
                        .descriptor
                        .clone()
                        .context("coverage leaf descriptor missing")?;
                    if record.left_hash.is_some()
                        || record.right_hash.is_some()
                        || record.duplicated.is_some()
                    {
                        bail!("coverage leaf has internal-node fields");
                    }
                    if descriptor.position != index
                        || leaf_aggregate(&descriptor) != record.aggregate
                    {
                        bail!("coverage leaf aggregate does not match descriptor");
                    }
                    Some(descriptor)
                } else {
                    if record.descriptor.is_some() {
                        bail!("coverage internal node has a descriptor");
                    }
                    None
                };
                let node_hash = if let Some(descriptor) = descriptor.as_ref() {
                    anchor_leaf_hash(descriptor)?
                } else {
                    let left_hash = record.left_hash.as_ref().context("leftHash missing")?;
                    let right_hash = record.right_hash.as_ref().context("rightHash missing")?;
                    let child_width = (leaf_count + (1i64 << (level - 1)) - 1) >> (level - 1);
                    let expected_duplicated = index * 2 + 1 >= child_width;
                    if record.duplicated != Some(expected_duplicated)
                        || (expected_duplicated && right_hash != left_hash)
                    {
                        bail!("coverage duplicate-child encoding is not canonical");
                    }
                    anchor_node_hash(level, index, &record.aggregate, left_hash, right_hash)?
                };
                if node_hash != record.node_hash || record.kind != expected_kind {
                    bail!("coverage frontier hash or kind mismatch");
                }
                match expected_kind {
                    "BASE_NEGATIVE" => {
                        if !base_negative(&record.aggregate, transcript)? {
                            bail!("unsafe Base-negative certificate");
                        }
                        stats.base_frontier += 1;
                        stats.base_anchors += record.aggregate.count;
                    }
                    "REFINED_NEGATIVE" => {
                        if variant != "full"
                            || !base_positive(&record.aggregate, transcript)?
                            || !refined_negative(&record.aggregate, transcript)?
                        {
                            bail!("unsafe Refined-negative certificate");
                        }
                        stats.refined_frontier += 1;
                        stats.refined_anchors += record.aggregate.count;
                    }
                    "REPLAY" => {
                        let descriptor = descriptor.context("replay descriptor missing")?;
                        if !base_pass(&descriptor, transcript)?
                            || (variant == "full" && !refined_pass(&descriptor, transcript)?)
                        {
                            bail!("invalid replay leaf");
                        }
                        insert_candidate(&mut insert, &descriptor)?;
                        stats.replay_frontier += 1;
                        stats.replay += 1;
                    }
                    _ => unreachable!(),
                }
                intervals.push((
                    record.aggregate.start_position,
                    record.aggregate.end_position,
                ));
                if frontier
                    .insert((level, index), (node_hash, record.aggregate.clone()))
                    .is_some()
                {
                    bail!("duplicate coverage frontier node");
                }
                if !is_sha256(&record.anchor_directory_node_hash) {
                    bail!("invalid Anchor directory frontier hash");
                }
                if let Some(descriptor) = record.descriptor.as_ref() {
                    if record.anchor_directory_node_hash
                        != anchor_directory_leaf_hash(
                            descriptor.closure_id,
                            descriptor.anchor_order_seq,
                        )?
                    {
                        bail!("Anchor directory leaf mapping mismatch");
                    }
                }
                directory_frontier.insert((level, index), record.anchor_directory_node_hash);
            }
            let elapsed = stage_started.elapsed().as_secs_f64() * 1000.0;
            match stage_name {
                "base" => stats.voneg_base_ms += elapsed,
                "refined" => stats.voneg_refined_ms += elapsed,
                _ => stats.vocov_ms += elapsed,
            }
        }
        let fold_started = Instant::now();
        intervals.sort_unstable();
        let mut expected_start = 0i64;
        for (start, end) in intervals {
            if start != expected_start || end < start {
                bail!("coverage frontier contains a gap or overlap");
            }
            expected_start = end + 1;
        }
        if expected_start != leaf_count {
            bail!("coverage frontier does not cover complete Anchor range");
        }
        let reconstructed = solve_summary(
            height,
            0,
            0,
            1i64 << height,
            leaf_count,
            &frontier,
        )?;
        if reconstructed.0 != roots.summary_root || reconstructed.1.count != leaf_count {
            bail!("coverage frontier does not reconstruct SummaryRoot");
        }
        if solve_directory(
            height,
            0,
            0,
            1i64 << height,
            leaf_count,
            &directory_frontier,
        )? != roots.anchor_directory_root
        {
            bail!("coverage frontier does not reconstruct AnchorDirectoryRoot");
        }
        if gzip_has_records(&coverage_dir.join("closure_domain_authenticated.jsonl.gz"))? {
            bail!("Anchor-frontier variant contains closure-domain records");
        }
        stats.vocov_ms += fold_started.elapsed().as_secs_f64() * 1000.0;
    } else {
        let stage_started = Instant::now();
        let mut reader = gzip_reader(&coverage_dir.join("closure_domain_authenticated.jsonl.gz"))?;
        let mut line = String::new();
        let mut closure_hashes = Vec::<String>::with_capacity(roots.closure_count as usize);
        let mut anchors = Vec::<Descriptor>::new();
        let mut position = 0i64;
        loop {
            line.clear();
            if reader.read_line(&mut line)? == 0 {
                break;
            }
            if line.trim().is_empty() {
                continue;
            }
            let record: ClosureRecord = serde_json::from_str(&line)?;
            if record.descriptor.closure_position != position
                || ![0, 1].contains(&record.descriptor.is_anchor)
            {
                bail!("closure-domain positions or is_anchor values are not canonical");
            }
            let leaf_hash = closure_leaf_hash(&record.descriptor)?;
            if leaf_hash != record.leaf_hash {
                bail!("closure-domain leaf hash mismatch");
            }
            closure_hashes.push(leaf_hash);
            if variant == "no_pruning" {
                let descriptor = record.descriptor.as_descriptor(position);
                insert_candidate(&mut insert, &descriptor)?;
                stats.replay += 1;
            } else if record.descriptor.is_anchor == 1 {
                let descriptor = record.descriptor.as_descriptor(anchors.len() as i64);
                if !base_pass(&descriptor, transcript)? {
                    stats.base_anchors += 1;
                } else if !refined_pass(&descriptor, transcript)? {
                    stats.refined_anchors += 1;
                } else {
                    insert_candidate(&mut insert, &descriptor)?;
                    stats.replay += 1;
                }
                anchors.push(descriptor);
            }
            position += 1;
            progress(
                &format!("coverage:{variant}:closure-domain"),
                position as u64,
                progress_every,
                stage_started,
            );
        }
        if position != roots.closure_count
            || root_from_leaves(closure_hashes, "closure-domain-node-v1")?
                != roots.closure_domain_root
        {
            bail!("closure-domain opening does not reconstruct Epoch root");
        }
        if variant == "no_anchor" {
            if anchors.len() as i64 != object_i64(&manifest, "anchorCount")? {
                bail!("derived Anchor subset count differs from manifest");
            }
            let (summary_root, directory_root) = anchor_roots_from_descriptors(anchors)?;
            if summary_root != roots.summary_root || directory_root != roots.anchor_directory_root {
                bail!("derived Anchor subset does not reconstruct Anchor roots");
            }
        }
        for file_name in [
            "negative_base.jsonl.gz",
            "negative_refined.jsonl.gz",
            "replay_authenticated.jsonl.gz",
        ] {
            if gzip_has_records(&coverage_dir.join(file_name))? {
                bail!("closure-domain variant contains Anchor frontier records");
            }
        }
        stats.vocov_ms += stage_started.elapsed().as_secs_f64() * 1000.0;
    }
    drop(insert);
    transaction.commit()?;
    candidate_db.execute_batch("ANALYZE candidates;")?;

    if stats.base_frontier != declared_base_frontier
        || stats.base_anchors != declared_base_anchors
        || stats.refined_frontier != declared_refined_frontier
        || stats.refined_anchors != declared_refined_anchors
        || stats.replay_frontier != declared_replay_frontier
        || stats.replay != declared_replay
        || declared_closure_records
            != if variant == "no_anchor" || variant == "no_pruning" {
                roots.closure_count
            } else {
                0
            }
    {
        bail!("coverage manifest counts differ from verified evidence");
    }
    let expected_partition = match variant.as_str() {
        "full" => {
            if stats.base_anchors + stats.refined_anchors + stats.replay != roots.anchor_count {
                bail!("Full coverage partition count mismatch");
            }
            "AnchorDomain=BaseNeg_disjoint_union_RefinedNeg_disjoint_union_Replay"
        }
        "no_refined" => {
            if stats.base_anchors + stats.replay != roots.anchor_count {
                bail!("No-Refined coverage partition count mismatch");
            }
            "AnchorDomain=BaseNeg_disjoint_union_Replay"
        }
        "no_anchor" => "ClosureDomainOpened;AnchorSubsetDerived;Replay=Anchor_and_Base_and_Refined",
        "no_pruning" => {
            if stats.replay != roots.closure_count {
                bail!("No-Pruning replay does not cover every closure");
            }
            "ClosureDomain=Replay"
        }
        _ => unreachable!(),
    };
    if object_text(&manifest, "partitionContract")? != expected_partition {
        bail!("coverage partition contract mismatch");
    }
    Ok(stats)
}

fn verify_proof(
    leaf_hash: &str,
    index: i64,
    path: &[MerklePathItem],
    root: &str,
    domain: &str,
    leaf_count: i64,
) -> Result<bool> {
    if leaf_count <= 0 || index < 0 || index >= leaf_count {
        return Ok(false);
    }
    let mut expected_height = 0usize;
    let mut width = leaf_count;
    while width > 1 {
        expected_height += 1;
        width = (width + 1) / 2;
    }
    if path.len() != expected_height {
        return Ok(false);
    }
    let mut current = leaf_hash.to_string();
    let mut position = index;
    width = leaf_count;
    for item in path {
        let sibling_index = position ^ 1;
        let duplicated = sibling_index >= width;
        let expected_on_left = !duplicated && sibling_index < position;
        if item.duplicated != duplicated || item.sibling_on_left != expected_on_left {
            return Ok(false);
        }
        if duplicated {
            if item.sibling != current {
                return Ok(false);
            }
            current = parent_hash(domain, &current, &current)?;
        } else if expected_on_left {
            current = parent_hash(domain, &item.sibling, &current)?;
        } else {
            current = parent_hash(domain, &current, &item.sibling)?;
        }
        position /= 2;
        width = (width + 1) / 2;
    }
    Ok(position == 0 && width == 1 && current == root)
}

fn expected_range_frontier_keys(
    total_count: i64,
    left: i64,
    right: i64,
) -> Result<HashSet<(i64, i64)>> {
    if total_count <= 0 || left < 0 || right < left || right >= total_count {
        bail!("invalid Merkle range");
    }
    let mut height = 0i64;
    let mut width = total_count;
    while width > 1 {
        height += 1;
        width = (width + 1) / 2;
    }
    fn visit(
        level: i64,
        index: i64,
        start: i64,
        span: i64,
        total: i64,
        left: i64,
        right: i64,
        out: &mut HashSet<(i64, i64)>,
    ) {
        let end = (start + span).min(total) - 1;
        if end < left || start > right {
            out.insert((level, index));
            return;
        }
        if left <= start && end <= right {
            return;
        }
        if level == 0 {
            return;
        }
        let half = span / 2;
        visit(level - 1, index * 2, start, half, total, left, right, out);
        if start + half < total {
            visit(
                level - 1,
                index * 2 + 1,
                start + half,
                half,
                total,
                left,
                right,
                out,
            );
        }
    }
    let mut out = HashSet::new();
    visit(height, 0, 0, 1i64 << height, total_count, left, right, &mut out);
    Ok(out)
}

fn verify_open_range(
    total_count: i64,
    left: i64,
    leaf_hashes: &[String],
    frontier: &[FrontierItem],
    expected_root: &str,
    domain: &str,
) -> Result<bool> {
    if total_count <= 0
        || leaf_hashes.is_empty()
        || left < 0
        || left + leaf_hashes.len() as i64 > total_count
        || !is_sha256(expected_root)
    {
        return Ok(false);
    }
    let right = left + leaf_hashes.len() as i64 - 1;
    let expected_keys = expected_range_frontier_keys(total_count, left, right)?;
    let supplied_keys = frontier
        .iter()
        .map(|item| (item.level, item.index))
        .collect::<Vec<_>>();
    if supplied_keys.iter().copied().collect::<HashSet<_>>().len() != supplied_keys.len()
        || supplied_keys.iter().copied().collect::<HashSet<_>>() != expected_keys
    {
        return Ok(false);
    }
    let mut height = 0usize;
    let mut width = total_count;
    let mut widths = vec![width];
    while width > 1 {
        height += 1;
        width = (width + 1) / 2;
        widths.push(width);
    }
    let mut nodes = (0..=height)
        .map(|_| HashMap::<i64, String>::new())
        .collect::<Vec<_>>();
    for (offset, digest) in leaf_hashes.iter().enumerate() {
        if !is_sha256(digest) {
            return Ok(false);
        }
        nodes[0].insert(left + offset as i64, digest.clone());
    }
    for item in frontier {
        let level = item.level as usize;
        if item.level < 0
            || level > height
            || item.index < 0
            || item.index >= widths[level]
            || !is_sha256(&item.hash)
            || nodes[level].insert(item.index, item.hash.clone()).is_some()
        {
            return Ok(false);
        }
    }
    for level in 0..height {
        let current = nodes[level].clone();
        let parent_indices = current.keys().map(|index| index / 2).collect::<HashSet<_>>();
        for parent_index in parent_indices {
            if nodes[level + 1].contains_key(&parent_index) {
                return Ok(false);
            }
            let left_index = parent_index * 2;
            let right_index = left_index + 1;
            let left_hash = match current.get(&left_index) {
                Some(value) => value,
                None => return Ok(false),
            };
            let right_hash = if right_index < widths[level] {
                match current.get(&right_index) {
                    Some(value) => value,
                    None => return Ok(false),
                }
            } else {
                left_hash
            };
            nodes[level + 1].insert(
                parent_index,
                parent_hash(domain, left_hash, right_hash)?,
            );
        }
    }
    Ok(nodes[height].len() == 1
        && nodes[height].get(&0).map(String::as_str) == Some(expected_root))
}

fn query_candidates_for_range(
    connection: &Connection,
    collection_id: i64,
    first_order: i64,
    last_order: i64,
) -> Result<Vec<Descriptor>> {
    let mut statement = connection.prepare(
        "SELECT position,closure_id,anchor_order_seq,start_ts,end_ts,end_order_seq,
                collection_id,transition_count,addr_count,nft_count,nft_ge2_count,
                b_out_event_count,root_event_type_id,l2_ba_nft_equal_min_delta,
                l2_ba_nft_different_min_delta,l3_bcca_all_equal_min_delta,
                l3_bcca_all_distinct_min_delta,l3_bcca_root_back_equal_min_delta
         FROM candidates
         WHERE collection_id=? AND anchor_order_seq BETWEEN ? AND ?
         ORDER BY anchor_order_seq",
    )?;
    let rows = statement.query_map(
        params![collection_id, first_order, last_order],
        descriptor_from_row,
    )?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn derive_window(
    descriptor: &Descriptor,
    opened: &[OpenedEvent],
    event_count: i64,
    delta: i64,
) -> Result<(usize, usize, i64, i64)> {
    let anchor = descriptor.anchor_order_seq;
    let anchor_offset = opened
        .binary_search_by_key(&anchor, |item| item.event.order_seq)
        .map_err(|_| anyhow::anyhow!("residual range omits authenticated replay anchor"))?;
    let anchor_event = &opened[anchor_offset].event;
    if anchor_event.block_ts != descriptor.start_ts {
        bail!("residual anchor timestamp differs from summary");
    }
    let deadline = (descriptor.start_ts + delta).min(descriptor.end_ts);
    let mut match_end = anchor_offset;
    while match_end + 1 < opened.len() {
        let next = &opened[match_end + 1].event;
        if next.block_ts <= deadline && next.order_seq <= descriptor.end_order_seq {
            match_end += 1;
        } else {
            break;
        }
    }
    let match_right_position = opened[match_end].position;
    let open_right_position = if match_right_position == event_count - 1 {
        match_right_position
    } else {
        if match_end + 1 >= opened.len() {
            bail!("residual range omits immediate right boundary witness");
        }
        let successor = &opened[match_end + 1];
        if successor.event.block_ts <= deadline
            && successor.event.order_seq <= descriptor.end_order_seq
        {
            bail!("residual payload stopped before complete query window");
        }
        if successor.position != match_right_position + 1 {
            bail!("right boundary witness is not immediate successor");
        }
        successor.position
    };
    Ok((
        anchor_offset,
        match_end,
        opened[anchor_offset].position,
        open_right_position,
    ))
}

fn event_value(event: &Event) -> Result<Value> {
    Ok(serde_json::to_value(event)?)
}

fn text(value: &Value, key: &str) -> Result<String> {
    Ok(value
        .get(key)
        .with_context(|| format!("missing event field {key}"))?
        .as_str()
        .with_context(|| format!("event field {key} is not text"))?
        .to_string())
}

fn integer(value: &Value, key: &str) -> Result<i64> {
    value
        .get(key)
        .with_context(|| format!("missing event field {key}"))?
        .as_i64()
        .with_context(|| format!("event field {key} is not integer"))
}

fn role_field(role: &str) -> Option<&'static str> {
    match role {
        "src" => Some("src_addr"),
        "dst" => Some("dst_addr"),
        "nft" => Some("nft"),
        "collection" => Some("collection"),
        _ => None,
    }
}

fn bind_pattern(
    pattern: &Value,
    event: &Value,
    variables: &BTreeMap<String, String>,
) -> Result<Option<BTreeMap<String, String>>> {
    let allowed = match pattern.get("event_type") {
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| value.as_str().unwrap_or("").to_string())
            .collect::<HashSet<_>>(),
        Some(value) => [value.as_str().unwrap_or("").to_string()]
            .into_iter()
            .collect(),
        None => HashSet::new(),
    };
    if !allowed.contains(&text(event, "event_type")?) {
        return Ok(None);
    }
    let mut output = variables.clone();
    for role in ["src", "dst", "nft", "collection"] {
        let Some(expected) = pattern.get(role).and_then(Value::as_str) else {
            continue;
        };
        let actual = text(event, role_field(role).context("unsupported role")?)?;
        if let Some(name) = expected.strip_prefix('$') {
            if let Some(previous) = output.get(name) {
                if previous != &actual {
                    return Ok(None);
                }
            } else {
                output.insert(name.to_string(), actual);
            }
        } else if expected != actual {
            return Ok(None);
        }
    }
    Ok(Some(output))
}

fn resolve_var(value: &Value, variables: &BTreeMap<String, String>) -> String {
    let text = value.as_str().unwrap_or("");
    if let Some(name) = text.strip_prefix('$') {
        variables.get(name).cloned().unwrap_or_default()
    } else {
        text.to_string()
    }
}

fn constraints_ok(
    constraints: &[Value],
    bindings: &BTreeMap<String, Value>,
    variables: &BTreeMap<String, String>,
) -> Result<bool> {
    for constraint in constraints {
        let kind = constraint.get("type").and_then(Value::as_str).unwrap_or("");
        let ok = match kind {
            "order" | "strict_time_order" => {
                let names = constraint
                    .get("sequence")
                    .and_then(Value::as_array)
                    .context("sequence missing")?;
                if names
                    .iter()
                    .any(|name| !bindings.contains_key(name.as_str().unwrap_or("")))
                {
                    true
                } else {
                    let field = if kind == "order" {
                        "order_seq"
                    } else {
                        "block_ts"
                    };
                    let values = names
                        .iter()
                        .map(|name| {
                            integer(
                                bindings.get(name.as_str().unwrap()).unwrap(),
                                field,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?;
                    values.windows(2).all(|window| window[0] < window[1])
                }
            }
            "within" => {
                let earlier = constraint
                    .get("earlier")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let later = constraint
                    .get("later")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !bindings.contains_key(earlier) || !bindings.contains_key(later) {
                    true
                } else {
                    integer(bindings.get(later).unwrap(), "block_ts")?
                        - integer(bindings.get(earlier).unwrap(), "block_ts")?
                        <= constraint
                            .get("delta_seconds")
                            .and_then(Value::as_i64)
                            .unwrap_or(0)
                }
            }
            "not_equal" => {
                let left = resolve_var(
                    constraint.get("left").unwrap_or(&Value::Null),
                    variables,
                );
                let right = resolve_var(
                    constraint.get("right").unwrap_or(&Value::Null),
                    variables,
                );
                left.is_empty() || right.is_empty() || left != right
            }
            "min_distinct" => {
                let values = constraint
                    .get("values")
                    .and_then(Value::as_array)
                    .context("values missing")?
                    .iter()
                    .map(|value| resolve_var(value, variables))
                    .collect::<Vec<_>>();
                values.iter().any(String::is_empty)
                    || values.iter().collect::<HashSet<_>>().len()
                        >= constraint
                            .get("min_count")
                            .and_then(Value::as_u64)
                            .unwrap_or(0) as usize
            }
            "distinct_event_ids" | "distinct_transactions" => {
                let names = constraint
                    .get("events")
                    .and_then(Value::as_array)
                    .context("events missing")?;
                if names
                    .iter()
                    .any(|name| !bindings.contains_key(name.as_str().unwrap_or("")))
                {
                    true
                } else {
                    let field = if kind == "distinct_event_ids" {
                        "event_id"
                    } else {
                        "tx_hash"
                    };
                    let values = names
                        .iter()
                        .map(|name| {
                            text(bindings.get(name.as_str().unwrap()).unwrap(), field)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    (kind != "distinct_transactions"
                        || values.iter().all(|value| !value.is_empty()))
                        && values.iter().collect::<HashSet<_>>().len() == values.len()
                }
            }
            _ => bail!("unsupported exact constraint: {kind}"),
        };
        if !ok {
            return Ok(false);
        }
    }
    Ok(true)
}

fn instance_row(
    query_id: &str,
    branch_id: &str,
    closure_id: i64,
    anchor_order: i64,
    bindings: &BTreeMap<String, Value>,
    variables: &BTreeMap<String, String>,
) -> Result<(String, String)> {
    let events = sorted_object(bindings.iter().map(|(key, value)| {
        (
            key.clone(),
            Value::String(text(value, "event_id").unwrap()),
        )
    }));
    let orders = sorted_object(bindings.iter().map(|(key, value)| {
        (
            key.clone(),
            Value::Number(integer(value, "order_seq").unwrap().into()),
        )
    }));
    let variable_values = sorted_object(
        variables
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone()))),
    );
    let key_payload = sorted_object([
        ("query_id".into(), Value::String(query_id.into())),
        ("branch_id".into(), Value::String(branch_id.into())),
        ("events".into(), events.clone()),
        ("variables".into(), variable_values.clone()),
    ]);
    let key = sha256_hex(compact(&key_payload)?.as_bytes());
    let row = sorted_object([
        ("query_id".into(), Value::String(query_id.into())),
        ("branch_id".into(), Value::String(branch_id.into())),
        ("closure_id".into(), Value::Number(closure_id.into())),
        ("anchor_order".into(), Value::Number(anchor_order.into())),
        ("instance_key".into(), Value::String(key.clone())),
        (
            "event_bindings_json".into(),
            Value::String(compact(&events)?),
        ),
        (
            "event_orders_json".into(),
            Value::String(compact(&orders)?),
        ),
        (
            "variable_bindings_json".into(),
            Value::String(compact(&variable_values)?),
        ),
    ]);
    Ok((key, compact(&row)?))
}

fn visit(
    patterns: &[Value],
    constraints: &[Value],
    index: usize,
    ordered: &[Value],
    query_id: &str,
    branch_id: &str,
    closure_id: i64,
    anchor_order: i64,
    bindings: &BTreeMap<String, Value>,
    variables: &BTreeMap<String, String>,
    insert: &mut rusqlite::Statement<'_>,
    count: &mut i64,
) -> Result<()> {
    if index == patterns.len() {
        let (key, row) = instance_row(
            query_id,
            branch_id,
            closure_id,
            anchor_order,
            bindings,
            variables,
        )?;
        insert.execute(params![key, row])?;
        *count += 1;
        return Ok(());
    }
    let pattern = &patterns[index];
    let pattern_id = pattern.get("id").and_then(Value::as_str).unwrap_or("");
    for event in ordered {
        if integer(event, "order_seq")? <= anchor_order {
            continue;
        }
        let event_id = text(event, "event_id")?;
        if bindings.values().any(|bound| {
            text(bound, "event_id")
                .map(|value| value == event_id)
                .unwrap_or(false)
        }) {
            continue;
        }
        let Some(next_variables) = bind_pattern(pattern, event, variables)? else {
            continue;
        };
        let mut next_bindings = bindings.clone();
        next_bindings.insert(pattern_id.to_string(), event.clone());
        if constraints_ok(constraints, &next_bindings, &next_variables)? {
            visit(
                patterns,
                constraints,
                index + 1,
                ordered,
                query_id,
                branch_id,
                closure_id,
                anchor_order,
                &next_bindings,
                &next_variables,
                insert,
                count,
            )?;
        }
    }
    Ok(())
}

fn exact_for_candidate(
    query_id: &str,
    branches: &[Value],
    descriptor: &Descriptor,
    events: &[OpenedEvent],
    anchor_offset: usize,
    match_end: usize,
    insert: &mut rusqlite::Statement<'_>,
    generated: &mut i64,
) -> Result<()> {
    let ordered = events[anchor_offset..=match_end]
        .iter()
        .map(|item| event_value(&item.event))
        .collect::<Result<Vec<_>>>()?;
    let root = ordered.first().context("anchor event missing")?.clone();
    for branch in branches {
        let patterns = branch
            .get("event_patterns")
            .and_then(Value::as_array)
            .context("event patterns missing")?;
        if patterns.is_empty()
            || patterns[0].get("id").and_then(Value::as_str) != Some("root")
        {
            bail!("branch does not start with root");
        }
        let constraints = branch
            .get("constraints")
            .and_then(Value::as_array)
            .context("constraints missing")?;
        let Some(variables) = bind_pattern(&patterns[0], &root, &BTreeMap::new())? else {
            continue;
        };
        let mut bindings = BTreeMap::new();
        bindings.insert("root".into(), root.clone());
        if constraints_ok(constraints, &bindings, &variables)? {
            visit(
                patterns,
                constraints,
                1,
                &ordered,
                query_id,
                branch
                    .get("branch_id")
                    .and_then(Value::as_str)
                    .unwrap_or(""),
                descriptor.closure_id,
                descriptor.anchor_order_seq,
                &bindings,
                &variables,
                insert,
                generated,
            )?;
        }
    }
    Ok(())
}

fn normalize_claim(
    record: &StringRecord,
    headers: &StringRecord,
    candidates: &Connection,
    expected_query: &str,
) -> Result<(String, String)> {
    let mut raw = HashMap::new();
    for (header, value) in headers.iter().zip(record.iter()) {
        raw.insert(header, value);
    }
    let query_id = *raw.get("query_id").context("query_id missing")?;
    if query_id != expected_query {
        bail!("cross-query claimed row");
    }
    let branch = *raw.get("branch_id").context("branch_id missing")?;
    let closure_id: i64 = raw
        .get("closure_id")
        .context("closure_id missing")?
        .parse()?;
    let anchor_order: i64 = raw
        .get("anchor_order")
        .context("anchor_order missing")?
        .parse()?;
    let authenticated_closure: Option<i64> = candidates
        .query_row(
            "SELECT closure_id FROM candidates WHERE anchor_order_seq=?",
            [anchor_order],
            |row| row.get(0),
        )
        .optional()?;
    if authenticated_closure != Some(closure_id) {
        bail!("closure_id is not the authenticated replay mapping");
    }
    let events: Value = serde_json::from_str(
        raw.get("event_bindings_json")
            .context("event bindings missing")?,
    )?;
    let orders: Value = serde_json::from_str(
        raw.get("event_orders_json")
            .context("event orders missing")?,
    )?;
    let variables: Value = serde_json::from_str(
        raw.get("variable_bindings_json")
            .context("variable bindings missing")?,
    )?;
    if orders.get("root").and_then(Value::as_i64) != Some(anchor_order) {
        bail!("anchor_order is not the root order");
    }
    let payload = sorted_object([
        ("query_id".into(), Value::String(query_id.into())),
        ("branch_id".into(), Value::String(branch.into())),
        ("events".into(), events.clone()),
        ("variables".into(), variables.clone()),
    ]);
    let key = sha256_hex(compact(&payload)?.as_bytes());
    if raw
        .get("instance_key")
        .copied()
        .unwrap_or("")
        .to_lowercase()
        != key
    {
        bail!("instance_key mismatch");
    }
    let row = sorted_object([
        ("query_id".into(), Value::String(query_id.into())),
        ("branch_id".into(), Value::String(branch.into())),
        ("closure_id".into(), Value::Number(closure_id.into())),
        ("anchor_order".into(), Value::Number(anchor_order.into())),
        ("instance_key".into(), Value::String(key.clone())),
        (
            "event_bindings_json".into(),
            Value::String(compact(&events)?),
        ),
        (
            "event_orders_json".into(),
            Value::String(compact(&orders)?),
        ),
        (
            "variable_bindings_json".into(),
            Value::String(compact(&variables)?),
        ),
    ]);
    Ok((key, compact(&row)?))
}

fn verify_residual_and_exact(
    package: &Path,
    query: &Value,
    transcript: &Value,
    roots: &Roots,
    candidates: &Connection,
    result_connection: &mut Connection,
    expected_variant: &str,
    progress_every: u64,
) -> Result<(f64, f64, i64, i64, i64)> {
    let residual_dir = package.join("residual");
    let mut manifest = read_json(&residual_dir.join("manifest.json"))?;
    let digest = object_text(&manifest, "payloadDigest")?.to_string();
    manifest
        .as_object_mut()
        .context("residual manifest is not an object")?
        .remove("payloadDigest");
    if hash_obj("residual-manifest-v3", &manifest)? != digest {
        bail!("residual manifest digest mismatch");
    }
    if object_text(&manifest, "payloadVersion")?
        != "derived_complete_collection_ranges_v3"
        || object_text(&manifest, "variant")? != expected_variant
        || object_text(&manifest, "eventStreamRoot")? != roots.event_stream_root
        || object_text(&manifest, "txBindingRoot")? != roots.tx_binding_root
        || object_i64(&manifest, "collectionCount")? != roots.collection_count
        || object_i64(&manifest, "eventCount")? != roots.event_count
        || object_text(&manifest, "rangesFile")? != "ranges.jsonl.gz"
        || object_text(&manifest, "deduplicationContract")?
            != "opened_collection_positions_are_disjoint_canonical_merged_ranges"
        || object_text(&manifest, "boundaryContract")?
            != "verifier_derives_each_replay_window_and_requires_immediate_successor_or_collection_end"
    {
        bail!("residual manifest contract mismatch");
    }
    let candidate_count: i64 =
        candidates.query_row("SELECT COUNT(*) FROM candidates", [], |row| row.get(0))?;
    if object_i64(&manifest, "replayCount")? != candidate_count {
        bail!("residual replay count differs from authenticated coverage");
    }
    let ranges_path = residual_dir.join("ranges.jsonl.gz");
    if sha256_file(&ranges_path)? != object_text(&manifest, "rangesSha256")? {
        bail!("residual transport digest mismatch");
    }
    let delta = object_i64(
        transcript
            .get("refined")
            .context("transcript refined missing")?,
        "maxCompletionDeltaSeconds",
    )?;
    let plan = query
        .get("full_binding_plan")
        .context("full_binding_plan missing")?;
    let query_id = plan
        .get("query_id")
        .and_then(Value::as_str)
        .or_else(|| query.get("query_name").and_then(Value::as_str))
        .context("query id missing")?;
    let branches = plan
        .get("branches")
        .and_then(Value::as_array)
        .context("branches missing")?;

    result_connection.execute_batch(
        "PRAGMA journal_mode=OFF;
         PRAGMA synchronous=OFF;
         CREATE TABLE generated(instance_key TEXT PRIMARY KEY,row_json TEXT NOT NULL);
         CREATE TABLE claimed(instance_key TEXT PRIMARY KEY,row_json TEXT NOT NULL);",
    )?;
    let transaction = result_connection.transaction()?;
    let mut generated_insert =
        transaction.prepare("INSERT INTO generated VALUES(?,?)")?;
    let mut reader = gzip_reader(&ranges_path)?;
    let mut line = String::new();
    let mut previous_interval: Option<(i64, i64, i64)> = None;
    let mut expected_range_id = 0i64;
    let mut processed_candidates = 0i64;
    let mut unique_opened_events = 0i64;
    let mut generated = 0i64;
    let mut vorep_duration = Duration::ZERO;
    let mut exact_duration = Duration::ZERO;
    let all_started = Instant::now();

    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        if line.trim().is_empty() {
            continue;
        }
        let proof_started = Instant::now();
        let row: RangeRecord = serde_json::from_str(&line)?;
        if row.range_id != expected_range_id {
            bail!("residual range ids are not contiguous");
        }
        expected_range_id += 1;
        let collection = &row.collection;
        if collection.event_count <= 0
            || collection.global_start_position < 0
            || collection.global_start_position + collection.event_count > roots.event_count
        {
            bail!("collection metadata exceeds Epoch event domain");
        }
        let collection_leaf = collection_leaf_hash(collection)?;
        if collection_leaf != collection.directory_leaf_hash
            || !verify_proof(
                &collection_leaf,
                collection.directory_position,
                &collection.directory_path,
                &roots.event_stream_root,
                "collection-directory-node-v2",
                roots.collection_count,
            )?
        {
            bail!("collection directory proof failed");
        }
        let left = row.left_position;
        let right = row.right_position;
        if left < 0
            || right >= collection.event_count
            || row.events.len() as i64 != right - left + 1
        {
            bail!("residual positions are not a contiguous range");
        }
        for (offset, item) in row.events.iter().enumerate() {
            if item.position != left + offset as i64 {
                bail!("residual event positions are not canonical");
            }
        }
        let current_interval = (collection.collection_id, left, right);
        if let Some(previous) = previous_interval {
            if current_interval <= previous
                || (current_interval.0 == previous.0 && current_interval.1 <= previous.2 + 1)
            {
                bail!("residual ranges overlap or are not maximally merged");
            }
        }
        previous_interval = Some(current_interval);
        let mut event_hashes = Vec::with_capacity(row.events.len());
        let mut tx_hashes = Vec::with_capacity(row.events.len());
        let mut seen_ids = HashSet::with_capacity(row.events.len());
        let mut previous_order: Option<i64> = None;
        let mut previous_ts: Option<i64> = None;
        for item in &row.events {
            let event = &item.event;
            if event.collection_id != collection.collection_id
                || previous_order.map(|value| event.order_seq <= value).unwrap_or(false)
                || previous_ts.map(|value| event.block_ts < value).unwrap_or(false)
                || !seen_ids.insert(event.event_id.clone())
            {
                bail!("opened event ordering, collection, or uniqueness contract failed");
            }
            previous_order = Some(event.order_seq);
            previous_ts = Some(event.block_ts);
            event_hashes.push(event_leaf_hash(event)?);
            tx_hashes.push(tx_leaf_hash(event)?);
        }
        if !verify_open_range(
            collection.event_count,
            left,
            &event_hashes,
            &row.range_frontier,
            &collection.event_root,
            "collection-event-node-v1",
        )? {
            bail!("residual event range proof failed");
        }
        if row.tx_left_position != collection.global_start_position + left
            || row.tx_right_position != collection.global_start_position + right
            || row.tx_leaf_hashes != tx_hashes
            || !verify_open_range(
                roots.event_count,
                row.tx_left_position,
                &tx_hashes,
                &row.tx_range_frontier,
                &roots.tx_binding_root,
                "tx-binding-node-v1",
            )?
        {
            bail!("transaction range proof or alignment failed");
        }
        unique_opened_events += row.events.len() as i64;

        let first_order = row.events.first().context("empty residual range")?.event.order_seq;
        let last_order = row.events.last().context("empty residual range")?.event.order_seq;
        let descriptors = query_candidates_for_range(
            candidates,
            collection.collection_id,
            first_order,
            last_order,
        )?;
        if descriptors.is_empty() {
            bail!("residual range contains no authenticated replay closure");
        }
        let mut merged: Option<(i64, i64)> = None;
        let mut prepared = Vec::with_capacity(descriptors.len());
        for descriptor in descriptors {
            let (anchor_offset, match_end, required_left, required_right) = derive_window(
                &descriptor,
                &row.events,
                collection.event_count,
                delta,
            )?;
            merged = Some(match merged {
                None => (required_left, required_right),
                Some((merged_left, merged_right)) => {
                    if required_left > merged_right + 1 {
                        bail!("residual range merges non-adjacent replay windows");
                    }
                    (merged_left, merged_right.max(required_right))
                }
            });
            prepared.push((descriptor, anchor_offset, match_end));
        }
        if merged != Some((left, right)) {
            bail!("residual range is not the canonical merge of replay windows");
        }
        processed_candidates += prepared.len() as i64;
        vorep_duration += proof_started.elapsed();

        let exact_started = Instant::now();
        for (descriptor, anchor_offset, match_end) in prepared {
            exact_for_candidate(
                query_id,
                branches,
                &descriptor,
                &row.events,
                anchor_offset,
                match_end,
                &mut generated_insert,
                &mut generated,
            )?;
        }
        exact_duration += exact_started.elapsed();
        progress(
            "residual+exact:ranges",
            expected_range_id as u64,
            progress_every.min(10_000).max(1),
            all_started,
        );
    }
    drop(generated_insert);
    transaction.commit()?;
    if expected_range_id != object_i64(&manifest, "rangeCount")?
        || processed_candidates != candidate_count
    {
        bail!("residual range or replay count differs from manifest");
    }
    Ok((
        vorep_duration.as_secs_f64() * 1000.0,
        exact_duration.as_secs_f64() * 1000.0,
        generated,
        unique_opened_events,
        expected_range_id,
    ))
}

fn compare_results(
    result_connection: &mut Connection,
    candidates: &Connection,
    instances: &Path,
    expected_query: &str,
) -> Result<(i64, Comparison, f64)> {
    let started = Instant::now();
    let mut reader = csv::ReaderBuilder::new().from_path(instances)?;
    let headers = reader.headers()?.clone();
    if !headers.iter().eq(COLUMNS.iter().copied()) {
        bail!("instances.csv has wrong header");
    }
    let transaction = result_connection.transaction()?;
    let mut insert = transaction.prepare("INSERT INTO claimed VALUES(?,?)")?;
    let mut claimed = 0i64;
    for record in reader.records() {
        let (key, row) = normalize_claim(&record?, &headers, candidates, expected_query)?;
        insert.execute(params![key, row])?;
        claimed += 1;
    }
    drop(insert);
    transaction.commit()?;
    let generated: i64 = result_connection.query_row(
        "SELECT COUNT(*) FROM generated",
        [],
        |row| row.get(0),
    )?;
    let missing: i64 = result_connection.query_row(
        "SELECT COUNT(*) FROM generated g LEFT JOIN claimed c USING(instance_key)
         WHERE c.instance_key IS NULL",
        [],
        |row| row.get(0),
    )?;
    let extra: i64 = result_connection.query_row(
        "SELECT COUNT(*) FROM claimed c LEFT JOIN generated g USING(instance_key)
         WHERE g.instance_key IS NULL",
        [],
        |row| row.get(0),
    )?;
    let changed: i64 = result_connection.query_row(
        "SELECT COUNT(*) FROM generated g JOIN claimed c USING(instance_key)
         WHERE g.row_json<>c.row_json",
        [],
        |row| row.get(0),
    )?;
    let comparison = Comparison {
        generated,
        claimed,
        missing,
        extra,
        changed,
        equal: missing == 0 && extra == 0 && changed == 0,
    };
    Ok((
        claimed,
        comparison,
        started.elapsed().as_secs_f64() * 1000.0,
    ))
}

fn load_roots(package: &Path) -> Result<Roots> {
    let manifest = read_json(&package.join("epoch_manifest.json"))?;
    let roots = manifest.get("roots").context("Epoch roots missing")?;
    let counts = manifest.get("counts").context("Epoch counts missing")?;
    Ok(Roots {
        summary_root: object_text(roots, "summaryRoot")?.to_string(),
        anchor_directory_root: object_text(roots, "anchorDirectoryRoot")?.to_string(),
        closure_domain_root: object_text(roots, "closureDomainRoot")?.to_string(),
        event_stream_root: object_text(roots, "eventStreamRoot")?.to_string(),
        tx_binding_root: object_text(roots, "txBindingRoot")?.to_string(),
        anchor_count: object_i64(counts, "anchorCount")?,
        closure_count: object_i64(counts, "closureCount")?,
        collection_count: object_i64(counts, "collectionCount")?,
        event_count: object_i64(counts, "eventCount")?,
    })
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.work_db.exists() {
        std::fs::remove_file(&args.work_db)?;
    }
    if let Some(parent) = args.work_db.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let candidate_db_path = args.work_db.with_extension("candidates.sqlite");
    let mut candidates = create_candidate_db(&candidate_db_path)?;
    let roots = load_roots(&args.package)?;
    let query = read_json(&args.query)?;
    let transcript = read_json(&args.package.join("transcript.json"))?;

    let coverage = verify_coverage(
        &args.package,
        &transcript,
        &roots,
        &mut candidates,
        args.progress_every,
    )?;
    let mut results = Connection::open(&args.work_db)?;
    let (vorep_ms, exact_match_ms, generated, opened_events, range_count) =
        verify_residual_and_exact(
            &args.package,
            &query,
            &transcript,
            &roots,
            &candidates,
            &mut results,
            &coverage.variant,
            args.progress_every,
        )?;
    let query_id = query
        .get("full_binding_plan")
        .and_then(|plan| plan.get("query_id"))
        .and_then(Value::as_str)
        .or_else(|| query.get("query_name").and_then(Value::as_str))
        .context("query id missing")?;
    let (claimed, comparison, result_compare_ms) =
        compare_results(&mut results, &candidates, &args.instances, query_id)?;
    if !comparison.equal || generated != comparison.generated {
        bail!("claimed result set differs from authenticated replay");
    }
    let cand0_count = if coverage.variant == "no_pruning" {
        roots.closure_count
    } else {
        roots.anchor_count
    };
    let cand1_count = if coverage.variant == "no_pruning" {
        cand0_count
    } else {
        roots.anchor_count - coverage.base_anchors
    };
    let report = Report {
        status: "VERIFIED",
        verifier_engine: "rust-full-proof+shared-range+exact-core",
        variant: coverage.variant.clone(),
        cand0_count,
        cand1_count,
        replay_count: coverage.replay,
        cand2_count: if coverage.variant == "full" || coverage.variant == "no_anchor" {
            Some(coverage.replay)
        } else {
            None
        },
        result_count: claimed,
        generated_result_count: generated,
        vocov_ms: coverage.vocov_ms,
        voneg_base_ms: coverage.voneg_base_ms,
        voneg_refined_ms: coverage.voneg_refined_ms,
        vorep_ms,
        exact_match_ms,
        result_compare_ms,
        unique_opened_event_count: opened_events,
        residual_range_count: range_count,
        direct_shared_range_replay: true,
        temporary_replay_bytes: 0,
        authenticated_candidate_db_bytes: std::fs::metadata(&candidate_db_path)
            .map(|metadata| metadata.len())
            .unwrap_or(0),
        comparison,
    };
    if let Some(parent) = args.out_json.parent() {
        std::fs::create_dir_all(parent)?;
    }
    serde_json::to_writer_pretty(File::create(&args.out_json)?, &report)?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
