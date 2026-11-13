use anyhow::{bail, Context, Result};
use clap::Parser;
use funnelq_residual_obligation::metrics::EvaluationMetrics;
use funnelq_residual_obligation::verify::{event_batch_digest, query_digest};
use funnelq_residual_obligation::{
    compile_query_document, evaluate_formal_episode_opening, optional_chain_preflight_config,
    read_bound_residual_epoch_root, verify_formal_episode_opening, AuthenticatedQueryResponse,
    ChainReadEvidence, FormalEventPageIndex, MatchRecord, ResidualEpochManifest, Scalar, ScopeSpec,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(about = "Batch residual-obligation evaluation over a frozen Cand2 closure plan")]
struct Args {
                                                                             
                                                                               
                                                                                
                                              
    #[arg(long)]
    residual_manifest: PathBuf,
                                                                               
                                                                                
                                                                            
    #[arg(long)]
    formal_manifest: PathBuf,
    #[arg(long)]
    artifact: PathBuf,
    #[arg(long)]
    query: PathBuf,
    #[arg(long)]
    closure_plan: PathBuf,
    #[arg(long)]
    epoch_audit: PathBuf,
    #[arg(long)]
    output_json: PathBuf,
    #[arg(long)]
    output_csv: PathBuf,
    #[arg(long, default_value = "root")]
    anchor_atom: String,
    #[arg(long)]
    window_seconds: Option<i64>,
    #[arg(long)]
    expected_artifact_sha256: Option<String>,
    #[arg(long)]
    expected_query_sha256: Option<String>,
    #[arg(long)]
    expected_plan_sha256: Option<String>,
    #[arg(long)]
    expected_manifest_root: Option<String>,
    #[arg(long)]
    expected_plan_rows: Option<usize>,
    #[arg(long)]
    expected_result_count: Option<usize>,
    #[arg(long)]
    max_closures: Option<usize>,
                                                                      
                                                                      
    #[arg(long)]
    chain_rpc_url: Option<String>,
                                                                     
    #[arg(long)]
    registry_address: Option<String>,
                                                                               
                                                                    
    #[arg(long)]
    chain_block_tag: Option<String>,
                                                                        
    #[arg(long)]
    expected_chain_id: Option<u64>,
    #[arg(long, default_value_t = 25)]
    progress_every: usize,
}

#[derive(Clone, Debug, Serialize)]
struct ClosurePlanRow {
    closure_id: i64,
    anchor_order: i64,
    end_ts: i64,
    collection_id: i64,
}

#[derive(Clone, Debug, Serialize)]
struct InputAudit {
    residual_manifest_path: String,
    residual_epoch_root: String,
    residual_manifest_integrity_verified: bool,
    residual_manifest_matches_formal_event_index: bool,
    residual_epoch_root_chain_bound: bool,
    formal_manifest_path: String,
    formal_manifest_root: String,
    formal_event_page_root: String,
    formal_manifest_and_page_tree_verified: bool,
    fresh_chain_eth_call_performed: bool,
    chain_read_evidence: Option<ChainReadEvidence>,
    artifact_path: String,
    artifact_sha256: String,
    artifact_sha256_verified: bool,
    query_path: String,
    query_sha256: String,
    query_sha256_verified: bool,
    closure_plan_path: String,
    closure_plan_sha256: String,
    closure_plan_sha256_verified: bool,
    epoch_audit_path: String,
    committed_canonical_event_sha256: String,
    legacy_manifest_root: String,
    manifest_root_verified: bool,
    plan_row_count: usize,
    plan_row_count_verified: bool,
    plan_closure_ids_unique: bool,
    plan_anchor_orders_unique: bool,
    plan_anchor_orders_strictly_increasing: bool,
    plan_closure_id_rule_verified: bool,
}

#[derive(Clone, Debug, Serialize)]
struct ClosureReport {
    closure_id: i64,
    anchor_order: i64,
    anchor_time: i64,
    descriptor_end_ts: i64,
    episode_end_ts: i64,
    window_seconds: i64,
    collection_id: i64,
    anchor_event_type: String,
    authenticated_opened_event_count: usize,
    semantic_global_envelope_event_count: usize,
    semantic_collection_event_count: usize,
    semantic_collection_first_order: Option<i64>,
    semantic_collection_last_order: Option<i64>,
    semantic_collection_first_time: Option<i64>,
    semantic_collection_last_time: Option<i64>,
    semantic_collection_digest: String,
    authenticated_opening_digest: String,
    range_proof_node_count: usize,
    closure_open_seconds: f64,
    server_online_seconds: f64,
    response_packaging_seconds: f64,
    verifier_online_seconds: f64,
    result_count_before_global_dedup: usize,
    vo_size_bytes: u64,
    verification_completed: bool,
    all_results_equal: bool,
    witness_replayed_equal: bool,
    compression_ratio: f64,
    reuse_ratio: f64,
    metrics: EvaluationMetrics,
}

#[derive(Clone, Debug, Serialize)]
struct BatchSummary {
    protocol: &'static str,
    preliminary_once: bool,
    query_id: String,
    epoch_built_before_query: bool,
    query_scope_was_legacy_implicit: bool,
    explicit_runtime_scope: String,
    epoch_root: String,
    formal_event_page_root: String,
    epoch_root_chain_bound: bool,
    fresh_chain_eth_call_performed: bool,
    chain_read_seconds: f64,
    epoch_event_count: usize,
    epoch_index_bytes: usize,
    epoch_query_specific_storage_bytes: usize,
    processed_closure_count: usize,
    expected_closure_count: Option<usize>,
    closure_count_equal_to_expected: Option<bool>,
    result_count_before_global_dedup: usize,
    duplicate_result_count: usize,
    result_count: usize,
    expected_result_count: Option<usize>,
    result_count_equal_to_expected: Option<bool>,
    result_digest_sha256: String,
    all_closures_verified: bool,
    all_results_equal_on_replay: bool,
    all_witnesses_equal_on_replay: bool,
    all_anchors_found: bool,
    all_anchor_types_valid: bool,
    all_anchor_collections_valid: bool,
    all_episode_boundaries_valid: bool,
    artifact_hash_seconds: f64,
    artifact_load_seconds: f64,
    epoch_index_build_seconds: f64,
    query_read_compile_seconds: f64,
    closure_plan_read_seconds: f64,
    closure_open_seconds: f64,
    server_online_seconds: f64,
    response_packaging_seconds: f64,
    result_materialization_seconds: f64,
    residual_server_e2e_seconds: f64,
    verifier_online_seconds: f64,
    batch_wall_seconds: f64,
    total_vo_size_bytes: u64,
    total_authenticated_opened_events: u64,
    total_semantic_global_envelope_events: u64,
    total_semantic_collection_events: u64,
    aggregate_metrics: EvaluationMetrics,
    aggregate_compression_ratio: f64,
    aggregate_reuse_ratio: f64,
    peak_rss_bytes: Option<u64>,
    peak_commit_bytes: Option<u64>,
}

#[derive(Serialize)]
struct BatchReport {
    input_audit: InputAudit,
    summary: BatchSummary,
    per_closure: Vec<ClosureReport>,
    unique_matches: Vec<MatchRecord>,
}

fn normalize_hash(value: &str) -> String {
    value.trim().trim_start_matches("0x").to_ascii_lowercase()
}

fn hash_file(path: &Path) -> Result<String> {
    let mut reader = BufReader::new(
        File::open(path).with_context(|| format!("open {} for SHA-256", path.display()))?,
    );
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn hash_verified(actual: &str, expected: &Option<String>, label: &str) -> Result<bool> {
    let Some(expected) = expected else {
        return Ok(false);
    };
    if normalize_hash(actual) != normalize_hash(expected) {
        bail!("{label} SHA-256 mismatch: expected {expected}, got {actual}")
    }
    Ok(true)
}

fn parse_i64(raw: &str, field: &str, line: usize) -> Result<i64> {
    raw.trim()
        .parse()
        .with_context(|| format!("invalid {field} at closure-plan line {line}"))
}

fn load_closure_plan(path: &Path) -> Result<Vec<ClosurePlanRow>> {
    let reader = BufReader::new(
        File::open(path).with_context(|| format!("open closure plan {}", path.display()))?,
    );
    let mut lines = reader.lines();
    let header = lines
        .next()
        .context("closure plan is empty")??
        .trim_start_matches('\u{feff}')
        .to_owned();
    if header != "closure_id,anchor_order,end_ts,collection_id" {
        bail!("unexpected closure-plan header {header:?}")
    }
    let mut rows = Vec::new();
    for (offset, line) in lines.enumerate() {
        let line_number = offset + 2;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let fields: Vec<_> = line.split(',').collect();
        if fields.len() != 4 {
            bail!("closure-plan line {line_number} must contain exactly four integer fields")
        }
        rows.push(ClosurePlanRow {
            closure_id: parse_i64(fields[0], "closure_id", line_number)?,
            anchor_order: parse_i64(fields[1], "anchor_order", line_number)?,
            end_ts: parse_i64(fields[2], "end_ts", line_number)?,
            collection_id: parse_i64(fields[3], "collection_id", line_number)?,
        });
    }
    if rows.is_empty() {
        bail!("closure plan contains no rows")
    }
    Ok(rows)
}

fn scalar_i64(event: &funnelq_residual_obligation::CanonicalEvent, field: &str) -> Result<i64> {
    match event.value(field) {
        Some(Scalar::Int(value)) => Ok(value),
        Some(Scalar::Text(value)) => value
            .parse()
            .with_context(|| format!("event {} has non-integer {field}", event.event_id)),
        _ => bail!("event {} has no integer {field}", event.event_id),
    }
}

fn digest_matches(matches: &[MatchRecord]) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"funnelq-phase9-canonical-match-set-v1\0");
    for item in matches {
        let encoded = serde_json::to_vec(item)?;
        hasher.update((encoded.len() as u64).to_le_bytes());
        hasher.update(encoded);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

fn csv_cell(value: impl ToString) -> String {
    let value = value.to_string();
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value
    }
}

fn write_csv(path: &Path, reports: &[ClosureReport]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut writer =
        BufWriter::new(File::create(path).with_context(|| format!("create {}", path.display()))?);
    writeln!(writer, "closure_id,anchor_order,anchor_time,descriptor_end_ts,episode_end_ts,window_seconds,collection_id,anchor_event_type,authenticated_opened_event_count,semantic_global_envelope_event_count,semantic_collection_event_count,semantic_collection_first_order,semantic_collection_last_order,semantic_collection_first_time,semantic_collection_last_time,semantic_collection_digest,authenticated_opening_digest,range_proof_node_count,closure_open_seconds,server_online_seconds,response_packaging_seconds,verifier_online_seconds,result_count_before_global_dedup,vo_size_bytes,verification_completed,all_results_equal,witness_replayed_equal,raw_candidate_probes,temporal_prefilter_rejections,nonterminal_residual_state_visits,concrete_partial_bindings_compat,distinct_residual_keys,residual_transitions,residual_split_count,quotient_merge_count,memoized_failed_states,witness_dag_nodes,compression_ratio,reuse_ratio")?;
    for row in reports {
        let values = vec![
            csv_cell(row.closure_id),
            csv_cell(row.anchor_order),
            csv_cell(row.anchor_time),
            csv_cell(row.descriptor_end_ts),
            csv_cell(row.episode_end_ts),
            csv_cell(row.window_seconds),
            csv_cell(row.collection_id),
            csv_cell(&row.anchor_event_type),
            csv_cell(row.authenticated_opened_event_count),
            csv_cell(row.semantic_global_envelope_event_count),
            csv_cell(row.semantic_collection_event_count),
            csv_cell(
                row.semantic_collection_first_order
                    .map_or(String::new(), |v| v.to_string()),
            ),
            csv_cell(
                row.semantic_collection_last_order
                    .map_or(String::new(), |v| v.to_string()),
            ),
            csv_cell(
                row.semantic_collection_first_time
                    .map_or(String::new(), |v| v.to_string()),
            ),
            csv_cell(
                row.semantic_collection_last_time
                    .map_or(String::new(), |v| v.to_string()),
            ),
            csv_cell(&row.semantic_collection_digest),
            csv_cell(&row.authenticated_opening_digest),
            csv_cell(row.range_proof_node_count),
            csv_cell(format!("{:.9}", row.closure_open_seconds)),
            csv_cell(format!("{:.9}", row.server_online_seconds)),
            csv_cell(format!("{:.9}", row.response_packaging_seconds)),
            csv_cell(format!("{:.9}", row.verifier_online_seconds)),
            csv_cell(row.result_count_before_global_dedup),
            csv_cell(row.vo_size_bytes),
            csv_cell(row.verification_completed),
            csv_cell(row.all_results_equal),
            csv_cell(row.witness_replayed_equal),
            csv_cell(row.metrics.raw_candidate_probes),
            csv_cell(row.metrics.temporal_prefilter_rejections),
            csv_cell(row.metrics.nonterminal_residual_state_visits),
            csv_cell(row.metrics.concrete_partial_bindings),
            csv_cell(row.metrics.distinct_residual_keys),
            csv_cell(row.metrics.residual_transitions),
            csv_cell(row.metrics.residual_split_count),
            csv_cell(row.metrics.quotient_merge_count),
            csv_cell(row.metrics.memoized_failed_states),
            csv_cell(row.metrics.witness_dag_nodes),
            csv_cell(format!("{:.6}", row.compression_ratio)),
            csv_cell(format!("{:.6}", row.reuse_ratio)),
        ];
        writeln!(writer, "{}", values.join(","))?;
    }
    writer.flush()?;
    Ok(())
}

#[cfg(windows)]
fn process_memory_peaks() -> (Option<u64>, Option<u64>) {
    use std::ffi::c_void;

    #[repr(C)]
    struct ProcessMemoryCounters {
        cb: u32,
        page_fault_count: u32,
        peak_working_set_size: usize,
        working_set_size: usize,
        quota_peak_paged_pool_usage: usize,
        quota_paged_pool_usage: usize,
        quota_peak_non_paged_pool_usage: usize,
        quota_non_paged_pool_usage: usize,
        pagefile_usage: usize,
        peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut c_void;
    }
    #[link(name = "psapi")]
    extern "system" {
        fn GetProcessMemoryInfo(
            process: *mut c_void,
            counters: *mut ProcessMemoryCounters,
            size: u32,
        ) -> i32;
    }

    let mut counters = ProcessMemoryCounters {
        cb: std::mem::size_of::<ProcessMemoryCounters>() as u32,
        page_fault_count: 0,
        peak_working_set_size: 0,
        working_set_size: 0,
        quota_peak_paged_pool_usage: 0,
        quota_paged_pool_usage: 0,
        quota_peak_non_paged_pool_usage: 0,
        quota_non_paged_pool_usage: 0,
        pagefile_usage: 0,
        peak_pagefile_usage: 0,
    };
                                                                              
                                                            
    let ok = unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<ProcessMemoryCounters>() as u32,
        )
    };
    if ok == 0 {
        (None, None)
    } else {
        (
            Some(counters.peak_working_set_size as u64),
            Some(counters.peak_pagefile_usage as u64),
        )
    }
}

#[cfg(not(windows))]
fn process_memory_peaks() -> (Option<u64>, Option<u64>) {
    (None, None)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let chain_config = optional_chain_preflight_config(
        args.chain_rpc_url.clone(),
        args.registry_address.clone(),
        args.chain_block_tag.clone(),
        args.expected_chain_id,
    )?;
    if args.progress_every == 0 {
        bail!("--progress-every must be positive")
    }

                                                                             
                                                                               
                                                                            
                                                  
    eprintln!("[phase9] loading query-oblivious Residual epoch Manifest");
    let residual_epoch = ResidualEpochManifest::load_canonical(&args.residual_manifest)?;
    residual_epoch.verify_integrity()?;

    let hash_started = Instant::now();
    let artifact_sha256 = hash_file(&args.artifact)?;
    let artifact_hash_seconds = hash_started.elapsed().as_secs_f64();
    let artifact_sha256_verified = hash_verified(
        &artifact_sha256,
        &args.expected_artifact_sha256,
        "canonical artifact",
    )?;

    let epoch_audit: Value = serde_json::from_slice(
        &fs::read(&args.epoch_audit)
            .with_context(|| format!("read epoch audit {}", args.epoch_audit.display()))?,
    )?;
    let committed_canonical_event_sha256 = epoch_audit
        .get("canonical_event_sha256")
        .and_then(Value::as_str)
        .context("epoch audit has no canonical_event_sha256")?
        .to_owned();
    if normalize_hash(&artifact_sha256) != normalize_hash(&committed_canonical_event_sha256) {
        bail!("artifact SHA-256 disagrees with the epoch audit")
    }
    let legacy_manifest_root = epoch_audit
        .get("r_manifest")
        .and_then(Value::as_str)
        .context("epoch audit has no r_manifest")?
        .to_owned();
    let manifest_root_verified = if let Some(expected) = &args.expected_manifest_root {
        if normalize_hash(&legacy_manifest_root) != normalize_hash(expected) {
            bail!("legacy Manifest root mismatch: expected {expected}, got {legacy_manifest_root}")
        }
        true
    } else {
        false
    };

    eprintln!("[phase9] loading and verifying formal collection-order event pages");
    let load_started = Instant::now();
    let formal_epoch = FormalEventPageIndex::load(&args.formal_manifest)?;
    let artifact_load_seconds = load_started.elapsed().as_secs_f64();
    residual_epoch.verify_against_formal_index(&formal_epoch)?;
    if normalize_hash(&formal_epoch.manifest().artifact_sha256) != normalize_hash(&artifact_sha256)
    {
        bail!("formal Manifest artifact digest disagrees with --artifact")
    }
    if normalize_hash(&formal_epoch.manifest().manifest_root)
        != normalize_hash(&legacy_manifest_root)
    {
        bail!("formal Manifest root disagrees with the pre-query epoch audit")
    }
    let expected_artifact_path = fs::canonicalize(&args.artifact)
        .with_context(|| format!("canonicalize {}", args.artifact.display()))?;
    let manifest_artifact_path = fs::canonicalize(&formal_epoch.manifest().artifact_path)
        .with_context(|| {
            format!(
                "canonicalize {}",
                formal_epoch.manifest().artifact_path.display()
            )
        })?;
    if expected_artifact_path != manifest_artifact_path {
        bail!("--artifact is not the artifact named by the formal Manifest")
    }
    let epoch_index_bytes = fs::metadata(&formal_epoch.manifest().artifact_path)?.len()
        + fs::metadata(&formal_epoch.manifest().tree_path)?.len()
        + fs::metadata(&args.formal_manifest)?.len()
        + fs::metadata(&args.residual_manifest)?.len();
                                                                               
                                                                              
    let epoch_index_build_seconds = 0.0;

                                                                        
                                                                              
                                                                             
                                     
    let chain_read_evidence = if let Some(config) = chain_config.as_ref() {
        eprintln!("[phase9] performing fresh fixed-block Residual root eth_call");
        Some(read_bound_residual_epoch_root(
            config,
            &residual_epoch.core.dataset.source_epoch_id,
            &residual_epoch.epoch_root,
        )?)
    } else {
        None
    };
    let epoch_root_chain_bound = chain_read_evidence.is_some();
    let fresh_chain_eth_call_performed = chain_read_evidence.is_some();
    let chain_read_seconds = chain_read_evidence
        .as_ref()
        .map(|evidence| evidence.chain_read_seconds)
        .unwrap_or(0.0);

                                                                              
                                                                         
    let query_started = Instant::now();
    let query_bytes =
        fs::read(&args.query).with_context(|| format!("read query {}", args.query.display()))?;
    let query_sha256 = format!("{:x}", Sha256::digest(&query_bytes));
    let query_sha256_verified = hash_verified(&query_sha256, &args.expected_query_sha256, "query")?;
    let query_document: Value = serde_json::from_slice(&query_bytes)?;
    let mut query = compile_query_document(&query_document)?;
    let query_scope_was_legacy_implicit = matches!(query.scope, ScopeSpec::ResidualClosure);
    if matches!(query.scope, ScopeSpec::SameTx) {
        bail!("residual_batch requires an Episode workload, not SameTx")
    }
    let document_window = query_document
        .pointer("/full_binding_plan/delta_seconds")
        .and_then(Value::as_i64);
    let window_seconds = args
        .window_seconds
        .or_else(|| match query.scope {
            ScopeSpec::Episode { window_seconds, .. } => Some(window_seconds),
            _ => None,
        })
        .or(document_window)
        .context("Episode window is absent from both CLI and query")?;
    if window_seconds <= 0 {
        bail!("Episode window must be positive")
    }
    query.scope = ScopeSpec::Episode {
        anchor_atom: args.anchor_atom.clone(),
        window_seconds,
    };
    let query_read_compile_seconds = query_started.elapsed().as_secs_f64();

    let plan_started = Instant::now();
    let closure_plan_sha256 = hash_file(&args.closure_plan)?;
    let closure_plan_sha256_verified = hash_verified(
        &closure_plan_sha256,
        &args.expected_plan_sha256,
        "closure plan",
    )?;
    let mut plan_rows = load_closure_plan(&args.closure_plan)?;
    let full_plan_row_count = plan_rows.len();
    let plan_row_count_verified = if let Some(expected) = args.expected_plan_rows {
        if full_plan_row_count != expected {
            bail!("closure plan row count mismatch: expected {expected}, got {full_plan_row_count}")
        }
        true
    } else {
        false
    };
    let plan_closure_ids_unique = plan_rows
        .iter()
        .map(|row| row.closure_id)
        .collect::<BTreeSet<_>>()
        .len()
        == full_plan_row_count;
    let plan_anchor_orders_unique = plan_rows
        .iter()
        .map(|row| row.anchor_order)
        .collect::<BTreeSet<_>>()
        .len()
        == full_plan_row_count;
    let plan_anchor_orders_strictly_increasing = plan_rows
        .windows(2)
        .all(|pair| pair[0].anchor_order < pair[1].anchor_order);
    let plan_closure_id_rule_verified = plan_rows
        .iter()
        .all(|row| row.closure_id == row.anchor_order.saturating_add(1));
    if !plan_closure_ids_unique
        || !plan_anchor_orders_unique
        || !plan_anchor_orders_strictly_increasing
        || !plan_closure_id_rule_verified
    {
        bail!("closure plan uniqueness/order/closure-id invariant failed")
    }
    if let Some(max) = args.max_closures {
        plan_rows.truncate(max.min(plan_rows.len()));
    }
    let closure_plan_read_seconds = plan_started.elapsed().as_secs_f64();

    let input_audit = InputAudit {
        residual_manifest_path: args.residual_manifest.display().to_string(),
        residual_epoch_root: residual_epoch.epoch_root.clone(),
        residual_manifest_integrity_verified: true,
        residual_manifest_matches_formal_event_index: true,
        residual_epoch_root_chain_bound: epoch_root_chain_bound,
        formal_manifest_path: args.formal_manifest.display().to_string(),
        formal_manifest_root: formal_epoch.manifest().manifest_root.clone(),
        formal_event_page_root: formal_epoch.page_root().to_owned(),
        formal_manifest_and_page_tree_verified: true,
        fresh_chain_eth_call_performed,
        chain_read_evidence: chain_read_evidence.clone(),
        artifact_path: args.artifact.display().to_string(),
        artifact_sha256,
        artifact_sha256_verified,
        query_path: args.query.display().to_string(),
        query_sha256,
        query_sha256_verified,
        closure_plan_path: args.closure_plan.display().to_string(),
        closure_plan_sha256,
        closure_plan_sha256_verified,
        epoch_audit_path: args.epoch_audit.display().to_string(),
        committed_canonical_event_sha256,
        legacy_manifest_root,
        manifest_root_verified,
        plan_row_count: full_plan_row_count,
        plan_row_count_verified,
        plan_closure_ids_unique,
        plan_anchor_orders_unique,
        plan_anchor_orders_strictly_increasing,
        plan_closure_id_rule_verified,
    };

    let batch_started = Instant::now();
    let mut reports = Vec::with_capacity(plan_rows.len());
    let mut all_matches = Vec::new();
    let mut aggregate_metrics = EvaluationMetrics::default();
    let mut closure_open_seconds = 0.0;
    let mut server_online_seconds = 0.0;
    let mut response_packaging_seconds = 0.0;
    let mut verifier_online_seconds = 0.0;
    let mut total_vo_size_bytes = 0_u64;
    let mut total_authenticated_opened_events = 0_u64;
    let mut total_semantic_global_envelope_events = 0_u64;
    let mut total_semantic_collection_events = 0_u64;

    eprintln!(
        "[phase9] evaluating {} frozen residual closures for {}",
        plan_rows.len(),
        query.query_id
    );
    for (offset, row) in plan_rows.iter().enumerate() {
        let anchor_event_id = format!("order:{}", row.anchor_order);
        let open_started = Instant::now();
        let opening = formal_epoch
            .open_episode(&anchor_event_id, window_seconds)
            .with_context(|| {
                format!(
                    "open formal Episode for closure {} anchor {anchor_event_id}",
                    row.closure_id
                )
            })?;
        let verified_opening = verify_formal_episode_opening(
            formal_epoch.page_root(),
            &anchor_event_id,
            window_seconds,
            &opening,
        )?;
        let open_elapsed = open_started.elapsed().as_secs_f64();
        closure_open_seconds += open_elapsed;
        let anchor = verified_opening
            .semantic_events
            .iter()
            .find(|event| event.event_id == anchor_event_id)
            .with_context(|| {
                format!(
                    "closure {} formal opening omits anchor {anchor_event_id}",
                    row.closure_id
                )
            })?;
        if anchor.order_seq != row.anchor_order {
            bail!("closure {} anchor order metadata mismatch", row.closure_id)
        }
        let anchor_collection = scalar_i64(anchor, "collection")?;
        if anchor_collection != row.collection_id {
            bail!(
                "closure {} collection mismatch: plan {}, event {}",
                row.closure_id,
                row.collection_id,
                anchor_collection
            )
        }
        for branch in &query.branches {
            let anchor_atom = branch
                .atoms
                .iter()
                .find(|atom| atom.id == args.anchor_atom)
                .with_context(|| {
                    format!(
                        "branch {} has no anchor atom {}",
                        branch.id, args.anchor_atom
                    )
                })?;
            if !anchor_atom.event_types.contains(&anchor.event_type) {
                bail!(
                    "closure {} anchor type {} is not accepted by branch {}",
                    row.closure_id,
                    anchor.event_type,
                    branch.id
                )
            }
        }
        let nominal_end = anchor.block_ts.saturating_add(window_seconds);
        if row.end_ts < anchor.block_ts {
            bail!(
                "closure {} descriptor ends before its anchor: end {}, anchor {}",
                row.closure_id,
                row.end_ts,
                anchor.block_ts
            )
        }
        let episode_end_ts = nominal_end.min(row.end_ts);
        if episode_end_ts != nominal_end {
            bail!(
                "closure {} descriptor window ({}) is shorter than the query Episode ({})",
                row.closure_id,
                row.end_ts - anchor.block_ts,
                window_seconds
            )
        }
        let semantic_collection_events = &verified_opening.semantic_events;
        if semantic_collection_events.is_empty()
            || !semantic_collection_events
                .iter()
                .any(|event| event.event_id == anchor_event_id)
        {
            bail!(
                "closure {} semantic collection Episode omits its anchor",
                row.closure_id
            )
        }
        if semantic_collection_events.iter().any(|event| {
            event.order_seq < row.anchor_order
                || event.block_ts < anchor.block_ts
                || event.block_ts > episode_end_ts
        }) {
            bail!("closure {} has an invalid Episode boundary", row.closure_id)
        }
        let semantic_collection_digest = event_batch_digest(semantic_collection_events)?;
        let authenticated_opening_digest = event_batch_digest(&verified_opening.opened_events)?;

        let fixed_atoms = BTreeMap::from([(args.anchor_atom.clone(), anchor_event_id.clone())]);
        let server_started = Instant::now();
        let mut server = evaluate_formal_episode_opening(
            formal_epoch.page_root(),
            &query,
            fixed_atoms.clone(),
            &opening,
        )?;
        let response = AuthenticatedQueryResponse {
            trusted_epoch_root: residual_epoch.epoch_root.clone(),
            closure_id: row.closure_id.to_string(),
            query_digest: query_digest(&query)?,
            fixed_atoms: fixed_atoms.clone(),
            matches: server.matches.clone(),
            witness: server.witness.clone(),
        };
        let server_elapsed = server_started.elapsed().as_secs_f64();
        server_online_seconds += server_elapsed;
        let packaging_started = Instant::now();
        let opening_wire = serde_json::to_vec(&opening)?;
        let response_wire = serde_json::to_vec(&response)?;
        let packaging_elapsed = packaging_started.elapsed().as_secs_f64();
        response_packaging_seconds += packaging_elapsed;
        let vo_size_bytes = (opening_wire.len() + response_wire.len()) as u64;
        let verifier_started = Instant::now();
        if response.trusted_epoch_root != residual_epoch.epoch_root {
            bail!(
                "closure {} response is bound to the wrong Residual epoch root",
                row.closure_id
            )
        }
        let mut verification = evaluate_formal_episode_opening(
            formal_epoch.page_root(),
            &query,
            fixed_atoms,
            &opening,
        )?;
        let verifier_elapsed = verifier_started.elapsed().as_secs_f64();
        verifier_online_seconds += verifier_elapsed;
        let all_results_equal = verification.matches == response.matches;
        let witness_replayed_equal = verification.witness == response.witness;
        let verification_completed = all_results_equal && witness_replayed_equal;
        if !verification_completed {
            bail!("closure {} failed authenticated replay", row.closure_id)
        }
                                                                             
                                                                                
                                                                            
        server.metrics.vo_size_bytes = vo_size_bytes;
        verification.metrics.verifier_replay_seconds = verifier_elapsed;
        verification.metrics.vo_size_bytes = vo_size_bytes;

        let result_count = response.matches.len();
        all_matches.extend(response.matches);
        aggregate_metrics.merge_from(&server.metrics);
        total_vo_size_bytes = total_vo_size_bytes.saturating_add(vo_size_bytes);
        total_authenticated_opened_events = total_authenticated_opened_events
            .saturating_add(verified_opening.opened_events.len() as u64);
        total_semantic_global_envelope_events = total_semantic_global_envelope_events
            .saturating_add(semantic_collection_events.len() as u64);
        total_semantic_collection_events = total_semantic_collection_events
            .saturating_add(semantic_collection_events.len() as u64);
        reports.push(ClosureReport {
            closure_id: row.closure_id,
            anchor_order: row.anchor_order,
            anchor_time: anchor.block_ts,
            descriptor_end_ts: row.end_ts,
            episode_end_ts,
            window_seconds,
            collection_id: row.collection_id,
            anchor_event_type: anchor.event_type.clone(),
            authenticated_opened_event_count: verified_opening.opened_events.len(),
            semantic_global_envelope_event_count: semantic_collection_events.len(),
            semantic_collection_event_count: semantic_collection_events.len(),
            semantic_collection_first_order: semantic_collection_events
                .first()
                .map(|event| event.order_seq),
            semantic_collection_last_order: semantic_collection_events
                .last()
                .map(|event| event.order_seq),
            semantic_collection_first_time: semantic_collection_events
                .first()
                .map(|event| event.block_ts),
            semantic_collection_last_time: semantic_collection_events
                .last()
                .map(|event| event.block_ts),
            semantic_collection_digest,
            authenticated_opening_digest,
            range_proof_node_count: opening.proof_nodes.len(),
            closure_open_seconds: open_elapsed,
            server_online_seconds: server_elapsed,
            response_packaging_seconds: packaging_elapsed,
            verifier_online_seconds: verifier_elapsed,
            result_count_before_global_dedup: result_count,
            vo_size_bytes,
            verification_completed,
            all_results_equal,
            witness_replayed_equal,
            compression_ratio: server.metrics.compression_ratio(),
            reuse_ratio: server.metrics.reuse_ratio(),
            metrics: server.metrics,
        });

        if (offset + 1) % args.progress_every == 0 || offset + 1 == plan_rows.len() {
            eprintln!(
                "[phase9] {}/{} closures; raw results {}; elapsed {:.2}s",
                offset + 1,
                plan_rows.len(),
                all_matches.len(),
                batch_started.elapsed().as_secs_f64()
            );
        }
    }

    let materialization_started = Instant::now();
    let result_count_before_global_dedup = all_matches.len();
    all_matches.sort();
    all_matches.dedup();
    let result_count = all_matches.len();
    let duplicate_result_count = result_count_before_global_dedup - result_count;
    let result_digest_sha256 = digest_matches(&all_matches)?;
    let result_materialization_seconds = materialization_started.elapsed().as_secs_f64();
    aggregate_metrics.final_result_count = result_count as u64;
    aggregate_metrics.server_eval_seconds = reports
        .iter()
        .map(|report| report.metrics.server_eval_seconds)
        .sum();
    aggregate_metrics.vo_size_bytes = total_vo_size_bytes;
    let aggregate_compression_ratio = aggregate_metrics.compression_ratio();
    let aggregate_reuse_ratio = aggregate_metrics.reuse_ratio();
    let expected_result_equal = args
        .expected_result_count
        .map(|expected| expected == result_count);
    let expected_closure_equal = args
        .expected_plan_rows
        .filter(|_| args.max_closures.is_none())
        .map(|expected| expected == reports.len());
    let (peak_rss_bytes, peak_commit_bytes) = process_memory_peaks();
    let residual_server_e2e_seconds = query_read_compile_seconds
        + closure_open_seconds
        + server_online_seconds
        + response_packaging_seconds
        + result_materialization_seconds;
    let summary = BatchSummary {
        protocol: "funnelq-residual-obligation-formal-page-phase9-batch-v4",
        preliminary_once: true,
        query_id: query.query_id.clone(),
        epoch_built_before_query: true,
        query_scope_was_legacy_implicit,
        explicit_runtime_scope: format!("Episode({}, {}s)", args.anchor_atom, window_seconds),
        epoch_root: residual_epoch.epoch_root.clone(),
        formal_event_page_root: formal_epoch.page_root().to_owned(),
        epoch_root_chain_bound,
        fresh_chain_eth_call_performed,
        chain_read_seconds,
        epoch_event_count: formal_epoch.manifest().record_count,
        epoch_index_bytes: usize::try_from(epoch_index_bytes)
            .context("formal epoch persisted bytes exceed usize")?,
        epoch_query_specific_storage_bytes: 0,
        processed_closure_count: reports.len(),
        expected_closure_count: args.expected_plan_rows,
        closure_count_equal_to_expected: expected_closure_equal,
        result_count_before_global_dedup,
        duplicate_result_count,
        result_count,
        expected_result_count: args.expected_result_count,
        result_count_equal_to_expected: expected_result_equal,
        result_digest_sha256,
        all_closures_verified: reports.iter().all(|row| row.verification_completed),
        all_results_equal_on_replay: reports.iter().all(|row| row.all_results_equal),
        all_witnesses_equal_on_replay: reports.iter().all(|row| row.witness_replayed_equal),
        all_anchors_found: true,
        all_anchor_types_valid: true,
        all_anchor_collections_valid: true,
        all_episode_boundaries_valid: true,
        artifact_hash_seconds,
        artifact_load_seconds,
        epoch_index_build_seconds,
        query_read_compile_seconds,
        closure_plan_read_seconds,
        closure_open_seconds,
        server_online_seconds,
        response_packaging_seconds,
        result_materialization_seconds,
        residual_server_e2e_seconds,
        verifier_online_seconds,
        batch_wall_seconds: batch_started.elapsed().as_secs_f64(),
        total_vo_size_bytes,
        total_authenticated_opened_events,
        total_semantic_global_envelope_events,
        total_semantic_collection_events,
        aggregate_metrics,
        aggregate_compression_ratio,
        aggregate_reuse_ratio,
        peak_rss_bytes,
        peak_commit_bytes,
    };
    let report = BatchReport {
        input_audit,
        summary,
        per_closure: reports,
        unique_matches: all_matches,
    };
    write_csv(&args.output_csv, &report.per_closure)?;
    if let Some(parent) = args.output_json.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.output_json, serde_json::to_vec_pretty(&report)?)?;

    if report.summary.result_count_equal_to_expected == Some(false) {
        bail!(
            "global result count mismatch: expected {}, got {}; reports were preserved",
            report.summary.expected_result_count.unwrap_or_default(),
            report.summary.result_count
        )
    }
    eprintln!(
        "[phase9] completed: {} unique results, digest {}, VO {} bytes",
        report.summary.result_count,
        report.summary.result_digest_sha256,
        report.summary.total_vo_size_bytes
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_digest_is_order_independent_after_canonical_sort() {
        let make = |event: &str| MatchRecord {
            query_id: "Q".to_owned(),
            branch_id: "b".to_owned(),
            event_bindings: BTreeMap::from([("x".to_owned(), event.to_owned())]),
            variable_bindings: BTreeMap::new(),
        };
        let mut left = vec![make("e2"), make("e1")];
        let mut right = vec![make("e1"), make("e2")];
        left.sort();
        right.sort();
        assert_eq!(
            digest_matches(&left).unwrap(),
            digest_matches(&right).unwrap()
        );
    }
}
