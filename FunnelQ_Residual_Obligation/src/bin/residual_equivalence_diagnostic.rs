use anyhow::{bail, Context, Result};
use clap::Parser;
use funnelq_residual_obligation::equivalence_diagnostic::{
    completion_oracle, completion_stats, frontier_ids_excluded_by_time, layered_stats, CompletionObservation,
    LayerAccumulator, PrefixObservation,
};
use funnelq_residual_obligation::{
    compile_query_document, evaluate_query_with_diagnostics, verify_formal_episode_opening,
    CanonicalEvent, EvaluationRequest, FormalEventPageIndex, MatchRecord,
    QueryIndependentIndex, ScopeSpec,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(about="Observe the unchanged Residual evaluator on frozen formal closures")]
struct Args {
    #[arg(long)] reference_report: PathBuf,
    #[arg(long)] output_dir: PathBuf,
    #[arg(long, default_value_t=10)] oracle_per_bucket: usize,
    #[arg(long, default_value_t=128)] oracle_max_events: usize,
}

#[derive(Clone)]
struct Sample {
    closure_id: i64,
    events: Vec<CanonicalEvent>,
    request: EvaluationRequest,
    prefixes: Vec<PrefixObservation>,
    matches: Vec<MatchRecord>,
    semantic_events: usize,
}

fn path_field(value: &Value, key: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(value[key].as_str().with_context(|| format!("missing {key}"))?))
}

fn check_hash(bytes: &[u8], expected: &Value, label: &str) -> Result<()> {
    let actual = format!("{:x}", Sha256::digest(bytes));
    if expected.as_str() != Some(actual.as_str()) { bail!("{label} differs from frozen reference") }
    Ok(())
}

fn bucket(size: usize) -> usize {
    [8,16,32,64,128].into_iter().find(|bound| size <= *bound).unwrap_or(usize::MAX)
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.output_dir)?;
    let reference: Value = serde_json::from_slice(&fs::read(&args.reference_report)?)?;
    let audit = &reference["input_audit"];
    let epoch = FormalEventPageIndex::load(&path_field(audit, "formal_manifest_path")?)?;
    if epoch.page_root() != audit["formal_event_page_root"].as_str().unwrap_or("") {
        bail!("formal page root differs from frozen reference")
    }
    let query_bytes = fs::read(path_field(audit, "query_path")?)?;
    check_hash(&query_bytes, &audit["query_sha256"], "query")?;
    let raw_query: Value = serde_json::from_slice(&query_bytes)?;
    let mut query = compile_query_document(&raw_query)?;
    let window = raw_query.pointer("/full_binding_plan/delta_seconds")
        .and_then(Value::as_i64).context("missing frozen Episode window")?;
    query.scope = ScopeSpec::Episode { anchor_atom: "root".to_owned(), window_seconds: window };
    let plan_bytes = fs::read(path_field(audit, "closure_plan_path")?)?;
    check_hash(&plan_bytes, &audit["closure_plan_sha256"], "closure plan")?;
    let plan = std::str::from_utf8(&plan_bytes)?;
    let mut lines = plan.trim_start_matches('\u{feff}').lines();
    if lines.next() != Some("closure_id,anchor_order,end_ts,collection_id") { bail!("unexpected plan header") }
    let mut rows = Vec::new();
    for line in lines.filter(|line| !line.trim().is_empty()) {
        let fields = line.split(',').map(str::parse::<i64>).collect::<std::result::Result<Vec<_>,_>>()?;
        if fields.len() != 4 { bail!("malformed closure plan") }
        rows.push((fields[0],fields[1],fields[2],fields[3]));
    }
    let mut csv = BufWriter::new(File::create(args.output_dir.join("residual_key_layers_all.csv"))?);
    writeln!(csv,"query,closure_id,closure_events,branch,depth,N_prefix,N_structure,N_binding,N_time,N_injective,compression,different_remaining_structure_pairs,different_live_binding_pairs,different_temporal_obligation_pairs,different_injectivity_frontier_pairs,other_pairs,same_full_key_pairs")?;
    let mut global: BTreeMap<(String,usize),LayerAccumulator> = BTreeMap::new();
    let mut scoped_rows = Vec::new();
    let mut frontier_time_audit: BTreeMap<usize,(u64,u64,u64)> = BTreeMap::new();
    let mut all_matches = Vec::new();
    let mut total_merges = 0u64;
    let mut total_visits = 0u64;
    let mut total_keys = 0u64;
    let mut random_samples: BTreeMap<usize,BTreeMap<u64,Sample>> = BTreeMap::new();
    let mut branching_samples: BTreeMap<usize,BTreeMap<(Reverse<usize>,u64),Sample>> = BTreeMap::new();
    let started = Instant::now();
    for (offset,(closure_id,anchor_order,end_ts,collection_id)) in rows.iter().enumerate() {
        let anchor_id = format!("order:{anchor_order}");
        let opening = epoch.open_episode(&anchor_id, window)?;
        if opening.anchor_time as i64 + window > *end_ts || opening.anchor_collection as i64 != *collection_id {
            bail!("closure descriptor mismatch for {closure_id}")
        }
        let verified = verify_formal_episode_opening(epoch.page_root(), &anchor_id, window, &opening)?;
        let semantic_count = verified.semantic_events.len();
        let request = EvaluationRequest {
            allowed_event_ids: Some(verified.semantic_events.iter().map(|e| e.event_id.clone()).collect()),
            fixed_atoms: BTreeMap::from([("root".to_owned(),anchor_id)]),
        };
        let index = QueryIndependentIndex::build(verified.opened_events)?;
        let output = evaluate_query_with_diagnostics(&index, &query, &request)?;
        total_merges += output.metrics.quotient_merge_count;
        total_visits += output.metrics.nonterminal_residual_state_visits;
        total_keys += output.metrics.distinct_residual_keys;
        let local = layered_stats(&output.diagnostic_prefixes);
        for ((branch,depth),stats) in &local {
            writeln!(csv,"{},{},{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{}",
                query.query_id,closure_id,semantic_count,branch,depth,stats.n_prefix,
                stats.n_structure,stats.n_binding,stats.n_time,stats.n_injective,stats.compression,
                stats.different_remaining_structure_pairs,stats.different_live_binding_pairs,
                stats.different_temporal_obligation_pairs,stats.different_injectivity_frontier_pairs,
                stats.other_pairs,stats.same_full_key_pairs)?;
            scoped_rows.push(json!({"closure_id":closure_id,"closure_events":semantic_count,"branch":branch,"depth":depth,"stats":stats}));
        }
        for prefix in &output.diagnostic_prefixes {
            global.entry((prefix.key.branch_id.clone(),prefix.binding_depth)).or_default().observe(prefix);
            let counts = frontier_time_audit.entry(prefix.binding_depth).or_default();
            counts.0 += 1;
            counts.1 += prefix.key.injectivity_frontier.len() as u64;
            counts.2 += frontier_ids_excluded_by_time(&index,&query,&prefix.key).len() as u64;
        }
        if semantic_count <= args.oracle_max_events {
            let b = bucket(semantic_count);
            let digest = Sha256::digest(closure_id.to_le_bytes());
            let hash = u64::from_le_bytes(digest[..8].try_into().unwrap());
            let excess = output.diagnostic_prefixes.len().saturating_sub(local.len());
            let random = random_samples.entry(b).or_default();
            let high = branching_samples.entry(b).or_default();
            let keep_random = random.len() < args.oracle_per_bucket || random.last_key_value().is_some_and(|(last,_)| hash < *last);
            let high_key = (Reverse(excess),hash);
            let keep_high = high.len() < args.oracle_per_bucket || high.last_key_value().is_some_and(|(last,_)| high_key < *last);
            if keep_random || keep_high {
                let sample = Sample { closure_id:*closure_id,events:index.events().to_vec(),request,
                    prefixes:output.diagnostic_prefixes.clone(),matches:output.matches.clone(),semantic_events:semantic_count };
                if keep_random { random.insert(hash,sample.clone()); if random.len() > args.oracle_per_bucket { random.pop_last(); } }
                if keep_high { high.insert(high_key,sample); if high.len() > args.oracle_per_bucket { high.pop_last(); } }
            }
        }
        all_matches.extend(output.matches);
        if (offset+1)%1000 == 0 || offset+1 == rows.len() {
            eprintln!("[equivalence] {} {}/{} closures; {} visits; {:.1}s",query.query_id,offset+1,rows.len(),total_visits,started.elapsed().as_secs_f64());
            csv.flush()?;
        }
    }
    csv.flush()?;
    all_matches.sort(); all_matches.dedup();
    let expected: Vec<MatchRecord> = serde_json::from_value(reference["unique_matches"].clone())?;
    if all_matches != expected { bail!("instrumented results differ from frozen full formal run") }
    let mut samples = BTreeMap::new();
    for sample in random_samples.into_values().flat_map(BTreeMap::into_values)
        .chain(branching_samples.into_values().flat_map(BTreeMap::into_values)) {
        samples.insert(sample.closure_id,sample);
    }
    let mut oracle_rows = Vec::new();
    let mut oracle_prefix_rows = Vec::new();
    for (number,sample) in samples.values().enumerate() {
        let index = QueryIndependentIndex::build(sample.events.clone())?;
        let observations = completion_oracle(&index,&query,&sample.request,&sample.prefixes,&sample.matches)?;
        let mut groups: BTreeMap<(String,usize),Vec<CompletionObservation>> = BTreeMap::new();
        for observation in observations {
            oracle_prefix_rows.push(json!({"closure_id":sample.closure_id,"observation":observation}));
            groups.entry((observation.branch_id.clone(),observation.binding_depth)).or_default().push(observation);
        }
        for ((branch,depth),observations) in groups {
            oracle_rows.push(json!({"closure_id":sample.closure_id,"closure_events":sample.semantic_events,
                "branch":branch,"depth":depth,"stats":completion_stats(&observations)}));
        }
        if (number+1)%10 == 0 || number+1 == samples.len() {
            eprintln!("[oracle] {} {}/{} complete real closures",query.query_id,number+1,samples.len());
        }
    }
    let unscoped: Vec<_> = global.into_iter().map(|((branch,depth),acc)|
        json!({"branch":branch,"depth":depth,"stats":acc.stats()})).collect();
    let report = json!({
        "protocol":"residual-equivalence-diagnostic-v1","query_id":query.query_id,
        "reference_report":args.reference_report,"formal_page_root":epoch.page_root(),
        "full_epoch_events":epoch.manifest().record_count,"processed_closures":rows.len(),
        "results_equal_to_frozen_reference":true,"result_count":all_matches.len(),
        "diagnostic_does_not_change_evaluator_semantics":true,"memo_scope":"one closure and one branch",
        "total_nonterminal_prefix_visits":total_visits,"total_residual_keys":total_keys,"quotient_merge_count":total_merges,
        "layer_projection_definition":{
            "structure":"remaining_atoms only; comparisons are grouped by closure, branch and depth",
            "binding":"structure + live_bindings + live_fields + same_tx_value",
            "time":"binding + temporal_obligations","injective":"N_injective reports actual full ResidualKey; other_pairs isolates pending constraints/component context beyond the layered fields",
            "blocker_counts":"pairwise first differing layer, compared only within a closure/branch/depth",
            "terminal_states":"excluded: evaluator returns before Residualize when no atom remains"},
        "closure_scoped_layer_rows":scoped_rows,
        "frontier_time_audit":frontier_time_audit.into_iter().map(|(depth,(prefixes,entries,excluded))|
            json!({"depth":depth,"prefixes":prefixes,"frontier_entries":entries,
                "frontier_entries_already_excluded_by_time":excluded})).collect::<Vec<_>>(),
        "query_global_unscoped_key_diagnostic":unscoped,
        "oracle_sampling":{"complete_real_closures":samples.len(),"max_semantic_events":args.oracle_max_events,
            "selection":"per size bucket: deterministic hash sample plus highest excess-prefix count; union deduplicated",
            "per_selection_per_bucket":args.oracle_per_bucket,"event_sampling":false,
            "scope":"P/K/C are compared within each closure; aggregate by summing scoped counts"},
        "completion_oracle_rows":oracle_rows,"completion_oracle_prefixes":oracle_prefix_rows
    });
    fs::write(args.output_dir.join("diagnostic.json"),serde_json::to_vec_pretty(&report)?)?;
    eprintln!("[equivalence] completed {}: {} prefixes, {} keys, {} merges, {} results",query.query_id,total_visits,total_keys,total_merges,all_matches.len());
    Ok(())
}
