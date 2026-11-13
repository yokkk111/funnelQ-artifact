use anyhow::{bail, Context, Result};
use clap::Parser;
use funnelq_residual_obligation::io::{load_compact_artifact, load_events_json};
use funnelq_residual_obligation::{
    compile_query_document, execute_authenticated_query, make_certified_closure,
    make_certified_episode_closure, make_certified_same_tx_closure,
    verify_by_query_oblivious_replay, QueryIndependentIndex, ScopeSpec, TrustedReplayContext,
};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, conflicts_with = "artifact")]
    events_json: Option<PathBuf>,
    #[arg(long, conflicts_with = "events_json")]
    artifact: Option<PathBuf>,
    #[arg(long)]
    query: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long)]
    closure_event_ids: Option<PathBuf>,
    #[arg(long)]
    fixed_root_event_id: Option<String>,
    #[arg(long, default_value = "residual-closure")]
    closure_id: String,
    #[arg(long, default_value_t = 0)]
    upstream_proof_bytes: u64,
}

#[derive(Serialize)]
struct RunReport {
    protocol: &'static str,
    epoch_built_before_query: bool,
    epoch_root: String,
    epoch_event_count: usize,
    epoch_index_bytes: usize,
    epoch_query_specific_storage_bytes: usize,
    authenticated_opening: &'static str,
    authenticated_opened_event_count: usize,
    semantic_closure_event_count: Option<usize>,
    range_proof_node_count: usize,
    query_id: String,
    result_count: usize,
    compression_ratio: f64,
    reuse_ratio: f64,
    server: funnelq_residual_obligation::EvaluationOutput,
    verification: funnelq_residual_obligation::ReplayVerificationReport,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let events = match (&args.events_json, &args.artifact) {
        (Some(path), None) => load_events_json(path)?,
        (None, Some(path)) => load_compact_artifact(path)?,
        _ => bail!("exactly one of --events-json or --artifact is required"),
    };

                                                                           
                                                           
    let epoch_index = QueryIndependentIndex::build(events)?;
    let epoch_metadata = epoch_index.metadata().clone();
    let query_document: Value = serde_json::from_slice(
        &fs::read(&args.query).with_context(|| format!("read {}", args.query.display()))?,
    )?;
    let query = compile_query_document(&query_document)?;

    let mut fixed_atoms = BTreeMap::new();
    if let Some(event_id) = args.fixed_root_event_id.as_ref() {
        let anchor_atom = match &query.scope {
            ScopeSpec::Episode { anchor_atom, .. } => anchor_atom.as_str(),
            _ => "root",
        };
        fixed_atoms.insert(anchor_atom.to_owned(), event_id.clone());
    }

    let closure = if let Some(path) = &args.closure_event_ids {
        let ids: Vec<String> = serde_json::from_slice(
            &fs::read(path).with_context(|| format!("read {}", path.display()))?,
        )?;
        let closure_events = ids
            .into_iter()
            .map(|id| {
                epoch_index
                    .event_index(&id)
                    .map(|index| epoch_index.event(index).clone())
                    .with_context(|| format!("closure references unknown event {id}"))
            })
            .collect::<Result<Vec<_>>>()?;
        make_certified_closure(
            epoch_metadata.epoch_root.clone(),
            &args.closure_id,
            closure_events,
            args.upstream_proof_bytes,
        )?
    } else if let ScopeSpec::Episode {
        anchor_atom,
        window_seconds,
    } = &query.scope
    {
        let anchor_event_id = fixed_atoms.get(anchor_atom).with_context(|| {
            format!("Episode query requires --fixed-root-event-id for atom {anchor_atom}")
        })?;
        make_certified_episode_closure(
            &epoch_index,
            &args.closure_id,
            anchor_event_id,
            *window_seconds,
            args.upstream_proof_bytes,
        )?
    } else if query.scope == ScopeSpec::SameTx {
        let anchor_event_id = fixed_atoms
            .get("root")
            .context("SameTx query without --closure-event-ids requires --fixed-root-event-id")?;
        make_certified_same_tx_closure(
            &epoch_index,
            &args.closure_id,
            anchor_event_id,
            args.upstream_proof_bytes,
        )?
    } else {
        make_certified_closure(
            epoch_metadata.epoch_root.clone(),
            &args.closure_id,
            epoch_index.events().to_vec(),
            args.upstream_proof_bytes,
        )?
    };
    let (response, server) =
        execute_authenticated_query(&epoch_index, &query, &closure, fixed_atoms)?;
                                                                               
                                                                           
                                                              
    let trusted = TrustedReplayContext {
        trusted_epoch_root: epoch_metadata.epoch_root.clone(),
        coverage_verified: true,
        synopsis_verified: true,
        chain_binding_verified: true,
    };
    let verification = verify_by_query_oblivious_replay(&trusted, &query, &closure, &response)?;
    let (authenticated_opening, semantic_closure_event_count, range_proof_node_count) =
        if let Some(range) = &closure.episode_range {
            (
                "query-time-episode-block-range",
                Some(range.semantic_event_end - range.semantic_event_start),
                range.authentication.proof_nodes.len(),
            )
        } else if let Some(range) = &closure.same_tx_range {
            (
                "query-time-same-tx-block-range",
                Some(range.semantic_event_end - range.semantic_event_start),
                range.authentication.proof_nodes.len(),
            )
        } else {
            ("external-upstream-certified-residual-closure", None, 0)
        };
    let report = RunReport {
        protocol: "funnelq-residual-obligation-v1",
        epoch_built_before_query: true,
        epoch_root: epoch_metadata.epoch_root,
        epoch_event_count: epoch_metadata.event_count,
        epoch_index_bytes: epoch_metadata.serialized_index_bytes,
        epoch_query_specific_storage_bytes: epoch_metadata.query_specific_storage_bytes,
        authenticated_opening,
        authenticated_opened_event_count: closure.canonical_events.len(),
        semantic_closure_event_count,
        range_proof_node_count,
        query_id: query.query_id,
        result_count: response.matches.len(),
        compression_ratio: server.metrics.compression_ratio(),
        reuse_ratio: server.metrics.reuse_ratio(),
        server,
        verification,
    };
    let encoded = serde_json::to_vec_pretty(&report)?;
    if let Some(path) = args.output {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, encoded)?;
    } else {
        println!("{}", String::from_utf8(encoded)?);
    }
    Ok(())
}
