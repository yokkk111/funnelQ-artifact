use crate::evaluator::{evaluate_query, EvaluationOutput, EvaluationRequest, MatchRecord};
use crate::index::{
    verify_authenticated_event_range, AuthenticatedEventRange, QueryIndependentIndex,
};
use crate::model::CanonicalEvent;
use crate::query::{QuerySpec, ScopeSpec};
use crate::witness::FactorizedWitness;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CertifiedEpisodeRange {
    pub authentication: AuthenticatedEventRange,
    pub anchor_event_id: String,
    pub anchor_order: i64,
    pub anchor_time: i64,
    pub window_seconds: i64,
    pub semantic_event_start: usize,
    pub semantic_event_end: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CertifiedSameTxRange {
    pub authentication: AuthenticatedEventRange,
    pub anchor_event_id: String,
    pub tx_hash: String,
    pub semantic_event_start: usize,
    pub semantic_event_end: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClosureAuthenticationMode {
                                                                          
                                                                               
    ExternalUpstreamProof,
    EpochEpisodeRange,
    EpochSameTxRange,
}

fn external_authentication_mode() -> ClosureAuthenticationMode {
    ClosureAuthenticationMode::ExternalUpstreamProof
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CertifiedResidualClosure {
    pub trusted_epoch_root: String,
    pub closure_id: String,
    pub canonical_events: Vec<CanonicalEvent>,
    pub event_batch_digest: String,
    pub coverage_verified: bool,
    pub synopsis_verified: bool,
    pub chain_binding_verified: bool,
    #[serde(default)]
    pub upstream_proof_bytes: u64,
    #[serde(default = "external_authentication_mode")]
    pub authentication_mode: ClosureAuthenticationMode,
                                                                           
                                                                  
    #[serde(default)]
    pub episode_range: Option<CertifiedEpisodeRange>,
    #[serde(default)]
    pub same_tx_range: Option<CertifiedSameTxRange>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthenticatedQueryResponse {
    pub trusted_epoch_root: String,
    pub closure_id: String,
    pub query_digest: String,
    pub fixed_atoms: BTreeMap<String, String>,
    pub matches: Vec<MatchRecord>,
    pub witness: FactorizedWitness,
}

                                                                            
                                                                        
                                                                        
                                                                             
                                         
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TrustedReplayContext {
    pub trusted_epoch_root: String,
    pub coverage_verified: bool,
    pub synopsis_verified: bool,
    pub chain_binding_verified: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayVerificationReport {
    pub completed: bool,
    pub query_digest_consistent: bool,
    pub epoch_consistent: bool,
    pub coverage_verified: bool,
    pub synopsis_verified: bool,
    pub chain_binding_verified: bool,
    pub all_results_equal: bool,
    pub witness_replayed_equal: bool,
    pub verifier_replay_seconds: f64,
    pub vo_size_bytes: u64,
    pub replay: EvaluationOutput,
}

pub fn query_digest(query: &QuerySpec) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"funnelq-residual-query-v1\0");
    hasher.update(serde_json::to_vec(query)?);
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn event_batch_digest(events: &[CanonicalEvent]) -> Result<String> {
    let mut events = events.to_vec();
    events.sort_by(|left, right| {
        (left.order_seq, &left.event_id).cmp(&(right.order_seq, &right.event_id))
    });
    let mut hasher = Sha256::new();
    hasher.update(b"funnelq-authenticated-residual-events-v1\0");
    for event in events {
        let encoded = serde_json::to_vec(&event)?;
        hasher.update((encoded.len() as u64).to_le_bytes());
        hasher.update(encoded);
    }
    Ok(format!("{:x}", hasher.finalize()))
}

pub fn make_certified_closure(
    trusted_epoch_root: impl Into<String>,
    closure_id: impl Into<String>,
    canonical_events: Vec<CanonicalEvent>,
    upstream_proof_bytes: u64,
) -> Result<CertifiedResidualClosure> {
    Ok(CertifiedResidualClosure {
        trusted_epoch_root: trusted_epoch_root.into(),
        closure_id: closure_id.into(),
        event_batch_digest: event_batch_digest(&canonical_events)?,
        canonical_events,
        coverage_verified: false,
        synopsis_verified: false,
        chain_binding_verified: false,
        upstream_proof_bytes,
        authentication_mode: ClosureAuthenticationMode::ExternalUpstreamProof,
        episode_range: None,
        same_tx_range: None,
    })
}

                                                                           
                                                                       
pub fn make_certified_episode_closure(
    epoch_index: &QueryIndependentIndex,
    closure_id: impl Into<String>,
    anchor_event_id: &str,
    window_seconds: i64,
    upstream_proof_bytes: u64,
) -> Result<CertifiedResidualClosure> {
    let opened = epoch_index.open_episode_range(anchor_event_id, window_seconds)?;
    let event_batch_digest = event_batch_digest(&opened.events)?;
    Ok(CertifiedResidualClosure {
        trusted_epoch_root: epoch_index.metadata().epoch_root.clone(),
        closure_id: closure_id.into(),
        canonical_events: opened.events,
        event_batch_digest,
        coverage_verified: true,
        synopsis_verified: true,
        chain_binding_verified: false,
        upstream_proof_bytes,
        authentication_mode: ClosureAuthenticationMode::EpochEpisodeRange,
        episode_range: Some(CertifiedEpisodeRange {
            authentication: opened.proof,
            anchor_event_id: opened.anchor_event_id,
            anchor_order: opened.anchor_order,
            anchor_time: opened.anchor_time,
            window_seconds: opened.window_seconds,
            semantic_event_start: opened.semantic_event_start,
            semantic_event_end: opened.semantic_event_end,
        }),
        same_tx_range: None,
    })
}

pub fn make_certified_same_tx_closure(
    epoch_index: &QueryIndependentIndex,
    closure_id: impl Into<String>,
    anchor_event_id: &str,
    upstream_proof_bytes: u64,
) -> Result<CertifiedResidualClosure> {
    let opened = epoch_index.open_same_tx_range(anchor_event_id)?;
    let event_batch_digest = event_batch_digest(&opened.events)?;
    Ok(CertifiedResidualClosure {
        trusted_epoch_root: epoch_index.metadata().epoch_root.clone(),
        closure_id: closure_id.into(),
        canonical_events: opened.events,
        event_batch_digest,
        coverage_verified: true,
        synopsis_verified: true,
        chain_binding_verified: false,
        upstream_proof_bytes,
        authentication_mode: ClosureAuthenticationMode::EpochSameTxRange,
        episode_range: None,
        same_tx_range: Some(CertifiedSameTxRange {
            authentication: opened.proof,
            anchor_event_id: opened.anchor_event_id,
            tx_hash: opened.tx_hash,
            semantic_event_start: opened.semantic_event_start,
            semantic_event_end: opened.semantic_event_end,
        }),
    })
}

fn validate_episode_range(
    query: &QuerySpec,
    closure: &CertifiedResidualClosure,
    fixed_atoms: &BTreeMap<String, String>,
) -> Result<()> {
    let Some(range) = &closure.episode_range else {
        return Ok(());
    };
    verify_authenticated_event_range(
        &closure.trusted_epoch_root,
        &closure.canonical_events,
        &range.authentication,
    )?;
    let ScopeSpec::Episode {
        anchor_atom,
        window_seconds,
    } = &query.scope
    else {
        bail!("an authenticated Episode range cannot be used for a non-Episode query")
    };
    if *window_seconds != range.window_seconds {
        bail!("authenticated Episode range uses a different query window")
    }
    if fixed_atoms.get(anchor_atom) != Some(&range.anchor_event_id) {
        bail!("authenticated Episode range is bound to a different anchor")
    }
    let opened_start = range.authentication.opened_event_start;
    let opened_end = opened_start + closure.canonical_events.len();
    if range.semantic_event_start < opened_start
        || range.semantic_event_start >= range.semantic_event_end
        || range.semantic_event_end > opened_end
    {
        bail!("semantic Episode is outside its authenticated opening")
    }
    let anchor = closure
        .canonical_events
        .iter()
        .find(|event| event.event_id == range.anchor_event_id)
        .ok_or_else(|| anyhow::anyhow!("authenticated Episode omits its anchor"))?;
    if anchor.order_seq != range.anchor_order || anchor.block_ts != range.anchor_time {
        bail!("authenticated Episode anchor metadata was modified")
    }
    let local_start = range.semantic_event_start - opened_start;
    let local_end = range.semantic_event_end - opened_start;
    if range.semantic_event_start > 0 {
        let predecessor = local_start
            .checked_sub(1)
            .and_then(|index| closure.canonical_events.get(index))
            .ok_or_else(|| anyhow::anyhow!("authenticated Episode omits its left boundary"))?;
        if predecessor.order_seq >= range.anchor_order {
            bail!("authenticated Episode has an invalid left boundary")
        }
    }
    let upper_time = range.anchor_time.saturating_add(range.window_seconds);
    let semantic_events = &closure.canonical_events[local_start..local_end];
    if semantic_events
        .iter()
        .any(|event| event.order_seq < range.anchor_order || event.block_ts > upper_time)
    {
        bail!("authenticated Episode semantic interval contains an out-of-range event")
    }
    if !semantic_events
        .iter()
        .any(|event| event.event_id == range.anchor_event_id)
    {
        bail!("authenticated Episode anchor is outside the semantic interval")
    }
    if range.semantic_event_end < range.authentication.epoch_event_count {
        let successor = closure
            .canonical_events
            .get(local_end)
            .ok_or_else(|| anyhow::anyhow!("authenticated Episode omits its right boundary"))?;
        if successor.block_ts <= upper_time {
            bail!("authenticated Episode was truncated before the query window ended")
        }
    }
    Ok(())
}

fn validate_same_tx_range(
    query: &QuerySpec,
    closure: &CertifiedResidualClosure,
    fixed_atoms: &BTreeMap<String, String>,
) -> Result<()> {
    let Some(range) = &closure.same_tx_range else {
        return Ok(());
    };
    verify_authenticated_event_range(
        &closure.trusted_epoch_root,
        &closure.canonical_events,
        &range.authentication,
    )?;
    if query.scope != ScopeSpec::SameTx {
        bail!("an authenticated SameTx range cannot be used for a non-SameTx query")
    }
    if fixed_atoms.get("root") != Some(&range.anchor_event_id) {
        bail!("authenticated SameTx range is bound to a different root anchor")
    }
    let opened_start = range.authentication.opened_event_start;
    let opened_end = opened_start + closure.canonical_events.len();
    if range.semantic_event_start < opened_start
        || range.semantic_event_start >= range.semantic_event_end
        || range.semantic_event_end > opened_end
    {
        bail!("semantic SameTx extent is outside its authenticated opening")
    }
    let anchor = closure
        .canonical_events
        .iter()
        .find(|event| event.event_id == range.anchor_event_id)
        .ok_or_else(|| anyhow::anyhow!("authenticated SameTx extent omits its anchor"))?;
    if anchor.tx_hash != range.tx_hash || range.tx_hash.is_empty() {
        bail!("authenticated SameTx anchor transaction was modified")
    }
    let local_start = range.semantic_event_start - opened_start;
    let local_end = range.semantic_event_end - opened_start;
    let semantic_events = &closure.canonical_events[local_start..local_end];
    if semantic_events.is_empty()
        || semantic_events
            .iter()
            .any(|event| event.tx_hash != range.tx_hash)
    {
        bail!("authenticated SameTx semantic extent contains another transaction")
    }
    if range.semantic_event_start > 0 {
        let predecessor = local_start
            .checked_sub(1)
            .and_then(|index| closure.canonical_events.get(index))
            .ok_or_else(|| {
                anyhow::anyhow!("authenticated SameTx extent omits its left boundary")
            })?;
        if predecessor.tx_hash == range.tx_hash {
            bail!("authenticated SameTx extent was truncated on the left")
        }
    }
    if range.semantic_event_end < range.authentication.epoch_event_count {
        let successor = closure.canonical_events.get(local_end).ok_or_else(|| {
            anyhow::anyhow!("authenticated SameTx extent omits its right boundary")
        })?;
        if successor.tx_hash == range.tx_hash {
            bail!("authenticated SameTx extent was truncated on the right")
        }
    }
    Ok(())
}

fn validate_authentication_mode(closure: &CertifiedResidualClosure) -> Result<()> {
    match closure.authentication_mode {
        ClosureAuthenticationMode::ExternalUpstreamProof => {
            if closure.episode_range.is_some() || closure.same_tx_range.is_some() {
                bail!("external closure cannot self-declare an epoch range proof")
            }
        }
        ClosureAuthenticationMode::EpochEpisodeRange => {
            if closure.episode_range.is_none() || closure.same_tx_range.is_some() {
                bail!("Episode authentication mode has inconsistent proof fields")
            }
        }
        ClosureAuthenticationMode::EpochSameTxRange => {
            if closure.same_tx_range.is_none() || closure.episode_range.is_some() {
                bail!("SameTx authentication mode has inconsistent proof fields")
            }
        }
    }
    Ok(())
}

fn vo_size(
    closure: &CertifiedResidualClosure,
    response: &AuthenticatedQueryResponse,
) -> Result<u64> {
    Ok(closure.upstream_proof_bytes
        + serde_json::to_vec(closure)?.len() as u64
        + serde_json::to_vec(response)?.len() as u64)
}

pub fn execute_authenticated_query(
    epoch_index: &QueryIndependentIndex,
    query: &QuerySpec,
    closure: &CertifiedResidualClosure,
    fixed_atoms: BTreeMap<String, String>,
) -> Result<(AuthenticatedQueryResponse, EvaluationOutput)> {
    if closure.trusted_epoch_root != epoch_index.metadata().epoch_root {
        bail!("certified closure is bound to a different epoch root")
    }
    if event_batch_digest(&closure.canonical_events)? != closure.event_batch_digest {
        bail!("certified residual event batch digest mismatch")
    }
    validate_authentication_mode(closure)?;
    validate_episode_range(query, closure, &fixed_atoms)?;
    validate_same_tx_range(query, closure, &fixed_atoms)?;
    let allowed_event_ids = closure
        .canonical_events
        .iter()
        .map(|event| event.event_id.clone())
        .collect::<BTreeSet<_>>();
    let request = EvaluationRequest {
        allowed_event_ids: Some(allowed_event_ids),
        fixed_atoms: fixed_atoms.clone(),
    };
    let mut output = evaluate_query(epoch_index, query, &request)?;
    let response = AuthenticatedQueryResponse {
        trusted_epoch_root: closure.trusted_epoch_root.clone(),
        closure_id: closure.closure_id.clone(),
        query_digest: query_digest(query)?,
        fixed_atoms,
        matches: output.matches.clone(),
        witness: output.witness.clone(),
    };
    output.metrics.vo_size_bytes = vo_size(closure, &response)?;
    Ok((response, output))
}

pub fn verify_by_query_oblivious_replay(
    trusted: &TrustedReplayContext,
    query: &QuerySpec,
    closure: &CertifiedResidualClosure,
    response: &AuthenticatedQueryResponse,
) -> Result<ReplayVerificationReport> {
    let started = Instant::now();
    if event_batch_digest(&closure.canonical_events)? != closure.event_batch_digest {
        bail!("authenticated residual event batch was modified")
    }
    if !trusted.coverage_verified || !trusted.synopsis_verified || !trusted.chain_binding_verified {
        bail!(
            "upstream closure coverage, synopsis, and chain binding must be verified before replay"
        )
    }
    if closure.trusted_epoch_root != trusted.trusted_epoch_root {
        bail!("residual closure is not bound to the independently trusted epoch root")
    }
    validate_authentication_mode(closure)?;
    validate_episode_range(query, closure, &response.fixed_atoms)?;
    validate_same_tx_range(query, closure, &response.fixed_atoms)?;
    let query_digest = query_digest(query)?;
    let query_digest_consistent = response.query_digest == query_digest;
    let epoch_consistent = response.trusted_epoch_root == trusted.trusted_epoch_root
        && closure.trusted_epoch_root == trusted.trusted_epoch_root
        && response.closure_id == closure.closure_id;
    if !query_digest_consistent || !epoch_consistent {
        bail!("response transcript does not match the query/epoch/closure")
    }
                                                                               
                                                                          
                                                               
    let replay_index = QueryIndependentIndex::build(closure.canonical_events.clone())?;
    let replay_request = EvaluationRequest {
        allowed_event_ids: None,
        fixed_atoms: response.fixed_atoms.clone(),
    };
    let mut replay = evaluate_query(&replay_index, query, &replay_request)?;
    let all_results_equal = replay.matches == response.matches;
    let witness_replayed_equal = replay.witness == response.witness;
    let verifier_replay_seconds = started.elapsed().as_secs_f64();
    let vo_size_bytes = vo_size(closure, response)?;
    replay.metrics.verifier_replay_seconds = verifier_replay_seconds;
    replay.metrics.vo_size_bytes = vo_size_bytes;
    Ok(ReplayVerificationReport {
        completed: all_results_equal && witness_replayed_equal,
        query_digest_consistent,
        epoch_consistent,
        coverage_verified: trusted.coverage_verified,
        synopsis_verified: trusted.synopsis_verified,
        chain_binding_verified: trusted.chain_binding_verified,
        all_results_equal,
        witness_replayed_equal,
        verifier_replay_seconds,
        vo_size_bytes,
        replay,
    })
}
