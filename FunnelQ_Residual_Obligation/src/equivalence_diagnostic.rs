                                                                              
                                                         

use crate::evaluator::{EvaluationRequest, MatchRecord};
use crate::index::QueryIndependentIndex;
use crate::oracle::brute_force_query;
use crate::query::QuerySpec;
use crate::residual::ResidualKey;
use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PrefixObservation {
    pub binding_depth: usize,
    pub assigned_events: BTreeMap<String, String>,
    pub key: ResidualKey,
}

fn encoded<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).expect("diagnostic projection is serializable")
}

fn pair_count(n: u64) -> u64 {
    n.saturating_mul(n.saturating_sub(1)) / 2
}

fn equal_pairs(counts: &BTreeMap<String, u64>) -> u64 {
    counts.values().map(|n| pair_count(*n)).sum()
}

#[derive(Default)]
pub struct LayerAccumulator {
    n_prefix: u64,
    structure: BTreeMap<String, u64>,
    variables: BTreeMap<String, u64>,
    fields: BTreeMap<String, u64>,
    binding: BTreeMap<String, u64>,
    time: BTreeMap<String, u64>,
    injective: BTreeMap<String, u64>,
    full: BTreeMap<String, u64>,
    time_only: BTreeMap<String, u64>,
    injective_only: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LayerStats {
    pub n_prefix: u64,
    pub n_structure: usize,
    pub n_binding: usize,
    pub n_live_variables: usize,
    pub n_live_fields: usize,
    pub n_time: usize,
    pub n_injective: usize,
    pub compression: f64,
    pub different_remaining_structure_pairs: u64,
    pub different_live_binding_pairs: u64,
    pub different_live_variable_pairs: u64,
    pub different_live_field_pairs: u64,
    pub different_same_tx_binding_pairs: u64,
    pub different_temporal_obligation_pairs: u64,
    pub different_injectivity_frontier_pairs: u64,
    pub other_pairs: u64,
    pub same_full_key_pairs: u64,
    pub different_temporal_any_same_structure_pairs: u64,
    pub different_injectivity_any_same_structure_pairs: u64,
}

impl LayerAccumulator {
    pub fn observe(&mut self, prefix: &PrefixObservation) {
        let key = &prefix.key;
                                                                              
                                                                            
                                                                      
        let structure = encoded(&key.remaining_atoms);
        let variables = encoded(&(&structure, &key.live_bindings));
        let fields = encoded(&(&variables, &key.live_fields));
        let binding = encoded(&(&fields, &key.same_tx_value));
        let time = encoded(&(&binding, &key.temporal_obligations));
        let injective = encoded(&(&time, &key.injectivity_frontier));
        let full = encoded(key);
        let time_only = encoded(&(&structure, &key.temporal_obligations));
        let injective_only = encoded(&(&structure, &key.injectivity_frontier));
        self.n_prefix += 1;
        *self.structure.entry(structure).or_default() += 1;
        *self.variables.entry(variables).or_default() += 1;
        *self.fields.entry(fields).or_default() += 1;
        *self.binding.entry(binding).or_default() += 1;
        *self.time.entry(time).or_default() += 1;
        *self.injective.entry(injective).or_default() += 1;
        *self.full.entry(full).or_default() += 1;
        *self.time_only.entry(time_only).or_default() += 1;
        *self.injective_only.entry(injective_only).or_default() += 1;
    }

    pub fn stats(&self) -> LayerStats {
        let s = equal_pairs(&self.structure);
        let b = equal_pairs(&self.binding);
        let v = equal_pairs(&self.variables);
        let l = equal_pairs(&self.fields);
        let t = equal_pairs(&self.time);
        let i = equal_pairs(&self.injective);
        let f = equal_pairs(&self.full);
        LayerStats {
            n_prefix: self.n_prefix,
            n_structure: self.structure.len(),
            n_binding: self.binding.len(),
            n_live_variables: self.variables.len(),
            n_live_fields: self.fields.len(),
            n_time: self.time.len(),
            n_injective: self.full.len(),
            compression: if self.full.is_empty() { 0.0 } else {
                self.n_prefix as f64 / self.full.len() as f64
            },
            different_remaining_structure_pairs: pair_count(self.n_prefix) - s,
            different_live_binding_pairs: s - b,
            different_live_variable_pairs: s - v,
            different_live_field_pairs: v - l,
            different_same_tx_binding_pairs: l - b,
            different_temporal_obligation_pairs: b - t,
            different_injectivity_frontier_pairs: t - i,
            other_pairs: i - f,
            same_full_key_pairs: f,
            different_temporal_any_same_structure_pairs: s - equal_pairs(&self.time_only),
            different_injectivity_any_same_structure_pairs: s - equal_pairs(&self.injective_only),
        }
    }
}

pub fn layered_stats(prefixes: &[PrefixObservation]) -> BTreeMap<(String, usize), LayerStats> {
    let mut groups: BTreeMap<(String, usize), LayerAccumulator> = BTreeMap::new();
    for prefix in prefixes {
        groups.entry((prefix.key.branch_id.clone(), prefix.binding_depth))
            .or_default().observe(prefix);
    }
    groups.into_iter().map(|(k, v)| (k, v.stats())).collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionObservation {
    pub binding_depth: usize,
    pub branch_id: String,
    pub key_hash: String,
    pub completion_hash: String,
    pub completion_count: usize,
    pub assigned_events: BTreeMap<String, String>,
    pub remaining_atoms: Vec<String>,
    pub residual_key: ResidualKey,
    pub frontier_ids_already_excluded_by_time: Vec<String>,
}

                                                                            
                                                                           
                                                              
pub fn frontier_ids_excluded_by_time(
    index: &QueryIndependentIndex,
    query: &QuerySpec,
    key: &ResidualKey,
) -> Vec<String> {
    let Some(branch) = query.branches.iter().find(|b| b.id == key.branch_id) else {
        return Vec::new();
    };
    key.injectivity_frontier.iter().filter(|id| {
        let Some(event_index) = index.event_index(id) else { return false; };
        let event = index.event(event_index);
        !branch.atoms.iter().filter(|atom| key.remaining_atoms.contains(&atom.id)).any(|atom| {
            atom.event_types.contains(&event.event_type)
                && key.temporal_obligations.iter().filter(|bound| bound.atom == atom.id).all(|bound| {
                    let Some(value) = event.value(&bound.field).and_then(|v| v.as_i64()) else {
                        return true;                                     
                    };
                    bound.lower_exclusive.is_none_or(|v| value > v)
                        && bound.lower_inclusive.is_none_or(|v| value >= v)
                        && bound.upper_exclusive.is_none_or(|v| value < v)
                        && bound.upper_inclusive.is_none_or(|v| value <= v)
                })
        })
    }).cloned().collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompletionStats {
    pub p: usize,
    pub k: usize,
    pub c: usize,
    pub empty_completion_prefixes: usize,
    pub nonempty_completion_classes: usize,
    pub keys_with_multiple_completion_hashes: usize,
    pub completion_classes_with_multiple_keys: usize,
    pub nonempty_completion_classes_with_multiple_keys: usize,
    pub p_over_k: f64,
    pub p_over_c: f64,
}

pub fn completion_stats(observations: &[CompletionObservation]) -> CompletionStats {
    let mut by_key: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut by_completion: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    let mut nonempty = BTreeSet::new();
    for row in observations {
        by_key.entry(&row.key_hash).or_default().insert(&row.completion_hash);
        by_completion.entry(&row.completion_hash).or_default().insert(&row.key_hash);
        if row.completion_count > 0 { nonempty.insert(row.completion_hash.as_str()); }
    }
    CompletionStats {
        p: observations.len(), k: by_key.len(), c: by_completion.len(),
        empty_completion_prefixes: observations.iter().filter(|r| r.completion_count == 0).count(),
        nonempty_completion_classes: nonempty.len(),
        keys_with_multiple_completion_hashes: by_key.values().filter(|v| v.len() > 1).count(),
        completion_classes_with_multiple_keys: by_completion.values().filter(|v| v.len() > 1).count(),
        nonempty_completion_classes_with_multiple_keys: by_completion.iter()
            .filter(|(hash, keys)| keys.len() > 1 && nonempty.contains(**hash)).count(),
        p_over_k: if by_key.is_empty() { 0.0 } else { observations.len() as f64 / by_key.len() as f64 },
        p_over_c: if by_completion.is_empty() { 0.0 } else { observations.len() as f64 / by_completion.len() as f64 },
    }
}

                                                                               
                                                                              
                                                                                
pub fn completion_oracle(
    index: &QueryIndependentIndex,
    query: &QuerySpec,
    request: &EvaluationRequest,
    prefixes: &[PrefixObservation],
    expected_matches: &[MatchRecord],
) -> Result<Vec<CompletionObservation>> {
    let full = brute_force_query(index, query, request)?;
    if full != expected_matches { bail!("diagnostic brute-force result mismatch") }
    let mut output = Vec::with_capacity(prefixes.len());
    for prefix in prefixes {
        if !prefix.key.deferred_atoms.is_empty() {
            bail!("completion oracle requires whole-branch prefixes, not split component states")
        }
        let mut completions = BTreeSet::new();
        for result in &full {
            if result.branch_id != prefix.key.branch_id || !prefix.assigned_events.iter()
                .all(|(atom, event)| result.event_bindings.get(atom) == Some(event)) { continue; }
            let suffix: BTreeMap<String, String> = result.event_bindings.iter()
                .filter(|(atom, _)| !prefix.assigned_events.contains_key(*atom))
                .map(|(atom, event)| (atom.clone(), event.clone())).collect();
            completions.insert(suffix);
        }
        output.push(CompletionObservation {
            binding_depth: prefix.binding_depth,
            branch_id: prefix.key.branch_id.clone(),
            key_hash: format!("{:x}", Sha256::digest(encoded(&prefix.key).as_bytes())),
            completion_hash: format!("{:x}", Sha256::digest(encoded(&completions).as_bytes())),
            completion_count: completions.len(),
            assigned_events: prefix.assigned_events.clone(),
            remaining_atoms: prefix.key.remaining_atoms.clone(),
            residual_key: prefix.key.clone(),
            frontier_ids_already_excluded_by_time: frontier_ids_excluded_by_time(index, query, &prefix.key),
        });
    }
    Ok(output)
}
