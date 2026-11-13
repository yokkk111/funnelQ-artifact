use crate::index::QueryIndependentIndex;
use crate::equivalence_diagnostic::PrefixObservation;
use crate::metrics::EvaluationMetrics;
use crate::model::Scalar;
use crate::query::{
    AtomSpec, BranchSpec, CompareOp, Constraint, QuerySpec, ScopeSpec, Term, ValueExpr,
};
use crate::residual::{normalize_residual, ResidualKey, ResidualObligation, TemporalObligation};
use crate::witness::{FactorizedWitness, WitnessArena, WitnessNodeId};
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

#[derive(Clone, Debug, Default)]
pub struct EvaluationRequest {
    pub allowed_event_ids: Option<BTreeSet<String>>,
    pub fixed_atoms: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MatchRecord {
    pub query_id: String,
    pub branch_id: String,
    pub event_bindings: BTreeMap<String, String>,
    pub variable_bindings: BTreeMap<String, Scalar>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationOutput {
    pub matches: Vec<MatchRecord>,
    pub witness: FactorizedWitness,
    pub metrics: EvaluationMetrics,
    pub observed_obligations: Vec<ResidualObligation>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostic_prefixes: Vec<PrefixObservation>,
}

#[derive(Clone, Debug)]
struct State {
    remaining: Vec<usize>,
    assigned: BTreeMap<String, usize>,
    bindings: BTreeMap<String, Scalar>,
    used_events: BTreeSet<usize>,
    same_tx: Option<String>,
}

#[derive(Clone, Debug)]
struct SuffixOutcome {
    assignments: Vec<Vec<(usize, usize)>>,
    witness: WitnessNodeId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Check {
    Pass,
    Pending,
    Reject,
}

struct BranchEvaluator<'a> {
    index: &'a QueryIndependentIndex,
    branch: &'a BranchSpec,
    scope: &'a ScopeSpec,
    allowed: Option<BTreeSet<usize>>,
    fixed: BTreeMap<String, usize>,
    memo: BTreeMap<ResidualKey, SuffixOutcome>,
    arena: WitnessArena,
    metrics: EvaluationMetrics,
    observed: BTreeMap<ResidualKey, ResidualObligation>,
    diagnostic_prefixes: Option<Vec<PrefixObservation>>,
}

fn scalar_compare(left: &Scalar, op: CompareOp, right: &Scalar) -> bool {
    match op {
        CompareOp::Eq => left == right,
        CompareOp::Ne => left != right,
        CompareOp::Lt => left.same_type_cmp(right).is_some_and(|order| order.is_lt()),
        CompareOp::Le => left.same_type_cmp(right).is_some_and(|order| order.is_le()),
        CompareOp::Gt => left.same_type_cmp(right).is_some_and(|order| order.is_gt()),
        CompareOp::Ge => left.same_type_cmp(right).is_some_and(|order| order.is_ge()),
    }
}

impl<'a> BranchEvaluator<'a> {
    fn new(
        index: &'a QueryIndependentIndex,
        branch: &'a BranchSpec,
        scope: &'a ScopeSpec,
        request: &EvaluationRequest,
        collect_diagnostics: bool,
    ) -> Result<Self> {
        let fixed: BTreeMap<String, usize> = request
            .fixed_atoms
            .iter()
            .map(|(atom, event)| {
                if !branch.atoms.iter().any(|candidate| candidate.id == *atom) {
                    bail!("fixed binding references unknown atom {atom}")
                }
                Ok((
                    atom.clone(),
                    index
                        .event_index(event)
                        .with_context(|| format!("unknown fixed event id {event}"))?,
                ))
            })
            .collect::<Result<_>>()?;
        let requested_allowed = request
            .allowed_event_ids
            .as_ref()
            .map(|ids| {
                ids.iter()
                    .map(|id| {
                        index
                            .event_index(id)
                            .with_context(|| format!("unknown residual event id {id}"))
                    })
                    .collect::<Result<BTreeSet<_>>>()
            })
            .transpose()?;
        let episode_allowed = if let ScopeSpec::Episode {
            anchor_atom,
            window_seconds,
        } = scope
        {
            let anchor_index = fixed.get(anchor_atom).with_context(|| {
                format!("standalone Episode evaluation requires a fixed {anchor_atom} event")
            })?;
            let anchor = index.event(*anchor_index);
                                                                             
                                                                         
                                                                          
                                                                            
                                                                     
            let candidates: Box<dyn Iterator<Item = usize> + '_> =
                if let Some(requested) = requested_allowed.as_ref() {
                    Box::new(requested.iter().copied())
                } else {
                    Box::new(index.all_indices().iter().copied())
                };
            Some(
                candidates
                    .filter(|event_index| {
                        let event = index.event(*event_index);
                        event.order_seq >= anchor.order_seq
                            && event.block_ts <= anchor.block_ts.saturating_add(*window_seconds)
                    })
                    .collect::<BTreeSet<_>>(),
            )
        } else {
            None
        };
        let allowed = match (requested_allowed, episode_allowed) {
            (Some(mut requested), Some(episode)) => {
                requested.retain(|index| episode.contains(index));
                Some(requested)
            }
            (Some(requested), None) => Some(requested),
            (None, Some(episode)) => Some(episode),
            (None, None) => None,
        };
        Ok(Self {
            index,
            branch,
            scope,
            allowed,
            fixed,
            memo: BTreeMap::new(),
            arena: WitnessArena::default(),
            metrics: EvaluationMetrics::default(),
            observed: BTreeMap::new(),
            diagnostic_prefixes: collect_diagnostics.then(Vec::new),
        })
    }

    fn resolve(&self, expr: &ValueExpr, state: &State) -> Option<Scalar> {
        match expr {
            ValueExpr::Variable(variable) => state.bindings.get(variable).cloned(),
            ValueExpr::Field(reference) => state
                .assigned
                .get(&reference.atom)
                .and_then(|event| self.index.event(*event).value(&reference.field)),
            ValueExpr::Constant(value) => Some(value.clone()),
        }
    }

    fn check_constraint(&self, constraint: &Constraint, state: &State) -> Check {
        match constraint {
            Constraint::Compare { left, op, right } => {
                match (self.resolve(left, state), self.resolve(right, state)) {
                    (Some(left), Some(right)) => {
                        if scalar_compare(&left, *op, &right) {
                            Check::Pass
                        } else {
                            Check::Reject
                        }
                    }
                    _ => Check::Pending,
                }
            }
            Constraint::Order {
                earlier,
                later,
                field,
            } => match (state.assigned.get(earlier), state.assigned.get(later)) {
                (Some(left), Some(right)) => {
                    let left = self
                        .index
                        .event(*left)
                        .value(field)
                        .and_then(|value| value.as_i64());
                    let right = self
                        .index
                        .event(*right)
                        .value(field)
                        .and_then(|value| value.as_i64());
                    if left.zip(right).is_some_and(|(left, right)| left < right) {
                        Check::Pass
                    } else {
                        Check::Reject
                    }
                }
                _ => Check::Pending,
            },
            Constraint::Within {
                earlier,
                later,
                delta_seconds,
            } => match (state.assigned.get(earlier), state.assigned.get(later)) {
                (Some(left), Some(right)) => {
                    let left = self.index.event(*left);
                    let right = self.index.event(*right);
                    if left.order_seq < right.order_seq
                        && right.block_ts <= left.block_ts.saturating_add(*delta_seconds)
                    {
                        Check::Pass
                    } else {
                        Check::Reject
                    }
                }
                _ => Check::Pending,
            },
            Constraint::DistinctValues {
                values,
                min_count,
                max_count,
                ignore_empty_text,
            } => {
                let resolved: Vec<_> = values
                    .iter()
                    .map(|value| self.resolve(value, state))
                    .collect();
                let unresolved = resolved.iter().filter(|value| value.is_none()).count();
                let ready: Vec<_> = resolved
                    .iter()
                    .filter_map(Option::as_ref)
                    .filter(|value| !*ignore_empty_text || !value.is_empty())
                    .collect();
                let distinct = ready.iter().collect::<BTreeSet<_>>().len();
                if distinct > *max_count || distinct + unresolved < *min_count {
                    Check::Reject
                } else if unresolved == 0 {
                    if distinct >= *min_count && distinct <= *max_count {
                        Check::Pass
                    } else {
                        Check::Reject
                    }
                } else {
                    Check::Pending
                }
            }
            Constraint::DistinctEvents(atoms) => {
                let ready: Vec<_> = atoms
                    .iter()
                    .filter_map(|atom| state.assigned.get(atom))
                    .collect();
                if ready.iter().collect::<BTreeSet<_>>().len() != ready.len() {
                    Check::Reject
                } else if ready.len() == atoms.len() {
                    Check::Pass
                } else {
                    Check::Pending
                }
            }
            Constraint::DistinctTransactions(atoms) => {
                let ready: Vec<_> = atoms
                    .iter()
                    .filter_map(|atom| {
                        state
                            .assigned
                            .get(atom)
                            .map(|event| self.index.event(*event).tx_hash.as_str())
                    })
                    .collect();
                if ready.iter().any(|value| value.is_empty())
                    || ready.iter().collect::<BTreeSet<_>>().len() != ready.len()
                {
                    Check::Reject
                } else if ready.len() == atoms.len() {
                    Check::Pass
                } else {
                    Check::Pending
                }
            }
            Constraint::NoEvent {
                after,
                event_types,
                fields,
                within_seconds,
            } => {
                let Some(anchor_index) = state.assigned.get(after) else {
                    return Check::Pending;
                };
                let resolved: Option<Vec<_>> = fields
                    .iter()
                    .map(|(field, value)| self.resolve(value, state).map(|value| (field, value)))
                    .collect();
                let Some(resolved) = resolved else {
                    return Check::Pending;
                };
                let anchor = self.index.event(*anchor_index);
                let mut events: Box<dyn Iterator<Item = usize> + '_> = match &self.allowed {
                    Some(allowed) => Box::new(allowed.iter().copied()),
                    None => Box::new(self.index.all_indices().iter().copied()),
                };
                let exists = events.any(|event_index| {
                    let event = self.index.event(event_index);
                    (event_types.is_empty() || event_types.contains(&event.event_type))
                        && event.order_seq > anchor.order_seq
                        && event.block_ts <= anchor.block_ts.saturating_add(*within_seconds)
                        && resolved
                            .iter()
                            .all(|(field, expected)| event.value(field).as_ref() == Some(expected))
                });
                if exists {
                    Check::Reject
                } else {
                    Check::Pass
                }
            }
        }
    }

    fn constraints_ok(&self, state: &State, final_state: bool) -> bool {
        self.branch.constraints.iter().all(|constraint| {
            match self.check_constraint(constraint, state) {
                Check::Pass => true,
                Check::Pending => !final_state,
                Check::Reject => false,
            }
        })
    }

    fn atom_accepts(
        &self,
        atom: &AtomSpec,
        event_index: usize,
        state: &State,
    ) -> Option<BTreeMap<String, Scalar>> {
        let event = self.index.event(event_index);
        if !atom.event_types.contains(&event.event_type) {
            return None;
        }
        let mut bindings = state.bindings.clone();
        for (field, term) in &atom.fields {
            let actual = event.value(field)?;
            match term {
                Term::Constant(expected) if actual != *expected => return None,
                Term::Constant(_) => {}
                Term::Variable(variable) => {
                    if actual.is_empty() {
                        return None;
                    }
                    if let Some(expected) = bindings.get(variable) {
                        if actual != *expected {
                            return None;
                        }
                    } else {
                        bindings.insert(variable.clone(), actual);
                    }
                }
            }
        }
        if !atom.local_predicates.iter().all(|predicate| {
            event
                .value(&predicate.field)
                .is_some_and(|actual| scalar_compare(&actual, predicate.op, &predicate.value))
        }) {
            return None;
        }
        Some(bindings)
    }

    fn satisfies_temporal_obligations(
        &self,
        atom_id: &str,
        event_index: usize,
        obligations: &[TemporalObligation],
    ) -> bool {
        let event = self.index.event(event_index);
        obligations
            .iter()
            .filter(|obligation| obligation.atom == atom_id)
            .all(|obligation| {
                event
                    .value(&obligation.field)
                    .and_then(|value| value.as_i64())
                    .is_some_and(|value| {
                        obligation.lower_exclusive.is_none_or(|lower| value > lower)
                            && obligation
                                .lower_inclusive
                                .is_none_or(|lower| value >= lower)
                            && obligation.upper_exclusive.is_none_or(|upper| value < upper)
                            && obligation
                                .upper_inclusive
                                .is_none_or(|upper| value <= upper)
                    })
            })
    }

    fn estimate_candidates_with_temporal(
        &self,
        atom_index: usize,
        state: &State,
        obligations: &[TemporalObligation],
    ) -> usize {
        let atom = &self.branch.atoms[atom_index];
        if !obligations
            .iter()
            .any(|obligation| obligation.atom == atom.id)
        {
            return self
                .index
                .estimate_candidates(atom, &state.bindings, self.allowed.as_ref());
        }
        let fixed = self.fixed.get(&atom.id).copied();
        self.index
            .open_candidates(atom, &state.bindings, self.allowed.as_ref(), fixed)
            .0
            .into_iter()
            .filter(|event| self.satisfies_temporal_obligations(&atom.id, *event, obligations))
            .count()
    }

    fn choose_atom(&self, state: &State, obligations: &[TemporalObligation]) -> usize {
        if let Some(fixed) = state
            .remaining
            .iter()
            .find(|atom| self.fixed.contains_key(&self.branch.atoms[**atom].id))
        {
            return *fixed;
        }
        *state
            .remaining
            .iter()
            .min_by_key(|atom| {
                let spec = &self.branch.atoms[**atom];
                (
                    self.estimate_candidates_with_temporal(**atom, state, obligations),
                    spec.id.as_str(),
                )
            })
            .expect("choose_atom requires a non-empty residual")
    }

    fn remaining_components(&self, state: &State) -> Vec<Vec<usize>> {
        if state.remaining.len() < 2 {
            return vec![state.remaining.clone()];
        }
        let remaining: BTreeSet<_> = state.remaining.iter().copied().collect();
        let mut adjacency: BTreeMap<usize, BTreeSet<usize>> = state
            .remaining
            .iter()
            .map(|atom| (*atom, BTreeSet::new()))
            .collect();
        let connect = |nodes: &BTreeSet<usize>,
                       adjacency: &mut BTreeMap<usize, BTreeSet<usize>>| {
            for left in nodes {
                for right in nodes {
                    if left != right {
                        adjacency.get_mut(left).unwrap().insert(*right);
                    }
                }
            }
        };
        let mut variable_atoms: BTreeMap<&str, BTreeSet<usize>> = BTreeMap::new();
        for atom_index in &state.remaining {
            for term in self.branch.atoms[*atom_index].fields.values() {
                if let Term::Variable(variable) = term {
                    variable_atoms
                        .entry(variable)
                        .or_default()
                        .insert(*atom_index);
                }
            }
        }
        for atoms in variable_atoms.values() {
            connect(atoms, &mut adjacency);
        }
        let atom_index: BTreeMap<_, _> = self
            .branch
            .atoms
            .iter()
            .enumerate()
            .map(|(index, atom)| (atom.id.as_str(), index))
            .collect();
        let variable_positions = |value: &ValueExpr, nodes: &mut BTreeSet<usize>| match value {
            ValueExpr::Variable(variable) => {
                if let Some(atoms) = variable_atoms.get(variable.as_str()) {
                    nodes.extend(atoms);
                }
            }
            ValueExpr::Field(reference) => {
                if let Some(index) = atom_index.get(reference.atom.as_str()) {
                    if remaining.contains(index) {
                        nodes.insert(*index);
                    }
                }
            }
            ValueExpr::Constant(_) => {}
        };
        for constraint in &self.branch.constraints {
            let mut nodes = BTreeSet::new();
            match constraint {
                Constraint::Compare { left, right, .. } => {
                    variable_positions(left, &mut nodes);
                    variable_positions(right, &mut nodes);
                }
                Constraint::Order { earlier, later, .. }
                | Constraint::Within { earlier, later, .. } => {
                    for atom in [earlier, later] {
                        if let Some(index) = atom_index.get(atom.as_str()) {
                            if remaining.contains(index) {
                                nodes.insert(*index);
                            }
                        }
                    }
                }
                Constraint::DistinctValues { values, .. } => {
                    for value in values {
                        variable_positions(value, &mut nodes);
                    }
                }
                Constraint::DistinctEvents(atoms) | Constraint::DistinctTransactions(atoms) => {
                    for atom in atoms {
                        if let Some(index) = atom_index.get(atom.as_str()) {
                            if remaining.contains(index) {
                                nodes.insert(*index);
                            }
                        }
                    }
                }
                Constraint::NoEvent { after, fields, .. } => {
                    if let Some(index) = atom_index.get(after.as_str()) {
                        if remaining.contains(index) {
                            nodes.insert(*index);
                        }
                    }
                    for value in fields.values() {
                        variable_positions(value, &mut nodes);
                    }
                }
            }
            connect(&nodes, &mut adjacency);
        }
        if self.branch.event_injective {
            for (offset, left) in state.remaining.iter().enumerate() {
                for right in state.remaining.iter().skip(offset + 1) {
                    let overlaps = self.branch.atoms[*left]
                        .event_types
                        .iter()
                        .any(|value| self.branch.atoms[*right].event_types.contains(value));
                    if overlaps {
                        adjacency.get_mut(left).unwrap().insert(*right);
                        adjacency.get_mut(right).unwrap().insert(*left);
                    }
                }
            }
        }
        if matches!(self.scope, ScopeSpec::SameTx) && state.same_tx.is_none() {
            connect(&remaining, &mut adjacency);
        }
        let mut unseen = remaining;
        let mut components: Vec<Vec<usize>> = Vec::new();
        while let Some(start) = unseen.iter().next().copied() {
            let mut stack = vec![start];
            let mut component = BTreeSet::new();
            unseen.remove(&start);
            while let Some(node) = stack.pop() {
                component.insert(node);
                for neighbor in &adjacency[&node] {
                    if unseen.remove(neighbor) {
                        stack.push(*neighbor);
                    }
                }
            }
            components.push(component.into_iter().collect());
        }
        components.sort_by_key(|component| {
            component
                .iter()
                .map(|index| self.branch.atoms[*index].id.as_str())
                .min()
                .unwrap_or("")
        });
        components
    }

    fn evaluate_state(&mut self, state: State, require_complete: bool) -> SuffixOutcome {
                                                                           
                                                                             
                                                                            
                                                              
        if state.assigned.is_empty() && !self.constraints_ok(&state, false) {
            return SuffixOutcome {
                assignments: Vec::new(),
                witness: self.arena.empty(),
            };
        }
        if state.remaining.is_empty() {
            return if self.constraints_ok(&state, require_complete) {
                SuffixOutcome {
                    assignments: vec![Vec::new()],
                    witness: self.arena.unit(),
                }
            } else {
                SuffixOutcome {
                    assignments: Vec::new(),
                    witness: self.arena.empty(),
                }
            };
        }
                                                                            
                                                                             
                                                                              
                                                                              
                                                                           
        self.metrics.concrete_partial_bindings += 1;
        self.metrics.nonterminal_residual_state_visits += 1;
        let (obligation, key) = normalize_residual(
            self.branch,
            self.index,
            &state.remaining,
            &state.assigned,
            &state.bindings,
            &state.used_events,
            state.same_tx.as_deref(),
        );
        if let Some(prefixes) = self.diagnostic_prefixes.as_mut() {
            prefixes.push(PrefixObservation {
                binding_depth: state.assigned.len(),
                assigned_events: state.assigned.iter().map(|(atom, event)| {
                    (atom.clone(), self.index.event(*event).event_id.clone())
                }).collect(),
                key: key.clone(),
            });
        }
        if let Some(cached) = self.memo.get(&key) {
            self.metrics.quotient_merge_count += 1;
            return cached.clone();
        }
        let temporal_obligations = obligation.temporal_obligations.clone();
        self.observed.entry(key.clone()).or_insert(obligation);
        self.metrics.distinct_residual_keys += 1;

        let components = self.remaining_components(&state);
        if components.len() > 1 {
            self.metrics.residual_split_count += 1;
            let mut combined_assignments = vec![Vec::new()];
            let mut combined_witness = self.arena.unit();
            for component in components {
                let mut component_state = state.clone();
                component_state.remaining = component;
                let child = self.evaluate_state(component_state, false);
                if child.assignments.is_empty() {
                    combined_assignments.clear();
                    combined_witness = self.arena.empty();
                    break;
                }
                let mut product = Vec::new();
                for prefix in &combined_assignments {
                    for suffix in &child.assignments {
                        let mut value = prefix.clone();
                        value.extend(suffix.iter().copied());
                        product.push(value);
                    }
                }
                combined_assignments = product;
                combined_witness = self.arena.product(combined_witness, child.witness);
            }
            if require_complete {
                combined_assignments.retain(|assignment| {
                    let mut completed = state.clone();
                    completed.remaining.clear();
                    for (atom, event) in assignment {
                        completed
                            .assigned
                            .insert(self.branch.atoms[*atom].id.clone(), *event);
                        completed.used_events.insert(*event);
                        let Some(next_bindings) =
                            self.atom_accepts(&self.branch.atoms[*atom], *event, &completed)
                        else {
                            return false;
                        };
                        completed.bindings = next_bindings;
                    }
                    self.constraints_ok(&completed, true)
                });
                if combined_assignments.is_empty() {
                    combined_witness = self.arena.empty();
                }
            }
            if combined_assignments.is_empty() {
                self.metrics.memoized_failed_states += 1;
            }
            let outcome = SuffixOutcome {
                assignments: combined_assignments,
                witness: combined_witness,
            };
            self.memo.insert(key, outcome.clone());
            return outcome;
        }

        let atom_index = self.choose_atom(&state, &temporal_obligations);
        let atom = &self.branch.atoms[atom_index];
        let fixed = self.fixed.get(&atom.id).copied();
        let (mut candidates, probes) =
            self.index
                .open_candidates(atom, &state.bindings, self.allowed.as_ref(), fixed);
        self.metrics.raw_candidate_probes += probes;
                                                                             
                                                                         
                                                                        
        let candidates_before_temporal = candidates.len();
        candidates.retain(|event| {
            self.satisfies_temporal_obligations(&atom.id, *event, &temporal_obligations)
        });
        self.metrics.temporal_prefilter_rejections +=
            (candidates_before_temporal - candidates.len()) as u64;
        let mut assignments = Vec::new();
        let mut witness_alternatives = Vec::new();
        for event_index in candidates {
            if self.branch.event_injective && state.used_events.contains(&event_index) {
                continue;
            }
            let event = self.index.event(event_index);
            if matches!(self.scope, ScopeSpec::SameTx)
                && state
                    .same_tx
                    .as_deref()
                    .is_some_and(|tx| tx != event.tx_hash)
            {
                continue;
            }
            let Some(bindings) = self.atom_accepts(atom, event_index, &state) else {
                continue;
            };
            let mut next = state.clone();
            next.remaining.retain(|candidate| *candidate != atom_index);
            next.assigned.insert(atom.id.clone(), event_index);
            next.bindings = bindings;
            next.used_events.insert(event_index);
            if matches!(self.scope, ScopeSpec::SameTx) && next.same_tx.is_none() {
                next.same_tx = Some(event.tx_hash.clone());
            }
            if !self.constraints_ok(&next, false) {
                continue;
            }
            self.metrics.residual_transitions += 1;
            let child = self.evaluate_state(next, require_complete);
            if child.assignments.is_empty() {
                continue;
            }
            let leaf = self.arena.leaf(atom.id.clone(), event.event_id.clone());
            witness_alternatives.push(self.arena.product(leaf, child.witness));
            for mut suffix in child.assignments {
                let mut value = Vec::with_capacity(suffix.len() + 1);
                value.push((atom_index, event_index));
                value.append(&mut suffix);
                assignments.push(value);
            }
        }
        let witness = self.arena.union(witness_alternatives);
        if assignments.is_empty() {
            self.metrics.memoized_failed_states += 1;
        }
        let outcome = SuffixOutcome {
            assignments,
            witness,
        };
        self.memo.insert(key, outcome.clone());
        outcome
    }

    fn run(mut self, query_id: &str) -> EvaluationOutput {
        self.metrics.residual_closure_event_count = self
            .allowed
            .as_ref()
            .map_or(self.index.events().len(), BTreeSet::len);
        self.metrics.residual_closure_event_count_sum =
            self.metrics.residual_closure_event_count as u64;
        self.metrics.residual_closure_event_count_max = self.metrics.residual_closure_event_count;
        let state = State {
            remaining: (0..self.branch.atoms.len()).collect(),
            assigned: BTreeMap::new(),
            bindings: BTreeMap::new(),
            used_events: BTreeSet::new(),
            same_tx: None,
        };
        let outcome = self.evaluate_state(state, true);
        let mut matches = Vec::with_capacity(outcome.assignments.len());
        for assignment in outcome.assignments {
            let mut event_bindings = BTreeMap::new();
            let mut variable_bindings = BTreeMap::new();
            for (atom_index, event_index) in assignment {
                let atom = &self.branch.atoms[atom_index];
                let event = self.index.event(event_index);
                event_bindings.insert(atom.id.clone(), event.event_id.clone());
                for (field, term) in &atom.fields {
                    if let Term::Variable(variable) = term {
                        if let Some(value) = event.value(field) {
                            variable_bindings.insert(variable.clone(), value);
                        }
                    }
                }
            }
            matches.push(MatchRecord {
                query_id: query_id.to_owned(),
                branch_id: self.branch.id.clone(),
                event_bindings,
                variable_bindings,
            });
        }
        matches.sort();
        matches.dedup();
        self.metrics.final_result_count = matches.len() as u64;
        self.metrics.witness_dag_nodes = self.arena.len() as u64;
        self.metrics.epoch_query_specific_storage_bytes = 0;
        self.metrics.materialized_compatibility_edges = 0;
        EvaluationOutput {
            matches,
            witness: self.arena.finish_with_branches(
                outcome.witness,
                BTreeMap::from([(self.branch.id.clone(), outcome.witness)]),
            ),
            metrics: self.metrics,
            observed_obligations: self.observed.into_values().collect(),
            diagnostic_prefixes: self.diagnostic_prefixes.unwrap_or_default(),
        }
    }
}

pub fn evaluate_query(
    index: &QueryIndependentIndex,
    query: &QuerySpec,
    request: &EvaluationRequest,
) -> Result<EvaluationOutput> {
    evaluate_query_internal(index, query, request, false)
}

                                                                           
                                                                                  
pub fn evaluate_query_with_diagnostics(
    index: &QueryIndependentIndex,
    query: &QuerySpec,
    request: &EvaluationRequest,
) -> Result<EvaluationOutput> {
    evaluate_query_internal(index, query, request, true)
}

fn evaluate_query_internal(
    index: &QueryIndependentIndex,
    query: &QuerySpec,
    request: &EvaluationRequest,
    collect_diagnostics: bool,
) -> Result<EvaluationOutput> {
    let started = Instant::now();
    let mut matches = Vec::new();
    let mut metrics = EvaluationMetrics::default();
    let mut obligations = Vec::new();
    let mut diagnostic_prefixes = Vec::new();
    let mut combined_arena = WitnessArena::default();
    let mut roots = Vec::new();
    let mut branch_roots = BTreeMap::new();
    for branch in &query.branches {
        let output =
            BranchEvaluator::new(index, branch, &query.scope, request, collect_diagnostics)?.run(&query.query_id);
        metrics.merge_from(&output.metrics);
        matches.extend(output.matches);
        obligations.extend(output.observed_obligations);
        diagnostic_prefixes.extend(output.diagnostic_prefixes);
        let imported = combined_arena.import(&output.witness);
        roots.push(imported);
        branch_roots.insert(branch.id.clone(), imported);
    }
    matches.sort();
    matches.dedup();
    metrics.final_result_count = matches.len() as u64;
                                                                             
                                                                          
                                                           
    metrics.residual_closure_event_count_sum = metrics.residual_closure_event_count as u64;
    metrics.residual_closure_event_count_max = metrics.residual_closure_event_count;
    metrics.server_eval_seconds = started.elapsed().as_secs_f64();
    metrics.witness_dag_nodes = combined_arena.len() as u64;
    let root = combined_arena.union(roots);
    Ok(EvaluationOutput {
        matches,
        witness: combined_arena.finish_with_branches(root, branch_roots),
        metrics,
        observed_obligations: obligations,
        diagnostic_prefixes,
    })
}
