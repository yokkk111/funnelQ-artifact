use crate::evaluator::{EvaluationRequest, MatchRecord};
use crate::index::QueryIndependentIndex;
use crate::model::Scalar;
use crate::query::{
    AtomSpec, BranchSpec, CompareOp, Constraint, QuerySpec, ScopeSpec, Term, ValueExpr,
};
use anyhow::{Context, Result};
use std::collections::{BTreeMap, BTreeSet};

fn compare(left: &Scalar, op: CompareOp, right: &Scalar) -> bool {
    match op {
        CompareOp::Eq => left == right,
        CompareOp::Ne => left != right,
        CompareOp::Lt => left.same_type_cmp(right).is_some_and(|order| order.is_lt()),
        CompareOp::Le => left.same_type_cmp(right).is_some_and(|order| order.is_le()),
        CompareOp::Gt => left.same_type_cmp(right).is_some_and(|order| order.is_gt()),
        CompareOp::Ge => left.same_type_cmp(right).is_some_and(|order| order.is_ge()),
    }
}

fn resolve(
    value: &ValueExpr,
    index: &QueryIndependentIndex,
    assigned: &BTreeMap<String, usize>,
    bindings: &BTreeMap<String, Scalar>,
) -> Option<Scalar> {
    match value {
        ValueExpr::Variable(name) => bindings.get(name).cloned(),
        ValueExpr::Field(reference) => assigned
            .get(&reference.atom)
            .and_then(|event| index.event(*event).value(&reference.field)),
        ValueExpr::Constant(value) => Some(value.clone()),
    }
}

fn atom_matches(
    atom: &AtomSpec,
    event_index: usize,
    index: &QueryIndependentIndex,
    bindings: &BTreeMap<String, Scalar>,
) -> Option<BTreeMap<String, Scalar>> {
    let event = index.event(event_index);
    if !atom.event_types.contains(&event.event_type) {
        return None;
    }
    let mut result = bindings.clone();
    for (field, term) in &atom.fields {
        let actual = event.value(field)?;
        match term {
            Term::Constant(expected) if actual != *expected => return None,
            Term::Constant(_) => {}
            Term::Variable(name) => match result.get(name) {
                Some(expected) if expected != &actual => return None,
                Some(_) => {}
                None if actual.is_empty() => return None,
                None => {
                    result.insert(name.clone(), actual);
                }
            },
        }
    }
    if atom.local_predicates.iter().any(|predicate| {
        !event
            .value(&predicate.field)
            .is_some_and(|actual| compare(&actual, predicate.op, &predicate.value))
    }) {
        return None;
    }
    Some(result)
}

fn full_constraints_hold(
    branch: &BranchSpec,
    index: &QueryIndependentIndex,
    allowed: &BTreeSet<usize>,
    assigned: &BTreeMap<String, usize>,
    bindings: &BTreeMap<String, Scalar>,
) -> bool {
    for constraint in &branch.constraints {
        let valid = match constraint {
            Constraint::Compare { left, op, right } => resolve(left, index, assigned, bindings)
                .zip(resolve(right, index, assigned, bindings))
                .is_some_and(|(left, right)| compare(&left, *op, &right)),
            Constraint::Order {
                earlier,
                later,
                field,
            } => assigned
                .get(earlier)
                .zip(assigned.get(later))
                .and_then(|(left, right)| {
                    index
                        .event(*left)
                        .value(field)?
                        .as_i64()
                        .zip(index.event(*right).value(field)?.as_i64())
                })
                .is_some_and(|(left, right)| left < right),
            Constraint::Within {
                earlier,
                later,
                delta_seconds,
            } => assigned
                .get(earlier)
                .zip(assigned.get(later))
                .is_some_and(|(left, right)| {
                    let left = index.event(*left);
                    let right = index.event(*right);
                    left.order_seq < right.order_seq
                        && right.block_ts <= left.block_ts.saturating_add(*delta_seconds)
                }),
            Constraint::DistinctValues {
                values,
                min_count,
                max_count,
                ignore_empty_text,
            } => {
                let values: Option<Vec<_>> = values
                    .iter()
                    .map(|value| resolve(value, index, assigned, bindings))
                    .collect();
                values.is_some_and(|values| {
                    let distinct = values
                        .iter()
                        .filter(|value| !*ignore_empty_text || !value.is_empty())
                        .collect::<BTreeSet<_>>()
                        .len();
                    distinct >= *min_count && distinct <= *max_count
                })
            }
            Constraint::DistinctEvents(atoms) => {
                let ready: Option<Vec<_>> = atoms.iter().map(|atom| assigned.get(atom)).collect();
                ready.is_some_and(|values| {
                    values.iter().collect::<BTreeSet<_>>().len() == values.len()
                })
            }
            Constraint::DistinctTransactions(atoms) => {
                let ready: Option<Vec<_>> = atoms
                    .iter()
                    .map(|atom| {
                        assigned
                            .get(atom)
                            .map(|event| index.event(*event).tx_hash.as_str())
                    })
                    .collect();
                ready.is_some_and(|values| {
                    values.iter().all(|value| !value.is_empty())
                        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
                })
            }
            Constraint::NoEvent {
                after,
                event_types,
                fields,
                within_seconds,
            } => {
                let Some(anchor_index) = assigned.get(after) else {
                    return false;
                };
                let Some(resolved): Option<Vec<_>> = fields
                    .iter()
                    .map(|(field, value)| {
                        resolve(value, index, assigned, bindings).map(|value| (field, value))
                    })
                    .collect()
                else {
                    return false;
                };
                let anchor = index.event(*anchor_index);
                !allowed.iter().any(|event_index| {
                    let event = index.event(*event_index);
                    (event_types.is_empty() || event_types.contains(&event.event_type))
                        && event.order_seq > anchor.order_seq
                        && event.block_ts <= anchor.block_ts.saturating_add(*within_seconds)
                        && resolved
                            .iter()
                            .all(|(field, value)| event.value(field).as_ref() == Some(value))
                })
            }
        };
        if !valid {
            return false;
        }
    }
    true
}

pub fn brute_force_query(
    index: &QueryIndependentIndex,
    query: &QuerySpec,
    request: &EvaluationRequest,
) -> Result<Vec<MatchRecord>> {
    let fixed: BTreeMap<String, usize> = request
        .fixed_atoms
        .iter()
        .map(|(atom, event)| {
            Ok((
                atom.clone(),
                index
                    .event_index(event)
                    .with_context(|| format!("unknown event {event}"))?,
            ))
        })
        .collect::<Result<_>>()?;
    let allowed: BTreeSet<usize> = match &request.allowed_event_ids {
        Some(ids) => ids
            .iter()
            .map(|id| {
                index
                    .event_index(id)
                    .with_context(|| format!("unknown event {id}"))
            })
            .collect::<Result<_>>()?,
        None => match &query.scope {
            ScopeSpec::Episode {
                anchor_atom,
                window_seconds,
            } => {
                let anchor_index = fixed.get(anchor_atom).with_context(|| {
                    format!("standalone Episode oracle requires fixed {anchor_atom}")
                })?;
                let anchor = index.event(*anchor_index);
                index
                    .all_indices()
                    .iter()
                    .copied()
                    .filter(|event_index| {
                        let event = index.event(*event_index);
                        event.order_seq >= anchor.order_seq
                            && event.block_ts <= anchor.block_ts.saturating_add(*window_seconds)
                    })
                    .collect()
            }
            _ => index.all_indices().iter().copied().collect(),
        },
    };
    let mut output = Vec::new();
    for branch in &query.branches {
        struct Search<'a> {
            index: &'a QueryIndependentIndex,
            query: &'a QuerySpec,
            branch: &'a BranchSpec,
            allowed: &'a BTreeSet<usize>,
            fixed: &'a BTreeMap<String, usize>,
            output: &'a mut Vec<MatchRecord>,
        }
        impl Search<'_> {
            fn visit(
                &mut self,
                position: usize,
                assigned: &mut BTreeMap<String, usize>,
                bindings: BTreeMap<String, Scalar>,
                used: &mut BTreeSet<usize>,
                same_tx: Option<String>,
            ) {
                if position == self.branch.atoms.len() {
                    if !full_constraints_hold(
                        self.branch,
                        self.index,
                        self.allowed,
                        assigned,
                        &bindings,
                    ) {
                        return;
                    }
                    self.output.push(MatchRecord {
                        query_id: self.query.query_id.clone(),
                        branch_id: self.branch.id.clone(),
                        event_bindings: assigned
                            .iter()
                            .map(|(atom, event)| {
                                (atom.clone(), self.index.event(*event).event_id.clone())
                            })
                            .collect(),
                        variable_bindings: bindings,
                    });
                    return;
                }
                let atom = &self.branch.atoms[position];
                let candidates: Box<dyn Iterator<Item = usize>> = match self.fixed.get(&atom.id) {
                    Some(event) => Box::new(std::iter::once(*event)),
                    None => Box::new(self.allowed.iter().copied()),
                };
                for event_index in candidates {
                    if used.contains(&event_index) {
                        continue;
                    }
                    let event = self.index.event(event_index);
                    if matches!(self.query.scope, ScopeSpec::SameTx)
                        && same_tx.as_deref().is_some_and(|tx| tx != event.tx_hash)
                    {
                        continue;
                    }
                    let Some(next_bindings) =
                        atom_matches(atom, event_index, self.index, &bindings)
                    else {
                        continue;
                    };
                    assigned.insert(atom.id.clone(), event_index);
                    used.insert(event_index);
                    let next_tx = if matches!(self.query.scope, ScopeSpec::SameTx) {
                        same_tx.clone().or_else(|| Some(event.tx_hash.clone()))
                    } else {
                        same_tx.clone()
                    };
                    self.visit(position + 1, assigned, next_bindings, used, next_tx);
                    used.remove(&event_index);
                    assigned.remove(&atom.id);
                }
            }
        }
        Search {
            index,
            query,
            branch,
            allowed: &allowed,
            fixed: &fixed,
            output: &mut output,
        }
        .visit(
            0,
            &mut BTreeMap::new(),
            BTreeMap::new(),
            &mut BTreeSet::new(),
            None,
        );
    }
    output.sort();
    output.dedup();
    Ok(output)
}
