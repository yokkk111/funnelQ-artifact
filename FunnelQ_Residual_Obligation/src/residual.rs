use crate::index::QueryIndependentIndex;
use crate::model::Scalar;
use crate::query::{BranchSpec, Constraint, FieldRef, ValueExpr};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LiveBinding {
    pub variable: String,
    pub value: Scalar,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct KnownField {
    pub reference: FieldRef,
    pub value: Scalar,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TemporalObligation {
    pub atom: String,
    pub field: String,
    pub lower_exclusive: Option<i64>,
    pub lower_inclusive: Option<i64>,
    pub upper_exclusive: Option<i64>,
    pub upper_inclusive: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidualObligation {
    pub remaining_query_structure: Vec<String>,
                                                                         
                                                                              
                                                                              
                       
    #[serde(default)]
    pub unresolved_constraints: Vec<Constraint>,
    pub live_bindings: Vec<LiveBinding>,
    pub live_fields: Vec<KnownField>,
    pub temporal_obligations: Vec<TemporalObligation>,
    pub injectivity_frontier: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ResidualKey {
    pub branch_id: String,
    pub remaining_atoms: Vec<String>,
    pub deferred_atoms: Vec<String>,
    #[serde(default)]
    pub unresolved_constraints: Vec<Constraint>,
    pub live_bindings: Vec<LiveBinding>,
    pub live_fields: Vec<KnownField>,
    pub temporal_obligations: Vec<TemporalObligation>,
    pub injectivity_frontier: Vec<String>,
    pub same_tx_value: Option<String>,
}

fn expr_variables(expr: &ValueExpr, out: &mut BTreeSet<String>) {
    if let ValueExpr::Variable(variable) = expr {
        out.insert(variable.clone());
    }
}

fn expr_fields(expr: &ValueExpr, out: &mut BTreeSet<FieldRef>) {
    if let ValueExpr::Field(reference) = expr {
        out.insert(reference.clone());
    }
}

fn constraint_dependencies(
    constraint: &Constraint,
    variables: &mut BTreeSet<String>,
    fields: &mut BTreeSet<FieldRef>,
    atoms: &mut BTreeSet<String>,
) {
    match constraint {
        Constraint::Compare { left, right, .. } => {
            expr_variables(left, variables);
            expr_variables(right, variables);
            expr_fields(left, fields);
            expr_fields(right, fields);
        }
        Constraint::Order { earlier, later, .. } | Constraint::Within { earlier, later, .. } => {
            atoms.insert(earlier.clone());
            atoms.insert(later.clone());
        }
        Constraint::DistinctValues { values, .. } => {
            for value in values {
                expr_variables(value, variables);
                expr_fields(value, fields);
            }
        }
        Constraint::DistinctEvents(values) | Constraint::DistinctTransactions(values) => {
            atoms.extend(values.iter().cloned());
        }
        Constraint::NoEvent {
            after,
            fields: predicates,
            ..
        } => {
            atoms.insert(after.clone());
            for value in predicates.values() {
                expr_variables(value, variables);
                expr_fields(value, fields);
            }
        }
    }
}

fn atom_assigned(assigned: &BTreeMap<String, usize>, atom: &str) -> bool {
    assigned.contains_key(atom)
}

fn expression_is_resolved(
    expression: &ValueExpr,
    index: &QueryIndependentIndex,
    assigned: &BTreeMap<String, usize>,
    bindings: &BTreeMap<String, Scalar>,
) -> bool {
    match expression {
        ValueExpr::Variable(variable) => bindings.contains_key(variable),
        ValueExpr::Field(reference) => assigned
            .get(&reference.atom)
            .is_some_and(|event| index.event(*event).value(&reference.field).is_some()),
        ValueExpr::Constant(_) => true,
    }
}

fn constraint_is_pending(
    constraint: &Constraint,
    index: &QueryIndependentIndex,
    assigned: &BTreeMap<String, usize>,
    bindings: &BTreeMap<String, Scalar>,
) -> bool {
    match constraint {
        Constraint::Order { earlier, later, .. } | Constraint::Within { earlier, later, .. } => {
            !atom_assigned(assigned, earlier) || !atom_assigned(assigned, later)
        }
        Constraint::DistinctEvents(atoms) | Constraint::DistinctTransactions(atoms) => {
            atoms.iter().any(|atom| !atom_assigned(assigned, atom))
        }
        Constraint::NoEvent { after, fields, .. } => {
            !atom_assigned(assigned, after)
                || fields.values().any(|expression| {
                    !expression_is_resolved(expression, index, assigned, bindings)
                })
        }
        Constraint::Compare { left, right, .. } => {
            !expression_is_resolved(left, index, assigned, bindings)
                || !expression_is_resolved(right, index, assigned, bindings)
        }
        Constraint::DistinctValues { values, .. } => values
            .iter()
            .any(|value| !expression_is_resolved(value, index, assigned, bindings)),
    }
}

fn merge_temporal_bound(
    obligations: &mut BTreeMap<(String, String), TemporalObligation>,
    candidate: TemporalObligation,
) {
    let key = (candidate.atom.clone(), candidate.field.clone());
    let entry = obligations
        .entry(key)
        .or_insert_with(|| TemporalObligation {
            atom: candidate.atom.clone(),
            field: candidate.field.clone(),
            lower_exclusive: None,
            lower_inclusive: None,
            upper_exclusive: None,
            upper_inclusive: None,
        });

    let current_lower = entry
        .lower_exclusive
        .map(|value| (value, true))
        .or_else(|| entry.lower_inclusive.map(|value| (value, false)));
    let candidate_lower = candidate
        .lower_exclusive
        .map(|value| (value, true))
        .or_else(|| candidate.lower_inclusive.map(|value| (value, false)));
    let strongest_lower = match (current_lower, candidate_lower) {
        (Some(left), Some(right)) => Some(
            if right.0 > left.0 || (right.0 == left.0 && right.1 && !left.1) {
                right
            } else {
                left
            },
        ),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    };
    entry.lower_exclusive = strongest_lower
        .filter(|(_, exclusive)| *exclusive)
        .map(|(v, _)| v);
    entry.lower_inclusive = strongest_lower
        .filter(|(_, exclusive)| !*exclusive)
        .map(|(v, _)| v);

    let current_upper = entry
        .upper_exclusive
        .map(|value| (value, true))
        .or_else(|| entry.upper_inclusive.map(|value| (value, false)));
    let candidate_upper = candidate
        .upper_exclusive
        .map(|value| (value, true))
        .or_else(|| candidate.upper_inclusive.map(|value| (value, false)));
    let strongest_upper = match (current_upper, candidate_upper) {
        (Some(left), Some(right)) => Some(
            if right.0 < left.0 || (right.0 == left.0 && right.1 && !left.1) {
                right
            } else {
                left
            },
        ),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    };
    entry.upper_exclusive = strongest_upper
        .filter(|(_, exclusive)| *exclusive)
        .map(|(v, _)| v);
    entry.upper_inclusive = strongest_upper
        .filter(|(_, exclusive)| !*exclusive)
        .map(|(v, _)| v);
}

fn canonical_variable_names(
    branch: &BranchSpec,
    remaining: &BTreeSet<String>,
    live: &BTreeSet<String>,
) -> BTreeMap<String, String> {
    let mut signatures = Vec::new();
    for variable in live {
        let mut incidence = Vec::new();
        for atom in &branch.atoms {
            if !remaining.contains(&atom.id) {
                continue;
            }
            for (field, term) in &atom.fields {
                if matches!(term, crate::query::Term::Variable(name) if name == variable) {
                    incidence.push(format!("{}.{field}", atom.id));
                }
            }
        }
        incidence.sort();
                                                                              
                                                                                 
        signatures.push((incidence, variable.clone()));
    }
    signatures.sort();
    signatures
        .into_iter()
        .enumerate()
        .map(|(index, (_, original))| (original, format!("v{index}")))
        .collect()
}

pub fn normalize_residual(
    branch: &BranchSpec,
    index: &QueryIndependentIndex,
    remaining_atom_indices: &[usize],
    assigned: &BTreeMap<String, usize>,
    bindings: &BTreeMap<String, Scalar>,
    used_event_indices: &BTreeSet<usize>,
    same_tx_value: Option<&str>,
) -> (ResidualObligation, ResidualKey) {
    let remaining_ids: BTreeSet<String> = remaining_atom_indices
        .iter()
        .map(|index| branch.atoms[*index].id.clone())
        .collect();
    let mut live_variables = BTreeSet::new();
    for atom_index in remaining_atom_indices {
        for term in branch.atoms[*atom_index].fields.values() {
            if let crate::query::Term::Variable(variable) = term {
                live_variables.insert(variable.clone());
            }
        }
    }
    let mut unresolved_constraints: Vec<_> = branch
        .constraints
        .iter()
        .filter(|constraint| constraint_is_pending(constraint, index, assigned, bindings))
        .cloned()
        .collect();
    unresolved_constraints.sort();
    unresolved_constraints.dedup();

    let mut live_field_refs = BTreeSet::new();
    for constraint in &unresolved_constraints {
        {
            let mut atoms = BTreeSet::new();
            constraint_dependencies(
                constraint,
                &mut live_variables,
                &mut live_field_refs,
                &mut atoms,
            );
        }
    }
    let variable_names = canonical_variable_names(branch, &remaining_ids, &live_variables);
    let mut live_bindings: Vec<_> = live_variables
        .iter()
        .filter_map(|variable| {
            bindings.get(variable).cloned().map(|value| LiveBinding {
                variable: variable_names
                    .get(variable)
                    .cloned()
                    .unwrap_or_else(|| variable.clone()),
                value,
            })
        })
        .collect();
    live_bindings.sort();

    let mut live_fields: Vec<_> = live_field_refs
        .into_iter()
        .filter_map(|reference| {
            let event_index = assigned.get(&reference.atom)?;
            index
                .event(*event_index)
                .value(&reference.field)
                .map(|value| KnownField { reference, value })
        })
        .collect();
                                                                              
                                                                               
    for constraint in &unresolved_constraints {
        if let Constraint::DistinctTransactions(atoms) = constraint {
            if atoms.iter().any(|atom| remaining_ids.contains(atom)) {
                for atom in atoms {
                    if let Some(event_index) = assigned.get(atom) {
                        live_fields.push(KnownField {
                            reference: FieldRef {
                                atom: atom.clone(),
                                field: "tx_hash".to_owned(),
                            },
                            value: index.event(*event_index).tx_hash.clone().into(),
                        });
                    }
                }
            }
        }
        if let Constraint::NoEvent { after, .. } = constraint {
            if let Some(event_index) = assigned.get(after) {
                let event = index.event(*event_index);
                live_fields.push(KnownField {
                    reference: FieldRef {
                        atom: after.clone(),
                        field: "order_seq".to_owned(),
                    },
                    value: event.order_seq.into(),
                });
                live_fields.push(KnownField {
                    reference: FieldRef {
                        atom: after.clone(),
                        field: "block_ts".to_owned(),
                    },
                    value: event.block_ts.into(),
                });
            }
        }
    }
    live_fields.sort();
    live_fields.dedup();

    let mut temporal_by_field = BTreeMap::new();
    for constraint in &unresolved_constraints {
        match constraint {
            Constraint::Order {
                earlier,
                later,
                field,
            } => match (assigned.get(earlier), assigned.get(later)) {
                (Some(bound), None) if remaining_ids.contains(later) => {
                    if let Some(value) = index.event(*bound).value(field).and_then(|v| v.as_i64()) {
                        merge_temporal_bound(
                            &mut temporal_by_field,
                            TemporalObligation {
                                atom: later.clone(),
                                field: field.clone(),
                                lower_exclusive: Some(value),
                                lower_inclusive: None,
                                upper_exclusive: None,
                                upper_inclusive: None,
                            },
                        );
                    }
                }
                (None, Some(bound)) if remaining_ids.contains(earlier) => {
                    if let Some(value) = index.event(*bound).value(field).and_then(|v| v.as_i64()) {
                        merge_temporal_bound(
                            &mut temporal_by_field,
                            TemporalObligation {
                                atom: earlier.clone(),
                                field: field.clone(),
                                lower_exclusive: None,
                                lower_inclusive: None,
                                upper_exclusive: Some(value),
                                upper_inclusive: None,
                            },
                        );
                    }
                }
                _ => {}
            },
            Constraint::Within {
                earlier,
                later,
                delta_seconds,
            } => match (assigned.get(earlier), assigned.get(later)) {
                (Some(bound), None) if remaining_ids.contains(later) => {
                    let event = index.event(*bound);
                    merge_temporal_bound(
                        &mut temporal_by_field,
                        TemporalObligation {
                            atom: later.clone(),
                            field: "block_ts".to_owned(),
                            lower_exclusive: None,
                            lower_inclusive: None,
                            upper_exclusive: None,
                            upper_inclusive: Some(event.block_ts.saturating_add(*delta_seconds)),
                        },
                    );
                    merge_temporal_bound(
                        &mut temporal_by_field,
                        TemporalObligation {
                            atom: later.clone(),
                            field: "order_seq".to_owned(),
                            lower_exclusive: Some(event.order_seq),
                            lower_inclusive: None,
                            upper_exclusive: None,
                            upper_inclusive: None,
                        },
                    );
                }
                (None, Some(bound)) if remaining_ids.contains(earlier) => {
                    let event = index.event(*bound);
                    merge_temporal_bound(
                        &mut temporal_by_field,
                        TemporalObligation {
                            atom: earlier.clone(),
                            field: "block_ts".to_owned(),
                            lower_exclusive: None,
                            lower_inclusive: Some(event.block_ts.saturating_sub(*delta_seconds)),
                            upper_exclusive: None,
                            upper_inclusive: None,
                        },
                    );
                    merge_temporal_bound(
                        &mut temporal_by_field,
                        TemporalObligation {
                            atom: earlier.clone(),
                            field: "order_seq".to_owned(),
                            lower_exclusive: None,
                            lower_inclusive: None,
                            upper_exclusive: Some(event.order_seq),
                            upper_inclusive: None,
                        },
                    );
                }
                _ => {}
            },
            _ => {}
        }
    }
    let temporal: Vec<_> = temporal_by_field.into_values().collect();

    let remaining_types: BTreeSet<_> = remaining_atom_indices
        .iter()
        .flat_map(|atom| branch.atoms[*atom].event_types.iter().cloned())
        .collect();
    let mut injectivity_frontier: Vec<_> = used_event_indices
        .iter()
        .filter_map(|event_index| {
            let event = index.event(*event_index);
            remaining_types
                .contains(&event.event_type)
                .then(|| event.event_id.clone())
        })
        .collect();
    injectivity_frontier.sort();
    injectivity_frontier.dedup();

    let mut remaining_query_structure: Vec<_> = remaining_ids.into_iter().collect();
    remaining_query_structure.sort();
    let assigned_ids: BTreeSet<_> = assigned.keys().cloned().collect();
    let deferred_atoms: Vec<_> = branch
        .atoms
        .iter()
        .map(|atom| atom.id.clone())
        .filter(|atom| !remaining_query_structure.contains(atom) && !assigned_ids.contains(atom))
        .collect();
    let obligation = ResidualObligation {
        remaining_query_structure: remaining_query_structure.clone(),
        unresolved_constraints: unresolved_constraints.clone(),
        live_bindings: live_bindings.clone(),
        live_fields: live_fields.clone(),
        temporal_obligations: temporal.clone(),
        injectivity_frontier: injectivity_frontier.clone(),
    };
    let key = ResidualKey {
        branch_id: branch.id.clone(),
        remaining_atoms: remaining_query_structure,
        deferred_atoms,
        unresolved_constraints,
        live_bindings,
        live_fields,
        temporal_obligations: temporal,
        injectivity_frontier,
        same_tx_value: same_tx_value.map(str::to_owned),
    };
    (obligation, key)
}
