use crate::model::Scalar;
use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

const ROLE_ALIASES: &[(&str, &str)] = &[
    ("src", "move_from"),
    ("dst", "move_to"),
    ("collection", "collection_contract"),
    ("nft", "nft"),
];

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ScopeSpec {
    ResidualClosure,
    SameTx,
    Episode {
        anchor_atom: String,
        window_seconds: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Term {
    Variable(String),
    Constant(Scalar),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AtomSpec {
    pub id: String,
    pub event_types: BTreeSet<String>,
    pub fields: BTreeMap<String, Term>,
    pub local_predicates: Vec<LocalPredicate>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FieldRef {
    pub atom: String,
    pub field: String,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ValueExpr {
    Variable(String),
    Field(FieldRef),
    Constant(Scalar),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LocalPredicate {
    pub field: String,
    pub op: CompareOp,
    pub value: Scalar,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Constraint {
    Compare {
        left: ValueExpr,
        op: CompareOp,
        right: ValueExpr,
    },
    Order {
        earlier: String,
        later: String,
        field: String,
    },
    Within {
        earlier: String,
        later: String,
        delta_seconds: i64,
    },
    DistinctValues {
        values: Vec<ValueExpr>,
        min_count: usize,
        max_count: usize,
        ignore_empty_text: bool,
    },
    DistinctEvents(Vec<String>),
    DistinctTransactions(Vec<String>),
    NoEvent {
        after: String,
        event_types: BTreeSet<String>,
        fields: BTreeMap<String, ValueExpr>,
        within_seconds: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BranchSpec {
    pub id: String,
    pub atoms: Vec<AtomSpec>,
    pub constraints: Vec<Constraint>,
    pub event_injective: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySpec {
    pub query_id: String,
    pub result_semantics: String,
    pub scope: ScopeSpec,
    pub branches: Vec<BranchSpec>,
}

fn variable(raw: &str) -> Option<String> {
    raw.strip_prefix('$')
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn parse_event_types(raw: Option<&Value>, allow_empty: bool) -> Result<BTreeSet<String>> {
    let values: Vec<&Value> = match raw {
        Some(Value::Array(values)) => values.iter().collect(),
        Some(value) => vec![value],
        None => Vec::new(),
    };
    let result: BTreeSet<_> = values
        .into_iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .context("event_type must be a string")
        })
        .collect::<Result<_>>()?;
    if result.is_empty() && !allow_empty {
        bail!("event atom has no event type")
    }
    if result.iter().any(|value| value.trim().is_empty()) {
        bail!("event type must not be empty")
    }
    Ok(result)
}

fn value_expr(raw: &Value) -> Result<ValueExpr> {
    if let Some(text) = raw.as_str() {
        if let Some(name) = variable(text) {
            return Ok(ValueExpr::Variable(name));
        }
        if let Some((atom, field)) = text.split_once('.') {
            if !atom.is_empty() && !field.is_empty() {
                return Ok(ValueExpr::Field(FieldRef {
                    atom: atom.to_owned(),
                    field: field.to_owned(),
                }));
            }
        }
    }
    Ok(ValueExpr::Constant(Scalar::from_json(raw)?))
}

fn parse_op(raw: &str) -> Result<CompareOp> {
    match raw {
        "eq" | "equal" => Ok(CompareOp::Eq),
        "neq" | "not_equal" => Ok(CompareOp::Ne),
        "lt" | "less_than" => Ok(CompareOp::Lt),
        "le" | "less_or_equal" => Ok(CompareOp::Le),
        "gt" | "greater_than" => Ok(CompareOp::Gt),
        "ge" | "greater_or_equal" => Ok(CompareOp::Ge),
        other => bail!("unsupported comparison operator {other}"),
    }
}

fn parse_atom(raw: &Map<String, Value>) -> Result<AtomSpec> {
    let id = raw
        .get("id")
        .and_then(Value::as_str)
        .context("event atom has no id")?
        .to_owned();
    if id.trim().is_empty() {
        bail!("event atom id must not be empty")
    }
    let mut fields = BTreeMap::new();
    for (alias, canonical) in ROLE_ALIASES {
        if let Some(value) = raw.get(*alias) {
            let term = if let Some(name) = value.as_str().and_then(variable) {
                Term::Variable(name)
            } else {
                Term::Constant(Scalar::from_json(value)?)
            };
            fields.insert((*canonical).to_owned(), term);
        }
    }
    if let Some(extra) = raw.get("fields").and_then(Value::as_object) {
        for (field, value) in extra {
            let term = if let Some(name) = value.as_str().and_then(variable) {
                Term::Variable(name)
            } else {
                Term::Constant(Scalar::from_json(value)?)
            };
            fields.insert(field.clone(), term);
        }
    }
    let mut local_predicates = Vec::new();
    if let Some(predicates) = raw.get("predicates").and_then(Value::as_array) {
        for predicate in predicates {
            let predicate = predicate
                .as_object()
                .context("local predicate must be an object")?;
            local_predicates.push(LocalPredicate {
                field: predicate
                    .get("field")
                    .and_then(Value::as_str)
                    .context("local predicate has no field")?
                    .to_owned(),
                op: parse_op(predicate.get("op").and_then(Value::as_str).unwrap_or("eq"))?,
                value: Scalar::from_json(
                    predicate
                        .get("value")
                        .context("local predicate has no value")?,
                )?,
            });
        }
    }
    Ok(AtomSpec {
        id,
        event_types: parse_event_types(raw.get("event_type"), false)?,
        fields,
        local_predicates,
    })
}

fn strings(raw: Option<&Value>, name: &str) -> Result<Vec<String>> {
    raw.and_then(Value::as_array)
        .context(format!("{name} must be an array"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .context(format!("{name} member must be a string"))
        })
        .collect()
}

fn parse_constraint(raw: &Map<String, Value>) -> Result<Vec<Constraint>> {
    let kind = raw
        .get("type")
        .and_then(Value::as_str)
        .context("constraint has no type")?;
    let one = match kind {
        "order" | "strict_time_order" => {
            let sequence = strings(raw.get("sequence"), "constraint sequence")?;
            if sequence.len() < 2 {
                bail!("{kind} constraint requires at least two event atoms")
            }
            let field = if kind == "order" {
                "order_seq"
            } else {
                "block_ts"
            };
            return Ok(sequence
                .windows(2)
                .map(|pair| Constraint::Order {
                    earlier: pair[0].clone(),
                    later: pair[1].clone(),
                    field: field.to_owned(),
                })
                .collect());
        }
        "within" => {
            let delta_seconds = raw
                .get("delta_seconds")
                .and_then(Value::as_i64)
                .context("within has no delta_seconds")?;
            if delta_seconds < 0 {
                bail!("within delta_seconds must be non-negative")
            }
            Constraint::Within {
                earlier: raw
                    .get("earlier")
                    .and_then(Value::as_str)
                    .context("within has no earlier atom")?
                    .to_owned(),
                later: raw
                    .get("later")
                    .and_then(Value::as_str)
                    .context("within has no later atom")?
                    .to_owned(),
                delta_seconds,
            }
        }
        "not_equal" | "less_than" | "equal" | "less_or_equal" | "greater_than"
        | "greater_or_equal" => Constraint::Compare {
            left: value_expr(raw.get("left").context("comparison has no left operand")?)?,
            op: parse_op(kind)?,
            right: value_expr(
                raw.get("right")
                    .context("comparison has no right operand")?,
            )?,
        },
        "min_distinct" | "distinct_count_between" => {
            let values = raw
                .get("values")
                .and_then(Value::as_array)
                .context("distinct constraint has no values")?
                .iter()
                .map(value_expr)
                .collect::<Result<Vec<_>>>()?;
            let min_count = raw.get("min_count").and_then(Value::as_u64).unwrap_or(0) as usize;
            let max_count = raw
                .get("max_count")
                .and_then(Value::as_u64)
                .unwrap_or(u32::MAX as u64) as usize;
            if min_count > max_count {
                bail!("distinct constraint min_count exceeds max_count")
            }
            Constraint::DistinctValues {
                values,
                min_count,
                max_count,
                ignore_empty_text: kind == "distinct_count_between",
            }
        }
        "distinct_event_ids" => {
            Constraint::DistinctEvents(strings(raw.get("events"), "distinct events")?)
        }
        "distinct_transactions" => {
            Constraint::DistinctTransactions(strings(raw.get("events"), "distinct transactions")?)
        }
        "no_event" => {
            let mut fields = BTreeMap::new();
            for (alias, canonical) in ROLE_ALIASES {
                if let Some(value) = raw.get(*alias) {
                    fields.insert((*canonical).to_owned(), value_expr(value)?);
                }
            }
            if let Some(extra) = raw.get("fields").and_then(Value::as_object) {
                for (field, value) in extra {
                    fields.insert(field.clone(), value_expr(value)?);
                }
            }
            let within_seconds = raw
                .get("within_seconds")
                .and_then(Value::as_i64)
                .unwrap_or(86_400);
            if within_seconds < 0 {
                bail!("no_event within_seconds must be non-negative")
            }
            Constraint::NoEvent {
                after: raw
                    .get("after")
                    .and_then(Value::as_str)
                    .unwrap_or("root")
                    .to_owned(),
                event_types: parse_event_types(raw.get("event_type"), true)?,
                fields,
                within_seconds,
            }
        }
        other => bail!("unsupported full-binding constraint {other}"),
    };
    Ok(vec![one])
}

fn validate_branch(branch: &BranchSpec) -> Result<()> {
    if branch.atoms.is_empty() {
        bail!("branch {} has no event atoms", branch.id)
    }
    let ids: BTreeSet<_> = branch.atoms.iter().map(|atom| atom.id.as_str()).collect();
    if ids.len() != branch.atoms.len() {
        bail!("branch {} contains duplicate atom ids", branch.id)
    }
    let bound_variables: BTreeSet<_> = branch
        .atoms
        .iter()
        .flat_map(|atom| atom.fields.values())
        .filter_map(|term| match term {
            Term::Variable(variable) => Some(variable.as_str()),
            Term::Constant(_) => None,
        })
        .collect();
    let referenced = |atom: &str| -> Result<()> {
        if !ids.contains(atom) {
            bail!(
                "constraint references unknown atom {atom} in branch {}",
                branch.id
            )
        }
        Ok(())
    };
    let validate_expr = |expr: &ValueExpr| -> Result<()> {
        match expr {
            ValueExpr::Variable(variable) => {
                if !bound_variables.contains(variable.as_str()) {
                    bail!(
                        "constraint references unbound variable ${variable} in branch {}",
                        branch.id
                    )
                }
            }
            ValueExpr::Field(reference) => referenced(&reference.atom)?,
            ValueExpr::Constant(_) => {}
        }
        Ok(())
    };
    for constraint in &branch.constraints {
        match constraint {
            Constraint::Order { earlier, later, .. }
            | Constraint::Within { earlier, later, .. } => {
                referenced(earlier)?;
                referenced(later)?;
            }
            Constraint::DistinctEvents(atoms) | Constraint::DistinctTransactions(atoms) => {
                for atom in atoms {
                    referenced(atom)?;
                }
            }
            Constraint::NoEvent { after, fields, .. } => {
                referenced(after)?;
                for value in fields.values() {
                    validate_expr(value)?;
                }
            }
            Constraint::Compare { left, right, .. } => {
                validate_expr(left)?;
                validate_expr(right)?;
            }
            Constraint::DistinctValues { values, .. } => {
                for value in values {
                    validate_expr(value)?;
                }
            }
        }
    }
    Ok(())
}

pub fn compile_query_document(document: &Value) -> Result<QuerySpec> {
    let full = document
        .get("full_binding_plan")
        .and_then(Value::as_object)
        .context("query is missing full_binding_plan")?;
    let query_id = full
        .get("query_id")
        .or_else(|| document.get("query_name"))
        .and_then(Value::as_str)
        .context("query has no id")?
        .to_owned();
    let result_semantics = full
        .get("result_semantics")
        .or_else(|| document.get("result_semantics"))
        .and_then(Value::as_str)
        .unwrap_or("full_event_binding_v1")
        .to_owned();
    if result_semantics != "full_event_binding_v1" {
        bail!("unsupported result semantics {result_semantics}")
    }
    let delta_seconds = full
        .get("delta_seconds")
        .and_then(Value::as_i64)
        .unwrap_or(86_400);
    let raw_scope = document
        .get("evidence_scope")
        .or_else(|| document.get("scope"));
    let scope = match raw_scope {
        Some(Value::String(kind)) if kind == "same_tx" => ScopeSpec::SameTx,
        Some(Value::String(kind)) if kind == "episode" => ScopeSpec::Episode {
            anchor_atom: "root".to_owned(),
            window_seconds: delta_seconds,
        },
        Some(Value::String(kind)) if kind == "residual_closure" => ScopeSpec::ResidualClosure,
        Some(Value::Object(value)) => match value.get("kind").and_then(Value::as_str) {
            Some("same_tx") => ScopeSpec::SameTx,
            Some("episode") => ScopeSpec::Episode {
                anchor_atom: value
                    .get("anchor_position")
                    .or_else(|| value.get("anchor_atom"))
                    .and_then(Value::as_str)
                    .unwrap_or("root")
                    .to_owned(),
                window_seconds: value
                    .get("delta_seconds")
                    .or_else(|| value.get("window_seconds"))
                    .and_then(Value::as_i64)
                    .unwrap_or(delta_seconds),
            },
            Some("residual_closure") => ScopeSpec::ResidualClosure,
            Some(other) => bail!("unsupported query scope {other}"),
            None => bail!("evidence_scope object has no kind"),
        },
        None => ScopeSpec::ResidualClosure,
        Some(other) => bail!("invalid query scope {other}"),
    };
    if matches!(scope, ScopeSpec::Episode { window_seconds, .. } if window_seconds <= 0) {
        bail!("Episode window must be positive")
    }
    let mut branches = Vec::new();
    let mut branch_ids = BTreeSet::new();
    for raw_branch in full
        .get("branches")
        .and_then(Value::as_array)
        .context("full_binding_plan has no branches")?
    {
        let raw_branch = raw_branch.as_object().context("branch must be an object")?;
        let id = raw_branch
            .get("branch_id")
            .and_then(Value::as_str)
            .unwrap_or("branch")
            .to_owned();
        if id.trim().is_empty() {
            bail!("branch id must not be empty")
        }
        if !branch_ids.insert(id.clone()) {
            bail!("query {query_id} contains duplicate branch id {id}")
        }
        let atoms = raw_branch
            .get("event_patterns")
            .and_then(Value::as_array)
            .context("branch has no event_patterns")?
            .iter()
            .map(|raw| parse_atom(raw.as_object().context("event pattern must be an object")?))
            .collect::<Result<Vec<_>>>()?;
        let mut constraints = Vec::new();
        for raw in raw_branch
            .get("constraints")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            constraints.extend(parse_constraint(
                raw.as_object().context("constraint must be an object")?,
            )?);
        }
        constraints.sort();
        constraints.dedup();
        let branch = BranchSpec {
            id,
            atoms,
            constraints,
            event_injective: true,
        };
        validate_branch(&branch)?;
        branches.push(branch);
    }
    if branches.is_empty() {
        bail!("query {query_id} has no branches")
    }
    if let ScopeSpec::Episode {
        anchor_atom,
        window_seconds,
    } = &scope
    {
        for branch in &branches {
            if !branch.atoms.iter().any(|atom| atom.id == *anchor_atom) {
                bail!(
                    "Episode anchor {anchor_atom} is absent from branch {}",
                    branch.id
                )
            }
            for constraint in &branch.constraints {
                if let Constraint::Within { delta_seconds, .. } = constraint {
                    if delta_seconds > window_seconds {
                        bail!(
                            "within window {delta_seconds} exceeds Episode window {window_seconds}"
                        )
                    }
                }
                if let Constraint::NoEvent { within_seconds, .. } = constraint {
                    if within_seconds > window_seconds {
                        bail!("no_event window {within_seconds} exceeds Episode window {window_seconds}")
                    }
                }
            }
        }
    }
    Ok(QuerySpec {
        query_id,
        result_semantics,
        scope,
        branches,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compiles_unregistered_query_without_query_kind() {
        let raw = serde_json::json!({
            "query_name": "UNSEEN_QUERY",
            "full_binding_plan": {
                "query_id": "UNSEEN_QUERY",
                "result_semantics": "full_event_binding_v1",
                "delta_seconds": 60,
                "branches": [{
                    "branch_id": "b",
                    "event_patterns": [
                        {"id":"root","event_type":"NFTTrade","src":"$a","dst":"$b","nft":"$x"},
                        {"id":"tail","event_type":["NFTTrade","NFTTransfer"],"src":"$b","dst":"$a","nft":"$x"}
                    ],
                    "constraints": [
                        {"type":"order","sequence":["root","tail"]},
                        {"type":"within","earlier":"root","later":"tail","delta_seconds":60}
                    ]
                }]
            }
        });
        let spec = compile_query_document(&raw).unwrap();
        assert_eq!(spec.query_id, "UNSEEN_QUERY");
        assert_eq!(spec.branches[0].atoms.len(), 2);
        assert_eq!(spec.branches[0].constraints.len(), 2);
    }

    #[test]
    fn rejects_unknown_field_references_and_unbound_variables() {
        let unknown_atom = serde_json::json!({
            "query_name": "BAD_FIELD_REF",
            "full_binding_plan": {
                "branches": [{
                    "branch_id": "b",
                    "event_patterns": [{"id":"root","event_type":"NFTTrade","src":"$owner"}],
                    "constraints": [{"type":"equal","left":"missing.src","right":"$owner"}]
                }]
            }
        });
        assert!(compile_query_document(&unknown_atom).is_err());

        let unbound_variable = serde_json::json!({
            "query_name": "BAD_VARIABLE",
            "full_binding_plan": {
                "branches": [{
                    "branch_id": "b",
                    "event_patterns": [{"id":"root","event_type":"NFTTrade"}],
                    "constraints": [{"type":"equal","left":"$never_bound","right":1}]
                }]
            }
        });
        assert!(compile_query_document(&unbound_variable).is_err());
    }

    #[test]
    fn rejects_ambiguous_branch_identity_and_invalid_windows() {
        let duplicate_branches = serde_json::json!({
            "query_name": "DUPLICATE_BRANCH",
            "full_binding_plan": {
                "branches": [
                    {"branch_id":"same","event_patterns":[{"id":"a","event_type":"A"}]},
                    {"branch_id":"same","event_patterns":[{"id":"b","event_type":"B"}]}
                ]
            }
        });
        assert!(compile_query_document(&duplicate_branches).is_err());

        let negative_window = serde_json::json!({
            "query_name": "NEGATIVE_WINDOW",
            "full_binding_plan": {
                "branches": [{
                    "branch_id":"b",
                    "event_patterns":[{"id":"a","event_type":"A"},{"id":"b","event_type":"B"}],
                    "constraints":[{"type":"within","earlier":"a","later":"b","delta_seconds":-1}]
                }]
            }
        });
        assert!(compile_query_document(&negative_window).is_err());
    }
}
