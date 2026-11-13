                                                                            
                                                                                
                                                                                       
use crate::model::{CanonicalEvent, Scalar};
use crate::query::{BranchSpec, CompareOp, Constraint, QuerySpec, ScopeSpec, Term, ValueExpr};
use anyhow::{bail, ensure, Context, Result};
use num_bigint::BigUint;
use num_traits::{One, Zero};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
type Assignment = Vec<Option<usize>>;
mod multi_range;
#[derive(Clone, Debug)]
enum Ref {
    Field(usize, String),
    Const(Scalar),
}
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ScalarView<'a> {
    Int(i64),
    Text(&'a str),
    Bool(bool),
}
impl<'a> From<&'a Scalar> for ScalarView<'a> {
    fn from(v: &'a Scalar) -> Self {
        match v {
            Scalar::Int(v) => Self::Int(*v),
            Scalar::Text(v) => Self::Text(v),
            Scalar::Bool(v) => Self::Bool(*v),
        }
    }
}
pub(crate) fn field_view<'a>(e: &'a CanonicalEvent, f: &str) -> Option<ScalarView<'a>> {
    Some(match f {
        "event_id" => ScalarView::Text(&e.event_id),
        "event_type" => ScalarView::Text(&e.event_type),
        "order_seq" => ScalarView::Int(e.order_seq),
        "block_ts" | "block_time_epoch" => ScalarView::Int(e.block_ts),
        "tx_hash" | "tx_id" => ScalarView::Text(&e.tx_hash),
        _ => e
            .fields
            .get(match f {
                "src" | "src_addr" => "move_from",
                "dst" | "dst_addr" => "move_to",
                "collection" => "collection_contract",
                _ => f,
            })?
            .into(),
    })
}
fn view_cmp(a: ScalarView<'_>, op: CompareOp, b: ScalarView<'_>) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        _ => {
            let order = match (a, b) {
                (ScalarView::Int(a), ScalarView::Int(b)) => Some(a.cmp(&b)),
                (ScalarView::Text(a), ScalarView::Text(b)) => Some(a.cmp(b)),
                (ScalarView::Bool(a), ScalarView::Bool(b)) => Some(a.cmp(&b)),
                _ => None,
            };
            order.is_some_and(|v| match op {
                CompareOp::Lt => v.is_lt(),
                CompareOp::Le => v.is_le(),
                CompareOp::Gt => v.is_gt(),
                CompareOp::Ge => v.is_ge(),
                _ => unreachable!(),
            })
        }
    }
}
#[derive(Clone, Debug)]
enum Pred {
    Compare(Ref, CompareOp, Ref),
    Gap {
        early: usize,
        late: usize,
        min: Option<i64>,
        max: i64,
    },
    Distinct(Vec<Ref>, usize, usize, bool),
    NoEvent {
        after: usize,
        scope_anchor: Option<(usize, i64)>,
        fields: Vec<(String, Ref)>,
        within: i64,
        blockers: Vec<usize>,
    },
}
impl Ref {
    fn slot(&self) -> Option<usize> {
        match self {
            Self::Field(s, _) => Some(*s),
            _ => None,
        }
    }
    fn value(&self, a: &Assignment, es: &[&CanonicalEvent]) -> Option<Scalar> {
        match self {
            Self::Field(s, f) => es[a[*s]?].value(f),
            Self::Const(v) => Some(v.clone()),
        }
    }
    fn borrowed<'a>(&'a self, a: &Assignment, es: &[&'a CanonicalEvent]) -> Option<ScalarView<'a>> {
        match self {
            Self::Field(s, f) => field_view(es[a[*s]?], f),
            Self::Const(v) => Some(v.into()),
        }
    }
}
fn cmp(a: &Scalar, op: CompareOp, b: &Scalar) -> bool {
    match op {
        CompareOp::Eq => a == b,
        CompareOp::Ne => a != b,
        CompareOp::Lt => a.same_type_cmp(b).is_some_and(|x| x.is_lt()),
        CompareOp::Le => a.same_type_cmp(b).is_some_and(|x| x.is_le()),
        CompareOp::Gt => a.same_type_cmp(b).is_some_and(|x| x.is_gt()),
        CompareOp::Ge => a.same_type_cmp(b).is_some_and(|x| x.is_ge()),
    }
}
impl Pred {
    fn deps(&self) -> BTreeSet<usize> {
        match self {
            Self::Compare(a, _, b) => a.slot().into_iter().chain(b.slot()).collect(),
            Self::Gap { early, late, .. } => BTreeSet::from([*early, *late]),
            Self::Distinct(v, ..) => v.iter().filter_map(Ref::slot).collect(),
            Self::NoEvent {
                after,
                scope_anchor,
                fields,
                ..
            } => std::iter::once(*after)
                .chain(scope_anchor.iter().map(|x| x.0))
                .chain(fields.iter().filter_map(|(_, r)| r.slot()))
                .collect(),
        }
    }
    fn holds(&self, a: &Assignment, es: &[&CanonicalEvent]) -> bool {
        match self {
            Self::Compare(l, op, r) => l
                .borrowed(a, es)
                .zip(r.borrowed(a, es))
                .is_some_and(|(l, r)| view_cmp(l, *op, r)),
            Self::Gap {
                early,
                late,
                min,
                max,
            } => a[*early].zip(a[*late]).is_some_and(|(i, j)| {
                let d = es[j].block_ts as i128 - es[i].block_ts as i128;
                min.is_none_or(|m| d >= m as i128) && d <= *max as i128
            }),
            Self::Distinct(v, min, max, empty) => {
                let Some(values): Option<Vec<_>> = v.iter().map(|r| r.borrowed(a, es)).collect()
                else {
                    return false;
                };
                let n = values
                    .iter()
                    .filter(|v| !(*empty && matches!(v, ScalarView::Text(""))))
                    .collect::<BTreeSet<_>>()
                    .len();
                n >= *min && n <= *max
            }
            Self::NoEvent {
                after,
                scope_anchor,
                fields,
                within,
                blockers,
            } => {
                let Some(i) = a[*after] else { return false };
                let anchor = &es[i];
                let Some(resolved): Option<Vec<_>> = fields
                    .iter()
                    .map(|(f, r)| r.borrowed(a, es).map(|v| (f, v)))
                    .collect()
                else {
                    return false;
                };
                !blockers.iter().any(|j| {
                    let e = &es[*j];
                    let in_scope = scope_anchor.is_none_or(|(s, d)| {
                        a[s].is_some_and(|i| {
                            e.order_seq >= es[i].order_seq
                                && e.block_ts <= es[i].block_ts.saturating_add(d)
                        })
                    });
                    in_scope
                        && e.order_seq > anchor.order_seq
                        && e.block_ts <= anchor.block_ts.saturating_add(*within)
                        && resolved
                            .iter()
                            .all(|(f, v)| field_view(e, f).as_ref() == Some(v))
                })
            }
        }
    }
}
struct Branch {
    id: String,
    atoms: Vec<String>,
    domains: Vec<Vec<usize>>,
    predicates: Vec<Pred>,
}
fn field(s: usize, f: &str) -> Ref {
    Ref::Field(s, f.into())
}
fn compile(
    es: &[&CanonicalEvent],
    q: &QuerySpec,
    b: &BranchSpec,
    complete_blockers: Option<&BTreeSet<String>>,
    certified: Option<&BTreeMap<(String, String), Vec<usize>>>,
) -> Result<Branch> {
    ensure!(!b.atoms.is_empty(), "empty branch unsupported");
    let slots: BTreeMap<_, _> = b
        .atoms
        .iter()
        .enumerate()
        .map(|(i, a)| (a.id.clone(), i))
        .collect();
    ensure!(slots.len() == b.atoms.len(), "duplicate atom id");
    let slot = |s: &str| -> Result<usize> { slots.get(s).copied().context("unknown atom") };
    let mut vars = BTreeMap::<String, Ref>::new();
    let mut ps = vec![];
    let mut domains = vec![];
    for (i, a) in b.atoms.iter().enumerate() {
        for (f, t) in &a.fields {
            if let Term::Variable(v) = t {
                if let Some(first) = vars.get(v) {
                    ps.push(Pred::Compare(first.clone(), CompareOp::Eq, field(i, f)));
                } else {
                    vars.insert(v.clone(), field(i, f));
                }
            }
        }
                                                                                
                                                                              
        let candidates = if let Some(ds) = certified {
            let ids = ds
                .get(&(b.id.clone(), a.id.clone()))
                .context("missing certified atomic domain")?;
            ensure!(
                ids.windows(2).all(|w| w[0] < w[1]) && ids.iter().all(|i| *i < es.len()),
                "certified index domain framing"
            );
            ids.clone()
        } else {
            (0..es.len()).collect()
        };
        domains.push(
            candidates
                .into_iter()
                .filter(|&i| {
                    let e = es[i];
                    a.event_types.contains(&e.event_type)
                        && a.fields.iter().all(|(f, t)| {
                            field_view(e, f).is_some_and(|x| match t {
                                Term::Constant(v) => x == v.into(),
                                Term::Variable(_) => !matches!(x, ScalarView::Text("")),
                            })
                        })
                        && a.local_predicates.iter().all(|p| {
                            field_view(e, &p.field)
                                .is_some_and(|x| view_cmp(x, p.op, (&p.value).into()))
                        })
                })
                .collect::<Vec<_>>(),
        );
    }
    let expr = |v: &ValueExpr| -> Result<Ref> {
        Ok(match v {
            ValueExpr::Field(f) => field(slot(&f.atom)?, &f.field),
            ValueExpr::Constant(v) => Ref::Const(v.clone()),
            ValueExpr::Variable(v) => vars.get(v).cloned().context("unbound variable")?,
        })
    };
    let distinct = |atoms: &[String], f: &str| -> Result<Vec<Ref>> {
        atoms.iter().map(|a| Ok(field(slot(a)?, f))).collect()
    };
    for c in &b.constraints {
        match c {
            Constraint::Compare { left, op, right } => {
                ps.push(Pred::Compare(expr(left)?, *op, expr(right)?))
            }
            Constraint::Order {
                earlier,
                later,
                field: f,
            } => ps.push(Pred::Compare(
                field(slot(earlier)?, f),
                CompareOp::Lt,
                field(slot(later)?, f),
            )),
            Constraint::Within {
                earlier,
                later,
                delta_seconds,
            } => {
                let (i, j) = (slot(earlier)?, slot(later)?);
                ps.push(Pred::Compare(
                    field(i, "order_seq"),
                    CompareOp::Lt,
                    field(j, "order_seq"),
                ));
                ps.push(Pred::Gap {
                    early: i,
                    late: j,
                    min: None,
                    max: *delta_seconds,
                });
            }
            Constraint::DistinctEvents(a) => {
                let v = distinct(a, "event_id")?;
                ps.push(Pred::Distinct(v, a.len(), a.len(), false));
            }
            Constraint::DistinctTransactions(a) => {
                let v = distinct(a, "tx_hash")?;
                for r in &v {
                    ps.push(Pred::Compare(
                        r.clone(),
                        CompareOp::Ne,
                        Ref::Const(Scalar::Text(String::new())),
                    ));
                }
                ps.push(Pred::Distinct(v, a.len(), a.len(), false));
            }
            Constraint::DistinctValues {
                values,
                min_count,
                max_count,
                ignore_empty_text,
            } => ps.push(Pred::Distinct(
                values.iter().map(expr).collect::<Result<_>>()?,
                *min_count,
                *max_count,
                *ignore_empty_text,
            )),
            Constraint::NoEvent {
                after,
                event_types,
                fields,
                within_seconds,
            } => {
                let complete=complete_blockers.context("NoEvent requires complete authenticated blocker envelope; positive domain is insufficient")?;
                ensure!(
                    event_types.is_subset(complete),
                    "missing complete blocker type"
                );
                ensure!(*within_seconds >= 0, "negative NoEvent window");
                let types = if event_types.is_empty() {
                    complete.clone()
                } else {
                    event_types.clone()
                };
                let scope_anchor = match &q.scope {
                    ScopeSpec::Episode {
                        anchor_atom,
                        window_seconds,
                    } => Some((slot(anchor_atom)?, *window_seconds)),
                    _ => None,
                };
                ps.push(Pred::NoEvent {
                    after: slot(after)?,
                    scope_anchor,
                    fields: fields
                        .iter()
                        .map(|(f, v)| Ok((f.clone(), expr(v)?)))
                        .collect::<Result<_>>()?,
                    within: *within_seconds,
                    blockers: es
                        .iter()
                        .enumerate()
                        .filter(|(_, e)| types.contains(&e.event_type))
                        .map(|(i, _)| i)
                        .collect(),
                });
            }
        }
    }
    if b.event_injective {
        for i in 0..b.atoms.len() {
            for j in i + 1..b.atoms.len() {
                ps.push(Pred::Compare(
                    field(i, "event_id"),
                    CompareOp::Ne,
                    field(j, "event_id"),
                ));
            }
        }
    }
    match &q.scope {
        ScopeSpec::Episode {
            anchor_atom,
            window_seconds,
        } => {
            ensure!(*window_seconds >= 0, "negative Episode window");
            let i = slot(anchor_atom)?;
            for j in 0..b.atoms.len() {
                if i != j {
                    ps.push(Pred::Compare(
                        field(i, "order_seq"),
                        CompareOp::Le,
                        field(j, "order_seq"),
                    ));
                    ps.push(Pred::Gap {
                        early: i,
                        late: j,
                        min: Some(0),
                        max: *window_seconds,
                    });
                }
            }
        }
        ScopeSpec::SameTx => {
            for j in 1..b.atoms.len() {
                ps.push(Pred::Compare(
                    field(0, "tx_hash"),
                    CompareOp::Eq,
                    field(j, "tx_hash"),
                ));
            }
        }
        ScopeSpec::ResidualClosure => {
            bail!("global count requires explicit SameTx/Episode semantics")
        }
    }
                                                                                
                                                                                      
    if es.iter().any(|e| e.value("chain_id").is_some()) {
        ensure!(
            es.iter().all(|e| e.value("chain_id").is_some()),
            "partial chain metadata"
        );
        for j in 1..b.atoms.len() {
            ps.push(Pred::Compare(
                field(0, "chain_id"),
                CompareOp::Eq,
                field(j, "chain_id"),
            ));
        }
    }
                                                                         
                                                                       
    ps = ps
        .into_iter()
        .flat_map(|p| match p {
            Pred::Distinct(v, lo, hi, false) if v.len() >= 2 && lo == v.len() && hi >= v.len() => {
                let mut out = vec![];
                for i in 0..v.len() {
                    for j in i + 1..v.len() {
                        out.push(Pred::Compare(v[i].clone(), CompareOp::Ne, v[j].clone()));
                    }
                }
                out
            }
            p => vec![p],
        })
        .collect();
                                                                             
    let deps = ps.iter().map(Pred::deps).collect::<Vec<_>>();
    for s in 0..domains.len() {
        let unary = deps
            .iter()
            .enumerate()
            .filter(|(_, d)| d.len() == 1 && d.contains(&s))
            .map(|(i, _)| i)
            .collect::<Vec<_>>();
        if unary.is_empty() {
            continue;
        }
        let mut a = vec![None; b.atoms.len()];
        domains[s].retain(|i| {
            a[s] = Some(*i);
            unary.iter().all(|&j| ps[j].holds(&a, es))
        });
    }
    ps = ps
        .into_iter()
        .zip(deps)
        .filter(|(_, d)| d.len() != 1)
        .map(|(p, _)| p)
        .collect();
    Ok(Branch {
        id: b.id.clone(),
        atoms: b.atoms.iter().map(|a| a.id.clone()).collect(),
        domains,
        predicates: ps,
    })
}
#[derive(Serialize, Default)]
pub struct CountStats {
    pub branches: usize,
    pub steps: Vec<StepStats>,
    pub count: String,
    pub full_answer_materialized: bool,
}
#[derive(Serialize, Default)]
pub struct StepStats {
    branch: String,
    eliminated: String,
    boundary: Vec<String>,
    rule: String,
    input_rows: usize,
    output_rows: usize,
    probes: u64,
    range_requests: u64,
}
struct Factor {
    vars: Vec<usize>,
    rows: BTreeMap<Vec<usize>, BigUint>,
    unit: Option<Vec<usize>>,
}
impl Factor {
    fn len(&self) -> usize {
        self.unit.as_ref().map_or(self.rows.len(), Vec::len)
    }
    fn contains_unary(&self, i: usize) -> bool {
        self.unit.as_ref().map_or_else(
            || self.rows.contains_key(&[i][..]),
            |d| d.binary_search(&i).is_ok(),
        )
    }
    fn unary_weight(&self, i: usize) -> BigUint {
        if self.unit.is_some() {
            BigUint::one()
        } else {
            self.rows[&[i][..]].clone()
        }
    }
}

pub struct ClientCount<'a> {
    events: Vec<&'a CanonicalEvent>,
    branches: Vec<Branch>,
}
impl<'a> ClientCount<'a> {
    pub fn new(events: &'a [CanonicalEvent], q: &QuerySpec) -> Result<Self> {
        Self::new_inner(events, q, None)
    }
                                                                                           
    pub fn with_authenticated_blockers(
        events: &'a [CanonicalEvent],
        q: &QuerySpec,
        types: &BTreeSet<String>,
    ) -> Result<Self> {
        Self::new_inner(events, q, Some(types))
    }
    fn new_inner(
        events: &'a [CanonicalEvent],
        q: &QuerySpec,
        types: Option<&BTreeSet<String>>,
    ) -> Result<Self> {
        Self::from_refs(events.iter().collect(), q, types, None)
    }
                                                                          
                                                                               
                                                                         
    pub fn from_authenticated_refs(
        events: Vec<&'a CanonicalEvent>,
        q: &QuerySpec,
        types: &BTreeSet<String>,
        domains: &BTreeMap<(String, String), Vec<usize>>,
    ) -> Result<Self> {
        Self::from_refs(events, q, Some(types), Some(domains))
    }
    fn from_refs(
        events: Vec<&'a CanonicalEvent>,
        q: &QuerySpec,
        types: Option<&BTreeSet<String>>,
        domains: Option<&BTreeMap<(String, String), Vec<usize>>>,
    ) -> Result<Self> {
        ensure!(
            q.result_semantics == "full_event_binding_v1",
            "unsupported identity semantics"
        );
        ensure!(
            events
                .iter()
                .map(|e| e.event_id.as_str())
                .collect::<HashSet<_>>()
                .len()
                == events.len(),
            "duplicate input identity"
        );
        ensure!(
            q.branches
                .iter()
                .map(|b| &b.id)
                .collect::<BTreeSet<_>>()
                .len()
                == q.branches.len(),
            "duplicate branch identity"
        );
        let branches = q
            .branches
            .iter()
            .map(|b| compile(&events, q, b, types, domains))
            .collect::<Result<_>>()?;
        Ok(Self { events, branches })
    }
                                                                                 
    pub fn restrict_authenticated_domains(
        &mut self,
        domains: &BTreeMap<(String, String), BTreeSet<String>>,
    ) -> Result<()> {
        for b in &mut self.branches {
            for (s, a) in b.atoms.iter().enumerate() {
                let ids = domains
                    .get(&(b.id.clone(), a.clone()))
                    .context("missing certified atomic domain")?;
                b.domains[s].retain(|i| ids.contains(&self.events[*i].event_id));
            }
        }
        Ok(())
    }
                                                                                 
                                                                                  
    pub fn restrict_authenticated_index_domains(
        &mut self,
        domains: &BTreeMap<(String, String), Vec<usize>>,
    ) -> Result<()> {
        for b in &mut self.branches {
            for (s, a) in b.atoms.iter().enumerate() {
                let ids = domains
                    .get(&(b.id.clone(), a.clone()))
                    .context("missing certified atomic domain")?;
                ensure!(
                    ids.windows(2).all(|w| w[0] < w[1])
                        && ids.iter().all(|i| *i < self.events.len()),
                    "certified index domain framing"
                );
                let mut j = 0;
                b.domains[s].retain(|i| {
                    while j < ids.len() && ids[j] < *i {
                        j += 1;
                    }
                    j < ids.len() && ids[j] == *i
                });
            }
        }
        Ok(())
    }
                                                                                 
    pub fn check_binding(&self, branch: &str, ids: &[usize]) -> Result<()> {
        let b = self
            .branches
            .iter()
            .find(|b| b.id == branch)
            .context("unknown answer branch")?;
        ensure!(
            ids.len() == b.atoms.len(),
            "missing/extra binding positions"
        );
        ensure!(
            ids.iter()
                .enumerate()
                .all(|(s, i)| b.domains[s].binary_search(i).is_ok()),
            "binding outside atomic domain"
        );
        let a = ids.iter().map(|i| Some(*i)).collect();
        ensure!(
            b.predicates.iter().all(|p| p.holds(&a, &self.events)),
            "binding violates QuerySpec"
        );
        Ok(())
    }
                                                                                
                                                           
    pub fn enumerate_indexed(&self, limit: u64) -> Result<Vec<(String, Vec<usize>)>> {
        let mut output = vec![];
        let mut probes = 0;
        for b in &self.branches {
            let vars = (0..b.atoms.len()).collect::<Vec<_>>();
            let access = search_access(&self.events, b, &vars, &[]);
            let mut row = vec![None; b.atoms.len()];
                                                                              
                                                                          
            let mut ready = vec![vec![]; vars.len()];
            for (i, p) in b.predicates.iter().enumerate() {
                ready[p.deps().last().copied().unwrap_or(0)].push(i);
            }
            fn visit(
                pos: usize,
                vars: &[usize],
                access: &[SearchAccess],
                ready: &[Vec<usize>],
                row: &mut Assignment,
                es: &[&CanonicalEvent],
                b: &Branch,
                output: &mut Vec<(String, Vec<usize>)>,
                probes: &mut u64,
                limit: u64,
            ) -> Result<()> {
                if pos == vars.len() {
                    output.push((b.id.clone(), row.iter().map(|i| i.unwrap()).collect()));
                    return Ok(());
                }
                let s = vars[pos];
                for &i in access[pos].candidates(row, es, &b.predicates).iter() {
                    *probes += 1;
                    ensure!(
                        *probes <= limit,
                        "indexed enumeration resource limit; no partial answers"
                    );
                    if !access[pos].factor_accepts(row, i) {
                        continue;
                    }
                    row[s] = Some(i);
                    if ready[pos].iter().all(|&j| b.predicates[j].holds(row, es)) {
                        visit(
                            pos + 1,
                            vars,
                            access,
                            ready,
                            row,
                            es,
                            b,
                            output,
                            probes,
                            limit,
                        )?;
                    }
                    row[s] = None;
                }
                Ok(())
            }
            visit(
                0,
                &vars,
                &access,
                &ready,
                &mut row,
                &self.events,
                b,
                &mut output,
                &mut probes,
                limit,
            )?;
        }
        Ok(output)
    }
    pub fn count(&self, probe_limit: u64) -> Result<(BigUint, CountStats)> {
                                                                           
                                                                                
                                                                              
        self.count_policy(
            probe_limit,
            true,
            std::env::var("FQ_MULTIBOUNDARY_COUNT").as_deref() == Ok("1"),
        )
    }
    fn count_impl(&self, probe_limit: u64, allow_range: bool) -> Result<(BigUint, CountStats)> {
        self.count_policy(probe_limit, allow_range, allow_range)
    }
    fn count_policy(
        &self,
        probe_limit: u64,
        allow_range: bool,
        allow_multi: bool,
    ) -> Result<(BigUint, CountStats)> {
        let mut total = BigUint::zero();
        let mut stats = CountStats {
            branches: self.branches.len(),
            ..Default::default()
        };
        for b in &self.branches {
            let mut factors: Vec<_> = b
                .domains
                .iter()
                .enumerate()
                .map(|(s, d)| Factor {
                    vars: vec![s],
                    rows: BTreeMap::new(),
                    unit: Some(d.clone()),
                })
                .collect();
            let mut pending = b.predicates.clone();
            let mut todo: BTreeSet<_> = (0..b.atoms.len()).collect();
            while !todo.is_empty() {
                let deps = |x: usize| -> BTreeSet<usize> {
                    factors
                        .iter()
                        .filter(|f| f.vars.contains(&x))
                        .flat_map(|f| f.vars.iter().copied())
                        .chain(
                            pending
                                .iter()
                                .filter(|p| p.deps().contains(&x))
                                .flat_map(Pred::deps),
                        )
                        .filter(|s| *s != x)
                        .collect()
                };
                                                                                 
                let x = *todo.iter().min_by_key(|x| (deps(**x).len(), **x)).unwrap();
                let boundary: Vec<_> = deps(x).into_iter().collect();
                let mut inputs = vec![];
                let mut rest = vec![];
                for f in factors {
                    if f.vars.contains(&x) {
                        inputs.push(f)
                    } else {
                        rest.push(f)
                    }
                }
                let mut incident = vec![];
                let mut left = vec![];
                for p in pending {
                    if p.deps().contains(&x) || p.deps().is_empty() {
                        incident.push(p)
                    } else {
                        left.push(p)
                    }
                }
                pending = left;
                let mut stat = StepStats {
                    branch: b.id.clone(),
                    eliminated: b.atoms[x].clone(),
                    boundary: boundary.iter().map(|s| b.atoms[*s].clone()).collect(),
                    input_rows: inputs.iter().map(Factor::len).sum(),
                    ..Default::default()
                };
                let rows = if boundary.is_empty() {
                    terminal_sum(
                        &self.events,
                        b,
                        x,
                        &incident,
                        &inputs,
                        probe_limit,
                        &mut stat,
                    )?
                } else if allow_range && inputs.len() == 1 && inputs[0].vars == [x] {
                    let simple = if boundary.len() == 1 {
                        range_eliminate(
                            &self.events,
                            b,
                            x,
                            boundary[0],
                            &incident,
                            &inputs[0],
                            &mut stat,
                        )?
                    } else {
                        None
                    };
                    if let Some(rows) = simple {
                        rows
                    } else if let Some(rows) = if allow_multi {
                        multi_range::eliminate(
                            &self.events,
                            b,
                            x,
                            &boundary,
                            &incident,
                            &inputs[0],
                            probe_limit,
                            &mut stat,
                        )?
                    } else {
                        None
                    } {
                        rows
                    } else {
                        fused_eliminate(
                            &self.events,
                            b,
                            x,
                            &boundary,
                            &incident,
                            &inputs,
                            probe_limit,
                            &mut stat,
                        )?
                    }
                } else {
                    fused_eliminate(
                        &self.events,
                        b,
                        x,
                        &boundary,
                        &incident,
                        &inputs,
                        probe_limit,
                        &mut stat,
                    )?
                };
                stat.output_rows = rows.len();
                stats.steps.push(stat);
                rest.push(Factor {
                    vars: boundary,
                    rows,
                    unit: None,
                });
                factors = rest;
                todo.remove(&x);
            }
            ensure!(
                pending.is_empty() && factors.iter().all(|f| f.vars.is_empty()),
                "incomplete elimination"
            );
            let mut n = BigUint::one();
            for f in factors {
                n *= f.rows.get(&vec![]).cloned().unwrap_or_default();
            }
            total += n;
        }
        stats.count = total.to_string();
        Ok((total, stats))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{AtomSpec, FieldRef};
    use crate::{evaluator::EvaluationRequest, index::QueryIndependentIndex, oracle};
    fn event(i: usize) -> CanonicalEvent {
        CanonicalEvent::new(
            format!("order:{i}"),
            "T",
            i as i64,
            (i / 2) as i64,
            format!("tx:{}", i / 3),
            BTreeMap::from([
                ("src".into(), Scalar::Int((i % 3) as i64)),
                ("dst".into(), Scalar::Int(((i + 1) % 3) as i64)),
                ("asset".into(), Scalar::Int((i % 2) as i64)),
            ]),
        )
    }
    fn atom(id: &str) -> AtomSpec {
        AtomSpec {
            id: id.into(),
            event_types: BTreeSet::from(["T".into()]),
            fields: BTreeMap::new(),
            local_predicates: vec![],
        }
    }
    fn q(n: usize) -> QuerySpec {
        QuerySpec {
            query_id: "unregistered".into(),
            result_semantics: "full_event_binding_v1".into(),
            scope: ScopeSpec::Episode {
                anchor_atom: "e0".into(),
                window_seconds: 5,
            },
            branches: vec![BranchSpec {
                id: "branch".into(),
                atoms: (0..n).map(|i| atom(&format!("e{i}"))).collect(),
                constraints: vec![],
                event_injective: true,
            }],
        }
    }
    fn brute(es: &[CanonicalEvent], q: &QuerySpec) -> usize {
        let ix = QueryIndependentIndex::build_plain(es.to_vec()).unwrap();
        match &q.scope {
            ScopeSpec::Episode { anchor_atom, .. } => es
                .iter()
                .map(|e| {
                    oracle::brute_force_query(
                        &ix,
                        q,
                        &EvaluationRequest {
                            allowed_event_ids: None,
                            fixed_atoms: BTreeMap::from([(
                                anchor_atom.clone(),
                                e.event_id.clone(),
                            )]),
                        },
                    )
                    .unwrap()
                    .len()
                })
                .sum(),
            _ => oracle::brute_force_query(&ix, q, &EvaluationRequest::default())
                .unwrap()
                .len(),
        }
    }
    #[test]
    fn general_branches_constraints_match_oracle() {
        let es: Vec<_> = (0..9).map(event).collect();
        for n in 2..=4 {
            for variant in 0..4 {
                let mut query = q(n);
                query.branches[0].constraints.push(Constraint::Order {
                    earlier: "e0".into(),
                    later: "e1".into(),
                    field: "block_ts".into(),
                });
                if variant >= 1 {
                    for a in &mut query.branches[0].atoms {
                        a.fields
                            .insert("asset".into(), Term::Variable("shared".into()));
                    }
                }
                if variant >= 2 {
                    query.branches[0]
                        .constraints
                        .push(Constraint::DistinctTransactions(vec![
                            "e0".into(),
                            "e1".into(),
                        ]));
                }
                if variant == 3 {
                    let values = query.branches[0]
                        .atoms
                        .iter()
                        .map(|a| {
                            ValueExpr::Field(FieldRef {
                                atom: a.id.clone(),
                                field: "src".into(),
                            })
                        })
                        .collect();
                    query.branches[0]
                        .constraints
                        .push(Constraint::DistinctValues {
                            values,
                            min_count: 2,
                            max_count: 3,
                            ignore_empty_text: false,
                        });
                }
                let client = ClientCount::new(&es, &query).unwrap();
                let (n, _) = client.count(2_000_000).unwrap();
                assert_eq!(n, BigUint::from(brute(&es, &query)));
                assert_eq!(n, client.count_impl(2_000_000, false).unwrap().0);
            }
        }
        let mut query = q(2);
        let mut second = query.branches[0].clone();
        second.id = "second".into();
        query.branches.push(second);
        assert_eq!(
            ClientCount::new(&es, &query)
                .unwrap()
                .count(100000)
                .unwrap()
                .0,
            BigUint::from(brute(&es, &query))
        );
        query.scope = ScopeSpec::SameTx;
        assert_eq!(
            ClientCount::new(&es, &query)
                .unwrap()
                .count(100000)
                .unwrap()
                .0,
            BigUint::from(brute(&es, &query))
        );
    }
    #[test]
    fn multi_boundary_ranges_joint_exclusions_and_nonmonotone_fallback() {
        for disorder in [false, true] {
            for distinct_tx in [false, true] {
                let mut es = (0..12).map(event).collect::<Vec<_>>();
                for (i, e) in es.iter_mut().enumerate() {
                    e.fields.insert("tag".into(), Scalar::Int((i % 3) as i64));
                }
                if disorder {
                    es[7].block_ts = -1;
                }
                let mut query = q(3);
                for a in &mut query.branches[0].atoms {
                    a.fields
                        .insert("asset".into(), Term::Variable("joint".into()));
                }
                for a in ["e1", "e2"] {
                    query.branches[0].constraints.push(Constraint::Order {
                        earlier: "e0".into(),
                        later: a.into(),
                        field: "block_ts".into(),
                    });
                    query.branches[0].constraints.push(Constraint::Compare {
                        left: ValueExpr::Field(FieldRef {
                            atom: "e0".into(),
                            field: "tag".into(),
                        }),
                        op: CompareOp::Ne,
                        right: ValueExpr::Field(FieldRef {
                            atom: a.into(),
                            field: "tag".into(),
                        }),
                    });
                }
                if distinct_tx {
                    query.branches[0]
                        .constraints
                        .push(Constraint::DistinctTransactions(vec![
                            "e0".into(),
                            "e1".into(),
                            "e2".into(),
                        ]));
                }
                let c = ClientCount::new(&es, &query).unwrap();
                let (n, s) = c.count_impl(1_000_000, true).unwrap();
                assert_eq!(n, c.count_impl(1_000_000, false).unwrap().0);
                assert_eq!(n, BigUint::from(brute(&es, &query)));
                if !disorder {
                    assert_eq!(s.steps[0].rule, "multi_boundary_joint_range_eliminate");
                }
            }
        }
    }
    #[test]
    fn range_retains_joint_time_order_and_same_tx() {
        for disorder in [false, true] {
            let mut es: Vec<_> = (0..18).map(event).collect();
            if disorder {
                es[7].block_ts = 0;
            }
            let mut query = q(2);
            query.branches[0].constraints.extend([
                Constraint::Order {
                    earlier: "e0".into(),
                    later: "e1".into(),
                    field: "block_ts".into(),
                },
                Constraint::DistinctTransactions(vec!["e0".into(), "e1".into()]),
            ]);
            let c = ClientCount::new(&es, &query).unwrap();
            let (n, s) = c.count(100000).unwrap();
            assert_eq!(n, BigUint::from(brute(&es, &query)));
            assert_eq!(n, c.count_impl(100000, false).unwrap().0);
            assert_eq!(
                s.steps[0].rule == "joint_key_weighted_range_eliminate",
                !disorder
            );
        }
    }
    #[test]
    fn big_integer_exceeds_u128_and_zero_domain() {
        let mut es: Vec<_> = (0..2).map(event).collect();
        for e in &mut es {
            e.tx_hash = "same".into();
        }
        let mut query = q(130);
        query.scope = ScopeSpec::SameTx;
        query.branches[0].event_injective = false;
        let n = ClientCount::new(&es, &query)
            .unwrap()
            .count(100000)
            .unwrap()
            .0;
        assert_eq!(n, BigUint::one() << 130usize);
        assert!(ClientCount::new(&[], &query)
            .unwrap()
            .count(100000)
            .unwrap()
            .0
            .is_zero());
    }
    #[test]
    fn fused_weights_beyond_u128_match_integer_semantics() {
        let events = (0..3).map(event).collect::<Vec<_>>();
        let es = events.iter().collect::<Vec<_>>();
        let b = Branch {
            id: "arbitrary-large-multiplicity".into(),
            atoms: vec!["x".into(), "y".into()],
            domains: vec![vec![0, 1, 2], vec![0, 1, 2]],
            predicates: vec![],
        };
        let huge = BigUint::one() << 150usize;
        let inputs = vec![
            Factor {
                vars: vec![0],
                rows: BTreeMap::from([
                    (vec![0], huge.clone()),
                    (vec![1], huge.clone()),
                    (vec![2], BigUint::one()),
                ]),
                unit: None,
            },
            Factor {
                vars: vec![0, 1],
                rows: (0..3)
                    .flat_map(|i| {
                        (0..3).map(move |j| (vec![i, j], BigUint::from((i + j + 1) as u64)))
                    })
                    .collect(),
                unit: None,
            },
        ];
        let mut stats = StepStats::default();
        let got = fused_eliminate(&es, &b, 0, &[1], &[], &inputs, 1000, &mut stats).unwrap();
        for j in 0..3 {
            assert_eq!(
                got[&vec![j]],
                &huge * (2 * j + 3) + BigUint::from((j + 3) as u64)
            );
        }
        let weighted = Factor {
            vars: vec![1],
            rows: got,
            unit: None,
        };
        let got = terminal_sum(
            &es,
            &b,
            1,
            &[],
            &[weighted],
            1000,
            &mut StepStats::default(),
        )
        .unwrap();
        assert_eq!(got[&Vec::<usize>::new()], huge * 15u8 + 12u8);
    }
    #[test]
    fn fixed_binding_checks_and_negative_input_gate() {
        let es: Vec<_> = (0..8).map(event).collect();
        let mut query = q(2);
        let c = ClientCount::new(&es, &query).unwrap();
        assert!(c.check_binding("branch", &[0, 4]).is_ok());
        assert!(c.check_binding("branch", &[0, 0]).is_err());
        assert!(c.check_binding("branch", &[4, 0]).is_err());
        assert!(c.check_binding("wrong", &[0, 4]).is_err());
        query.branches[0].constraints.push(Constraint::NoEvent {
            after: "e0".into(),
            event_types: BTreeSet::from(["T".into()]),
            fields: BTreeMap::new(),
            within_seconds: 5,
        });
        assert!(ClientCount::new(&es, &query).is_err());
    }
    #[test]
    fn sparse_factor_lookup_drives_candidates() {
        let es: Vec<_> = (0..300).map(event).collect();
        let es = es.iter().collect::<Vec<_>>();
        let b = Branch {
            id: "regression".into(),
            atoms: vec!["x".into(), "y".into()],
            domains: vec![(0..300).collect(), (0..300).collect()],
            predicates: vec![],
        };
        let factor = Factor {
            vars: vec![0, 1],
            rows: (0..300).map(|i| (vec![i, i], BigUint::one())).collect(),
            unit: None,
        };
        let access = search_access(&es, &b, &[0, 1], &[factor]);
        let row = vec![Some(111), None];
        assert_eq!(access[1].candidates(&row, &es, &[]).as_ref(), &[111]);
    }
    #[test]
    fn borrowed_premasked_domains_match_legacy_and_keep_blockers() {
        let es = (0..8).map(event).collect::<Vec<_>>();
        let types = BTreeSet::from(["T".into()]);
        let mut query = q(2);
        for negative in [false, true] {
            if negative {
                query.branches[0].constraints.push(Constraint::NoEvent {
                    after: "e0".into(),
                    event_types: types.clone(),
                    fields: BTreeMap::from([("asset".into(), ValueExpr::Constant(Scalar::Int(1)))]),
                    within_seconds: 5,
                });
            }
            for mask in 0..256usize {
                let domains = BTreeMap::from([
                    (("branch".into(), "e0".into()), vec![0]),
                    (
                        ("branch".into(), "e1".into()),
                        (0..8).filter(|i| mask >> i & 1 == 1).collect(),
                    ),
                ]);
                let mut old =
                    ClientCount::with_authenticated_blockers(&es, &query, &types).unwrap();
                old.restrict_authenticated_index_domains(&domains).unwrap();
                let new = ClientCount::from_authenticated_refs(
                    es.iter().collect(),
                    &query,
                    &types,
                    &domains,
                )
                .unwrap();
                assert_eq!(
                    old.enumerate_indexed(10000).unwrap(),
                    new.enumerate_indexed(10000).unwrap()
                );
                assert_eq!(old.count(10000).unwrap().0, new.count(10000).unwrap().0);
                if negative {
                    assert!(new.count(10000).unwrap().0.is_zero());
                }
                for i in 0..8 {
                    assert_eq!(
                        old.check_binding("branch", &[0, i]).is_ok(),
                        new.check_binding("branch", &[0, i]).is_ok()
                    );
                }
            }
        }
        for invalid in [vec![1, 1], vec![2, 1], vec![8]] {
            let domains = BTreeMap::from([
                (("branch".into(), "e0".into()), vec![0]),
                (("branch".into(), "e1".into()), invalid),
            ]);
            assert!(ClientCount::from_authenticated_refs(
                es.iter().collect(),
                &query,
                &types,
                &domains
            )
            .is_err());
        }
    }
}
fn fused_eliminate(
    es: &[&CanonicalEvent],
    b: &Branch,
    x: usize,
    boundary: &[usize],
    ps: &[Pred],
    inputs: &[Factor],
    limit: u64,
    stat: &mut StepStats,
) -> Result<BTreeMap<Vec<usize>, BigUint>> {
    stat.rule = "fused_sparse_sum_product".into();
    let mut out = BTreeMap::<Vec<usize>, BigUint>::new();
    let mut vars = vec![x];
    vars.extend_from_slice(boundary);
    let access = search_access(es, b, &vars, inputs);
    let mut row = vec![None; b.atoms.len()];
    let positions = vars
        .iter()
        .enumerate()
        .map(|(i, &s)| (s, i))
        .collect::<BTreeMap<_, _>>();
    let mut ready = vec![vec![]; vars.len()];
    for (i, p) in ps.iter().enumerate() {
        let depth = p
            .deps()
            .iter()
            .map(|s| {
                positions
                    .get(s)
                    .copied()
                    .context("unresolved factor dependency")
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        ready[depth].push(i);
    }
                                                                               
                                                                                 
    let mut factor_key = vec![];
    let mut boundary_key = vec![0; boundary.len()];
    fn visit(
        pos: usize,
        vars: &[usize],
        access: &[SearchAccess],
        ready: &[Vec<usize>],
        row: &mut Assignment,
        es: &[&CanonicalEvent],
        b: &Branch,
        ps: &[Pred],
        inputs: &[Factor],
        boundary: &[usize],
        out: &mut BTreeMap<Vec<usize>, BigUint>,
        factor_key: &mut Vec<usize>,
        boundary_key: &mut Vec<usize>,
        limit: u64,
        stat: &mut StepStats,
    ) -> Result<()> {
        if pos == vars.len() {
            let mut weight = BigUint::one();
            for f in inputs.iter().filter(|f| f.unit.is_none()) {
                factor_key.clear();
                factor_key.extend(f.vars.iter().map(|s| row[*s].unwrap()));
                let Some(w) = f.rows.get(factor_key.as_slice()) else {
                    return Ok(());
                };
                weight *= w;
            }
            if !weight.is_zero() {
                for (v, s) in boundary_key.iter_mut().zip(boundary) {
                    *v = row[*s].unwrap();
                }
                if let Some(previous) = out.get_mut(boundary_key.as_slice()) {
                    *previous += weight;
                } else {
                    out.insert(boundary_key.clone(), weight);
                }
            }
            return Ok(());
        }
        let s = vars[pos];
        let mut acceptance_key = vec![];
        for &i in access[pos].candidates(row, es, ps).iter() {
            stat.probes += 1;
            ensure!(
                stat.probes <= limit,
                "exact factor resource limit; no partial count accepted"
            );
            if !access[pos].factor_accepts_with(row, i, &mut acceptance_key) {
                continue;
            }
            row[s] = Some(i);
            if ready[pos].iter().all(|&j| ps[j].holds(row, es)) {
                visit(
                    pos + 1,
                    vars,
                    access,
                    ready,
                    row,
                    es,
                    b,
                    ps,
                    inputs,
                    boundary,
                    out,
                    factor_key,
                    boundary_key,
                    limit,
                    stat,
                )?;
            }
            row[s] = None;
        }
        Ok(())
    }
    visit(
        0,
        &vars,
        &access,
        &ready,
        &mut row,
        es,
        b,
        ps,
        inputs,
        boundary,
        &mut out,
        &mut factor_key,
        &mut boundary_key,
        limit,
        stat,
    )?;
    Ok(out)
}

                                                                            
                                                                          
                                                    
fn terminal_sum(
    es: &[&CanonicalEvent],
    b: &Branch,
    x: usize,
    ps: &[Pred],
    inputs: &[Factor],
    limit: u64,
    stat: &mut StepStats,
) -> Result<BTreeMap<Vec<usize>, BigUint>> {
    ensure!(
        inputs.iter().all(|f| f.vars == [x]) && ps.iter().all(|p| p.deps().iter().all(|s| *s == x)),
        "terminal factor boundary"
    );
    stat.rule = "terminal_sparse_weighted_sum".into();
    let sparse = inputs
        .iter()
        .filter(|f| f.unit.is_none())
        .min_by_key(|f| f.rows.len());
    let ids: Vec<_> = if let Some(f) = sparse {
        f.rows.keys().map(|k| k[0]).collect()
    } else {
        b.domains[x].clone()
    };
    let mut assignment = vec![None; b.atoms.len()];
    let mut sum = BigUint::zero();
    for i in ids {
        stat.probes += 1;
        ensure!(
            stat.probes <= limit,
            "exact factor resource limit; no partial count accepted"
        );
        if b.domains[x].binary_search(&i).is_err() || !inputs.iter().all(|f| f.contains_unary(i)) {
            continue;
        }
        assignment[x] = Some(i);
        if ps.iter().all(|p| p.holds(&assignment, es)) {
            let mut w = BigUint::one();
            for f in inputs.iter().filter(|f| f.unit.is_none()) {
                w *= &f.rows[&[i][..]];
            }
            sum += w;
        }
    }
    Ok(if sum.is_zero() {
        BTreeMap::new()
    } else {
        BTreeMap::from([(vec![], sum)])
    })
}

                                                                                 
                                                             
struct SearchAccess<'a> {
    slot: usize,
    refs: Vec<Ref>,
    groups: HashMap<Vec<ScalarView<'a>>, Vec<usize>>,
    all: Vec<usize>,
    factors: Vec<(Vec<usize>, BTreeMap<Vec<usize>, BTreeSet<usize>>)>,
    monotone: bool,
}
fn search_access<'a>(
    es: &[&'a CanonicalEvent],
    b: &Branch,
    vars: &[usize],
    inputs: &[Factor],
) -> Vec<SearchAccess<'a>> {
    let mut bound = BTreeSet::new();
    let mut access = vec![];
    for &s in vars {
        let mut fields = vec![];
        let mut refs = vec![];
        for p in &b.predicates {
            if let Pred::Compare(Ref::Field(i, f), CompareOp::Eq, Ref::Field(j, g)) = p {
                if *i == s && bound.contains(j) {
                    fields.push(f.clone());
                    refs.push(Ref::Field(*j, g.clone()));
                } else if *j == s && bound.contains(i) {
                    fields.push(g.clone());
                    refs.push(Ref::Field(*i, f.clone()));
                }
            }
        }
        let mut groups = HashMap::<Vec<ScalarView<'a>>, Vec<usize>>::new();
        let mut support: Option<BTreeSet<usize>> = None;
        let mut factors = vec![];
        for f in inputs.iter().filter(|f| f.vars.contains(&s)) {
            if f.unit.is_some() {
                continue;
            }
            let at = f.vars.iter().position(|v| *v == s).unwrap();
            let previous = f
                .vars
                .iter()
                .copied()
                .filter(|v| bound.contains(v))
                .collect::<Vec<_>>();
            let mut map = BTreeMap::<Vec<usize>, BTreeSet<usize>>::new();
            let mut possible = BTreeSet::new();
            for k in f.rows.keys() {
                let key = previous
                    .iter()
                    .map(|v| k[f.vars.iter().position(|x| x == v).unwrap()])
                    .collect();
                map.entry(key).or_default().insert(k[at]);
                possible.insert(k[at]);
            }
            if let Some(old) = &mut support {
                old.retain(|i| possible.contains(i));
            } else {
                support = Some(possible);
            }
            factors.push((previous, map));
        }
        let mut all: Vec<_> = support.map_or_else(
            || b.domains[s].clone(),
            |ids| {
                ids.into_iter()
                    .filter(|i| b.domains[s].binary_search(i).is_ok())
                    .collect()
            },
        );
        for f in inputs.iter().filter(|f| f.vars.contains(&s)) {
            if let Some(domain) = &f.unit {
                all.retain(|i| domain.binary_search(i).is_ok());
            }
        }
        all.sort_by_key(|i| (es[*i].order_seq, *i));
        let monotone = all
            .windows(2)
            .all(|w| es[w[0]].block_ts <= es[w[1]].block_ts);
        if !fields.is_empty() {
            for &i in &all {
                if let Some(key) = fields
                    .iter()
                    .map(|f| field_view(es[i], f))
                    .collect::<Option<Vec<_>>>()
                {
                    groups.entry(key).or_default().push(i);
                }
            }
        }
        access.push(SearchAccess {
            slot: s,
            refs,
            groups,
            all,
            factors,
            monotone,
        });
        bound.insert(s);
    }
    access
}
impl SearchAccess<'_> {
    fn factor_accepts(&self, row: &Assignment, i: usize) -> bool {
        self.factor_accepts_with(row, i, &mut vec![])
    }
    fn factor_accepts_with(&self, row: &Assignment, i: usize, key: &mut Vec<usize>) -> bool {
        self.factors.iter().all(|(prev, map)| {
            key.clear();
            key.extend(prev.iter().map(|s| row[*s].unwrap()));
            map.get(key.as_slice()).is_some_and(|v| v.contains(&i))
        })
    }
    fn candidates<'a>(
        &'a self,
        row: &Assignment,
        es: &[&'a CanonicalEvent],
        ps: &[Pred],
    ) -> std::borrow::Cow<'a, [usize]> {
        let ids = if self.refs.is_empty() {
            &self.all
        } else {
            let Some(key) = self
                .refs
                .iter()
                .map(|r| r.borrowed(row, es))
                .collect::<Option<Vec<_>>>()
            else {
                return std::borrow::Cow::Borrowed(&[]);
            };
            let Some(ids) = self.groups.get(&key) else {
                return std::borrow::Cow::Borrowed(&[]);
            };
            ids
        };
        let (mut lo, mut hi, mut tl, mut th) = (i128::MIN, i128::MAX, i128::MIN, i128::MAX);
        for p in ps {
            match p {
                Pred::Gap {
                    early,
                    late,
                    min,
                    max,
                } if *late == self.slot && row[*early].is_some() => {
                    let t = es[row[*early].unwrap()].block_ts as i128;
                    th = th.min(t + *max as i128);
                    if let Some(m) = min {
                        tl = tl.max(t + *m as i128);
                    }
                }
                Pred::Gap {
                    early,
                    late,
                    min,
                    max,
                } if *early == self.slot && row[*late].is_some() => {
                    let t = es[row[*late].unwrap()].block_ts as i128;
                    tl = tl.max(t - *max as i128);
                    if let Some(m) = min {
                        th = th.min(t - *m as i128);
                    }
                }
                Pred::Compare(Ref::Field(i, f), op, Ref::Field(j, g))
                    if i != j && f == g && (f == "order_seq" || f == "block_ts") =>
                {
                    let (op, v) = if *i == self.slot && row[*j].is_some() {
                        (*op, es[row[*j].unwrap()].value(g))
                    } else if *j == self.slot && row[*i].is_some() {
                        (
                            match op {
                                CompareOp::Lt => CompareOp::Gt,
                                CompareOp::Le => CompareOp::Ge,
                                CompareOp::Gt => CompareOp::Lt,
                                CompareOp::Ge => CompareOp::Le,
                                o => *o,
                            },
                            es[row[*i].unwrap()].value(f),
                        )
                    } else {
                        continue;
                    };
                    if let Some(Scalar::Int(v)) = v {
                        let (l, h) = if f == "order_seq" {
                            (&mut lo, &mut hi)
                        } else {
                            (&mut tl, &mut th)
                        };
                        let v = v as i128;
                        match op {
                            CompareOp::Lt => *h = (*h).min(v - 1),
                            CompareOp::Le => *h = (*h).min(v),
                            CompareOp::Gt => *l = (*l).max(v + 1),
                            CompareOp::Ge => *l = (*l).max(v),
                            CompareOp::Eq => {
                                *l = (*l).max(v);
                                *h = (*h).min(v);
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        let mut a = ids.partition_point(|i| (es[*i].order_seq as i128) < lo);
        let mut z = ids.partition_point(|i| (es[*i].order_seq as i128) <= hi);
        if self.monotone {
            a = a.max(ids.partition_point(|i| (es[*i].block_ts as i128) < tl));
            z = z.min(ids.partition_point(|i| (es[*i].block_ts as i128) <= th));
        }
        if a >= z {
            return std::borrow::Cow::Borrowed(&[]);
        }
        let base = &ids[a..z];
        let mut smallest = None;
        for (prev, map) in &self.factors {
            let key = prev.iter().map(|s| row[*s].unwrap()).collect::<Vec<_>>();
            let Some(bucket) = map.get(&key) else {
                return std::borrow::Cow::Borrowed(&[]);
            };
            if bucket.len() < base.len()
                && smallest.is_none_or(|b: &BTreeSet<usize>| bucket.len() < b.len())
            {
                smallest = Some(bucket);
            }
        }
        if let Some(bucket) = smallest {
            std::borrow::Cow::Owned(
                bucket
                    .iter()
                    .copied()
                    .filter(|i| {
                        base.binary_search_by_key(&(es[*i].order_seq, *i), |j| {
                            (es[*j].order_seq, *j)
                        })
                        .is_ok()
                    })
                    .collect(),
            )
        } else {
            std::borrow::Cow::Borrowed(base)
        }
    }
}

struct RangeGroup {
    ids: Vec<usize>,
    prefix: Option<Vec<BigUint>>,
}
impl RangeGroup {
    fn build(mut ids: Vec<usize>, es: &[&CanonicalEvent], weights: &Factor) -> Self {
        ids.sort_by_key(|i| es[*i].order_seq);
        let prefix = if weights.unit.is_some() {
            None
        } else {
            let mut p = vec![BigUint::zero()];
            for i in &ids {
                p.push(p.last().unwrap() + &weights.rows[&[*i][..]]);
            }
            Some(p)
        };
        Self { ids, prefix }
    }
    fn sum(&self, es: &[&CanonicalEvent], lo: i128, hi: i128, tlo: i128, thi: i128) -> BigUint {
        let a = self
            .ids
            .partition_point(|i| (es[*i].order_seq as i128) < lo)
            .max(
                self.ids
                    .partition_point(|i| (es[*i].block_ts as i128) < tlo),
            );
        let z = self
            .ids
            .partition_point(|i| (es[*i].order_seq as i128) <= hi)
            .min(
                self.ids
                    .partition_point(|i| (es[*i].block_ts as i128) <= thi),
            );
        if a >= z {
            BigUint::zero()
        } else {
            self.prefix
                .as_ref()
                .map_or_else(|| BigUint::from(z - a), |p| &p[z] - &p[a])
        }
    }
}
                                                                          
                                                                             
fn range_eliminate(
    es: &[&CanonicalEvent],
    b: &Branch,
    x: usize,
    y: usize,
    ps: &[Pred],
    weights: &Factor,
    stat: &mut StepStats,
) -> Result<Option<BTreeMap<Vec<usize>, BigUint>>> {
    let mut eq = vec![];
    let mut inequalities = vec![];
    let mut gaps = vec![];
    let mut tx_ne = false;
    let mut id_ne = false;
    for p in ps {
        match p {
            Pred::Compare(Ref::Field(i, f), op, Ref::Field(j, g))
                if (*i == x && *j == y) || (*i == y && *j == x) =>
            {
                let (xf, yf, op) = if *i == x {
                    (f.clone(), g.clone(), *op)
                } else {
                    (
                        g.clone(),
                        f.clone(),
                        match op {
                            CompareOp::Lt => CompareOp::Gt,
                            CompareOp::Le => CompareOp::Ge,
                            CompareOp::Gt => CompareOp::Lt,
                            CompareOp::Ge => CompareOp::Le,
                            v => *v,
                        },
                    )
                };
                match op {
                    CompareOp::Eq => eq.push((xf, yf)),
                    CompareOp::Ne if xf == "tx_hash" && yf == "tx_hash" => tx_ne = true,
                    CompareOp::Ne if xf == "event_id" && yf == "event_id" => id_ne = true,
                    CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
                        if xf == yf && (xf == "order_seq" || xf == "block_ts") =>
                    {
                        inequalities.push((xf, op))
                    }
                    _ => return Ok(None),
                }
            }
            Pred::Gap {
                early,
                late,
                min,
                max,
            } if (*early == x && *late == y) || (*early == y && *late == x) => {
                gaps.push((*early == x, *min, *max))
            }
            _ => return Ok(None),
        }
    }
    let key = |i: usize, left: bool| -> Option<Vec<ScalarView<'_>>> {
        eq.iter()
            .map(|(l, r)| field_view(es[i], if left { l } else { r }))
            .collect()
    };
    let mut ids = BTreeMap::<Vec<ScalarView<'_>>, Vec<usize>>::new();
    for &i in &b.domains[x] {
        if weights.contains_unary(i) {
            let Some(k) = key(i, true) else {
                return Ok(None);
            };
            ids.entry(k).or_default().push(i);
        }
    }
    let mut groups = BTreeMap::new();
    let mut txgroups = BTreeMap::new();
    for (k, ids) in ids {
        let group = RangeGroup::build(ids, es, weights);
        if !group
            .ids
            .windows(2)
            .all(|w| es[w[0]].block_ts <= es[w[1]].block_ts)
        {
            return Ok(None);
        }
        if tx_ne {
            let mut tx = BTreeMap::<&str, Vec<usize>>::new();
            for i in &group.ids {
                tx.entry(es[*i].tx_hash.as_str()).or_default().push(*i);
            }
            for (t, ids) in tx {
                txgroups.insert((k.clone(), t), RangeGroup::build(ids, es, weights));
            }
        }
        groups.insert(k, group);
    }
    stat.rule = "joint_key_weighted_range_eliminate".into();
    let mut out = BTreeMap::new();
    for &j in &b.domains[y] {
        let Some(k) = key(j, false) else {
            return Ok(None);
        };
        let Some(g) = groups.get(&k) else { continue };
        let (mut lo, mut hi, mut tlo, mut thi) = (i128::MIN, i128::MAX, i128::MIN, i128::MAX);
        for (f, op) in &inequalities {
            let v = if f == "order_seq" {
                es[j].order_seq as i128
            } else {
                es[j].block_ts as i128
            };
            let (l, h) = if f == "order_seq" {
                (&mut lo, &mut hi)
            } else {
                (&mut tlo, &mut thi)
            };
            match op {
                CompareOp::Lt => *h = (*h).min(v - 1),
                CompareOp::Le => *h = (*h).min(v),
                CompareOp::Gt => *l = (*l).max(v + 1),
                CompareOp::Ge => *l = (*l).max(v),
                _ => unreachable!(),
            }
        }
        for (early, min, max) in &gaps {
            let t = es[j].block_ts as i128;
            if *early {
                tlo = tlo.max(t - *max as i128);
                if let Some(m) = min {
                    thi = thi.min(t - *m as i128)
                }
            } else {
                thi = thi.min(t + *max as i128);
                if let Some(m) = min {
                    tlo = tlo.max(t + *m as i128)
                }
            }
        }
        let mut n = g.sum(es, lo, hi, tlo, thi);
        stat.range_requests += 1;
        if tx_ne {
            if let Some(t) = txgroups.get(&(k, es[j].tx_hash.as_str())) {
                let bad = t.sum(es, lo, hi, tlo, thi);
                ensure!(bad <= n, "invalid sameTx subtraction");
                n -= bad;
                stat.range_requests += 1;
            }
        }
                                                                           
        if id_ne
            && !tx_ne
            && g.ids.contains(&j)
            && (es[j].order_seq as i128) >= lo
            && (es[j].order_seq as i128) <= hi
            && (es[j].block_ts as i128) >= tlo
            && (es[j].block_ts as i128) <= thi
        {
            let bad = weights.unary_weight(j);
            ensure!(n >= bad, "invalid identity exclusion");
            n -= bad;
        }
        if !n.is_zero() {
            out.insert(vec![j], n);
        }
    }
    Ok(Some(out))
}
