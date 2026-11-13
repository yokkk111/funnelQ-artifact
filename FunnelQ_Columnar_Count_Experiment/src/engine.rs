                                                                            
                                                                     
use crate::{
    columns::{Answer, Row, RowAnswer, RowRef},
    key_projection::{Projection, ProjectionCache},
    model::Scalar,
    query::*,
};
use anyhow::{bail, ensure, Context, Result};
use num_bigint::BigUint;
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
type Assignment = Vec<Option<usize>>;

                                                                             
                                                                             
#[derive(Default)]
struct ExactCount {
    small: u64,
    big: Option<BigUint>,
}
impl ExactCount {
    fn add_u64(&mut self, value: u64) {
        if let Some(big) = &mut self.big {
            *big += value;
        } else if let Some(sum) = self.small.checked_add(value) {
            self.small = sum;
        } else {
            self.big = Some(BigUint::from(self.small) + BigUint::from(value));
        }
    }
    fn add_big(&mut self, value: BigUint) {
        *self.big.get_or_insert_with(|| BigUint::from(self.small)) += value;
    }
    fn add_usize(&mut self, value: usize) {
        if let Ok(value) = u64::try_from(value) { self.add_u64(value); }
        else { self.add_big(BigUint::from(value)); }
    }
    fn add_product(&mut self, factor: Self, n: usize) {
        if n == 0 { return; }
        if factor.big.is_none() {
            if let Ok(n) = u64::try_from(n) {
                if let Some(value) = factor.small.checked_mul(n) {
                    self.add_u64(value);
                    return;
                }
            }
        }
        self.add_big(factor.into_big() * BigUint::from(n));
    }
    fn into_big(self) -> BigUint { self.big.unwrap_or_else(|| BigUint::from(self.small)) }
}

#[cfg(test)]
mod exact_count_tests {
    use super::*;
    #[test]
    fn promotion_preserves_exact_addition_and_products() {
        let mut sum = ExactCount::default();
        sum.add_u64(u64::MAX);
        assert!(sum.big.is_none());
        sum.add_u64(1);
        assert_eq!(sum.into_big(), BigUint::from(u64::MAX)+1u32);
        let mut sum = ExactCount::default();
        let mut m = ExactCount::default();
        m.add_u64(u64::MAX);
        sum.add_product(m, 3);
        assert_eq!(sum.into_big(), BigUint::from(u64::MAX)*3u32);
        let mut sum = ExactCount::default();
        let mut m = ExactCount::default();
        m.add_u64(u64::MAX); m.add_u64(7);
        sum.add_product(m, 5);
        sum.add_usize(9);
        assert_eq!(sum.into_big(), (BigUint::from(u64::MAX)+7u32)*5u32+9u32);
    }
}

#[derive(Default, Serialize, Debug)]
pub struct Stats {
    pub prefix_probes: u64,
    pub terminal_probes: u64,
    pub requests: u64,
    pub distinct_requests: u64,
    pub aggregate_requests: u64,
    pub exact_fallback_requests: u64,
    pub posting_build_rows: u64,
    pub posting_lookups: u64,
    pub answers_materialized: usize,
    pub bitmap_requests: u64,
    pub bitmap_candidate_rows: u64,
    pub bitmap_build_rows: u64,
    pub bitmap_operations: u64,
    pub bitmap_counted_bindings: u64,
    pub prepared_scan_requests: u64,
    pub prepared_scan_rows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count_audit: Option<CountAudit>,
}
#[derive(Default, Serialize, Debug)]
pub struct CountAudit {
    pub suffix_size_histogram: BTreeMap<String, u64>,
    pub suffix_candidate_rows: u64,
    pub range_eligible_requests: u64,
    pub range_eligible_rows: u64,
    pub small_eligible_requests: u64,
    pub small_eligible_rows: u64,
    pub rejected_requests: BTreeMap<String, u64>,
    pub rejected_rows: BTreeMap<String, u64>,
    pub eligible_distinct_requests: usize,
}
struct Access {
    slot: usize,
    refs: Vec<Ref>,
    index: Arc<JointIndex>,
}
struct JointIndex {
    groups: HashMap<Vec<Scalar>, usize>,
    shared: Option<(Arc<Projection>, Vec<usize>)>,
    buckets: Vec<Vec<usize>>,
    monotone: Vec<bool>,
}
impl std::ops::Deref for Access {
    type Target = JointIndex;
    fn deref(&self) -> &JointIndex {
        &self.index
    }
}
type IndexCache = HashMap<(Vec<usize>, Vec<usize>), Arc<JointIndex>>;
fn reversed(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
        x => x,
    }
}
impl Access {
    fn build(
        s: usize,
        b: &Branch,
        es: &[&Row<'_>],
        cache: &mut IndexCache,
        projections: Option<&ProjectionCache>,
    ) -> Self {
        let (mut fields, mut refs) = (vec![], vec![]);
        for p in &b.predicates {
            if let Pred::Compare(Ref::Field(i, f, fi), CompareOp::Eq, Ref::Field(j, g, gj)) = p {
                if *i == s && *j < s {
                    fields.push(*fi);
                    refs.push(Ref::Field(*j, g.clone(), *gj));
                } else if *j == s && *i < s {
                    fields.push(*gj);
                    refs.push(Ref::Field(*i, f.clone(), *fi));
                }
            }
        }
                                                                                 
                                                                                    
        let key = (b.domains[s].clone(), fields.clone());
        if let Some(index) = cache.get(&key) {
            if let Some((p, _)) = &index.shared {
                let table = es[b.domains[s][0]].table;
                refs = p
                    .fields
                    .iter()
                    .map(|name| {
                        let slot = table.field_ids[name];
                        refs[fields.iter().position(|f| *f == slot).unwrap()].clone()
                    })
                    .collect();
            }
            return Self {
                slot: s,
                refs,
                index: Arc::clone(index),
            };
        }
        let mut ids = b.domains[s].clone();
        if !ids
            .windows(2)
            .all(|w| (es[w[0]].order_seq, w[0]) <= (es[w[1]].order_seq, w[1]))
        {
            ids.sort_by_key(|i| (es[*i].order_seq, *i));
        }
        let (mut groups, mut buckets) = (HashMap::new(), vec![]);
        let mut shared = None;
        if let (Some(pc), Some(&first)) = (projections, ids.first()) {
            let table = es[first].table;
            let names = fields
                .iter()
                .map(|fi| {
                    table
                        .field_ids
                        .iter()
                        .find(|(_, v)| **v == *fi)
                        .unwrap()
                        .0
                        .clone()
                })
                .collect::<Vec<_>>();
            let set = names
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            if set.len() == names.len()
                && !names.is_empty()
                && ids
                    .iter()
                    .all(|&i| es[i].event_type == pc.event_type && std::ptr::eq(es[i].table, table))
            {
                for p in pc.projections.values() {
                    if p.fields.len() != names.len()
                        || p.fields
                            .iter()
                            .cloned()
                            .collect::<std::collections::BTreeSet<_>>()
                            != set
                    {
                        continue;
                    }
                    if !p.fields.iter().zip(&p.sources).all(|(f, c)| {
                        table.columns.get(f).is_some_and(|v| {
                            v.same_storage(c) && v.maps_source_rows(&table.source_rows)
                        })
                    }) {
                        continue;
                    }
                    refs = p
                        .fields
                        .iter()
                        .map(|f| refs[names.iter().position(|n| n == f).unwrap()].clone())
                        .collect();
                    let mut map = vec![usize::MAX; p.groups_len()];
                    for &i in &ids {
                        let group = p.codes[es[i].source_row()];
                        if group == usize::MAX {
                            continue;
                        }
                        if map[group] == usize::MAX {
                            map[group] = buckets.len();
                            buckets.push(vec![]);
                        }
                        buckets[map[group]].push(i);
                    }
                    shared = Some((p.clone(), map));
                    break;
                }
            }
        }
        if shared.is_some() {
                                                                                       
        } else if fields.is_empty() {
            buckets.push(ids)
        } else {
            for i in ids {
                if let Some(k) = fields
                    .iter()
                    .map(|f| es[i].get_slot(*f).cloned())
                    .collect::<Option<Vec<_>>>()
                {
                    let at = *groups.entry(k).or_insert_with(|| {
                        buckets.push(vec![]);
                        buckets.len() - 1
                    });
                    buckets[at].push(i)
                }
            }
        }
        let monotone = buckets
            .iter()
            .map(|v| v.windows(2).all(|w| es[w[0]].block_ts <= es[w[1]].block_ts))
            .collect();
        let index = Arc::new(JointIndex {
            groups,
            shared,
            buckets,
            monotone,
        });
        cache.insert(key, Arc::clone(&index));
        Self {
            slot: s,
            refs,
            index,
        }
    }
    fn locate(
        &self,
        row: &Assignment,
        es: &[&Row<'_>],
        ps: &[Pred],
    ) -> Option<(usize, usize, usize)> {
        let g = if self.refs.is_empty() {
            0
        } else {
            let k = self
                .refs
                .iter()
                .map(|r| r.value(row, es))
                .collect::<Option<Vec<_>>>()?;
            if let Some((p, map)) = &self.shared {
                let at = map[p.lookup(&k)?];
                if at == usize::MAX {
                    return None;
                }
                at
            } else {
                *self.groups.get(&k)?
            }
        };
        let ids = &self.buckets[g];
        let (mut lo, mut hi, mut tl, mut th) = (i128::MIN, i128::MAX, i128::MIN, i128::MAX);
        for p in ps {
            match p {
                Pred::Gap {
                    early,
                    late,
                    min,
                    max,
                } if *late == self.slot && row[*early].is_some() => {
                    let t = es[row[*early]?].block_ts as i128;
                    th = th.min(t + *max as i128);
                    if let Some(m) = min {
                        tl = tl.max(t + *m as i128)
                    }
                }
                Pred::Gap {
                    early,
                    late,
                    min,
                    max,
                } if *early == self.slot && row[*late].is_some() => {
                    let t = es[row[*late]?].block_ts as i128;
                    tl = tl.max(t - *max as i128);
                    if let Some(m) = min {
                        th = th.min(t - *m as i128)
                    }
                }
                Pred::Compare(l, op, r) => {
                    let part = match (l, r) {
                        (Ref::Field(i, f, fi), r)
                            if *i == self.slot && r.slot() != Some(self.slot) =>
                        {
                            Some((f, *op, r))
                        }
                        (l, Ref::Field(i, f, fi))
                            if *i == self.slot && l.slot() != Some(self.slot) =>
                        {
                            Some((f, reversed(*op), l))
                        }
                        _ => None,
                    };
                    if let Some((f, op, r)) = part {
                        let f = crate::columns::canonical(f);
                        if f != "order_seq" && f != "block_ts" {
                            continue;
                        }
                        if let Some(Scalar::Int(v)) = r.value(row, es) {
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
                                    *h = (*h).min(v)
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        let (mut a, mut z) = (
            ids.partition_point(|i| (es[*i].order_seq as i128) < lo),
            ids.partition_point(|i| (es[*i].order_seq as i128) <= hi),
        );
        if self.monotone[g] {
            a = a.max(ids.partition_point(|i| (es[*i].block_ts as i128) < tl));
            z = z.min(ids.partition_point(|i| (es[*i].block_ts as i128) <= th));
        }
        if a < z {
            Some((g, a, z))
        } else {
            None
        }
    }
}
                                                                            
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Filter {
    field: String,
    allowed: bool,
    values: Vec<Scalar>,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Request {
    bucket: usize,
    lo: usize,
    hi: usize,
    filters: Vec<Filter>,
}
fn request(
    slot: usize,
    loc: (usize, usize, usize),
    row: &Assignment,
    es: &[&Row<'_>],
    ps: &[Pred],
    monotone: bool,
) -> std::result::Result<Request, &'static str> {
    if !monotone {
        return Err("nonmonotone_time_bucket");
    }
    let mut filters = vec![];
    for p in ps {
        match p {
            Pred::Gap { early, late, .. } if (*early == slot) ^ (*late == slot) => {}
            Pred::Compare(l, op, r) => {
                let (f, op, r) = match (l, r) {
                    (Ref::Field(i, f, fi), r) if *i == slot && r.slot() != Some(slot) => {
                        (f, *op, r)
                    }
                    (l, Ref::Field(i, f, fi)) if *i == slot && l.slot() != Some(slot) => {
                        (f, reversed(*op), l)
                    }
                    _ => return Err("same_slot_or_unbound_comparison"),
                };
                let f = crate::columns::canonical(f);
                let value = r.value(row, es).ok_or("missing_boundary_value")?;
                if (f == "order_seq" || f == "block_ts")
                    && matches!(value, Scalar::Int(_))
                    && op != CompareOp::Ne
                {
                    continue;
                }
                if op != CompareOp::Eq && op != CompareOp::Ne {
                    return Err("non_range_ordered_comparison");
                }
                filters.push(Filter {
                    field: f.into(),
                    allowed: op == CompareOp::Eq,
                    values: vec![value],
                });
            }
            Pred::Distinct(refs, min, max, ignore_empty) => {
                if *ignore_empty {
                    return Err("distinct_ignores_empty");
                }
                let target = refs
                    .iter()
                    .filter_map(|r| match r {
                        Ref::Field(i, f, fi) if *i == slot => Some(f),
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                if target.len() != 1 {
                    return Err("distinct_multiple_target_fields");
                }
                let values = refs
                    .iter()
                    .filter(|r| r.slot() != Some(slot))
                    .map(|r| r.value(row, es))
                    .collect::<Option<BTreeSet<_>>>().ok_or("missing_distinct_boundary")?;
                let n = values.len();
                let same = n >= *min && n <= *max;
                let fresh = n + 1 >= *min && n + 1 <= *max;
                if same && fresh {
                    filters.push(Filter {
                        field: crate::columns::canonical(target[0]).into(),
                        allowed: false,
                        values: vec![],
                    });
                    continue;
                }
                if !same && !fresh {
                    return Ok(Request {
                        bucket: loc.0,
                        lo: 0,
                        hi: 0,
                        filters: vec![],
                    });
                }
                filters.push(Filter {
                    field: crate::columns::canonical(target[0]).into(),
                    allowed: same,
                    values: values.into_iter().collect(),
                });
            }
            Pred::NoEvent { .. } => return Err("no_event"),
            _ => return Err("unsupported_predicate_shape"),
        }
    }
    filters.sort();
    filters.dedup();
    Ok(Request {
        bucket: loc.0,
        lo: loc.1,
        hi: loc.2,
        filters,
    })
}
struct Prepared {
    b: Branch,
    access: Vec<Access>,
    ready: Vec<Vec<Pred>>,
    suffix_plans: Vec<Option<SmallCountPlan>>,
}

                                                                                
                                                                                 
                                                                         
struct ValueBitmap<'a> {
    present: u64,
    values: HashMap<ScalarView<'a>, u64>,
}
enum SmallFilter {
    Compare(usize, CompareOp, Ref),
    NumericBoundary(Ref),
    Distinct(usize, Vec<Ref>, usize, usize),
}
struct SmallCountPlan { filters: Vec<SmallFilter> }
impl SmallCountPlan {
    fn compile(slot: usize, ps: &[Pred]) -> Option<Self> {
        let mut filters = vec![];
        for p in ps {
            match p {
                Pred::Gap { early, late, .. } if (*early == slot) ^ (*late == slot) => {},
                Pred::Compare(l, op, r) => {
                    let (f, fi, op, other) = match (l, r) {
                        (Ref::Field(i, f, fi), other) if *i == slot && other.slot() != Some(slot) => (f, *fi, *op, other),
                        (other, Ref::Field(i, f, fi)) if *i == slot && other.slot() != Some(slot) => (f, *fi, reversed(*op), other),
                        _ => return None,
                    };
                                                                             
                                                                                 
                    if op == CompareOp::Eq && other.slot().is_some_and(|s| s < slot) { continue; }
                    if (f == "order_seq" || f == "block_ts") && op != CompareOp::Ne {
                        filters.push(SmallFilter::NumericBoundary(other.clone()));
                    } else if op == CompareOp::Eq || op == CompareOp::Ne {
                        filters.push(SmallFilter::Compare(fi, op, other.clone()));
                    } else { return None; }
                }
                Pred::Distinct(refs, min, max, false) => {
                    let target = refs.iter().filter_map(|r| match r {
                        Ref::Field(i, _, fi) if *i == slot => Some(*fi), _ => None,
                    }).collect::<Vec<_>>();
                    if target.len() != 1 { return None; }
                    filters.push(SmallFilter::Distinct(target[0], refs.iter()
                        .filter(|r| r.slot() != Some(slot)).cloned().collect(), *min, *max));
                }
                                                                           
                                                            
                _ => return None,
            }
        }
        Some(Self { filters })
    }
    fn numeric_boundaries_hold(&self, row: &Assignment, es: &[&Row<'_>]) -> bool {
        self.filters.iter().all(|f| match f {
            SmallFilter::NumericBoundary(r) => matches!(r.borrowed(row, es), Some(ScalarView::Int(_))),
            _ => true,
        })
    }
                                                                            
                                                                     
    fn accepts(&self, i: usize, row: &Assignment, es: &[&Row<'_>]) -> bool {
        self.filters.iter().all(|f| match f {
            SmallFilter::NumericBoundary(_) => true,
            SmallFilter::Compare(fi, op, r) => es[i].get_slot(*fi)
                .map(ScalarView::from).zip(r.borrowed(row, es))
                .is_some_and(|(l,r)| view_cmp(l, *op, r)),
            SmallFilter::Distinct(fi, refs, min, max) => {
                let Some(v) = es[i].get_slot(*fi) else { return false; };
                let Some(mut values) = refs.iter().map(|r| r.borrowed(row, es))
                    .collect::<Option<BTreeSet<_>>>() else { return false; };
                values.insert(v.into());
                values.len() >= *min && values.len() <= *max
            }
        })
    }
    fn count<'a>(
        &self, loc: (usize, usize, usize), row: &Assignment,
        access: &Access, es: &[&'a Row<'_>],
        cache: &mut HashMap<(usize, usize), ValueBitmap<'a>>,
        reuse_credit: &mut [usize], st: &mut Stats,
    ) -> Option<usize> {
        let (g, lo, hi) = loc;
        let ids = &access.buckets[g];
        if !access.monotone[g] || !self.numeric_boundaries_hold(row, es) { return None; }
        let width = hi-lo;
                                                                            
                                                                              
                                                                           
        reuse_credit[g] = reuse_credit[g].saturating_add(width);
        if width < 4 || (ids.len() > 64 && width < 16)
            || (ids.len() <= 64 && reuse_credit[g] < 2*ids.len()) {
            st.prepared_scan_requests += 1;
            st.prepared_scan_rows += width as u64;
            st.terminal_probes += width as u64;
            return Some(ids[lo..hi].iter().filter(|&&i| self.accepts(i,row,es)).count());
        }
        if ids.len() > 64 { return None; }
        let below = |n: usize| if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        let mut mask = below(hi) & !below(lo);
        for f in &self.filters {
            if let SmallFilter::NumericBoundary(r) = f {
                if !matches!(r.borrowed(row, es), Some(ScalarView::Int(_))) { return None; }
                continue;
            }
            let fi = match f { SmallFilter::Compare(fi, ..) | SmallFilter::Distinct(fi, ..) => *fi, _ => unreachable!() };
                                                                           
                                                                                
            if !cache.contains_key(&(g, fi)) && cache.len() >= 65536 { return None; }
            let bitmap = cache.entry((g, fi)).or_insert_with(|| {
                let mut v = ValueBitmap { present: 0, values: HashMap::new() };
                for (pos, &i) in ids.iter().enumerate() {
                    st.bitmap_build_rows += 1;
                    if let Some(value) = es[i].get_slot(fi) {
                        let bit = 1u64 << pos;
                        v.present |= bit;
                        *v.values.entry(value.into()).or_default() |= bit;
                    }
                }
                v
            });
            let allowed = match f {
                SmallFilter::Compare(_, op, r) => {
                    let value = r.borrowed(row, es)?;
                    let equal = bitmap.values.get(&value).copied().unwrap_or(0);
                    if *op == CompareOp::Eq { equal } else { bitmap.present & !equal }
                }
                SmallFilter::Distinct(_, refs, min, max) => {
                    let values = refs.iter().map(|r| r.borrowed(row, es)).collect::<Option<BTreeSet<_>>>()?;
                    let n = values.len();
                    let same = n >= *min && n <= *max;
                    let fresh = n+1 >= *min && n+1 <= *max;
                    let equal = values.iter().fold(0, |m,v| m | bitmap.values.get(v).copied().unwrap_or(0));
                    (if same { equal } else { 0 }) | (if fresh { bitmap.present & !equal } else { 0 })
                }
                _ => unreachable!(),
            };
            st.bitmap_operations += 1;
            mask &= allowed;
            if mask == 0 { break; }
        }
        let n = mask.count_ones() as usize;
        st.bitmap_requests += 1;
        st.bitmap_candidate_rows += (hi-lo) as u64;
        st.bitmap_counted_bindings += n as u64;
        Some(n)
    }
}
pub struct Engine<'a, 'b> {
    es: Vec<&'a Row<'b>>,
    branches: Vec<Prepared>,
    type_ranges: BTreeMap<String, (usize, usize)>,
}
impl<'a, 'b> Engine<'a, 'b> {
    pub fn new(rows: &'a [Row<'b>], q: &QuerySpec, types: &BTreeSet<String>) -> Result<Self> {
        Self::with_projections(rows, q, types, None)
    }
    pub fn with_projections(
        rows: &'a [Row<'b>],
        q: &QuerySpec,
        types: &BTreeSet<String>,
        projections: Option<&ProjectionCache>,
    ) -> Result<Self> {
        Self::with_verified_domains(rows, q, types, projections, None)
    }
                                                                                
                                                                               
    pub(crate) fn with_verified_domains(
        rows: &'a [Row<'b>],
        q: &QuerySpec,
        types: &BTreeSet<String>,
        projections: Option<&ProjectionCache>,
        atomic_domains: Option<&BTreeMap<(String, String), Vec<usize>>>,
    ) -> Result<Self> {
                                                                                    
                                                                                 
        let mut complete = types.clone();
        for b in &q.branches {
            for c in &b.constraints {
                if let Constraint::NoEvent { event_types, .. } = c {
                    complete.extend(event_types.clone());
                }
            }
        }
        let es = rows.iter().collect::<Vec<_>>();
        let mut branches = vec![];
        let mut domain_cache = BTreeMap::new();
        let mut index_cache = IndexCache::new();
        for bs in &q.branches {
            let b = compile(
                &es,
                q,
                bs,
                Some(&complete),
                atomic_domains,
                &mut domain_cache,
            )?;
            let access = (0..b.atoms.len())
                .map(|s| Access::build(s, &b, &es, &mut index_cache, projections))
                .collect();
            let mut ready = vec![vec![]; b.atoms.len()];
            for p in &b.predicates {
                ready[p.deps().into_iter().max().unwrap_or(0)].push(p.clone())
            }
            let suffix_plans = ready.iter().enumerate().map(|(s,ps)| SmallCountPlan::compile(s, ps)).collect();
            branches.push(Prepared { b, access, ready, suffix_plans });
        }
        let type_ranges = types
            .iter()
            .map(|ty| {
                let lo = es.partition_point(|r| r.event_type < ty.as_str());
                let hi = es.partition_point(|r| r.event_type <= ty.as_str());
                (ty.clone(), (lo, hi - lo))
            })
            .collect();
        Ok(Self {
            es,
            branches,
            type_ranges,
        })
    }
    pub fn reused_projection_indexes(&self) -> usize {
        self.branches
            .iter()
            .flat_map(|b| &b.access)
            .filter(|a| a.shared.is_some())
            .count()
    }
    pub fn replay_rows(&self) -> Result<(Vec<RowAnswer>, Stats)> {
        let mut out = vec![];
        let mut st = Stats::default();
        for b in &self.branches {
            let mut a = vec![None; b.b.atoms.len()];
            self.walk(
                b,
                0,
                &mut a,
                &mut st,
                &mut |row, st| {
                    out.push(RowAnswer {
                        branch: b.b.id.clone(),
                        rows: row
                            .iter()
                            .map(|i| {
                                let r = self.es[i.unwrap()];
                                RowRef(r.event_type.into(), r.source_row())
                            })
                            .collect(),
                    });
                    st.answers_materialized += 1;
                    Ok(())
                },
                false,
            )?;
        }
        out.sort();
        ensure!(
            out.windows(2).all(|w| w[0] != w[1]),
            "duplicate generated answer"
        );
        Ok((out, st))
    }
    fn resolve_row(&self, r: &RowRef) -> Result<usize> {
        let (start, n) = self
            .type_ranges
            .get(&r.0)
            .context("unknown authenticated table")?;
        let at = if *n > 0 {
            if let Some(ids) = &self.es[*start].table.source_rows {
                ids.binary_search(&r.1)
                    .map_err(|_| anyhow::anyhow!("row outside sparse domain"))?
            } else {
                ensure!(r.1 < *n, "row reference out of bounds");
                r.1
            }
        } else {
            anyhow::bail!("empty row domain")
        };
        Ok(start + at)
    }
    pub fn canonical_answers(&self, answers: &[RowAnswer]) -> Result<Vec<Answer>> {
        let mut out = answers
            .iter()
            .map(|a| {
                Ok(Answer {
                    branch: a.branch.clone(),
                    events: a
                        .rows
                        .iter()
                        .map(|r| Ok(self.es[self.resolve_row(r)?].id().into()))
                        .collect::<Result<_>>()?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        out.sort();
        Ok(out)
    }
    pub fn replay(&self) -> Result<(Vec<Answer>, Stats)> {
        let (a, s) = self.replay_rows()?;
        Ok((self.canonical_answers(&a)?, s))
    }
    fn walk(
        &self,
        b: &Prepared,
        s: usize,
        a: &mut Assignment,
        st: &mut Stats,
        visit: &mut impl FnMut(&Assignment, &mut Stats) -> Result<()>,
        prefix: bool,
    ) -> Result<()> {
        if s == b.b.atoms.len() || prefix && s + 1 == b.b.atoms.len() {
            return visit(a, st);
        }
        if let Some((g, l, h)) = b.access[s].locate(a, &self.es, &b.ready[s]) {
            let residual = b.suffix_plans[s].as_ref().filter(|p|
                b.access[s].monotone[g] && p.numeric_boundaries_hold(a, &self.es));
            for &i in &b.access[s].buckets[g][l..h] {
                if s + 1 == b.b.atoms.len() {
                    st.terminal_probes += 1
                } else {
                    st.prefix_probes += 1
                }
                ensure!(
                    st.prefix_probes + st.terminal_probes < 2_000_000_000,
                    "search resource cap; no partial result"
                );
                a[s] = Some(i);
                if residual.map_or_else(|| b.ready[s].iter().all(|p| p.holds(a, &self.es)),
                    |p| p.accepts(i, a, &self.es)) {
                    self.walk(b, s + 1, a, st, visit, prefix)?;
                }
            }
        }
        a[s] = None;
        Ok(())
    }
    pub fn sound_unique(&self, answers: &[RowAnswer]) -> Result<()> {
                                                                             
                                                                             
        if !answers.windows(2).all(|w| w[0] < w[1]) {
            let mut ordered = answers.iter().collect::<Vec<_>>();
            ordered.sort_unstable();
            ensure!(ordered.windows(2).all(|w| w[0] != w[1]), "duplicate answer");
        }
        enum Membership<'d> { All, Mask(Vec<bool>), Sorted(&'d [usize]) }
        let mut budget = 8_000_000usize;
        let domains = self.branches.iter().map(|p| p.b.domains.iter().map(|d| {
            if d.len() == self.es.len() && d.iter().enumerate().all(|(i,r)| i == *r) {
                Membership::All
            } else if self.es.len() <= budget {
                budget -= self.es.len();
                let mut mask = vec![false;self.es.len()];
                for &i in d { mask[i] = true; }
                Membership::Mask(mask)
            } else { Membership::Sorted(d.as_slice()) }
        }).collect::<Vec<_>>()).collect::<Vec<_>>();
        let branch_ids = self.branches.iter().enumerate()
            .map(|(i,p)| (p.b.id.as_str(),i)).collect::<HashMap<_,_>>();
        ensure!(branch_ids.len() == self.branches.len(), "duplicate branch identity");
        let mut active: Option<usize> = None;
        let mut a = Assignment::new();
        for ans in answers {
            let bi = match active {
                Some(i) if self.branches[i].b.id == ans.branch => i,
                _ => *branch_ids.get(ans.branch.as_str()).context("unknown answer branch")?,
            };
            active = Some(bi);
            let p = &self.branches[bi];
            ensure!(ans.rows.len() == p.b.atoms.len(), "binding arity");
            a.clear();
            for r in &ans.rows { a.push(Some(self.resolve_row(r)?)); }
            for (s, i) in a.iter().enumerate() {
                let i = i.unwrap();
                ensure!(match &domains[bi][s] {
                    Membership::All => true,
                    Membership::Mask(mask) => mask[i],
                    Membership::Sorted(ids) => ids.binary_search(&i).is_ok(),
                }, "answer atomic domain");
            }
            ensure!(
                p.b.predicates.iter().all(|p| p.holds(&a, &self.es)),
                "false binding"
            );
        }
        Ok(())
    }
    pub fn count(&self) -> Result<(BigUint, Stats)> {
        let mut sum = ExactCount::default();
        let mut st = Stats::default();
                                                                                     
        if std::env::var_os("FUNNELQ_COUNT_AUDIT").is_some() {
            st.count_audit = Some(CountAudit::default());
        }
        for b in &self.branches {
            let s = b.b.atoms.len() - 1;
            let acc = &b.access[s];
            let ps = &b.ready[s];
            let mut requests = BTreeMap::<Request, ExactCount>::new();
            let mut a = vec![None; b.b.atoms.len()];
            let mut row = vec![None; b.b.atoms.len()];
            let mut audit_requests = BTreeSet::new();
            let small_plan = &b.suffix_plans[s];
            let mut bitmap_cache = HashMap::new();
            let mut reuse_credit = vec![0usize; acc.buckets.len()];
            self.walk(
                b,
                0,
                &mut a,
                &mut st,
                &mut |a, st| {
                    if let Some(loc) = acc.locate(a, &self.es, ps) {
                        st.requests += 1;
                        if let Some(audit) = &mut st.count_audit {
                            let width = loc.2 - loc.1;
                            let label = match width { 1 => "1", 2..=3 => "2-3", 4..=7 => "4-7", 8..=15 => "8-15", 16..=63 => "16-63", _ => "64+" };
                            *audit.suffix_size_histogram.entry(label.into()).or_default() += 1;
                            audit.suffix_candidate_rows += width as u64;
                            match request(s, loc, a, &self.es, ps, acc.monotone[loc.0]) {
                                Ok(r) => {
                                    audit.range_eligible_requests += 1;
                                    audit.range_eligible_rows += width as u64;
                                    if width < 16 {
                                        audit.small_eligible_requests += 1;
                                        audit.small_eligible_rows += width as u64;
                                    }
                                    audit_requests.insert(r);
                                }
                                Err(reason) => {
                                    *audit.rejected_requests.entry(reason.into()).or_default() += 1;
                                    *audit.rejected_rows.entry(reason.into()).or_default() += width as u64;
                                }
                            }
                        }
                        if let Some(n) = small_plan.as_ref().and_then(|p|
                            p.count(loc, a, acc, &self.es, &mut bitmap_cache, &mut reuse_credit, st)) {
                            sum.add_usize(n);
                            return Ok(());
                        }
                                                                                             
                                                                                               
                        if loc.2 - loc.1 >= 16 {
                            if let Ok(r) = request(s, loc, a, &self.es, ps, acc.monotone[loc.0]) {
                                requests.entry(r).or_default().add_u64(1);
                                return Ok(());
                            }
                        }
                        st.exact_fallback_requests += 1;
                        row.copy_from_slice(a);
                        let mut valid = 0usize;
                        for &i in &acc.buckets[loc.0][loc.1..loc.2] {
                            row[s] = Some(i);
                            st.terminal_probes += 1;
                            if ps.iter().all(|p| p.holds(&row, &self.es)) {
                                valid += 1;
                            }
                        }
                        sum.add_usize(valid);
                    }
                    Ok(())
                },
                true,
            )?;
            if let Some(audit) = &mut st.count_audit {
                audit.eligible_distinct_requests += audit_requests.len();
            }
            st.distinct_requests += requests.len() as u64;
            let mut indexes =
                HashMap::<(usize, Vec<String>), HashMap<Vec<Scalar>, Vec<usize>>>::new();
            let mut presence = HashMap::new();
            for (r, m) in requests {
                st.aggregate_requests += 1;
                let n = aggregate(&r, acc, &self.es, &mut indexes, &mut presence, &mut st)?;
                sum.add_product(m, n);
            }
        }
        Ok((sum.into_big(), st))
    }
}
                                                                             
                                                                                 
fn aggregate(
    r: &Request,
    access: &Access,
    es: &[&Row<'_>],
    indexes: &mut HashMap<(usize, Vec<String>), HashMap<Vec<Scalar>, Vec<usize>>>,
    presence: &mut HashMap<(usize, String), bool>,
    st: &mut Stats,
) -> Result<usize> {
                                                                                  
    if r.filters.iter().any(|f| {
        !*presence
            .entry((r.bucket, f.field.clone()))
            .or_insert_with(|| {
                access.buckets[r.bucket]
                    .iter()
                    .all(|i| es[*i].get(&f.field).is_some())
            })
    }) {
        st.exact_fallback_requests += 1;
        return Ok(access.buckets[r.bucket][r.lo..r.hi]
            .iter()
            .filter(|i| {
                st.terminal_probes += 1;
                r.filters.iter().all(|f| {
                    es[**i]
                        .get(&f.field)
                        .is_some_and(|v| f.values.contains(v) == f.allowed)
                })
            })
            .count());
    }
    if r.lo == r.hi {
        return Ok(0);
    }
    let mut terms = vec![(1i128, BTreeMap::<String, Scalar>::new())];
    for f in &r.filters {
        let mut next = vec![];
        for (sign, t) in &terms {
            if !f.allowed {
                next.push((*sign, t.clone()))
            }
            for v in &f.values {
                if t.get(&f.field).is_some_and(|old| old != v) {
                    continue;
                }
                let mut t = t.clone();
                t.insert(f.field.clone(), v.clone());
                next.push((if f.allowed { *sign } else { -*sign }, t))
            }
        }
        terms = next;
        if terms.len() > 256 {
            st.exact_fallback_requests += 1;
            let n = access.buckets[r.bucket][r.lo..r.hi]
                .iter()
                .filter(|i| {
                    st.terminal_probes += 1;
                    r.filters.iter().all(|f| {
                        es[**i]
                            .get(&f.field)
                            .is_some_and(|v| f.values.contains(v) == f.allowed)
                    })
                })
                .count();
            return Ok(n);
        }
    }
    let mut n = 0i128;
    for (sign, t) in terms {
        if t.is_empty() {
            n += sign * (r.hi - r.lo) as i128;
            continue;
        }
        let fields = t.keys().cloned().collect::<Vec<_>>();
        let key = t.values().cloned().collect::<Vec<_>>();
        let index = indexes
            .entry((r.bucket, fields.clone()))
            .or_insert_with(|| {
                let mut map = HashMap::<Vec<Scalar>, Vec<usize>>::new();
                for (pos, i) in access.buckets[r.bucket].iter().enumerate() {
                    st.posting_build_rows += 1;
                    if let Some(k) = fields
                        .iter()
                        .map(|f| es[*i].value(f))
                        .collect::<Option<Vec<_>>>()
                    {
                        map.entry(k).or_default().push(pos)
                    }
                }
                map
            });
        st.posting_lookups += 1;
        if let Some(ids) = index.get(&key) {
            let l = ids.partition_point(|i| *i < r.lo);
            let h = ids.partition_point(|i| *i < r.hi);
            n += sign * (h - l) as i128;
        }
    }
    ensure!(
        n >= 0 && n <= (r.hi - r.lo) as i128,
        "exact exclusion arithmetic"
    );
    Ok(n as usize)
}
#[derive(Clone, Debug)]
enum Ref {
    Field(usize, String, usize),
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
pub(crate) fn field_view<'a>(e: &'a Row<'_>, f: &str) -> Option<ScalarView<'a>> {
    e.get(f).map(ScalarView::from)
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
            Self::Field(s, _, _) => Some(*s),
            _ => None,
        }
    }
    fn value(&self, a: &Assignment, es: &[&Row<'_>]) -> Option<Scalar> {
        match self {
            Self::Field(s, _, slot) => es[a[*s]?].get_slot(*slot).cloned(),
            Self::Const(v) => Some(v.clone()),
        }
    }
    fn borrowed<'a>(&'a self, a: &Assignment, es: &[&'a Row<'_>]) -> Option<ScalarView<'a>> {
        match self {
            Self::Field(s, _, slot) => es[a[*s]?].get_slot(*slot).map(ScalarView::from),
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
    fn holds(&self, a: &Assignment, es: &[&Row<'_>]) -> bool {
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
    Ref::Field(s, crate::columns::canonical(f).into(), usize::MAX)
}
fn compile(
    es: &[&Row<'_>],
    q: &QuerySpec,
    b: &BranchSpec,
    complete_blockers: Option<&BTreeSet<String>>,
    certified: Option<&BTreeMap<(String, String), Vec<usize>>>,
    domain_cache: &mut BTreeMap<AtomSpec, Vec<usize>>,
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
        let mut domain_key = a.clone();
        domain_key.id.clear();
        for t in domain_key.fields.values_mut() {
            if let Term::Variable(v) = t {
                *v = String::new();
            }
        }
        if certified.is_none() {
            if let Some(d) = domain_cache.get(&domain_key) {
                domains.push(d.clone());
                continue;
            }
        }
        let field_slots = a
            .fields
            .iter()
            .map(|(f, t)| (es.first().map_or(usize::MAX, |e| e.field_slot(f)), t))
            .collect::<Vec<_>>();
        let local_slots = a
            .local_predicates
            .iter()
            .map(|p| {
                (
                    es.first().map_or(usize::MAX, |e| e.field_slot(&p.field)),
                    p.op,
                    &p.value,
                )
            })
            .collect::<Vec<_>>();
                                                                                
                                                                              
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
                    a.event_types.contains(e.event_type)
                        && field_slots.iter().all(|(f, t)| {
                            e.get_slot(*f)
                                .map(ScalarView::from)
                                .is_some_and(|x| match t {
                                    Term::Constant(v) => x == v.into(),
                                    Term::Variable(_) => !matches!(x, ScalarView::Text("")),
                                })
                        })
                        && local_slots.iter().all(|(f, op, value)| {
                            e.get_slot(*f)
                                .map(ScalarView::from)
                                .is_some_and(|x| view_cmp(x, *op, (*value).into()))
                        })
                })
                .collect::<Vec<_>>(),
        );
        if certified.is_none() {
            domain_cache.insert(domain_key, domains.last().unwrap().clone());
        }
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
                        .filter(|(_, e)| types.contains(e.event_type))
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
                                                                             
                                                                         
                                                                    
    let mut implied = BTreeSet::new();
    for i in 0..b.atoms.len() {
        for j in i + 1..b.atoms.len() {
            if b.atoms[i].event_types.is_disjoint(&b.atoms[j].event_types) {
                implied.insert((i, j));
            }
        }
    }
    for p in &ps {
        if let Pred::Compare(
            Ref::Field(i, f, _),
            CompareOp::Lt | CompareOp::Gt,
            Ref::Field(j, g, _),
        ) = p
        {
            if i != j && crate::columns::canonical(f) == crate::columns::canonical(g) {
                implied.insert(((*i).min(*j), (*i).max(*j)));
            }
        }
    }
    ps.retain(|p|!matches!(p,Pred::Compare(Ref::Field(i,f,_),CompareOp::Ne,Ref::Field(j,g,_)) if f=="event_id"&&g=="event_id"&&implied.contains(&((*i).min(*j),(*i).max(*j)))));
                                                                             
    let resolve = |r: &mut Ref| {
        if let Ref::Field(_, f, slot) = r {
            *slot = es.first().map_or(usize::MAX, |e| e.field_slot(f));
        }
    };
    for p in &mut ps {
        match p {
            Pred::Compare(a, _, b) => {
                resolve(a);
                resolve(b)
            }
            Pred::Distinct(v, ..) => {
                for r in v {
                    resolve(r)
                }
            }
            Pred::NoEvent { fields, .. } => {
                for (_, r) in fields {
                    resolve(r)
                }
            }
            _ => {}
        }
    }
                                                                             
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
