                                                                                    
                                                                                   
use crate::{
    columns::{canonical, Column},
    key_projection::{KeyDomains, KeyPlan, Projection},
    model::Scalar,
    query::{CompareOp, Constraint, QuerySpec, ScopeSpec, ValueExpr},
};
use anyhow::{ensure, Context, Result};
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

const INF: i128 = 1i128 << 100;
type Bounds = Vec<Vec<i128>>;
fn matrix(n: usize) -> Bounds {
    let mut m = vec![vec![INF; n]; n];
    for (i, row) in m.iter_mut().enumerate() {
        row[i] = 0;
    }
    m
}
fn close(m: &mut Bounds) {
    for k in 0..m.len() {
        for i in 0..m.len() {
            for j in 0..m.len() {
                if m[i][k] < INF && m[k][j] < INF {
                    m[i][j] = m[i][j].min(m[i][k] + m[k][j]);
                }
            }
        }
    }
}
fn comparison(m: &mut Bounds, a: usize, b: usize, op: CompareOp) {
                                                                                  
    match op {
        CompareOp::Lt => m[b][a] = m[b][a].min(-1),
        CompareOp::Le => m[b][a] = m[b][a].min(0),
        CompareOp::Gt => m[a][b] = m[a][b].min(-1),
        CompareOp::Ge => m[a][b] = m[a][b].min(0),
        CompareOp::Eq => {
            m[a][b] = m[a][b].min(0);
            m[b][a] = m[b][a].min(0);
        }
        CompareOp::Ne => {}                                                       
    }
}
pub struct TemporalPlan {
    time: Bounds,
    order: Bounds,
}
impl TemporalPlan {
    pub fn compile(q: &QuerySpec) -> Result<Self> {
                                                                                  
        KeyPlan::compile(q)?;
        let b = &q.branches[0];
        let slots = b
            .atoms
            .iter()
            .enumerate()
            .map(|(i, a)| (a.id.as_str(), i))
            .collect::<BTreeMap<_, _>>();
        ensure!(slots.len() == b.atoms.len(), "duplicate atom id");
        let at = |s: &str| slots.get(s).copied().context("unknown temporal atom");
        let mut time = matrix(b.atoms.len());
        let mut order = matrix(b.atoms.len());
        for c in &b.constraints {
            match c {
                Constraint::Order {
                    earlier,
                    later,
                    field,
                } => {
                    let (i, j) = (at(earlier)?, at(later)?);
                    match canonical(field) {
                        "block_ts" => comparison(&mut time, i, j, CompareOp::Lt),
                        "order_seq" => comparison(&mut order, i, j, CompareOp::Lt),
                        _ => {}
                    }
                }
                Constraint::Within {
                    earlier,
                    later,
                    delta_seconds,
                } => {
                    let (i, j) = (at(earlier)?, at(later)?);
                                                                                           
                    comparison(&mut order, i, j, CompareOp::Lt);
                    time[i][j] = time[i][j].min(*delta_seconds as i128);
                }
                Constraint::Compare {
                    left: ValueExpr::Field(l),
                    op,
                    right: ValueExpr::Field(r),
                } if canonical(&l.field) == canonical(&r.field) => {
                    let (i, j) = (at(&l.atom)?, at(&r.atom)?);
                    match canonical(&l.field) {
                        "block_ts" => comparison(&mut time, i, j, *op),
                        "order_seq" => comparison(&mut order, i, j, *op),
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        if let ScopeSpec::Episode {
            anchor_atom,
            window_seconds,
        } = &q.scope
        {
            ensure!(*window_seconds >= 0, "negative Episode window");
            let a = at(anchor_atom)?;
            for j in 0..b.atoms.len() {
                time[a][j] = time[a][j].min(*window_seconds as i128);
                time[j][a] = time[j][a].min(0);
                order[j][a] = order[j][a].min(0);
            }
        }
        close(&mut time);
        close(&mut order);
        Ok(Self { time, order })
    }
    pub fn apply(
        &self,
        plan: &KeyPlan,
        cols: &BTreeMap<String, Arc<Column>>,
        d: &mut KeyDomains,
    ) -> Result<TemporalStats> {
        let n = d.union.len();
        let read = |f: &str| -> Result<Vec<i64>> {
            let c = cols
                .get(f)
                .context("missing authenticated temporal column")?;
            ensure!(c.len() == n, "incomplete temporal column");
            (0..n)
                .map(|i| match c.get(i) {
                    Some(Scalar::Int(v)) => Ok(*v),
                    _ => anyhow::bail!("non-integer temporal column"),
                })
                .collect()
        };
        let ts = read("block_ts")?;
        let ord = read("order_seq")?;
        let mut st = TemporalStats {
            before_union: d.union.iter().filter(|x| **x).count(),
            before_atoms: d
                .per_atom
                .iter()
                .map(|m| m.iter().filter(|x| **x).count())
                .collect(),
            monotone_scan: ts.windows(2).all(|w| w[0] <= w[1])
                && ord.windows(2).all(|w| w[0] <= w[1]),
            ..Default::default()
        };
        if (0..self.time.len()).any(|i| self.time[i][i] < 0 || self.order[i][i] < 0) {
            for m in &mut d.per_atom {
                m.fill(false);
            }
        } else {
                                                                                       
            let mut buckets = BTreeMap::new();
            for pair in &plan.pairs {
                let (a, b) = (pair.a, pair.b);
                if [
                    self.time[a][b],
                    self.time[b][a],
                    self.order[a][b],
                    self.order[b][a],
                ]
                .iter()
                .all(|v| *v == INF)
                {
                    continue;
                }
                for layout in [&pair.left, &pair.right] {
                    if !buckets.contains_key(layout) {
                        buckets.insert(
                            layout.clone(),
                            Buckets::new(&d.projections.projections[layout]),
                        );
                        st.bucket_input_rows += n;
                    }
                }
                let l = &d.projections.projections[&pair.left];
                let r = &d.projections.projections[&pair.right];
                let lg = &buckets[&pair.left];
                let rg = &buckets[&pair.right];
                for (li, ri) in l.matching_groups(r) {
                    scan(
                        lg.get(li),
                        rg.get(ri),
                        &ts,
                        &ord,
                        [
                            -self.time[b][a],
                            self.time[a][b],
                            -self.order[b][a],
                            self.order[a][b],
                        ],
                        &mut d.per_atom[a],
                        &mut st,
                    );
                    scan(
                        rg.get(ri),
                        lg.get(li),
                        &ts,
                        &ord,
                        [
                            -self.time[a][b],
                            self.time[b][a],
                            -self.order[a][b],
                            self.order[b][a],
                        ],
                        &mut d.per_atom[b],
                        &mut st,
                    );
                    st.matched_group_pairs += 1;
                }
                st.pair_passes += 1;
            }
        }
        d.union.fill(false);
        for mask in &d.per_atom {
            for (i, &yes) in mask.iter().enumerate() {
                d.union[i] |= yes;
            }
        }
        st.after_union = d.union.iter().filter(|x| **x).count();
        st.after_atoms = d
            .per_atom
            .iter()
            .map(|m| m.iter().filter(|x| **x).count())
            .collect();
        Ok(st)
    }
}
#[derive(Default, Serialize)]
pub struct TemporalStats {
    pub before_union: usize,
    pub after_union: usize,
    pub before_atoms: Vec<usize>,
    pub after_atoms: Vec<usize>,
    pub monotone_scan: bool,
    pub bucket_input_rows: usize,
    pub pair_passes: usize,
    pub matched_group_pairs: usize,
    pub source_rows: usize,
    pub pointer_steps: usize,
    pub fallback_checks: usize,
}
struct Buckets {
    offsets: Vec<usize>,
    rows: Vec<usize>,
}
impl Buckets {
    fn new(p: &Projection) -> Self {
        let mut offsets = vec![0; p.groups_len() + 1];
        for &g in &p.codes {
            if g != usize::MAX {
                offsets[g + 1] += 1;
            }
        }
        for i in 1..offsets.len() {
            offsets[i] += offsets[i - 1];
        }
        let mut rows = vec![0; *offsets.last().unwrap()];
        let mut cursors = offsets.clone();
        for (i, &g) in p.codes.iter().enumerate() {
            if g != usize::MAX {
                rows[cursors[g]] = i;
                cursors[g] += 1;
            }
        }
        Self { offsets, rows }
    }
    fn get(&self, g: usize) -> &[usize] {
        &self.rows[self.offsets[g]..self.offsets[g + 1]]
    }
}
fn scan(
    src: &[usize],
    dst: &[usize],
    ts: &[i64],
    ord: &[i64],
    bounds: [i128; 4],
    keep: &mut [bool],
    st: &mut TemporalStats,
) {
    let (mut lo, mut hi) = (0, 0);
    for &x in src {
        if !keep[x] {
            continue;
        }
        st.source_rows += 1;
        let [tl, th, ol, oh] = [
            ts[x] as i128 + bounds[0],
            ts[x] as i128 + bounds[1],
            ord[x] as i128 + bounds[2],
            ord[x] as i128 + bounds[3],
        ];
        if st.monotone_scan {
            while lo < dst.len() && ((ts[dst[lo]] as i128) < tl || (ord[dst[lo]] as i128) < ol) {
                lo += 1;
                st.pointer_steps += 1;
            }
            while hi < dst.len() && (ts[dst[hi]] as i128) <= th && (ord[dst[hi]] as i128) <= oh {
                hi += 1;
                st.pointer_steps += 1;
            }
            keep[x] &= lo < hi;
        } else {
                                                                                  
                                                                                  
            keep[x] &= dst.iter().any(|&y| {
                st.fallback_checks += 1;
                let (t, o) = (ts[y] as i128, ord[y] as i128);
                tl <= t && t <= th && ol <= o && o <= oh
            });
        }
    }
}
pub fn local_domains(
    q: &QuerySpec,
    d: &KeyDomains,
    ids: &[usize],
) -> Result<BTreeMap<(String, String), Vec<usize>>> {
    ensure!(
        ids.windows(2).all(|w| w[0] < w[1]) && ids.iter().all(|i| *i < d.union.len()),
        "domain row mapping"
    );
    Ok(q.branches[0]
        .atoms
        .iter()
        .enumerate()
        .map(|(a, atom)| {
            (
                (q.branches[0].id.clone(), atom.id.clone()),
                ids.iter()
                    .enumerate()
                    .filter_map(|(local, &global)| d.per_atom[a][global].then_some(local))
                    .collect(),
            )
        })
        .collect())
}
