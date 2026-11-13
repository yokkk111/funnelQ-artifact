                                                                             
use crate::{
    columns::{canonical, Column},
    model::Scalar,
    query::{Constraint, QuerySpec, Term},
};
use anyhow::{ensure, Context, Result};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::Arc,
};

#[derive(Clone, Hash, PartialEq, Eq)]
enum Key {
    Small([u32; 4]),
    Wide(Vec<u32>),
}
impl Key {
    fn of(values: impl Iterator<Item = u32>, width: usize) -> Self {
        if width <= 4 {
            let mut k = [0; 4];
            for (i, v) in values.enumerate() {
                k[i] = v;
            }
            Self::Small(k)
        } else {
            Self::Wide(values.collect())
        }
    }
}
pub struct Projection {
    pub fields: Vec<String>,
    pub sources: Vec<Arc<Column>>,
    pub codes: Vec<usize>,
    groups: HashMap<Key, usize>,
    values: Option<Arc<HashMap<Scalar, u32>>>,
}
impl Projection {
    pub(crate) fn matching_groups(&self, other: &Self) -> Vec<(usize, usize)> {
        self.groups
            .iter()
            .filter_map(|(k, &a)| other.groups.get(k).map(|&b| (a, b)))
            .collect()
    }
    pub fn groups_len(&self) -> usize {
        self.groups.len()
    }
    pub fn lookup(&self, values: &[Scalar]) -> Option<usize> {
        if values.len() != self.fields.len() {
            return None;
        }
        let ids = values
            .iter()
            .map(|v| match &self.values {
                Some(dict) => dict.get(v).copied(),
                None => {
                    if let Scalar::Int(x) = v {
                        u32::try_from(*x).ok()
                    } else {
                        None
                    }
                }
            })
            .collect::<Option<Vec<_>>>()?;
        self.groups
            .get(&Key::of(ids.into_iter(), values.len()))
            .copied()
    }
}
pub struct ProjectionCache {
    pub event_type: String,
    pub projections: BTreeMap<Vec<String>, Arc<Projection>>,
}
pub(crate) struct Pair {
    pub a: usize,
    pub b: usize,
    pub left: Vec<String>,
    pub right: Vec<String>,
}
pub struct KeyDomains {
    pub union: Vec<bool>,
    pub per_atom: Vec<Vec<bool>>,
    pub projections: ProjectionCache,
}
pub struct KeyPlan {
    pub event_type: String,
    pub fields: BTreeSet<String>,
    atoms: usize,
    pub(crate) pairs: Vec<Pair>,
}
impl KeyPlan {
    pub fn compile(q: &QuerySpec) -> Result<Self> {
        ensure!(q.branches.len() == 1, "hybrid envelope supports one branch");
        let b = &q.branches[0];
        ensure!(!b.atoms.is_empty(), "empty query");
        ensure!(
            b.constraints
                .iter()
                .all(|c| !matches!(c, Constraint::NoEvent { .. })),
            "NoEvent requires a complete blocker envelope"
        );
        ensure!(
            b.atoms.iter().all(|a| a.event_types.len() == 1),
            "one-type atom envelope"
        );
        let ty = b.atoms[0].event_types.iter().next().unwrap().clone();
        ensure!(
            b.atoms.iter().all(|a| a.event_types.contains(&ty)),
            "same-type hybrid envelope"
        );
        let pats = b
            .atoms
            .iter()
            .map(|a| {
                let mut p = BTreeMap::new();
                for (f, t) in &a.fields {
                    if let Term::Variable(v) = t {
                        p.entry(v.clone())
                            .or_insert_with(|| canonical(f).to_string());
                    }
                }
                p
            })
            .collect::<Vec<_>>();
        let mut fields = BTreeSet::new();
        let mut pairs = Vec::new();
        for a in 0..pats.len() {
            for b in a + 1..pats.len() {
                let shared = pats[a]
                    .keys()
                    .filter(|v| pats[b].contains_key(*v))
                    .collect::<Vec<_>>();
                if shared.is_empty() {
                    continue;
                }
                let left = shared
                    .iter()
                    .map(|v| pats[a][*v].clone())
                    .collect::<Vec<_>>();
                let right = shared
                    .iter()
                    .map(|v| pats[b][*v].clone())
                    .collect::<Vec<_>>();
                fields.extend(left.iter().chain(&right).cloned());
                pairs.push(Pair { a, b, left, right });
            }
        }
        Ok(Self {
            event_type: ty,
            fields,
            atoms: pats.len(),
            pairs,
        })
    }
    pub fn execute(
        &self,
        columns: &BTreeMap<String, Arc<Column>>,
        n: usize,
    ) -> Result<(Vec<bool>, ProjectionCache)> {
        let d = self.execute_domains(columns, n)?;
        Ok((d.union, d.projections))
    }
    pub fn execute_domains(
        &self,
        columns: &BTreeMap<String, Arc<Column>>,
        n: usize,
    ) -> Result<KeyDomains> {
                                                                              
                                                                                   
        let numeric = self.fields.iter().all(|f| {
            columns.get(f).is_some_and(|c| {
                c.dict
                    .iter()
                    .all(|v| matches!(v,Scalar::Int(x) if u32::try_from(*x).is_ok()))
            })
        });
        let mut interner = HashMap::new();
        let mut recoded = BTreeMap::new();
        for f in &self.fields {
            let c = columns.get(f).context("missing complete key column")?;
            ensure!(c.len() == n, "incomplete key column");
            let mut codes = vec![0];
            for v in c.dict.iter() {
                if numeric {
                    if let Scalar::Int(x) = v {
                        codes.push(u32::try_from(*x)?);
                    }
                } else {
                    let next = u32::try_from(interner.len() + 1)?;
                    codes.push(*interner.entry(v.clone()).or_insert(next));
                }
            }
            recoded.insert(f.clone(), codes);
        }
        let interner = if numeric {
            None
        } else {
            Some(Arc::new(interner))
        };
        let layouts = self
            .pairs
            .iter()
            .flat_map(|p| [p.left.clone(), p.right.clone()])
            .collect::<BTreeSet<_>>();
        let mut projections = BTreeMap::new();
        for layout in layouts {
            let cols = layout
                .iter()
                .map(|f| (&columns[f], &recoded[f]))
                .collect::<Vec<_>>();
            let mut groups = HashMap::new();
            let mut codes = Vec::with_capacity(n);
            for row in 0..n {
                if cols.iter().any(|(c, _)| c.code(row) == Some(0)) {
                    codes.push(usize::MAX);
                    continue;
                }
                let k = Key::of(
                    cols.iter().map(|(c, m)| m[c.code(row).unwrap() as usize]),
                    layout.len(),
                );
                let next = groups.len();
                codes.push(*groups.entry(k).or_insert(next));
            }
            let sources = layout.iter().map(|f| columns[f].clone()).collect();
            projections.insert(
                layout.clone(),
                Arc::new(Projection {
                    fields: layout,
                    sources,
                    codes,
                    groups,
                    values: interner.clone(),
                }),
            );
        }
        let mut keep = vec![vec![true; n]; self.atoms];
        for pair in &self.pairs {
            let l = &projections[&pair.left];
            let r = &projections[&pair.right];
            let mut left_live = vec![false; l.groups.len()];
            let mut right_live = vec![false; r.groups.len()];
            for (k, &a) in &l.groups {
                if let Some(&b) = r.groups.get(k) {
                    left_live[a] = true;
                    right_live[b] = true;
                }
            }
            for row in 0..n {
                keep[pair.a][row] &= l.codes[row] != usize::MAX && left_live[l.codes[row]];
                keep[pair.b][row] &= r.codes[row] != usize::MAX && right_live[r.codes[row]];
            }
        }
        let mut union = vec![false; n];
        for mask in &keep {
            for (i, &yes) in mask.iter().enumerate() {
                union[i] |= yes;
            }
        }
        Ok(KeyDomains {
            union,
            per_atom: keep,
            projections: ProjectionCache {
                event_type: self.event_type.clone(),
                projections,
            },
        })
    }
}
