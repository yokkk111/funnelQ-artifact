                                                                                  
                                                                            
                                                                                
use super::*;
struct Program {
    eq: Vec<(String, Ref)>,
    ne: BTreeMap<String, Vec<Ref>>,
    bounds: Vec<(String, CompareOp, Ref)>,
    gaps: Vec<(bool, usize, Option<i64>, i64)>,
    guards: Vec<Pred>,
}
fn reverse(op: CompareOp) -> CompareOp {
    match op {
        CompareOp::Lt => CompareOp::Gt,
        CompareOp::Le => CompareOp::Ge,
        CompareOp::Gt => CompareOp::Lt,
        CompareOp::Ge => CompareOp::Le,
        o => o,
    }
}
fn program(x: usize, ps: &[Pred]) -> Option<Program> {
    let mut p = Program {
        eq: vec![],
        ne: BTreeMap::new(),
        bounds: vec![],
        gaps: vec![],
        guards: vec![],
    };
    for pred in ps {
        match pred {
            Pred::Compare(a, op, b) => {
                let (f, r, op) = match (a, b) {
                    (Ref::Field(i, f), r) if *i == x && r.slot() != Some(x) => {
                        (f.clone(), r.clone(), *op)
                    }
                    (r, Ref::Field(i, f)) if *i == x && r.slot() != Some(x) => {
                        (f.clone(), r.clone(), reverse(*op))
                    }
                    _ => return None,
                };
                match op {
                    CompareOp::Eq => p.eq.push((f, r)),
                    CompareOp::Ne => p.ne.entry(f).or_default().push(r),
                    _ if f == "order_seq" || f == "block_ts" => p.bounds.push((f, op, r)),
                    _ => return None,
                }
            }
            Pred::Gap {
                early,
                late,
                min,
                max,
            } if (*early == x) != (*late == x) => p.gaps.push((
                *early == x,
                if *early == x { *late } else { *early },
                *min,
                *max,
            )),
            _ => return None,
        }
    }
                                                                               
                                                                              
    if let Some(ids) = p.ne.get("event_id").cloned() {
        let keep = ids
            .into_iter()
            .filter(|r| !match r {
                Ref::Field(s, f) if f == "event_id" => {
                    p.bounds.iter().any(|(xf, op, r)| {
                        matches!(op, CompareOp::Lt | CompareOp::Gt)
                            && matches!(r,Ref::Field(t,g) if t==s&&g==xf)
                    }) || p.ne.get("tx_hash").is_some_and(|rs| {
                        rs.iter()
                            .any(|r| matches!(r,Ref::Field(t,g) if t==s&&g=="tx_hash"))
                    })
                }
                _ => false,
            })
            .collect::<Vec<_>>();
        if keep.is_empty() {
            p.ne.remove("event_id");
        } else {
            p.ne.insert("event_id".into(), keep);
        }
    }
                                                                             
                                                                                
    for (f, r) in &p.eq {
        if let Some(rs) = p.ne.remove(f) {
            for other in rs {
                p.guards
                    .push(Pred::Compare(r.clone(), CompareOp::Ne, other));
            }
        }
    }
                                                                               
    if p.ne.len() > 3 {
        return None;
    }
    Some(p)
}
struct Index {
    groups: BTreeMap<Vec<Scalar>, RangeGroup>,
    excluded: Vec<BTreeMap<(Vec<Scalar>, Vec<Scalar>), RangeGroup>>,
}
pub(super) fn eliminate(
    es: &[&CanonicalEvent],
    b: &Branch,
    x: usize,
    boundary: &[usize],
    ps: &[Pred],
    weights: &Factor,
    limit: u64,
    stat: &mut StepStats,
) -> Result<Option<BTreeMap<Vec<usize>, BigUint>>> {
    let Some(p) = program(x, ps) else {
        return Ok(None);
    };
    let fields = p.ne.keys().cloned().collect::<Vec<_>>();
    let mut groups = BTreeMap::<Vec<Scalar>, Vec<usize>>::new();
    for &i in &b.domains[x] {
        if weights.contains_unary(i) {
            let Some(k) =
                p.eq.iter()
                    .map(|(f, _)| es[i].value(f))
                    .collect::<Option<Vec<_>>>()
            else {
                return Ok(None);
            };
                                                                                 
            if fields.iter().any(|f| es[i].value(f).is_none()) {
                return Ok(None);
            }
            groups.entry(k).or_default().push(i);
        }
    }
    let mut idx = Index {
        groups: BTreeMap::new(),
        excluded: (0..(1 << fields.len())).map(|_| BTreeMap::new()).collect(),
    };
    for (k, ids) in groups {
        let g = RangeGroup::build(ids, es, weights);
        if !g
            .ids
            .windows(2)
            .all(|w| es[w[0]].block_ts <= es[w[1]].block_ts)
        {
            return Ok(None);
        }
        for mask in 1usize..1 << fields.len() {
            let mut sub = BTreeMap::<Vec<Scalar>, Vec<usize>>::new();
            for &i in &g.ids {
                let key = fields
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| mask >> j & 1 == 1)
                    .map(|(_, f)| es[i].value(f).unwrap())
                    .collect();
                sub.entry(key).or_default().push(i);
            }
            for (v, ids) in sub {
                idx.excluded[mask].insert((k.clone(), v), RangeGroup::build(ids, es, weights));
            }
        }
        idx.groups.insert(k, g);
    }
                                                                          
                                                                             
                                                                                 
    let bs = boundary.iter().copied().collect::<BTreeSet<_>>();
    let mut bp = b
        .predicates
        .iter()
        .filter(|p| p.deps().is_subset(&bs))
        .cloned()
        .collect::<Vec<_>>();
    bp.extend(p.guards.iter().cloned());
    for i in 0..p.eq.len() {
        for j in i + 1..p.eq.len() {
            if p.eq[i].0 == p.eq[j].0 {
                bp.push(Pred::Compare(
                    p.eq[i].1.clone(),
                    CompareOp::Eq,
                    p.eq[j].1.clone(),
                ));
            }
        }
    }
    let bb = Branch {
        id: b.id.clone(),
        atoms: b.atoms.clone(),
        domains: b.domains.clone(),
        predicates: bp,
    };
    let access = search_access(es, &bb, boundary, &[]);
    let mut ready = vec![vec![]; boundary.len()];
    for (i, p) in bb.predicates.iter().enumerate() {
        let pos = p
            .deps()
            .iter()
            .map(|s| boundary.iter().position(|v| v == s).unwrap())
            .max()
            .unwrap_or(0);
        ready[pos].push(i);
    }
    let mut row = vec![None; b.atoms.len()];
    let mut out = BTreeMap::new();
    fn visit(
        pos: usize,
        boundary: &[usize],
        access: &[SearchAccess],
        ready: &[Vec<usize>],
        bb: &Branch,
        row: &mut Assignment,
        es: &[&CanonicalEvent],
        p: &Program,
        idx: &Index,
        limit: u64,
        st: &mut StepStats,
        out: &mut BTreeMap<Vec<usize>, BigUint>,
    ) -> Result<()> {
        if pos < boundary.len() {
            let s = boundary[pos];
            for &i in access[pos].candidates(row, es, &bb.predicates).iter() {
                st.probes += 1;
                ensure!(
                    st.probes <= limit,
                    "multi-boundary resource limit; no partial count"
                );
                row[s] = Some(i);
                if ready[pos].iter().all(|j| bb.predicates[*j].holds(row, es)) {
                    visit(
                        pos + 1,
                        boundary,
                        access,
                        ready,
                        bb,
                        row,
                        es,
                        p,
                        idx,
                        limit,
                        st,
                        out,
                    )?;
                }
                row[s] = None;
            }
            return Ok(());
        }
        let Some(key) =
            p.eq.iter()
                .map(|(_, r)| r.value(row, es))
                .collect::<Option<Vec<_>>>()
        else {
            return Ok(());
        };
        let Some(g) = idx.groups.get(&key) else {
            return Ok(());
        };
        let (mut lo, mut hi, mut tl, mut th) = (i128::MIN, i128::MAX, i128::MIN, i128::MAX);
        for (f, op, r) in &p.bounds {
            let Some(Scalar::Int(v)) = r.value(row, es) else {
                return Ok(());
            };
            let v = v as i128;
            let (l, h) = if f == "order_seq" {
                (&mut lo, &mut hi)
            } else {
                (&mut tl, &mut th)
            };
            match op {
                CompareOp::Lt => *h = (*h).min(v - 1),
                CompareOp::Le => *h = (*h).min(v),
                CompareOp::Gt => *l = (*l).max(v + 1),
                CompareOp::Ge => *l = (*l).max(v),
                _ => unreachable!(),
            }
        }
        for (early, s, min, max) in &p.gaps {
            let t = es[row[*s].unwrap()].block_ts as i128;
            if *early {
                tl = tl.max(t - *max as i128);
                if let Some(m) = min {
                    th = th.min(t - *m as i128)
                }
            } else {
                th = th.min(t + *max as i128);
                if let Some(m) = min {
                    tl = tl.max(t + *m as i128)
                }
            }
        }
        let mut positive = g.sum(es, lo, hi, tl, th);
        let mut negative = BigUint::zero();
        st.range_requests += 1;
        let Some(forbidden) =
            p.ne.values()
                .map(|rs| {
                    rs.iter()
                        .map(|r| r.value(row, es))
                        .collect::<Option<BTreeSet<_>>>()
                })
                .collect::<Option<Vec<_>>>()
        else {
            return Ok(());
        };
        for mask in 1usize..idx.excluded.len() {
            let mut tuples = vec![vec![]];
            for (j, vs) in forbidden.iter().enumerate() {
                if mask >> j & 1 == 1 {
                    tuples = tuples
                        .into_iter()
                        .flat_map(|v| {
                            vs.iter().map(move |x| {
                                let mut v = v.clone();
                                v.push(x.clone());
                                v
                            })
                        })
                        .collect();
                }
            }
            for values in tuples {
                st.range_requests += 1;
                if let Some(sub) = idx.excluded[mask].get(&(key.clone(), values)) {
                    let v = sub.sum(es, lo, hi, tl, th);
                    if mask.count_ones() % 2 == 1 {
                        negative += v
                    } else {
                        positive += v
                    }
                }
            }
        }
        ensure!(
            positive >= negative,
            "joint exclusion inclusion-exclusion invariant"
        );
        let n = positive - negative;
        if !n.is_zero() {
            out.insert(boundary.iter().map(|s| row[*s].unwrap()).collect(), n);
        }
        Ok(())
    }
    stat.rule = "multi_boundary_joint_range_eliminate".into();
    visit(
        0, boundary, &access, &ready, &bb, &mut row, es, &p, &idx, limit, stat, &mut out,
    )?;
    Ok(Some(out))
}
