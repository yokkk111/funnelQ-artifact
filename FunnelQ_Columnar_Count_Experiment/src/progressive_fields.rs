                                                                           
                                                                            
                                                                                
                                                                              
use crate::{
    columns::{self, Column, Manifest, RowAnswer},
    engine::Engine,
    hybrid_key_audit::{data, Trade},
    hybrid_live_server,
    hybrid_packet_client::{field_hash, hash_node},
    key_projection::{KeyDomains, KeyPlan},
    model::Scalar,
    query::QuerySpec,
    temporal_demand::{local_domains, TemporalPlan},
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::Path,
    process::Command,
    sync::Arc,
    time::Instant,
};

const KEYS: [&str; 4] = ["collection_contract", "move_from", "move_to", "nft"];
const PAYLOAD: [&str; 5] = [
    "collection_contract",
    "move_from",
    "move_to",
    "nft",
    "tx_hash",
];
const WIDTHS: [usize; 3] = [4, 8, 12];
const PAGES: [usize; 3] = [16, 64, 256];
const MULTIPLIER: u32 = 0x9e3779b1;
const MAGIC: &[u8; 8] = b"FQPF0001";
type Cols = BTreeMap<String, Arc<Column>>;
type Demands = BTreeMap<String, Vec<usize>>;
type Trees = BTreeMap<(usize, String), Vec<Vec<[u8; 32]>>>;

#[derive(Serialize, Deserialize, Clone)]
struct Trusted {
    version: String,
    source: String,
    manifest: String,
    n: usize,
    integer_bits: usize,
    tx_bits: usize,
    multiplier: u32,
    coarse_roots: BTreeMap<String, String>,
    field_roots: BTreeMap<String, String>,
}
struct Bank {
    trusted: Trusted,
    coarse: BTreeMap<usize, Cols>,
    blobs: BTreeMap<(usize, String), Vec<u8>>,
    trees: Trees,
}
#[derive(Serialize, Deserialize)]
struct Header {
    version: String,
    trusted: String,
    query: String,
    bits: usize,
    page: usize,
    answers: Vec<RowAnswer>,
}
struct Fine {
    values: Vec<u32>,
    known: Vec<bool>,
}
impl Fine {
    fn new(n: usize) -> Self {
        Self {
            values: vec![0; n],
            known: vec![false; n],
        }
    }
}
struct State {
    ids: Arc<Vec<usize>>,
    domains: KeyDomains,
    full: Cols,
    fine: BTreeMap<String, Fine>,
    stages: Value,
}
struct Packet {
    wire: Vec<u8>,
    parts: Value,
    candidates: usize,
    coarse_candidates: usize,
    field_rows: usize,
    proof_bytes: usize,
}

fn write_new(p: &Path, b: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)?
        .write_all(b)?;
    Ok(())
}
fn put(b: &mut Vec<u8>, n: usize) {
    b.extend((n as u64).to_le_bytes());
}
fn frame(b: &mut Vec<u8>, v: &[u8]) {
    put(b, v.len());
    b.extend(v);
}
fn take<'a>(b: &'a [u8], p: &mut usize, n: usize) -> Result<&'a [u8]> {
    ensure!(
        n <= b.len().saturating_sub(*p),
        "truncated progressive packet"
    );
    let x = &b[*p..*p + n];
    *p += n;
    Ok(x)
}
fn read(b: &[u8], p: &mut usize) -> Result<usize> {
    Ok(usize::try_from(u64::from_le_bytes(
        take(b, p, 8)?.try_into()?,
    ))?)
}
fn blob<'a>(b: &'a [u8], p: &mut usize) -> Result<&'a [u8]> {
    let n = read(b, p)?;
    take(b, p, n)
}
fn var(b: &mut Vec<u8>, mut x: usize) {
    while x >= 128 {
        b.push((x as u8) | 128);
        x >>= 7;
    }
    b.push(x as u8);
}
fn unvar(b: &[u8], p: &mut usize) -> Result<usize> {
    let mut x = 0u64;
    for s in (0..=63).step_by(7) {
        let a = take(b, p, 1)?[0];
        ensure!(s < 63 || a <= 1, "varint overflow");
        x |= ((a & 127) as u64) << s;
        if a < 128 {
            return Ok(usize::try_from(x)?);
        }
    }
    anyhow::bail!("varint overflow")
}
fn mask(bits: usize) -> u32 {
    if bits == 32 {
        u32::MAX
    } else {
        (1u32 << bits) - 1
    }
}
fn inverse(x: u32) -> u32 {
    let mut y = x;
    for _ in 0..5 {
        y = y.wrapping_mul(2u32.wrapping_sub(x.wrapping_mul(y)));
    }
    y
}
fn code(x: u32, t: &Trusted) -> u32 {
    x.wrapping_mul(t.multiplier) & mask(t.integer_bits)
}
fn original(x: u32, t: &Trusted) -> u32 {
    x.wrapping_mul(inverse(t.multiplier)) & mask(t.integer_bits)
}
fn width(x: u32) -> usize {
    (32 - x.leading_zeros() as usize).max(1)
}
fn value(r: &Trade, f: &str) -> u32 {
    match f {
        "collection_contract" => r.collection,
        "move_from" => r.src,
        "move_to" => r.dst,
        "nft" => r.nft,
        "tx_hash" => r.tx,
        _ => unreachable!(),
    }
}
fn scalar(x: u32, f: &str) -> Scalar {
    if f == "tx_hash" {
        Scalar::Text(format!("tx:{x}"))
    } else {
        Scalar::Int(x as i64)
    }
}
fn pack(xs: impl Iterator<Item = u32>, n: usize, bits: usize) -> Result<Vec<u8>> {
    ensure!((1..=32).contains(&bits), "packed width");
    let mut b = Vec::with_capacity((n * bits).div_ceil(8));
    let (mut a, mut used, mut count) = (0u64, 0usize, 0usize);
    for x in xs {
        ensure!(x <= mask(bits), "packed value overflow");
        a |= (x as u64) << used;
        used += bits;
        count += 1;
        while used >= 8 {
            b.push(a as u8);
            a >>= 8;
            used -= 8;
        }
    }
    if used > 0 {
        b.push(a as u8);
    }
    ensure!(count == n, "packed row count");
    Ok(b)
}
fn unpack(b: &[u8], n: usize, bits: usize) -> Result<Vec<u32>> {
    ensure!(
        (1..=32).contains(&bits) && b.len() == (n * bits).div_ceil(8),
        "packed length"
    );
    let (mut a, mut used, mut p) = (0u64, 0usize, 0usize);
    let mut v = Vec::with_capacity(n);
    for _ in 0..n {
        while used < bits {
            a |= (b[p] as u64) << used;
            p += 1;
            used += 8;
        }
        v.push((a as u32) & mask(bits));
        a >>= bits;
        used -= bits;
    }
    ensure!(a == 0, "packed padding");
    Ok(v)
}
fn coarse_hash(t: &Trusted, f: &str, bits: usize, b: &[u8]) -> Result<String> {
    let mut h = Sha256::new();
    h.update(serde_json::to_vec(&(
        "complete-reversible-prefix/v1",
        &t.source,
        t.n,
        t.integer_bits,
        t.multiplier,
        f,
        bits,
        b.len(),
    ))?);
    h.update(b);
    Ok(format!("{:x}", h.finalize()))
}
fn leaf(t: &Trusted, f: &str, page: usize, index: usize, v: &[u32]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"progressive-field-page/v1");
    h.update(t.source.as_bytes());
    h.update(f.as_bytes());
    for n in [t.n, page, index, v.len()] {
        h.update((n as u64).to_le_bytes());
    }
    for x in v {
        h.update(x.to_le_bytes());
    }
    h.finalize().into()
}
fn hex(x: &[u8; 32]) -> String {
    x.iter().map(|v| format!("{v:02x}")).collect()
}
fn tree(rows: &[Trade], t: &Trusted, f: &str, page: usize) -> Vec<Vec<[u8; 32]>> {
    let cap = rows.len().div_ceil(page).next_power_of_two();
    let mut base = Vec::with_capacity(cap);
    for (i, rs) in rows.chunks(page).enumerate() {
        base.push(leaf(
            t,
            f,
            page,
            i,
            &rs.iter().map(|r| value(r, f)).collect::<Vec<_>>(),
        ));
    }
    for i in base.len()..cap {
        base.push(leaf(t, f, page, i, &[]));
    }
    let mut levels = vec![base];
    while levels.last().unwrap().len() > 1 {
        let next = levels
            .last()
            .unwrap()
            .chunks_exact(2)
            .map(|a| hash_node(&a[0], &a[1]))
            .collect();
        levels.push(next);
    }
    levels
}
fn proof(levels: &[Vec<[u8; 32]>], ids: &[usize]) -> Vec<u8> {
    let mut active = ids.iter().copied().collect::<BTreeSet<_>>();
    let mut b = vec![];
    for l in &levels[..levels.len() - 1] {
        for &i in &active {
            if !active.contains(&(i ^ 1)) {
                b.extend(l[i ^ 1]);
            }
        }
        active = active.into_iter().map(|i| i / 2).collect();
    }
    b
}
fn verify(mut a: BTreeMap<usize, [u8; 32]>, b: &[u8], height: usize, root: &str) -> Result<()> {
    ensure!(b.len() % 32 == 0, "field proof framing");
    if a.is_empty() {
        ensure!(b.is_empty(), "empty field proof");
        return Ok(());
    }
    let mut p = 0;
    for _ in 0..height {
        let mut next = BTreeMap::new();
        for (&i, v) in &a {
            let s = if let Some(v) = a.get(&(i ^ 1)) {
                *v
            } else {
                take(b, &mut p, 32)?.try_into()?
            };
            let h = if i % 2 == 0 {
                hash_node(v, &s)
            } else {
                hash_node(&s, v)
            };
            if let Some(old) = next.insert(i / 2, h) {
                ensure!(old == h, "inconsistent field parent");
            }
        }
        a = next;
    }
    ensure!(
        p == b.len() && a.len() == 1 && a.contains_key(&0),
        "field proof length"
    );
    ensure!(hex(&a[&0]) == root, "field root mismatch");
    Ok(())
}
fn prepare(rows: &[Trade], seal: &columns::Seal) -> Result<Bank> {
    ensure!(!rows.is_empty(), "nonempty source");
    let base = &seal.tables["NFTTrade"];
    ensure!(base.n == rows.len(), "source layout");
    for (i, r) in rows.iter().enumerate() {
        for f in PAYLOAD {
            ensure!(
                base.columns[f].get(i) == Some(&scalar(value(r, f), f)),
                "full source field consistency"
            );
        }
        for (f, x) in [("order_seq", r.order), ("block_ts", r.time)] {
            ensure!(
                base.columns[f].get(i) == Some(&Scalar::Int(i64::try_from(x)?)),
                "time source consistency"
            );
        }
        ensure!(
            base.columns["event_id"].get(i) == Some(&Scalar::Text(format!("order:{}", r.order))),
            "source identity consistency"
        );
    }
    let mut t = Trusted {
        version: "progressive-field-seal/v1".into(),
        source: seal.manifest.source_digest.clone(),
        manifest: columns::digest(&serde_json::to_vec(&seal.manifest)?),
        n: rows.len(),
        integer_bits: width(
            rows.iter()
                .flat_map(|r| KEYS.map(|f| value(r, f)))
                .max()
                .unwrap(),
        )
        .max(16),
        tx_bits: width(rows.iter().map(|r| r.tx).max().unwrap()),
        multiplier: MULTIPLIER,
        coarse_roots: BTreeMap::new(),
        field_roots: BTreeMap::new(),
    };
    let (mut coarse, mut blobs, mut trees) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    for bits in WIDTHS {
        ensure!(bits < t.integer_bits, "refinement needs suffix bits");
        let mut cols = BTreeMap::new();
        for f in KEYS {
            let xs = rows
                .iter()
                .map(|r| code(value(r, f), &t) >> (t.integer_bits - bits))
                .collect::<Vec<_>>();
            let b = pack(xs.iter().copied(), rows.len(), bits)?;
            t.coarse_roots
                .insert(format!("{bits}:{f}"), coarse_hash(&t, f, bits, &b)?);
            cols.insert(
                f.into(),
                Arc::new(Column::from_values(
                    xs.into_iter().map(|x| Some(Scalar::Int(x as i64))),
                )),
            );
            blobs.insert((bits, f.into()), b);
        }
        coarse.insert(bits, cols);
    }
    for page in PAGES {
        for f in PAYLOAD {
            let l = tree(rows, &t, f, page);
            t.field_roots
                .insert(format!("{page}:{f}"), hex(&l.last().unwrap()[0]));
            trees.insert((page, f.into()), l);
        }
    }
    Ok(Bank {
        trusted: t,
        coarse,
        blobs,
        trees,
    })
}
fn temporal_columns(seal: &columns::Seal) -> Cols {
    ["block_ts", "order_seq"]
        .into_iter()
        .map(|f| (f.into(), seal.tables["NFTTrade"].columns[f].clone()))
        .collect()
}
fn coarse_domain(q: &QuerySpec, cols: &Cols, n: usize) -> Result<(KeyPlan, KeyDomains, Value)> {
    let t = Instant::now();
    let p = KeyPlan::compile(q)?;
    ensure!(
        p.event_type == "NFTTrade" && p.fields.iter().all(|f| KEYS.contains(&f.as_str())),
        "integer NFTTrade field envelope"
    );
    let mut d = p.execute_domains(cols, n)?;
    let key_s = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let stats = TemporalPlan::compile(q)?.apply(&p, cols, &mut d)?;
    Ok((
        p,
        d,
        json!({"coarse_key_s":key_s,"coarse_temporal_s":t.elapsed().as_secs_f64(),"coarse_temporal_stats":stats}),
    ))
}
fn demands(p: &KeyPlan, d: &KeyDomains, page: usize) -> Demands {
    let mut atom_fields = vec![BTreeSet::new(); d.per_atom.len()];
    for pair in &p.pairs {
        atom_fields[pair.a].extend(pair.left.iter().cloned());
        atom_fields[pair.b].extend(pair.right.iter().cloned());
    }
    p.fields
        .iter()
        .map(|f| {
            let ids = (0..d.union.len())
                .filter(|&r| {
                    d.per_atom
                        .iter()
                        .enumerate()
                        .any(|(a, m)| m[r] && atom_fields[a].contains(f))
                })
                .map(|r| r / page)
                .collect::<BTreeSet<_>>();
            (f.clone(), ids.into_iter().collect())
        })
        .collect()
}
fn open_source(rows: &[Trade], request: &Demands, page: usize, fine: &mut BTreeMap<String, Fine>) {
    for (f, ids) in request {
        let v = fine
            .entry(f.clone())
            .or_insert_with(|| Fine::new(rows.len()));
        for &i in ids {
            for r in i * page..((i + 1) * page).min(rows.len()) {
                v.values[r] = value(&rows[r], f);
                v.known[r] = true;
            }
        }
    }
}
fn partial_columns(
    ids: &[usize],
    time: &Cols,
    fine: &BTreeMap<String, Fine>,
    fields: &BTreeSet<String>,
) -> Result<Cols> {
    let selected = Arc::new(ids.to_vec());
    let mut cols = time
        .iter()
        .map(|(f, c)| Ok((f.clone(), Arc::new(c.project(selected.clone())?))))
        .collect::<Result<Cols>>()?;
    for f in fields {
        let v = &fine[f];
        cols.insert(
            f.clone(),
            Arc::new(Column::from_values(
                ids.iter()
                    .map(|&r| v.known[r].then(|| scalar(v.values[r], f))),
            )),
        );
    }
    Ok(cols)
}
fn exact_domain(
    q: &QuerySpec,
    p: &KeyPlan,
    coarse: &KeyDomains,
    time: &Cols,
    fine: &BTreeMap<String, Fine>,
) -> Result<(Arc<Vec<usize>>, KeyDomains, Value)> {
    let t = Instant::now();
    let ids = Arc::new(
        (0..coarse.union.len())
            .filter(|&r| coarse.union[r])
            .collect::<Vec<_>>(),
    );
    let cols = partial_columns(&ids, time, fine, &p.fields)?;
    let mut d = p.execute_domains(&cols, ids.len())?;
    let stats = TemporalPlan::compile(q)?.apply(p, &cols, &mut d)?;
    d.union.fill(false);
    for (a, m) in d.per_atom.iter_mut().enumerate() {
        for (i, x) in m.iter_mut().enumerate() {
            *x &= coarse.per_atom[a][ids[i]];
            d.union[i] |= *x;
        }
    }
    Ok((
        ids,
        d,
        json!({"exact_refinement_s":t.elapsed().as_secs_f64(),"exact_temporal_stats":stats}),
    ))
}
fn remaining(ids: &[usize], fine: &BTreeMap<String, Fine>, page: usize) -> Demands {
    PAYLOAD
        .into_iter()
        .map(|f| {
            let req = ids
                .iter()
                .filter(|&&r| fine.get(f).is_none_or(|v| !v.known[r]))
                .map(|&r| r / page)
                .collect::<BTreeSet<_>>();
            (f.into(), req.into_iter().collect())
        })
        .filter(|(_, v): &(String, Vec<usize>)| !v.is_empty())
        .collect()
}
fn final_tables(state: &State) -> Result<BTreeMap<String, columns::Table>> {
    let source = Arc::new(
        state
            .ids
            .iter()
            .enumerate()
            .filter_map(|(i, &r)| state.domains.union[i].then_some(r))
            .collect::<Vec<_>>(),
    );
    let mut cols = BTreeMap::new();
    for f in PAYLOAD {
        let v = state.fine.get(f).context("missing exact field")?;
        ensure!(
            source.iter().all(|&r| v.known[r]),
            "missing demanded exact value"
        );
        cols.insert(
            f.into(),
            Arc::new(Column::from_values(
                source.iter().map(|&r| Some(scalar(v.values[r], f))),
            )),
        );
    }
    for (f, c) in &state.full {
        cols.insert(f.clone(), Arc::new(c.project(source.clone())?));
    }
    let orders = cols["order_seq"].clone();
    cols.insert(
        "event_id".into(),
        Arc::new(Column::from_values((0..source.len()).map(
            |i| match orders.get(i) {
                Some(Scalar::Int(o)) => Some(Scalar::Text(format!("order:{o}"))),
                _ => unreachable!(),
            },
        ))),
    );
    cols.insert(
        "event_type".into(),
        Arc::new(Column::from_values(
            (0..source.len()).map(|_| Some(Scalar::Text("NFTTrade".into()))),
        )),
    );
    columns::sparse_view("NFTTrade", cols, source)
}
fn atomic_final(q: &QuerySpec, state: &State) -> Result<BTreeMap<(String, String), Vec<usize>>> {
    let local = (0..state.ids.len())
        .filter(|&i| state.domains.union[i])
        .collect::<Vec<_>>();
    local_domains(q, &state.domains, &local)
}
fn server_state(
    rows: &[Trade],
    seal: &columns::Seal,
    bank: &Bank,
    q: &QuerySpec,
    bits: usize,
    page: usize,
) -> Result<(State, Demands, Demands)> {
    let full = temporal_columns(seal);
    let mut cols = bank.coarse[&bits].clone();
    cols.extend(full.clone());
    let (p, d, mut stages) = coarse_domain(q, &cols, rows.len())?;
    let first = demands(&p, &d, page);
    let mut fine = BTreeMap::new();
    open_source(rows, &first, page, &mut fine);
    let (ids, exact, refine) = exact_domain(q, &p, &d, &full, &fine)?;
    stages
        .as_object_mut()
        .unwrap()
        .extend(refine.as_object().unwrap().clone());
    let final_ids = ids
        .iter()
        .enumerate()
        .filter_map(|(i, &r)| exact.union[i].then_some(r))
        .collect::<Vec<_>>();
    let second = remaining(&final_ids, &fine, page);
    open_source(rows, &second, page, &mut fine);
    for f in PAYLOAD {
        fine.entry(f.into())
            .or_insert_with(|| Fine::new(rows.len()));
    }
    Ok((
        State {
            ids,
            domains: exact,
            full,
            fine,
            stages,
        },
        first,
        second,
    ))
}
fn encode_fields(
    out: &mut Vec<u8>,
    rows: &[Trade],
    bank: &Bank,
    bits: usize,
    page: usize,
    req: &Demands,
    coarse: &Cols,
) -> Result<(usize, usize)> {
    put(out, req.len());
    let (mut total, mut hashes) = (0, 0);
    for (f, indices) in req {
        frame(out, f.as_bytes());
        put(out, indices.len());
        let mut last = 0;
        for &i in indices {
            var(out, i - last);
            last = i;
        }
        let ids = indices
            .iter()
            .flat_map(|&i| i * page..((i + 1) * page).min(rows.len()))
            .collect::<Vec<_>>();
        total += ids.len();
        let suffix = coarse.contains_key(f);
        let w = if suffix {
            bank.trusted.integer_bits - bits
        } else if f == "tx_hash" {
            bank.trusted.tx_bits
        } else {
            bank.trusted.integer_bits
        };
        let b = pack(
            ids.iter().map(|&r| {
                if f == "tx_hash" {
                    value(&rows[r], f)
                } else {
                    code(value(&rows[r], f), &bank.trusted) & mask(w)
                }
            }),
            ids.len(),
            w,
        )?;
        frame(out, &b);
        let pr = proof(&bank.trees[&(page, f.clone())], indices);
        hashes += pr.len();
        frame(out, &pr);
    }
    Ok((total, hashes))
}
fn decode_fields(
    b: &[u8],
    t: &Trusted,
    bits: usize,
    page: usize,
    req: &Demands,
    coarse: &Cols,
    fine: &mut BTreeMap<String, Fine>,
) -> Result<(usize, usize)> {
    let mut p = 0;
    ensure!(
        read(b, &mut p)? == req.len(),
        "incomplete field demand coverage"
    );
    let (mut rows, mut hashes) = (0, 0);
    for (f, expected) in req {
        ensure!(blob(b, &mut p)? == f.as_bytes(), "field demand order");
        let n = read(b, &mut p)?;
        ensure!(n == expected.len(), "incomplete field page coverage");
        let (mut ids, mut last) = (Vec::with_capacity(n), 0usize);
        for i in 0..n {
            last = last
                .checked_add(unvar(b, &mut p)?)
                .context("field index overflow")?;
            ensure!(i == 0 || last > ids[i - 1], "duplicate field page");
            ids.push(last);
        }
        ensure!(ids == *expected, "incomplete field page coverage");
        let count = ids
            .iter()
            .map(|&i| (t.n - i * page).min(page))
            .sum::<usize>();
        rows += count;
        let suffix = coarse.contains_key(f);
        let w = if suffix {
            t.integer_bits - bits
        } else if f == "tx_hash" {
            t.tx_bits
        } else {
            t.integer_bits
        };
        let data = unpack(blob(b, &mut p)?, count, w)?;
        let pr = blob(b, &mut p)?;
        hashes += pr.len();
        let mut leaves = BTreeMap::new();
        let mut at = 0;
        let vals = fine.entry(f.clone()).or_insert_with(|| Fine::new(t.n));
        for &i in &ids {
            let mut leaf_values = vec![];
            for r in i * page..((i + 1) * page).min(t.n) {
                let raw = data[at];
                at += 1;
                let x = if f == "tx_hash" {
                    raw
                } else {
                    let high = if suffix {
                        match coarse[f].get(r) {
                            Some(Scalar::Int(x)) => u32::try_from(*x)?,
                            _ => anyhow::bail!("coarse integer"),
                        }
                    } else {
                        0
                    };
                    original(if suffix { (high << w) | raw } else { raw }, t)
                };
                ensure!(
                    !vals.known[r] || vals.values[r] == x,
                    "inconsistent repeated field"
                );
                vals.values[r] = x;
                vals.known[r] = true;
                leaf_values.push(x);
            }
            leaves.insert(i, leaf(t, f, page, i, &leaf_values));
        }
        verify(
            leaves,
            pr,
            t.n.div_ceil(page).next_power_of_two().ilog2() as usize,
            &t.field_roots[&format!("{page}:{f}")],
        )?;
    }
    ensure!(p == b.len(), "trailing field section");
    Ok((rows, hashes))
}
fn packet(
    rows: &[Trade],
    seal: &columns::Seal,
    bank: &Bank,
    q: &QuerySpec,
    bits: usize,
    page: usize,
    state: &State,
    first: &Demands,
    second: &Demands,
    answers: Vec<RowAnswer>,
) -> Result<Packet> {
    let h = Header {
        version: "progressive-fields/v1".into(),
        trusted: columns::digest(&serde_json::to_vec(&bank.trusted)?),
        query: columns::digest(&serde_json::to_vec(q)?),
        bits,
        page,
        answers,
    };
    let fields = KeyPlan::compile(q)?.fields;
    let coarse = bank.coarse[&bits]
        .iter()
        .filter(|(f, _)| fields.contains(*f))
        .map(|(f, c)| (f.clone(), c.clone()))
        .collect::<Cols>();
    let mut whole = vec![];
    for f in fields {
        frame(&mut whole, &bank.blobs[&(bits, f)]);
    }
    for f in ["block_ts", "order_seq"] {
        frame(&mut whole, &seal.blobs[&("NFTTrade".into(), f.into())]);
    }
    let mut stage1 = vec![];
    let (r1, p1) = encode_fields(&mut stage1, rows, bank, bits, page, first, &coarse)?;
    let mut stage2 = vec![];
    let (r2, p2) = encode_fields(&mut stage2, rows, bank, bits, page, second, &coarse)?;
    let raw = [serde_json::to_vec(&h)?, whole, stage1, stage2];
    let zipped = raw
        .iter()
        .map(|b| crate::compress(b))
        .collect::<Result<Vec<_>>>()?;
    let mut wire = MAGIC.to_vec();
    for b in &zipped {
        frame(&mut wire, b);
    }
    let parts = json!({"header_answers":zipped[0].len(),"complete_coarse_time":zipped[1].len(),"key_refinement_with_proofs":zipped[2].len(),"remaining_fields_with_proofs":zipped[3].len(),"framing":40,
        "raw_key_refinement":raw[2].len(),"raw_remaining":raw[3].len(),"field_page_rows":r1+r2,"merkle_hash_bytes":p1+p2});
    Ok(Packet {
        wire,
        parts,
        candidates: state.domains.union.iter().filter(|x| **x).count(),
        coarse_candidates: state.ids.len(),
        field_rows: r1 + r2,
        proof_bytes: p1 + p2,
    })
}
fn sections(wire: &[u8]) -> Result<Vec<Vec<u8>>> {
    let mut p = 0;
    ensure!(take(wire, &mut p, 8)? == MAGIC, "progressive magic");
    let mut v = vec![];
    for _ in 0..4 {
        v.push(crate::decompress(blob(wire, &mut p)?)?);
    }
    ensure!(p == wire.len(), "trailing response");
    Ok(v)
}
fn authenticate(
    wire: &[u8],
    trusted: &Trusted,
    manifest: &Manifest,
    q: &QuerySpec,
    mode: &str,
) -> Result<Value> {
    let start = Instant::now();
    ensure!(matches!(mode, "count" | "replay" | "both"), "client mode");
    let ss = sections(wire)?;
    let h: Header = serde_json::from_slice(&ss[0])?;
    ensure!(
        h.version == "progressive-fields/v1"
            && h.trusted == columns::digest(&serde_json::to_vec(trusted)?)
            && h.query == columns::digest(&serde_json::to_vec(q)?),
        "epoch/query binding"
    );
    ensure!(
        trusted.manifest == columns::digest(&serde_json::to_vec(manifest)?)
            && trusted.source == manifest.source_digest
            && trusted.n == manifest.types["NFTTrade"].n,
        "trusted source layout"
    );
    ensure!(
        WIDTHS.contains(&h.bits)
            && PAGES.contains(&h.page)
            && h.bits < trusted.integer_bits
            && trusted.integer_bits <= 32
            && trusted.tx_bits <= 32
            && trusted.multiplier % 2 == 1,
        "refinement parameters"
    );
    let plan = KeyPlan::compile(q)?;
    ensure!(
        plan.event_type == "NFTTrade" && plan.fields.iter().all(|f| KEYS.contains(&f.as_str())),
        "integer field envelope"
    );
    let mut p = 0;
    let (mut coarse, mut full) = (BTreeMap::new(), BTreeMap::new());
    for f in &plan.fields {
        let b = blob(&ss[1], &mut p)?;
        ensure!(
            coarse_hash(trusted, f, h.bits, b)? == trusted.coarse_roots[&format!("{}:{f}", h.bits)],
            "coarse column authentication"
        );
        let xs = unpack(b, trusted.n, h.bits)?;
        coarse.insert(
            f.clone(),
            Arc::new(Column::from_values(
                xs.into_iter().map(|x| Some(Scalar::Int(x as i64))),
            )),
        );
    }
    for f in ["block_ts", "order_seq"] {
        let b = blob(&ss[1], &mut p)?;
        ensure!(
            field_hash(manifest, f, b)? == manifest.types["NFTTrade"].fields[f],
            "complete time authentication"
        );
        full.insert(f.into(), Arc::new(Column::decode(b, trusted.n)?));
    }
    ensure!(p == ss[1].len(), "trailing complete columns");
    let complete_s = start.elapsed().as_secs_f64();
    let mut cols = coarse.clone();
    cols.extend(full.clone());
    let (_, d, mut stages) = coarse_domain(q, &cols, trusted.n)?;
    let first = demands(&plan, &d, h.page);
    let t = Instant::now();
    let mut fine = BTreeMap::new();
    let (r1, p1) = decode_fields(&ss[2], trusted, h.bits, h.page, &first, &coarse, &mut fine)?;
    let fine1_s = t.elapsed().as_secs_f64();
    let (ids, exact, refine) = exact_domain(q, &plan, &d, &full, &fine)?;
    stages
        .as_object_mut()
        .unwrap()
        .extend(refine.as_object().unwrap().clone());
    let final_ids = ids
        .iter()
        .enumerate()
        .filter_map(|(i, &r)| exact.union[i].then_some(r))
        .collect::<Vec<_>>();
    let second = remaining(&final_ids, &fine, h.page);
    let t = Instant::now();
    let (r2, p2) = decode_fields(&ss[3], trusted, h.bits, h.page, &second, &coarse, &mut fine)?;
    let fine2_s = t.elapsed().as_secs_f64();
    for f in PAYLOAD {
        fine.entry(f.into()).or_insert_with(|| Fine::new(trusted.n));
    }
    let state = State {
        ids,
        domains: exact,
        full,
        fine,
        stages,
    };
    let t = Instant::now();
    let tables = final_tables(&state)?;
    let rs = columns::rows(&tables)?;
    let atomic = atomic_final(q, &state)?;
    let eng = Engine::with_verified_domains(
        &rs,
        q,
        &BTreeSet::from(["NFTTrade".into()]),
        None,
        Some(&atomic),
    )?;
    let compile_s = t.elapsed().as_secs_f64();
    let mut claims = h.answers;
    claims.sort();
    ensure!(claims.windows(2).all(|w| w[0] != w[1]), "duplicate answer");
    let (mut count_s, mut sound_s, mut replay_s) = (0., 0., 0.);
    if mode != "replay" {
        let t = Instant::now();
        let count = eng.count()?.0;
        count_s = t.elapsed().as_secs_f64();
        let t = Instant::now();
        eng.sound_unique(&claims)?;
        ensure!(
            count == num_bigint::BigUint::from(claims.len()),
            "cardinality seal mismatch"
        );
        sound_s = t.elapsed().as_secs_f64();
    }
    if mode != "count" {
        let t = Instant::now();
        let mut ans = eng.replay_rows()?.0;
        ans.sort();
        ensure!(ans == claims, "full binding set mismatch");
        replay_s = t.elapsed().as_secs_f64();
    }
    Ok(
        json!({"verified":true,"mode":mode,"answers":claims.len(),"wire_bytes":wire.len(),"candidate_rows":final_ids.len(),"coarse_candidates":state.ids.len(),"field_page_rows":r1+r2,"proof_bytes":p1+p2,
        "complete_auth_s":complete_s,"key_field_auth_s":fine1_s,"remaining_field_auth_s":fine2_s,"stages":state.stages,"subset_compile_s":compile_s,"exact_count_s":count_s,"sound_unique_s":sound_s,"exact_replay_s":replay_s,
        "client_s":start.elapsed().as_secs_f64(),"client_process_peak_rss_bytes":crate::memory::peak_bytes()}),
    )
}
pub fn client(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 9,
        "progressive-client DOC COMPILED TRUSTED MANIFEST RESPONSE OUTPUT MODE"
    );
    let q = crate::load_query(Path::new(&args[2]))?;
    let compiled: QuerySpec = serde_json::from_slice(&fs::read(&args[3])?)?;
    ensure!(
        serde_json::to_vec(&q)? == serde_json::to_vec(&compiled)?,
        "compiled QuerySpec mismatch"
    );
    let trust: Trusted = serde_json::from_slice(&fs::read(&args[4])?)?;
    let m: Manifest = serde_json::from_slice(&fs::read(&args[5])?)?;
    let wire = fs::read(&args[6])?;
    let r = authenticate(&wire, &trust, &m, &q, &args[8])?;
    crate::save(Path::new(&args[7]), &r)?;
    eprintln!("PROGRESSIVE_CLIENT {}", r);
    Ok(())
}

pub fn run(args: &[String]) -> Result<()> {
    ensure!(args.len() == 5, "progressive-fields ROOT NEW_OUTPUT ROUNDS");
    let root = Path::new(&args[2]);
    let out = Path::new(&args[3]);
    let rounds: usize = args[4].parse()?;
    ensure!((1..=5).contains(&rounds), "rounds");
    fs::create_dir(out)?;
    let t = Instant::now();
    let trades = data(root)?;
    let (events, source) = crate::events(root, None)?;
    let seal = columns::seal(&events, source.clone())?;
    drop(events);
    let bank = prepare(&trades, &seal)?;
    let old_tree = hybrid_live_server::tree(&trades)?;
    let offline_s = t.elapsed().as_secs_f64();
    crate::save(&out.join("trusted_progressive.json"), &bank.trusted)?;
    crate::save(&out.join("trusted_manifest.json"), &seal.manifest)?;
    crate::save(
        &out.join("trusted_row_root.json"),
        &json!({"nrows":trades.len(),"block_rows":16,"root":hex(&old_tree.last().unwrap()[0]),"height":old_tree.len()-1}),
    )?;
    let mut paths = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|p| p.map(|p| p.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    ensure!(paths.len() == 5, "formal five query workload");
    let qs = paths
        .iter()
        .map(|p| crate::load_query(p))
        .collect::<Result<Vec<_>>>()?;
    let mut grid = vec![];
                                                                                 
    let mut reference = vec![];
    for (i, q) in qs.iter().enumerate() {
        let t = Instant::now();
        let rs = columns::rows(&seal.tables)?;
        let answers = Engine::new(&rs, q, &BTreeSet::from(["NFTTrade".into()]))?
            .replay_rows()?
            .0;
        let oracle = crate::temporal_compare::oracle(root, i + 1, &trades, &answers)?;
        crate::save(
            &out.join(format!("q{}_reference_check.json", i + 1)),
            &json!({"FP":0,"FN":0,"answers":answers.len(),"source":source,"query_hash":columns::digest(&serde_json::to_vec(q)?),"oracle_hash":oracle,"fresh_full_generic_s":t.elapsed().as_secs_f64()}),
        )?;
        reference.push(answers);
    }
    for bits in WIDTHS {
        for page in PAGES {
            for (i, q) in qs.iter().enumerate() {
                let t = Instant::now();
                let (state, first, second) = server_state(&trades, &seal, &bank, q, bits, page)?;
                let tables = final_tables(&state)?;
                let rs = columns::rows(&tables)?;
                let atom = atomic_final(q, &state)?;
                let eng = Engine::with_verified_domains(
                    &rs,
                    q,
                    &BTreeSet::from(["NFTTrade".into()]),
                    None,
                    Some(&atom),
                )?;
                let mut answers = eng.replay_rows()?.0;
                answers.sort();
                let mut expected = reference[i].clone();
                expected.sort();
                ensure!(answers == expected, "grid full binding equality");
                let query_s = t.elapsed().as_secs_f64();
                let t = Instant::now();
                let packet = packet(
                    &trades, &seal, &bank, q, bits, page, &state, &first, &second, answers,
                )?;
                let packet_s = t.elapsed().as_secs_f64();
                let checked = authenticate(&packet.wire, &bank.trusted, &seal.manifest, q, "both")?;
                let record = json!({"query":i+1,"bits":bits,"page":page,"wire_bytes":packet.wire.len(),"parts":packet.parts,"coarse_candidates":packet.coarse_candidates,"candidate_rows":packet.candidates,"field_page_rows":packet.field_rows,"proof_bytes":packet.proof_bytes,"query_s":query_s,"packet_s":packet_s,"client":checked,"FP":0,"FN":0});
                eprintln!("PROGRESSIVE_GRID Q{} bits={bits} page={page} wire={:.3}MB coarse={} exact={} FP=FN=0",i+1,packet.wire.len()as f64/1e6,packet.coarse_candidates,packet.candidates);
                crate::save(
                    &out.join(format!("q{}_b{bits}_p{page}_screen.json", i + 1)),
                    &record,
                )?;
                grid.push(record);
            }
        }
    }
    crate::save(&out.join("grid.json"), &grid)?;
    let (bits, page) = WIDTHS
        .into_iter()
        .flat_map(|b| PAGES.map(move |p| (b, p)))
        .min_by_key(|&(b, p)| {
            grid.iter()
                .filter(|r| r["bits"] == b && r["page"] == p)
                .map(|r| r["wire_bytes"].as_u64().unwrap())
                .sum::<u64>()
        })
        .unwrap();
    eprintln!(
        "PROGRESSIVE_UNIFORM bits={bits} page={page}; selected by sum of five response bytes"
    );
    let mut records = vec![];
    for rep in 0..=rounds {
        for i in if rep % 2 == 0 {
            (0..5).collect::<Vec<_>>()
        } else {
            (0..5).rev().collect()
        } {
            let q = &qs[i];
            for progressive in if (rep + i) % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                let backend = if progressive {
                    "progressive"
                } else {
                    "temporal"
                };
                let dir = out.join(format!("q{}_r{rep}_{backend}", i + 1));
                fs::create_dir(&dir)?;
                let t = Instant::now();
                let (wire, parts, candidates, field_rows, proof_bytes, query_s, packet_s, stage) =
                    if progressive {
                        let (state, first, second) =
                            server_state(&trades, &seal, &bank, q, bits, page)?;
                        let tables = final_tables(&state)?;
                        let rs = columns::rows(&tables)?;
                        let atom = atomic_final(q, &state)?;
                        let eng = Engine::with_verified_domains(
                            &rs,
                            q,
                            &BTreeSet::from(["NFTTrade".into()]),
                            None,
                            Some(&atom),
                        )?;
                        let ans = eng.replay_rows()?.0;
                        let query_s = t.elapsed().as_secs_f64();
                        crate::temporal_compare::oracle(root, i + 1, &trades, &ans)?;
                        let t = Instant::now();
                        let pack = packet(
                            &trades, &seal, &bank, q, bits, page, &state, &first, &second, ans,
                        )?;
                        let packet_s = t.elapsed().as_secs_f64();
                        (
                            pack.wire,
                            pack.parts,
                            pack.candidates,
                            pack.field_rows,
                            pack.proof_bytes,
                            query_s,
                            packet_s,
                            state.stages,
                        )
                    } else {
                        let ev = crate::temporal_compare::evaluate(&seal, q, true)?;
                        let query_s = t.elapsed().as_secs_f64();
                        crate::temporal_compare::oracle(root, i + 1, &trades, &ev.answers)?;
                        let t = Instant::now();
                        let (w, opened, proof) = hybrid_live_server::packet_version(
                            &trades,
                            &seal,
                            &old_tree,
                            q,
                            &ev.ids,
                            &ev.fields,
                            &ev.answers,
                            "hybrid-temporal-audit/v1",
                        )?;
                        (
                            w,
                            Value::Null,
                            ev.ids.len(),
                            opened,
                            proof,
                            query_s,
                            t.elapsed().as_secs_f64(),
                            ev.stages,
                        )
                    };
                write_new(&dir.join("response.bin"), &wire)?;
                crate::save(&dir.join("query.json"), q)?;
                let mut modes = serde_json::Map::new();
                for mode in if rep % 2 == 0 {
                    ["count", "replay"]
                } else {
                    ["replay", "count"]
                } {
                    let t = Instant::now();
                    let rpath = dir.join(format!("{mode}.json"));
                    let mut cmd = Command::new(std::env::current_exe()?);
                    cmd.arg(if progressive {
                        "progressive-client"
                    } else {
                        "temporal-client"
                    })
                    .arg(&paths[i])
                    .arg(dir.join("query.json"));
                    if progressive {
                        cmd.arg(out.join("trusted_progressive.json"))
                            .arg(out.join("trusted_manifest.json"));
                    } else {
                        cmd.arg(out.join("trusted_manifest.json"))
                            .arg(out.join("trusted_row_root.json"));
                    }
                    let result = cmd
                        .arg(dir.join("response.bin"))
                        .arg(&rpath)
                        .arg(mode)
                        .output()?;
                    let wall = t.elapsed().as_secs_f64();
                    ensure!(
                        result.status.success(),
                        "client failed {}",
                        String::from_utf8_lossy(&result.stderr)
                    );
                    let client: Value = serde_json::from_slice(&fs::read(&rpath)?)?;
                    ensure!(
                        client["verified"] == true
                            && client["answers"].as_u64() == Some(reference[i].len() as u64),
                        "client oracle result"
                    );
                    modes.insert(mode.into(),json!({"client_wall_s":wall,"compute_e2e_s":query_s+packet_s+wall,"details":client}));
                }
                let rec = json!({"query":i+1,"round":rep,"warmup":rep==0,"backend":backend,"bits":bits,"page":page,"wire_bytes":wire.len(),"parts":parts,"candidate_rows":candidates,"field_page_rows":field_rows,"proof_bytes":proof_bytes,"server_query_s":query_s,"packet_s":packet_s,"stages":stage,"modes":modes,"FP":0,"FN":0});
                eprintln!(
                    "PROGRESSIVE_PAIRED Q{} {backend} r{rep} E2E={:.3}s wire={:.3}MB",
                    i + 1,
                    rec["modes"]["count"]["compute_e2e_s"].as_f64().unwrap(),
                    wire.len() as f64 / 1e6
                );
                crate::save(&dir.join("record.json"), &rec)?;
                records.push(rec);
            }
        }
    }
    crate::save(
        &out.join("summary.json"),
        &json!({"version":"progressive-fields-paired/v1","input_rows":trades.len(),"source":source,"rounds":rounds,"bits":bits,"page":page,"offline_build_s":offline_s,"trusted_sealer":true,"on_chain_deployed":false,"network_included":false,"records":records,
        "timing":"same CPU/binary, fresh packet every run, server query + packet + isolated client wall; network/offline/server-write/oracle excluded"}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{AtomSpec, BranchSpec, Constraint, ScopeSpec, Term};
    fn fixture() -> Result<(Vec<Trade>, Vec<crate::model::CanonicalEvent>, columns::Seal)> {
        let trades = (0..96)
            .map(|i| Trade {
                order: i + 1,
                time: i / 3,
                src: if i % 3 == 0 { 2 } else { 1 },
                dst: if i % 3 == 0 { 1 } else { 2 },
                nft: if i < 12 {
                    50000
                } else {
                    50001 + (i / 4) as u32
                },
                collection: 10,
                tx: (i % 7) as u32,
            })
            .collect::<Vec<_>>();
        let es = trades
            .iter()
            .map(|r| {
                crate::model::CanonicalEvent::new(
                    format!("order:{}", r.order),
                    "NFTTrade",
                    r.order as i64,
                    r.time as i64,
                    format!("tx:{}", r.tx),
                    BTreeMap::from(KEYS.map(|f| (f.into(), scalar(value(r, f), f)))),
                )
            })
            .collect::<Vec<_>>();
        let seal = columns::seal(&es, "fixture-source".into())?;
        Ok((trades, es, seal))
    }
    fn query(nft: bool) -> QuerySpec {
        let mut left = BTreeMap::from([
            ("move_from".into(), Term::Variable("x".into())),
            ("move_to".into(), Term::Variable("y".into())),
            ("collection_contract".into(), Term::Variable("c".into())),
        ]);
        let mut right = BTreeMap::from([
            ("move_from".into(), Term::Variable("y".into())),
            ("move_to".into(), Term::Variable("x".into())),
            ("collection_contract".into(), Term::Variable("c".into())),
        ]);
        if nft {
            left.insert("nft".into(), Term::Variable("n".into()));
            right.insert("nft".into(), Term::Variable("n".into()));
        }
        QuerySpec {
            query_id: "never-registered-refinement-query".into(),
            result_semantics: "full_event_binding_v1".into(),
            scope: ScopeSpec::Episode {
                anchor_atom: "a".into(),
                window_seconds: 8,
            },
            branches: vec![BranchSpec {
                id: "runtime".into(),
                event_injective: true,
                atoms: vec![
                    AtomSpec {
                        id: "a".into(),
                        event_types: BTreeSet::from(["NFTTrade".into()]),
                        fields: left,
                        local_predicates: vec![],
                    },
                    AtomSpec {
                        id: "b".into(),
                        event_types: BTreeSet::from(["NFTTrade".into()]),
                        fields: right,
                        local_predicates: vec![],
                    },
                ],
                constraints: vec![
                    Constraint::Order {
                        earlier: "a".into(),
                        later: "b".into(),
                        field: "order_seq".into(),
                    },
                    Constraint::Order {
                        earlier: "a".into(),
                        later: "b".into(),
                        field: "block_ts".into(),
                    },
                    Constraint::DistinctTransactions(vec!["a".into(), "b".into()]),
                ],
            }],
        }
    }
    fn rewrap(ss: &[Vec<u8>]) -> Result<Vec<u8>> {
        let mut w = MAGIC.to_vec();
        for s in ss {
            frame(&mut w, &crate::compress(s)?);
        }
        Ok(w)
    }
    #[test]
    fn reversible_prefix_suffix_preserves_zero_and_exact_values() -> Result<()> {
        let (_, _, seal) = fixture()?;
        let t = prepare(&fixture()?.0, &seal)?.trusted;
        for x in [0, 1, 2, 255, 256, 50000, mask(t.integer_bits)] {
            let y = code(x, &t);
            assert_eq!(original(y, &t), x);
            for bits in WIDTHS {
                assert_eq!(
                    ((y >> (t.integer_bits - bits)) << (t.integer_bits - bits))
                        | (y & mask(t.integer_bits - bits)),
                    y
                );
            }
        }
        let b = pack([0, 1, 7].into_iter(), 3, 3)?;
        assert_eq!(unpack(&b, 3, 3)?, vec![0, 1, 7]);
        let mut bad = b;
        *bad.last_mut().unwrap() |= 128;
        assert!(unpack(&bad, 3, 3).is_err());
        Ok(())
    }
    #[test]
    fn progressive_full_semantics_matches_bruteforce_and_rejects_bad_packets() -> Result<()> {
        let (trades, es, seal) = fixture()?;
        let bank = prepare(&trades, &seal)?;
        for nft in [false, true] {
            let q = query(nft);
            let expected = crate::reference(&es, &q)?;
            for bits in WIDTHS {
                for page in PAGES {
                    let (state, first, second) =
                        server_state(&trades, &seal, &bank, &q, bits, page)?;
                    let tables = final_tables(&state)?;
                    let rs = columns::rows(&tables)?;
                    let eng = Engine::new(&rs, &q, &BTreeSet::from(["NFTTrade".into()]))?;
                    let ans = eng.replay_rows()?.0;
                    assert_eq!(eng.canonical_answers(&ans)?, expected);
                    let packet = packet(
                        &trades, &seal, &bank, &q, bits, page, &state, &first, &second, ans,
                    )?;
                    let r = authenticate(&packet.wire, &bank.trusted, &seal.manifest, &q, "both")?;
                    assert_eq!(r["answers"].as_u64(), Some(expected.len() as u64));
                    if bits == 8 && page == 16 {
                        let ss = sections(&packet.wire)?;
                        let mut bad = ss.clone();
                        let mut h: Header = serde_json::from_slice(&bad[0])?;
                        h.answers.pop();
                        bad[0] = serde_json::to_vec(&h)?;
                        assert!(authenticate(
                            &rewrap(&bad)?,
                            &bank.trusted,
                            &seal.manifest,
                            &q,
                            "count"
                        )
                        .is_err());
                        let mut bad = ss.clone();
                        bad[1][10] ^= 1;
                        assert!(authenticate(
                            &rewrap(&bad)?,
                            &bank.trusted,
                            &seal.manifest,
                            &q,
                            "count"
                        )
                        .is_err());
                        let mut bad = ss.clone();
                        bad[2][0] = 0;
                        assert!(authenticate(
                            &rewrap(&bad)?,
                            &bank.trusted,
                            &seal.manifest,
                            &q,
                            "count"
                        )
                        .is_err());
                        let mut bad = ss;
                        *bad[3].last_mut().unwrap() ^= 1;
                        assert!(authenticate(
                            &rewrap(&bad)?,
                            &bank.trusted,
                            &seal.manifest,
                            &q,
                            "count"
                        )
                        .is_err());
                    }
                }
            }
        }
        Ok(())
    }
    #[test]
    fn field_proof_binds_source_field_row_and_demand() -> Result<()> {
        let (trades, _, seal) = fixture()?;
        let bank = prepare(&trades, &seal)?;
        let req = BTreeMap::from([("nft".into(), vec![1])]);
        let empty = Cols::new();
        let mut b = vec![];
        encode_fields(&mut b, &trades, &bank, 8, 16, &req, &empty)?;
        decode_fields(&b, &bank.trusted, 8, 16, &req, &empty, &mut BTreeMap::new())?;
        let mut bad = b.clone();
        *bad.last_mut().unwrap() ^= 1;
        assert!(decode_fields(
            &bad,
            &bank.trusted,
            8,
            16,
            &req,
            &empty,
            &mut BTreeMap::new()
        )
        .is_err());
        assert!(decode_fields(
            &b,
            &bank.trusted,
            8,
            16,
            &BTreeMap::from([("nft".into(), vec![2])]),
            &empty,
            &mut BTreeMap::new()
        )
        .is_err());
        let mut wrong = bank.trusted.clone();
        wrong.source.push('x');
        assert!(decode_fields(&b, &wrong, 8, 16, &req, &empty, &mut BTreeMap::new()).is_err());
        Ok(())
    }
    #[test]
    fn full_u32_field_opening_without_prefix_roundtrips() -> Result<()> {
        let (mut trades, mut es, _) = fixture()?;
        trades[0].nft = u32::MAX;
        es[0]
            .fields
            .insert("nft".into(), Scalar::Int(u32::MAX as i64));
        let seal = columns::seal(&es, "fixture-source".into())?;
        let bank = prepare(&trades, &seal)?;
        assert_eq!(bank.trusted.integer_bits, 32);
        let req = BTreeMap::from([("nft".into(), vec![0])]);
        let mut bytes = vec![];
        encode_fields(&mut bytes, &trades, &bank, 8, 16, &req, &Cols::new())?;
        let mut fine = BTreeMap::new();
        decode_fields(&bytes, &bank.trusted, 8, 16, &req, &Cols::new(), &mut fine)?;
        assert_eq!(fine["nft"].values[0], u32::MAX);
        Ok(())
    }
}
