                                                                            
                                                                             
                                                                                  
   
                                                                           
                                                                          
                                                                          
                                                                            
                                                                            
                                                                              
                                                                            
                                                                               
                                                                            
   
                                                                            
                                                                               
                                                                          
                                                                               
                                                                                  
use crate::{
    columns::{self, Answer, RowAnswer, RowRef},
    engine::Engine,
    hybrid_key_audit::{self, Trade},
    model::CanonicalEvent,
    query::{Constraint, QuerySpec, ScopeSpec, Term},
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs,
    io::Write,
    path::Path,
    process::Command,
    time::Instant,
};

const VERSION: &str = "multiscale-scc-complete-prefix/v1";
                                                                         
const SCALES: [u64; 8] = [512, 1024, 2048, 4096, 8192, 16384, 32768, 65536];
const PAGE: usize = 128;

                                                                                
#[cfg(test)]
#[path = "defi_compatibility_tests.rs"]
mod defi_compatibility_tests;

#[path = "semantic_ablation.rs"]
pub mod ablation;
type Hash = [u8; 32];

#[derive(Clone, Serialize, Deserialize)]
struct Manifest {
    version: String,
    source_sha256: String,
    schema: String,
    trust: String,
    scales: Vec<u64>,
    prefix_counts: Vec<usize>,
    nrows: usize,
    page_rows: usize,
    root: Hash,
                                                                               
                                                                                
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    domain_only: bool,
}
impl Manifest {
    fn context(&self) -> Vec<u8> {
        let base = serde_json::to_vec(&(
            &self.version,
            &self.source_sha256,
            &self.schema,
            &self.scales,
            &self.prefix_counts,
            self.nrows,
            self.page_rows,
        ))
        .unwrap();
        if self.domain_only {
            [b"scc-domain-only/v1:".as_slice(), base.as_slice()].concat()
        } else {
            base
        }
    }
    fn digest(&self) -> String {
        columns::digest(&serde_json::to_vec(self).unwrap())
    }
    fn validate(&self) -> Result<()> {
        ensure!(
            self.version == VERSION && self.page_rows == PAGE,
            "manifest version/page size"
        );
        ensure!(
            self.scales.iter().all(|h| SCALES.contains(h))
                && self.scales.windows(2).all(|w| w[0] < w[1])
                && self.prefix_counts.len() == self.scales.len()
                && (!self.domain_only || self.scales.len() == 1),
            "scale configuration"
        );
        ensure!(
            self.prefix_counts.windows(2).all(|x| x[0] <= x[1])
                && self.prefix_counts.iter().all(|x| *x <= self.nrows),
            "nested cardinalities"
        );
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
struct Plan {
    level: Option<usize>,
    reason: String,
}

                                                                             
                                                                                
                                                                              
fn plan(q: &QuerySpec, m: &Manifest) -> Result<Plan> {
    ensure!(
        q.result_semantics == "full_event_binding_v1" && !q.branches.is_empty(),
        "result semantics"
    );
    for b in &q.branches {
        ensure!(!b.atoms.is_empty(), "empty branch");
        for a in &b.atoms {
            ensure!(
                a.event_types == BTreeSet::from(["NFTTrade".to_string()]),
                "epoch experiment contains only complete NFTTrade; unsupported event type"
            );
        }
        for c in &b.constraints {
            if let Constraint::NoEvent { event_types, .. } = c {
                ensure!(
                    *event_types == BTreeSet::from(["NFTTrade".to_string()]),
                    "blocker type not committed in this experiment"
                );
            }
        }
    }
    let fallback = |s: &str| {
        ensure!(!m.domain_only, "partial-domain index cannot authenticate complete-input fallback");
        Ok(Plan {
            level: None,
            reason: s.to_string(),
        })
    };
    let (anchor, delta) = match &q.scope {
        ScopeSpec::Episode {
            anchor_atom,
            window_seconds,
        } => {
            ensure!(*window_seconds >= 0, "negative Episode bound");
            (anchor_atom, *window_seconds as u64)
        }
        _ => return fallback("no finite Episode bound: complete input"),
    };
    let Some(level) = m.scales.iter().position(|h| *h >= delta) else {
        return fallback("Episode exceeds sealed scales: complete input");
    };
    for b in &q.branches {
        ensure!(
            b.atoms.iter().any(|a| &a.id == anchor),
            "Episode anchor missing"
        );
        if b.constraints
            .iter()
            .any(|c| matches!(c, Constraint::NoEvent { .. }))
        {
            return fallback("negative blocker needs complete input");
        }
        let value = |i: usize, field: &str| -> Option<&Term> {
            b.atoms[i]
                .fields
                .iter()
                .find(|(k, _)| columns::canonical(k) == columns::canonical(field))
                .map(|(_, v)| v)
        };
        let Some(collection) = value(0, "collection") else {
            return fallback("collection equality not established");
        };
        if (0..b.atoms.len()).any(|i| value(i, "collection") != Some(collection)) {
            return fallback("atoms do not provably share collection");
        }
        let mut vertices = BTreeMap::<Term, usize>::new();
        let mut edges = Vec::new();
        for i in 0..b.atoms.len() {
            let (Some(s), Some(t)) = (value(i, "src"), value(i, "dst")) else {
                return fallback("role projection not established");
            };
            let n = vertices.len();
            let u = *vertices.entry(s.clone()).or_insert(n);
            let n = vertices.len();
            let v = *vertices.entry(t.clone()).or_insert(n);
            edges.push((u, v));
        }
        let cs = scc(vertices.len(), &edges);
        if edges.iter().any(|(u, v)| cs[*u] != cs[*v]) {
            return fallback("a positive edge is not on a directed closed walk");
        }
    }
    Ok(Plan {
        level: Some(level),
        reason: "all positive edges lie on bounded same-collection directed closed walks".into(),
    })
}

                                                              
fn scc(n: usize, edges: &[(usize, usize)]) -> Vec<usize> {
    let mut out = vec![Vec::new(); n];
    let mut rev = vec![Vec::new(); n];
    for &(u, v) in edges {
        out[u].push(v);
        rev[v].push(u);
    }
    let mut seen = vec![false; n];
    let mut finish = Vec::with_capacity(n);
    for start in 0..n {
        if seen[start] {
            continue;
        }
        seen[start] = true;
        let mut stack = vec![(start, 0)];
        while let Some((v, i)) = stack.last_mut() {
            if *i < out[*v].len() {
                let w = out[*v][*i];
                *i += 1;
                if !seen[w] {
                    seen[w] = true;
                    stack.push((w, 0));
                }
            } else {
                finish.push(*v);
                stack.pop();
            }
        }
    }
    let mut component = vec![usize::MAX; n];
    let mut nc = 0;
    for start in finish.into_iter().rev() {
        if component[start] != usize::MAX {
            continue;
        }
        component[start] = nc;
        let mut stack = vec![start];
        while let Some(v) = stack.pop() {
            for &w in &rev[v] {
                if component[w] == usize::MAX {
                    component[w] = nc;
                    stack.push(w);
                }
            }
        }
        nc += 1;
    }
    component
}

fn retained(data: &[Trade], collections: &BTreeMap<u32, Vec<usize>>, h: u64) -> Vec<bool> {
    let mut keep = vec![false; data.len()];
    for ids in collections.values() {
        let mut vertices = HashMap::<(i64, u32), usize>::new();
        let mut edges = Vec::with_capacity(2 * ids.len());
        for &i in ids {
            let r = data[i];
            let b = (r.time / h) as i64;
            for j in [b - 1, b] {
                let n = vertices.len();
                let u = *vertices.entry((j, r.src)).or_insert(n);
                let n = vertices.len();
                let v = *vertices.entry((j, r.dst)).or_insert(n);
                edges.push((u, v));
            }
        }
        let cs = scc(vertices.len(), &edges);
        for (k, &i) in ids.iter().enumerate() {
            keep[i] = edges[2 * k..2 * k + 2]
                .iter()
                .any(|(u, v)| cs[*u] == cs[*v]);
        }
    }
    keep
}

#[derive(Clone)]
struct Record {
    trade: Trade,
    birth: u8,
}
fn raw(r: &Record) -> Vec<u8> {
    let t = r.trade;
    let mut b = Vec::with_capacity(38);
    b.extend(t.order.to_le_bytes());
    b.extend(t.time.to_le_bytes());
    for v in [t.src, t.dst, t.nft, t.collection, t.tx] {
        b.extend(v.to_le_bytes());
    }
    b.push(1);
    b.push(r.birth);
    b
}
fn parse_raw(b: &[u8]) -> Result<Record> {
    ensure!(b.len() == 38 && b[36] == 1, "record framing/type");
    let u64at = |i| u64::from_le_bytes(b[i..i + 8].try_into().unwrap());
    let u32at = |i| u32::from_le_bytes(b[i..i + 4].try_into().unwrap());
    ensure!(
        b[37] == 255 || (b[37] as usize) < SCALES.len(),
        "birth level"
    );
    Ok(Record {
        trade: Trade {
            order: u64at(0),
            time: u64at(8),
            src: u32at(16),
            dst: u32at(20),
            nft: u32at(24),
            collection: u32at(28),
            tx: u32at(32),
        },
        birth: b[37],
    })
}
fn leaf(context: &[u8], page: usize, records: &[Record]) -> Hash {
    let mut hash = Sha256::new();
    hash.update(b"scc-prefix-leaf/v1");
    hash.update(context);
    hash.update((page as u64).to_le_bytes());
    hash.update((records.len() as u64).to_le_bytes());
    for r in records {
        hash.update(raw(r));
    }
    hash.finalize().into()
}
fn parent(l: Hash, r: Hash) -> Hash {
    let mut h = Sha256::new();
    h.update(b"scc-prefix-parent/v1");
    h.update(l);
    h.update(r);
    h.finalize().into()
}
struct Index {
    manifest: Manifest,
    records: Vec<Record>,
    tree: Vec<Vec<Hash>>,
    build: Value,
}
fn build(data: &[Trade], sha: String) -> Result<Index> {
    build_scales(data, sha, &SCALES)
}
fn build_scales(data: &[Trade], sha: String, scales: &[u64]) -> Result<Index> {
    let time = Instant::now();
    let mut collections = BTreeMap::<u32, Vec<usize>>::new();
    for (i, r) in data.iter().enumerate() {
        collections.entry(r.collection).or_default().push(i);
    }
    let mut birth = vec![255; data.len()];
    let mut previous = vec![false; data.len()];
    let mut counts = vec![];
    let mut stages = vec![];
    for (k, &h) in scales.iter().enumerate() {
        let t = Instant::now();
        let keep = retained(data, &collections, h);
        ensure!(
            previous.iter().zip(&keep).all(|(a, b)| !a || *b),
            "nested SCC domain invariant"
        );
        for i in 0..data.len() {
            if keep[i] && birth[i] == 255 {
                birth[i] = k as u8;
            }
        }
        let count = keep.iter().filter(|x| **x).count();
        counts.push(count);
        stages.push(json!({"h":h,"rows":count,"seconds":t.elapsed().as_secs_f64()}));
        eprintln!("SEALED SCC h={h} rows={count}");
        previous = keep;
    }
    let mut records = data
        .iter()
        .enumerate()
        .map(|(i, t)| Record {
            trade: *t,
            birth: birth[i],
        })
        .collect::<Vec<_>>();
    records.sort_unstable_by_key(|r| (r.birth, r.trade.order));
    let manifest=Manifest {version:VERSION.into(),source_sha256:sha,
        schema:"single-chain NFTTrade; src->dst; partition=collection; covers=[j*h,(j+2)*h); origin=0; index=(min-level,canonical-order); never=255; all original fields and identities".into(),
        trust:"local trusted sealer independently reads complete canonical file; derived-index correctness trusted; no chain deployment or distributed attestation".into(),
        scales:scales.to_vec(),prefix_counts:counts,nrows:data.len(),page_rows:PAGE,root:[0;32],domain_only:false};
    let mut index = seal_records(records, manifest)?;
    index.build = json!({"scales":stages,"build_seconds":time.elapsed().as_secs_f64(),
        "canonical_rows":data.len(),"raw_min_scale_index_bytes":index.records.len()*38,
        "merkle_hash_bytes":index.tree.iter().map(|x|x.len()*32).sum::<usize>(),
        "copies_per_event_in_new_index":1,"cross_scale_row_duplication":0,
        "additional_canonical_copy_beyond_source_file":true,"nested_domain_checked":true});
    Ok(index)
}
fn seal_records(records: Vec<Record>, mut manifest: Manifest) -> Result<Index> {
    manifest.validate()?;
    ensure!(records.len() == manifest.nrows, "sealed records length");
    let context = manifest.context();
    let pages = records.len().div_ceil(PAGE).max(1).next_power_of_two();
    let mut leaves = Vec::with_capacity(pages);
    for i in 0..pages {
        let lo = (i * PAGE).min(records.len());
        let hi = ((i + 1) * PAGE).min(records.len());
        leaves.push(leaf(&context, i, &records[lo..hi]));
    }
    let mut tree = vec![leaves];
    while tree.last().unwrap().len() > 1 {
        tree.push(
            tree.last()
                .unwrap()
                .chunks_exact(2)
                .map(|x| parent(x[0], x[1]))
                .collect(),
        );
    }
    manifest.root = tree.last().unwrap()[0];
    Ok(Index {
        manifest,
        records,
        tree,
        build: Value::Null,
    })
}

fn prefix_proof(index: &Index, opened_pages: usize) -> Vec<Hash> {
    fn walk(tree: &[Vec<Hash>], level: usize, pos: usize, opened: usize, out: &mut Vec<Hash>) {
        if (pos << level) >= opened {
            out.push(tree[level][pos]);
            return;
        }
        if level > 0 {
            walk(tree, level - 1, 2 * pos, opened, out);
            walk(tree, level - 1, 2 * pos + 1, opened, out);
        }
    }
    let mut out = vec![];
    walk(&index.tree, index.tree.len() - 1, 0, opened_pages, &mut out);
    out
}
fn verify_prefix(m: &Manifest, records: &[Record], proof: &[Hash]) -> Result<()> {
    let pages = m.nrows.div_ceil(PAGE).max(1).next_power_of_two();
    let context = m.context();
    let opened = records.len().div_ceil(PAGE);
    let mut it = proof.iter();
    fn walk(
        level: u32,
        pos: usize,
        opened: usize,
        rs: &[Record],
        ctx: &[u8],
        it: &mut std::slice::Iter<Hash>,
    ) -> Result<Hash> {
        if (pos << level) >= opened {
            return it.next().copied().context("missing prefix frontier");
        }
        if level == 0 {
            return Ok(leaf(
                ctx,
                pos,
                &rs[pos * PAGE..((pos + 1) * PAGE).min(rs.len())],
            ));
        }
        Ok(parent(
            walk(level - 1, 2 * pos, opened, rs, ctx, it)?,
            walk(level - 1, 2 * pos + 1, opened, rs, ctx, it)?,
        ))
    }
    ensure!(
        walk(
            pages.trailing_zeros(),
            0,
            opened,
            records,
            &context,
            &mut it
        )? == m.root,
        "prefix Merkle root"
    );
    ensure!(it.next().is_none(), "excess prefix frontier");
    Ok(())
}
fn event(t: &Trade) -> Result<CanonicalEvent> {
    Ok(CanonicalEvent::new(
        format!("order:{}", t.order),
        "NFTTrade",
        i64::try_from(t.order)?,
        i64::try_from(t.time)?,
        format!("tx:{}", t.tx),
        BTreeMap::from([
            ("move_from".into(), (t.src as i64).into()),
            ("move_to".into(), (t.dst as i64).into()),
            ("nft".into(), (t.nft as i64).into()),
            ("collection_contract".into(), (t.collection as i64).into()),
        ]),
    ))
}
struct ExecutionTables {
    tables: BTreeMap<String, columns::Table>,
}
fn table(records: &[Record], cut: usize) -> Result<ExecutionTables> {
    use crate::model::Scalar;
    use std::sync::Arc;
    ensure!(cut <= records.len(), "execution domain length");
    let mut rows = records[..cut].iter().map(|r| &r.trade).collect::<Vec<_>>();
    rows.sort_unstable_by_key(|r| r.order);
                                                                            
                                                                             
    ensure!(rows.windows(2).all(|w| w[0].order < w[1].order), "duplicate event identity");
    ensure!(rows.iter().all(|r| r.order <= i64::MAX as u64 && r.time <= i64::MAX as u64),
        "canonical integer range");
    let mut cols = BTreeMap::new();
    for field in ["event_id", "event_type", "order_seq", "block_ts", "tx_hash",
        "move_from", "move_to", "nft", "collection_contract"] {
        let c = columns::Column::from_values(rows.iter().map(|r| Some(match field {
            "event_id" => Scalar::Text(format!("order:{}", r.order)),
            "event_type" => Scalar::Text("NFTTrade".into()),
            "order_seq" => Scalar::Int(r.order as i64),
            "block_ts" => Scalar::Int(r.time as i64),
            "tx_hash" => Scalar::Text(format!("tx:{}", r.tx)),
            "move_from" => Scalar::Int(r.src as i64),
            "move_to" => Scalar::Int(r.dst as i64),
            "nft" => Scalar::Int(r.nft as i64),
            "collection_contract" => Scalar::Int(r.collection as i64),
            _ => unreachable!(),
        })));
        cols.insert(field.to_string(), Arc::new(c));
    }
    Ok(ExecutionTables {tables:columns::dense_execution_view("NFTTrade",cols,rows.len())?})
}

#[derive(Clone, Serialize, Deserialize)]
struct Header {
    version: String,
    manifest: String,
    query: String,
    plan: Plan,
    cut: usize,
    opened: usize,
    proof: Vec<Hash>,
}
fn query_hash(q: &QuerySpec) -> String {
    columns::digest(&serde_json::to_vec(q).unwrap())
}
fn put_blob(out: &mut Vec<u8>, blob: &[u8]) {
    columns::put(out, blob.len() as u64);
    out.extend(blob);
}
fn get_blob<'a>(b: &mut &'a [u8]) -> Result<&'a [u8]> {
    let n = usize::try_from(columns::read(b)?)?;
    columns::take(b, n)
}
                                                                                 
fn encode_records(rs: &[Record]) -> Result<Vec<u8>> {
    let raw = rs.iter().flat_map(raw).collect::<Vec<_>>();
    let mut c = Vec::with_capacity(raw.len());
    for (off, len) in [
        (0, 8),
        (8, 8),
        (16, 4),
        (20, 4),
        (24, 4),
        (28, 4),
        (32, 4),
        (36, 1),
        (37, 1),
    ] {
        for row in raw.chunks_exact(38) {
            c.extend(&row[off..off + len]);
        }
    }
    crate::compress(&c)
}
fn decode_records(b: &[u8], n: usize) -> Result<Vec<Record>> {
    ensure!(n <= 10_000_000, "row allocation bound");
    let c = crate::decompress(b)?;
    ensure!(
        c.len() == n.checked_mul(38).context("size overflow")?,
        "column length"
    );
    let mut raw = vec![0u8; c.len()];
    let mut p = 0;
    for (off, len) in [
        (0, 8),
        (8, 8),
        (16, 4),
        (20, 4),
        (24, 4),
        (28, 4),
        (32, 4),
        (36, 1),
        (37, 1),
    ] {
        for i in 0..n {
            raw[i * 38 + off..i * 38 + off + len].copy_from_slice(&c[p..p + len]);
            p += len;
        }
    }
    raw.chunks_exact(38).map(parse_raw).collect()
}
fn encode_answers(q: &QuerySpec, answers: &[RowAnswer]) -> Result<Vec<u8>> {
    let mut b = Vec::new();
    columns::put(&mut b, answers.len() as u64);
    for a in answers {
        let branch = q
            .branches
            .iter()
            .position(|x| x.id == a.branch)
            .context("answer branch")?;
        b.extend(u32::try_from(branch)?.to_le_bytes());
        ensure!(
            a.rows.len() == q.branches[branch].atoms.len(),
            "answer arity"
        );
        for r in &a.rows {
            ensure!(r.0 == "NFTTrade", "answer type");
            b.extend(u32::try_from(r.1)?.to_le_bytes());
        }
    }
    crate::compress(&b)
}
fn decode_answers(q: &QuerySpec, b: &[u8]) -> Result<Vec<RowAnswer>> {
    let bytes = crate::decompress(b)?;
    let mut b = bytes.as_slice();
    let n = usize::try_from(columns::read(&mut b)?)?;
    ensure!(n <= b.len() / 8, "answer count bound");
    let mut ans = Vec::with_capacity(n);
    for _ in 0..n {
        let k = u32::from_le_bytes(columns::take(&mut b, 4)?.try_into().unwrap()) as usize;
        let branch = q.branches.get(k).context("invalid branch")?;
        let mut rows = vec![];
        for _ in &branch.atoms {
            let i = u32::from_le_bytes(columns::take(&mut b, 4)?.try_into().unwrap()) as usize;
            rows.push(RowRef("NFTTrade".into(), i));
        }
        ans.push(RowAnswer {
            branch: branch.id.clone(),
            rows,
        });
    }
    ensure!(b.is_empty(), "trailing answers");
    Ok(ans)
}
fn pack(
    h: &Header,
    rs: &[Record],
    q: &QuerySpec,
    answers: &[RowAnswer],
) -> Result<(Vec<u8>, Value)> {
    let header = serde_json::to_vec(h)?;
    let records = encode_records(rs)?;
    let ans = encode_answers(q, answers)?;
    let mut b = b"SCCPFX01".to_vec();
    put_blob(&mut b, &header);
    put_blob(&mut b, &records);
    put_blob(&mut b, &ans);
    let sizes = json!({"metadata_and_frontier_bytes":header.len(),"frontier_hash_bytes":h.proof.len()*32,
        "event_payload_bytes":records.len(),"answer_payload_bytes":ans.len(),"framing_bytes":32,"response_bytes":b.len()});
    Ok((b, sizes))
}
fn header(index: &Index, q: &QuerySpec) -> Result<Header> {
    let p = plan(q, &index.manifest)?;
    let cut = p
        .level
        .map(|k| index.manifest.prefix_counts[k])
        .unwrap_or(index.manifest.nrows);
    let opened = cut
        .div_ceil(PAGE)
        .saturating_mul(PAGE)
        .min(index.manifest.nrows);
    Ok(Header {
        version: VERSION.into(),
        manifest: index.manifest.digest(),
        query: query_hash(q),
        plan: p,
        cut,
        opened,
        proof: prefix_proof(index, opened.div_ceil(PAGE)),
    })
}
fn authenticate(
    m: &Manifest,
    q: &QuerySpec,
    wire: &[u8],
) -> Result<(Header, Vec<Record>, Vec<RowAnswer>)> {
    m.validate()?;
    let mut b = wire;
    ensure!(columns::take(&mut b, 8)? == b"SCCPFX01", "packet magic");
    let h: Header = serde_json::from_slice(get_blob(&mut b)?)?;
    ensure!(
        h.version == VERSION && h.manifest == m.digest() && h.query == query_hash(q),
        "epoch/query binding"
    );
    let p = plan(q, m)?;
    ensure!(h.plan == p, "independent applicability plan");
    let cut = p.level.map(|k| m.prefix_counts[k]).unwrap_or(m.nrows);
    ensure!(
        h.cut == cut && h.opened == cut.div_ceil(PAGE).saturating_mul(PAGE).min(m.nrows),
        "complete prefix length"
    );
    let rs = decode_records(get_blob(&mut b)?, h.opened)?;
    verify_prefix(m, &rs, &h.proof)?;
    ensure!(
        rs.windows(2)
            .all(|x| (x[0].birth, x[0].trade.order) < (x[1].birth, x[1].trade.order)),
        "prefix order"
    );
    if let Some(k) = p.level {
        ensure!(
            rs[..cut].iter().all(|r| r.birth as usize <= k),
            "included birth levels"
        );
        ensure!(
            rs[cut..].iter().all(|r| r.birth as usize > k),
            "prefix boundary"
        );
    }
    let answers = decode_answers(q, get_blob(&mut b)?)?;
    ensure!(b.is_empty(), "packet trailing bytes");
    Ok((h, rs, answers))
}
fn verify(m: &Manifest, q: &QuerySpec, wire: &[u8], mode: &str) -> Result<Value> {
    let total = Instant::now();
    let t = Instant::now();
    let (h, rs, answers) = authenticate(m, q, wire)?;
    let auth = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let sealed = table(&rs, h.cut)?;
    let adapter = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let typed = columns::rows(&sealed.tables)?;
    let row_view = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let engine = Engine::new(&typed, q, &BTreeSet::from(["NFTTrade".into()]))?;
    let compile = t.elapsed().as_secs_f64();
    let prepare = adapter + row_view + compile;
    let t = Instant::now();
                                                                                  
                                                                        
    if mode == "count" {
        engine.sound_unique(&answers)?;
    }
    let sound = t.elapsed().as_secs_f64();
    let t = Instant::now();
    let stats = match mode {
        "count" => {
            let (n, s) = engine.count()?;
            ensure!(
                n == num_bigint::BigUint::from(answers.len()),
                "cardinality seal"
            );
            serde_json::to_value(s)?
        }
        "replay" => {
            let (actual, s) = engine.replay_rows()?;
            let mut claimed = answers.clone();
            claimed.sort();
            let mut actual = actual;
            actual.sort();
            ensure!(claimed == actual, "full binding replay");
            serde_json::to_value(s)?
        }
        _ => anyhow::bail!("unknown client mode"),
    };
    let evaluation = t.elapsed().as_secs_f64();
    Ok(
        json!({"accepted":true,"mode":mode,"answers":answers.len(),"domain_rows":h.cut,"opened_rows":h.opened,
        "authentication_decode_seconds":auth,"table_prepare_seconds":prepare,"sound_unique_seconds":sound,
        "execution_adapter_seconds":adapter,"row_view_seconds":row_view,"engine_compile_seconds":compile,
        "count_or_replay_seconds":evaluation,"verify_seconds":total.elapsed().as_secs_f64(),
        "client_peak_rss_bytes":crate::memory::peak_bytes(),"stats":stats}),
    )
}
pub fn client(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 8,
        "semantic-client MANIFEST QUERY WIRE RESULT count|replay reserved"
    );
    let start = Instant::now();
    let m: Manifest = serde_json::from_slice(&fs::read(&args[2])?)?;
    let q: QuerySpec = serde_json::from_slice(&fs::read(&args[3])?)?;
    let wire = fs::read(&args[4])?;
    let mut result = verify(&m, &q, &wire, &args[6])?;
    result["client_read_verify_seconds"] = json!(start.elapsed().as_secs_f64());
    crate::save(Path::new(&args[5]), &result)
}
fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?
        .write_all(bytes)?;
    Ok(())
}
fn truth(root: &Path, i: usize, q: &QuerySpec) -> Result<Vec<Answer>> {
    let doc: Value = serde_json::from_slice(&fs::read(root.join(format!(
        "FunnelQ_ScopeFactor_Feasibility/results/q{}_reference.json",
        i + 1
    )))?)?;
    let mut ans = vec![];
    for a in doc["answers"].as_array().context("reference answers")? {
        ans.push(Answer {
            branch: q.branches[0].id.clone(),
            events: a
                .as_array()
                .context("binding")?
                .iter()
                .map(|x| Ok(format!("order:{}", x.as_u64().context("order")?)))
                .collect::<Result<_>>()?,
        });
    }
    ans.sort();
    Ok(ans)
}

pub fn run(args: &[String]) -> Result<()> {
    ensure!(args.len() >= 4, "semantic-core ROOT NEW_OUTPUT [rounds]");
    let root = Path::new(&args[2]);
    let out = Path::new(&args[3]);
    fs::create_dir(out)?;
    let rounds = args
        .get(4)
        .map(|x| x.parse::<usize>())
        .transpose()?
        .unwrap_or(3);
    ensure!(rounds > 0, "positive rounds");
    let start = Instant::now();
    let trades = hybrid_key_audit::data(root)?;
    ensure!(
        trades.windows(2).all(|w| w[0].time <= w[1].time),
        "this single-chain epoch must have nondecreasing canonical timestamps"
    );
    let sha=columns::digest(&fs::read(root.join("FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin"))?);
                                                
    let index = build(&trades, sha)?;
    let sealing = start.elapsed().as_secs_f64();
    crate::save(&out.join("trusted_manifest.json"), &index.manifest)?;
    write_new(
        &out.join("sealed_min_scale_index.bin"),
        &index.records.iter().flat_map(raw).collect::<Vec<_>>(),
    )?;
    write_new(
        &out.join("sealed_merkle_nodes.bin"),
        &index
            .tree
            .iter()
            .flatten()
            .flat_map(|h| h.iter().copied())
            .collect::<Vec<_>>(),
    )?;
    crate::save(
        &out.join("offline_build.json"),
        &json!({"build":index.build,"read_and_build_seconds":sealing,
        "sealer_peak_rss_bytes":crate::memory::peak_bytes(),"query_files_read_before_sealing":false}),
    )?;
    let mut files = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|x| x.map(|x| x.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    files.sort();
    let mut results = vec![];
    for (i, file) in files.iter().enumerate() {
        let q = crate::load_query(file)?;
        let expected = truth(root, i, &q)?;
        let dir = out.join(format!("q{}", i + 1));
        fs::create_dir(&dir)?;
        crate::save(&dir.join("query.json"), &q)?;
        let h = header(&index, &q)?;
        let mut records = vec![];
        for rep in 0..=rounds {
            let t = Instant::now();
            let sealed = table(&index.records, h.cut)?;
            let typed = columns::rows(&sealed.tables)?;
            let engine = Engine::new(&typed, &q, &BTreeSet::from(["NFTTrade".into()]))?;
            let (answers, _) = engine.replay_rows()?;
            let query_seconds = t.elapsed().as_secs_f64();
            ensure!(
                engine.canonical_answers(&answers)? == expected,
                "complete reference binding mismatch"
            );
            let t = Instant::now();
            let fresh_header = header(&index, &q)?;
            let (wire, sizes) = pack(
                &fresh_header,
                &index.records[..fresh_header.opened],
                &q,
                &answers,
            )?;
            let generation_seconds = t.elapsed().as_secs_f64();
            let packet_path = dir.join(format!("rep{rep}.packet"));
            write_new(&packet_path, &wire)?;
            let mut clients = vec![];
            for mode in if rep % 2 == 0 {
                ["count", "replay"]
            } else {
                ["replay", "count"]
            } {
                let destination = dir.join(format!("rep{rep}_{mode}.json"));
                let t = Instant::now();
                let status = Command::new(std::env::current_exe()?)
                    .arg("semantic-client")
                    .arg(out.join("trusted_manifest.json"))
                    .arg(dir.join("query.json"))
                    .arg(&packet_path)
                    .arg(&destination)
                    .arg(mode)
                    .arg("v1")
                    .output()?;
                let wall = t.elapsed().as_secs_f64();
                ensure!(
                    status.status.success(),
                    "isolated client failed: {}",
                    String::from_utf8_lossy(&status.stderr)
                );
                let mut report: Value = serde_json::from_slice(&fs::read(destination)?)?;
                report["client_process_wall_seconds"] = json!(wall);
                report["e2e_compute_seconds"] = json!(query_seconds + generation_seconds + wall);
                clients.push(report);
            }
            eprintln!(
                "Q{} rep={} rows={} packet={} server={:.3}s",
                i + 1,
                rep,
                h.cut,
                wire.len(),
                query_seconds + generation_seconds
            );
            records.push(json!({"rep":rep,"warmup":rep==0,"query_seconds":query_seconds,"proof_packet_generation_seconds":generation_seconds,
                "sizes":sizes,"clients":clients,"FP":0,"FN":0,"answers":answers.len()}));
            if rep == 0 {
                let attacks = attacks(&index, &q, &h, &answers)?;
                crate::save(&dir.join("tamper_tests.json"), &attacks)?;
            }
        }
        let report = json!({"query":q.query_id,"plan":h.plan,"domain_rows":h.cut,"opened_rows":h.opened,"runs":records});
        crate::save(&dir.join("summary.json"), &report)?;
        results.push(report);
    }
    crate::save(
        &out.join("summary.json"),
        &json!({"version":VERSION,"complete_three_month":true,"input_rows":trades.len(),
        "warmup_per_query":1,"measured_rounds":rounds,"offline_build":index.build,"read_and_build_seconds":sealing,
        "server_peak_rss_bytes":crate::memory::peak_bytes(),"cpu_only":true,"network_included":false,
        "cold_response":true,"client_has_no_data_cache":true,"trusted_sealer_required":true,"on_chain_deployed":false,
        "results":results}),
    )?;
    Ok(())
}

fn attacks(index: &Index, q: &QuerySpec, h: &Header, ans: &[RowAnswer]) -> Result<Value> {
    let rs = &index.records[..h.opened];
    let mut result = vec![];
    let mut reject = |name: &str,
                      m: &Manifest,
                      qq: &QuerySpec,
                      hh: &Header,
                      rr: &[Record],
                      aa: &[RowAnswer]|
     -> Result<()> {
        let (wire, _) = pack(hh, rr, q, aa)?;
        let e = verify(m, qq, &wire, "count")
            .err()
            .context(format!("attack accepted: {name}"))?;
        result.push(json!({"attack":name,"rejected":true,"reason":e.to_string()}));
        Ok(())
    };
    if !rs.is_empty() {
        let mut changed = rs.to_vec();
        changed[0].trade.src ^= 1;
        reject(
            "mutated canonical role",
            &index.manifest,
            q,
            h,
            &changed,
            ans,
        )?;
        let mut hh = h.clone();
        hh.cut = hh.cut.saturating_sub(1);
        reject("omit one domain event", &index.manifest, q, &hh, rs, ans)?;
        let mut hh = h.clone();
        hh.plan.level = Some(0);
        reject(
            "unsafe smaller time scale",
            &index.manifest,
            q,
            &hh,
            rs,
            ans,
        )?;
    }
    if !ans.is_empty() {
        reject("omit answer", &index.manifest, q, h, rs, &ans[1..])?;
        let mut a = ans.to_vec();
        a.push(ans[0].clone());
        reject("duplicate answer", &index.manifest, q, h, rs, &a)?;
        let mut a = ans.to_vec();
        a[0].rows[0].1 = usize::try_from(u32::MAX)?;
        reject("invalid binding reference", &index.manifest, q, h, rs, &a)?;
    }
    let mut wrong = q.clone();
    wrong.query_id.push_str("-other-query");
    reject("query replay", &index.manifest, &wrong, h, rs, ans)?;
    let mut wrong = index.manifest.clone();
    wrong.source_sha256.push('0');
    reject("epoch replay", &wrong, q, h, rs, ans)?;
    let mut hh = h.clone();
    hh.proof.push([0; 32]);
    reject("extra frontier", &index.manifest, q, &hh, rs, ans)?;
    Ok(json!({"passed":result.len(),"attacks":result}))
}

                                                                                
                                                                           
                                                                               
pub fn extra(args: &[String]) -> Result<()> {
    ensure!(args.len() == 5, "semantic-extra ROOT SEALED_DIR NEW_OUTPUT");
    let root = Path::new(&args[2]);
    let sealed = Path::new(&args[3]);
    let out = Path::new(&args[4]);
    fs::create_dir(out)?;
    let manifest: Manifest =
        serde_json::from_slice(&fs::read(sealed.join("trusted_manifest.json"))?)?;
    manifest.validate()?;
    let raw = fs::read(sealed.join("sealed_min_scale_index.bin"))?;
    ensure!(raw.len() == manifest.nrows * 38, "sealed index length");
    let records = raw
        .chunks_exact(38)
        .map(parse_raw)
        .collect::<Result<Vec<_>>>()?;
    drop(raw);
    let hashes = fs::read(sealed.join("sealed_merkle_nodes.bin"))?;
    let mut b = hashes.as_slice();
    let mut tree = vec![];
    let mut n = manifest.nrows.div_ceil(PAGE).max(1).next_power_of_two();
    loop {
        tree.push(
            columns::take(&mut b, n * 32)?
                .chunks_exact(32)
                .map(|x| x.try_into().unwrap())
                .collect::<Vec<Hash>>(),
        );
        if n == 1 {
            break;
        }
        n /= 2;
    }
    ensure!(
        b.is_empty() && tree.last().unwrap()[0] == manifest.root,
        "persisted root"
    );
    let index = Index {
        manifest,
        records,
        tree,
        build: Value::Null,
    };
    let trades = hybrid_key_audit::data(root)?;
    let mut counts = BTreeMap::<u32, (usize, usize)>::new();
    for r in &index.records {
        let c = counts.entry(r.trade.collection).or_default();
        c.0 += 1;
        if r.birth <= 2 {
            c.1 += 1;
        }
    }
    let (&collection, &(total, core)) = counts
        .iter()
        .filter(|(_, (n, c))| (8..=40).contains(n) && *c >= 4)
        .max_by_key(|(_, (_, c))| *c)
        .context("bounded real collection")?;
    let es = trades
        .iter()
        .filter(|t| t.collection == collection)
        .map(event)
        .collect::<Result<Vec<_>>>()?;
    let make = |n: usize, closed: bool| {
        let atoms = (0..n)
            .map(|i| crate::query::AtomSpec {
                id: format!("e{i}"),
                event_types: BTreeSet::from(["NFTTrade".into()]),
                fields: BTreeMap::from([
                    ("src".into(), Term::Variable(format!("p{i}"))),
                    (
                        "dst".into(),
                        Term::Variable(format!("p{}", if closed { (i + 1) % n } else { i + 1 })),
                    ),
                    (
                        "collection".into(),
                        Term::Constant((collection as i64).into()),
                    ),
                ]),
                local_predicates: vec![],
            })
            .collect();
        let mut constraints = vec![];
        for i in 0..n - 1 {
            for field in ["order_seq", "block_ts"] {
                constraints.push(Constraint::Order {
                    earlier: format!("e{i}"),
                    later: format!("e{}", i + 1),
                    field: field.into(),
                });
            }
        }
        QuerySpec {
            query_id: format!(
                "unregistered_{}_event_{}",
                n,
                if closed { "cycle" } else { "chain" }
            ),
            result_semantics: "full_event_binding_v1".into(),
            scope: ScopeSpec::Episode {
                anchor_atom: "e0".into(),
                window_seconds: 1800,
            },
            branches: vec![crate::query::BranchSpec {
                id: "runtime_branch".into(),
                atoms,
                constraints,
                event_injective: true,
            }],
        }
    };
    let mut results = vec![];
    for q in [make(4, true), make(2, false)] {
        let expected = crate::reference(&es, &q)?;
        let h = header(&index, &q)?;
        let s = table(&index.records, h.cut)?;
        let rs = columns::rows(&s.tables)?;
        let engine = Engine::new(&rs, &q, &BTreeSet::from(["NFTTrade".into()]))?;
        let (answers, _) = engine.replay_rows()?;
        ensure!(
            engine.canonical_answers(&answers)? == expected,
            "unseen query oracle"
        );
        let (wire, sizes) = pack(&h, &index.records[..h.opened], &q, &answers)?;
        let counts = verify(&index.manifest, &q, &wire, "count")?;
        let replay = verify(&index.manifest, &q, &wire, "replay")?;
        crate::save(&out.join(format!("{}.query.json", q.query_id)), &q)?;
        write_new(&out.join(format!("{}.packet", q.query_id)), &wire)?;
        results.push(
            json!({"query":q.query_id,"plan":h.plan,"answers":answers.len(),"FP":0,"FN":0,
            "sizes":sizes,"count":counts,"replay":replay}),
        );
        eprintln!(
            "UNSEEN {} answers={} domain={}",
            q.query_id,
            answers.len(),
            h.cut
        );
    }
    crate::save(
        &out.join("summary.json"),
        &json!({"same_preexisting_manifest":index.manifest.digest(),
        "full_epoch_rows":trades.len(),"oracle_restricted_only_by_query_collection_constant":true,
        "real_collection":collection,"collection_rows":total,"collection_core_rows":core,"results":results}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{AtomSpec, BranchSpec, CompareOp, LocalPredicate};
    fn row(id: u64, t: u64, s: u32, d: u32) -> Trade {
        Trade {
            order: id,
            time: t,
            src: s,
            dst: d,
            nft: 1,
            collection: 7,
            tx: id as u32,
        }
    }
    #[test]
    fn direct_adapter_matches_all_old_columns_and_rejects_identity_or_integer_corruption() -> Result<()> {
        let data = vec![row(90,700,1,2),row(2,0,u32::MAX,1),row(7,700,2,1)];
        let records = data.iter().enumerate().map(|(i,t)| Record {trade:*t,birth:i as u8}).collect::<Vec<_>>();
        let direct = table(&records,records.len())?;
        let old = columns::seal(&data.iter().map(event).collect::<Result<Vec<_>>>()?,"local".into())?;
        let dt = &direct.tables["NFTTrade"];
        let ot = &old.tables["NFTTrade"];
        assert_eq!(dt.n,ot.n);
        assert_eq!(dt.field_ids,ot.field_ids);
        for (f,c) in &ot.columns {
            for i in 0..ot.n { assert_eq!(c.get(i),dt.columns[f].get(i),"field {f} row {i}"); }
        }
        let mut bad = records.clone();
        bad[1].trade.order = bad[0].trade.order;
        assert!(table(&bad,bad.len()).is_err());
        let mut bad = records;
        bad[0].trade.time = i64::MAX as u64 + 1;
        assert!(table(&bad,bad.len()).is_err());
        Ok(())
    }
    #[test]
    fn optimized_soundness_preserves_unsorted_inputs_and_rejects_nonadjacent_duplicates() -> Result<()> {
        let data = vec![row(1,10,1,2),row(2,20,2,1),row(3,30,2,1),row(4,40,1,2)];
        let ix = build(&data,"sound".into())?;
        let q = cycle(2,1800);
        let t = table(&ix.records,ix.records.len())?;
        let rows = columns::rows(&t.tables)?;
        let e = Engine::new(&rows,&q,&BTreeSet::from(["NFTTrade".into()]))?;
        let (mut a,_) = e.replay_rows()?;
        assert!(a.len()>=3);
        e.sound_unique(&a)?;
        a.reverse(); e.sound_unique(&a)?;
        let mut dup = a.clone(); dup.push(a[0].clone());
        assert!(e.sound_unique(&dup).is_err());
        let mut bad = a.clone(); bad[0].rows[0].1 = rows.len();
        assert!(e.sound_unique(&bad).is_err());
        let mut bad = a.clone(); bad[0].branch = "missing".into();
        assert!(e.sound_unique(&bad).is_err());
        let mut bad = a.clone(); bad[0].rows[0] = bad[0].rows[1].clone();
        assert!(e.sound_unique(&bad).is_err());
        let mut filtered = q.clone();
        filtered.branches[0].atoms[0].local_predicates.push(LocalPredicate {
            field:"nft".into(),op:CompareOp::Eq,value:99i64.into()});
        let f = Engine::new(&rows,&filtered,&BTreeSet::from(["NFTTrade".into()]))?;
        assert!(f.sound_unique(&a).is_err());
        Ok(())
    }
    fn cycle(n: usize, window: i64) -> QuerySpec {
        let atoms = (0..n)
            .map(|i| AtomSpec {
                id: format!("a{i}"),
                event_types: BTreeSet::from(["NFTTrade".into()]),
                fields: BTreeMap::from([
                    ("src".into(), Term::Variable(format!("v{i}"))),
                    ("dst".into(), Term::Variable(format!("v{}", (i + 1) % n))),
                    ("collection".into(), Term::Variable("c".into())),
                ]),
                local_predicates: vec![],
            })
            .collect();
        QuerySpec {
            query_id: format!("unseen-cycle-{n}"),
            result_semantics: "full_event_binding_v1".into(),
            scope: ScopeSpec::Episode {
                anchor_atom: "a0".into(),
                window_seconds: window,
            },
            branches: vec![BranchSpec {
                id: "new-runtime-branch".into(),
                atoms,
                event_injective: true,
                constraints: (0..n - 1)
                    .map(|i| Constraint::Order {
                        earlier: format!("a{i}"),
                        later: format!("a{}", i + 1),
                        field: "order_seq".into(),
                    })
                    .collect(),
            }],
        }
    }
    fn compare(data: &[Trade], q: &QuerySpec) -> Result<()> {
        let ix = build(data, "small-epoch".into())?;
        let h = header(&ix, q)?;
        let s = table(&ix.records, h.cut)?;
        let rows = columns::rows(&s.tables)?;
        let engine = Engine::new(&rows, q, &BTreeSet::from(["NFTTrade".into()]))?;
        let (answers, _) = engine.replay_rows()?;
        let es = data.iter().map(event).collect::<Result<Vec<_>>>()?;
        ensure!(
            engine.canonical_answers(&answers)? == crate::reference(&es, q)?,
            "brute-force equivalence"
        );
        let (wire, _) = pack(&h, &ix.records[..h.opened], q, &answers)?;
        verify(&ix.manifest, q, &wire, "count")?;
        verify(&ix.manifest, q, &wire, "replay")?;
        Ok(())
    }
    #[test]
    fn unseen_cycles_and_parallel_identity() -> Result<()> {
        for n in [2, 3, 4, 5] {
            let mut data = (0..n)
                .map(|i| {
                    row(
                        2 * i as u64 + 1,
                        2047 + 100 * i as u64,
                        i as u32,
                        (i + 1) as u32 % n as u32,
                    )
                })
                .collect::<Vec<_>>();
                                                                                  
            data.push(row(2, 2047, 0, 1));
            data.push(row(30, 3000, 80, 81));
            compare(&data, &cycle(n, 1800))?;
        }
        Ok(())
    }
    #[test]
    fn scope_boundary_and_self_loop() -> Result<()> {
        let data = vec![
            row(1, 2047, 1, 2),
            row(2, 3847, 2, 1),
            row(3, 3848, 2, 1),
            row(4, 2048, 9, 9),
        ];
        compare(&data, &cycle(2, 1800))?;
        compare(&data, &cycle(1, 1800))?;
        let ix = build(&data, "boundary".into())?;
        assert!(ix.manifest.prefix_counts[1] < ix.manifest.prefix_counts[2]);
        assert_eq!(plan(&cycle(2, 1800), &ix.manifest)?.level, Some(2));
        Ok(())
    }
    #[test]
    fn applicability_fallbacks_not_query_ids() -> Result<()> {
        let data = vec![row(1, 1, 1, 2), row(2, 2, 2, 1), row(3, 3, 2, 3)];
        let ix = build(&data, "applicability".into())?;
        let q = cycle(2, 1800);
        assert_eq!(plan(&q, &ix.manifest)?.level, Some(2));
        let mut renamed = q.clone();
        renamed.query_id = "completely-new-name".into();
        assert_eq!(plan(&q, &ix.manifest)?, plan(&renamed, &ix.manifest)?);
        let mut chain = q.clone();
        chain.branches[0].atoms[1]
            .fields
            .insert("dst".into(), Term::Variable("tail".into()));
        assert!(plan(&chain, &ix.manifest)?.level.is_none());
        compare(&data, &chain)?;
        let mut mixed = q.clone();
        mixed.branches[0].atoms[1]
            .fields
            .insert("collection".into(), Term::Variable("other".into()));
        assert!(plan(&mixed, &ix.manifest)?.level.is_none());
        compare(&data, &mixed)?;
        let mut large = q.clone();
        large.scope = ScopeSpec::Episode {
            anchor_atom: "a0".into(),
            window_seconds: 65537,
        };
        assert!(plan(&large, &ix.manifest)?.level.is_none());
        let mut negative = q.clone();
        negative.branches[0].constraints.push(Constraint::NoEvent {
            after: "a0".into(),
            event_types: BTreeSet::from(["NFTTrade".into()]),
            fields: BTreeMap::new(),
            within_seconds: 1800,
        });
        assert!(plan(&negative, &ix.manifest)?.level.is_none());
        compare(&data, &negative)?;
        negative.branches[0].constraints.push(Constraint::NoEvent {
            after: "a0".into(),
            event_types: BTreeSet::from(["NFTTransfer".into()]),
            fields: BTreeMap::new(),
            within_seconds: 1800,
        });
        assert!(plan(&negative, &ix.manifest).is_err());
        Ok(())
    }
    #[test]
    fn complete_prefix_page_boundaries_and_tampering() -> Result<()> {
        let data = (0..400).map(|i| row(i + 1, i, 1, 1)).collect::<Vec<_>>();
        let ix = build(&data, "pages".into())?;
        for pages in 0..=4 {
            let n = (pages * PAGE).min(data.len());
            let p = prefix_proof(&ix, pages);
            verify_prefix(&ix.manifest, &ix.records[..n], &p)?;
            if n > 0 {
                let mut rs = ix.records[..n].to_vec();
                rs[0].trade.nft += 1;
                assert!(verify_prefix(&ix.manifest, &rs, &p).is_err());
            }
        }
        let data = vec![row(1, 1, 1, 2), row(2, 2, 2, 1)];
        let ix = build(&data, "attacks".into())?;
        let q = cycle(2, 1800);
        let h = header(&ix, &q)?;
        let s = table(&ix.records, h.cut)?;
        let rows = columns::rows(&s.tables)?;
        let engine = Engine::new(&rows, &q, &BTreeSet::from(["NFTTrade".into()]))?;
        let (a, _) = engine.replay_rows()?;
        assert!(!a.is_empty());
        attacks(&ix, &q, &h, &a)?;
        Ok(())
    }
    #[test]
    fn scc_matches_reachability_exhaustively() {
        let n = 3;
        for mask in 0..1usize << (n * n) {
            let edges = (0..n)
                .flat_map(|u| (0..n).map(move |v| (u, v)))
                .filter(|(u, v)| mask & (1 << (u * n + v)) != 0)
                .collect::<Vec<_>>();
            let cs = scc(n, &edges);
            let mut r = vec![vec![false; n]; n];
            for i in 0..n {
                r[i][i] = true;
            }
            for &(u, v) in &edges {
                r[u][v] = true;
            }
            for k in 0..n {
                for u in 0..n {
                    for v in 0..n {
                        r[u][v] |= r[u][k] && r[k][v];
                    }
                }
            }
            for u in 0..n {
                for v in 0..n {
                    assert_eq!(cs[u] == cs[v], r[u][v] && r[v][u]);
                }
            }
        }
    }
    #[test]
    fn local_predicates_do_not_relax_semantics() -> Result<()> {
        let data = vec![row(1, 1, 1, 2), row(2, 2, 2, 1)];
        let mut q = cycle(2, 1800);
        q.branches[0].atoms[0]
            .local_predicates
            .push(LocalPredicate {
                field: "nft".into(),
                op: CompareOp::Eq,
                value: 99i64.into(),
            });
        compare(&data, &q)
    }
    #[test]
    fn empty_reduced_domain_is_authenticated() -> Result<()> {
        compare(&[row(1, 1, 1, 2), row(2, 2, 2, 3)], &cycle(2, 1800))
    }
}
