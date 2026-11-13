                                                                               
                                                                             
                                                                              
use crate::{
    columns::{self, Column, RowAnswer},
    engine::Engine,
    hybrid_key_audit::{data, Trade},
    hybrid_live_server,
    hybrid_packet_client::{hash_leaf, hash_node},
    key_projection::KeyPlan,
    model::Scalar,
    query::QuerySpec,
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
    sync::Arc,
    time::Instant,
};

const WIDTHS: [usize; 5] = [4, 8, 12, 16, 20];
const PAGES: [usize; 3] = [1, 4, 16];
const KEYS: [&str; 4] = ["collection_contract", "move_from", "move_to", "nft"];
const FIELDS: [&str; 7] = [
    "collection_contract",
    "move_from",
    "move_to",
    "nft",
    "order_seq",
    "block_ts",
    "tx_hash",
];
#[derive(Serialize, Deserialize)]
struct Trusted {
    version: String,
    source: String,
    n: usize,
    prefix_roots: BTreeMap<String, String>,
    page_roots: BTreeMap<usize, String>,
}
#[derive(Serialize, Deserialize)]
struct Header {
    version: String,
    trusted: String,
    query: String,
    bits: usize,
    page: usize,
    fields: Vec<String>,
    answers: Vec<RowAnswer>,
}
struct Prefix {
    cols: BTreeMap<String, Arc<Column>>,
    blobs: BTreeMap<String, Vec<u8>>,
}
struct PreparedPacket {
    wire: Vec<u8>,
    parts: Value,
    candidates: usize,
    opened: usize,
    proof_bytes: usize,
}
fn put(b: &mut Vec<u8>, n: usize) {
    b.extend((n as u64).to_le_bytes());
}
fn take<'a>(b: &'a [u8], p: &mut usize, n: usize) -> Result<&'a [u8]> {
    ensure!(
        n <= b.len().saturating_sub(*p),
        "truncated structure packet"
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
fn var(b: &mut Vec<u8>, mut n: usize) {
    while n >= 128 {
        b.push((n as u8) | 128);
        n >>= 7;
    }
    b.push(n as u8);
}
fn unvar(b: &[u8], p: &mut usize) -> Result<usize> {
    let mut n = 0u64;
    for s in (0..=63).step_by(7) {
        let x = take(b, p, 1)?[0];
        ensure!(s < 63 || x <= 1, "varint overflow");
        n |= ((x & 127) as u64) << s;
        if x < 128 {
            return Ok(usize::try_from(n)?);
        }
    }
    anyhow::bail!("varint overflow")
}
fn write_new(p: &Path, b: &[u8]) -> Result<()> {
    fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(p)?
        .write_all(b)?;
    Ok(())
}
fn raw(r: &Trade) -> Vec<u8> {
    let mut b = Vec::with_capacity(37);
    b.extend(r.order.to_le_bytes());
    b.extend(r.time.to_le_bytes());
    for x in [r.src, r.dst, r.nft, r.collection, r.tx] {
        b.extend(x.to_le_bytes());
    }
    b.push(1);
    b
}
fn value(r: &Trade, f: &str) -> Scalar {
    match f {
        "collection_contract" => Scalar::Int(r.collection as i64),
        "move_from" => Scalar::Int(r.src as i64),
        "move_to" => Scalar::Int(r.dst as i64),
        "nft" => Scalar::Int(r.nft as i64),
        "order_seq" => Scalar::Int(r.order as i64),
        "block_ts" => Scalar::Int(r.time as i64),
        "tx_hash" => Scalar::Text(format!("tx:{}", r.tx)),
        _ => unreachable!(),
    }
}
fn fingerprint(source: &str, x: &Scalar) -> Result<u32> {
    let mut h = Sha256::new();
    h.update(b"typed-value-prefix/v1");
    h.update(source.as_bytes());
    h.update(serde_json::to_vec(x)?);
    Ok(u32::from_be_bytes(h.finalize()[..4].try_into()?))
}
fn pack(c: &Column, bits: usize) -> Result<Vec<u8>> {
    ensure!((1..=32).contains(&bits), "prefix width");
    let n = c.len();
    let validity = n.div_ceil(8);
    let mut b = vec![0u8; validity + (n * bits).div_ceil(8)];
    let mut acc = 0u64;
    let mut used = 0usize;
    let mut at = validity;
    for i in 0..n {
        let x = match c.get(i) {
            Some(Scalar::Int(x)) => {
                b[i / 8] |= 1 << (i % 8);
                u32::try_from(*x)?
            }
            None => 0,
            _ => anyhow::bail!("coarse scalar type"),
        };
        ensure!(bits == 32 || (x as u64) < (1u64 << bits), "prefix overflow");
        acc |= (x as u64) << used;
        used += bits;
        while used >= 8 {
            b[at] = acc as u8;
            at += 1;
            acc >>= 8;
            used -= 8;
        }
    }
    if used > 0 {
        b[at] = acc as u8;
    }
    Ok(b)
}
fn unpack(b: &[u8], n: usize, bits: usize) -> Result<Column> {
    ensure!((1..=32).contains(&bits), "prefix width");
    let v = n.div_ceil(8);
    ensure!(
        b.len() == v + (n * bits).div_ceil(8),
        "complete prefix length"
    );
    let mut values = Vec::with_capacity(n);
    let mut acc = 0u64;
    let mut used = 0;
    let mut at = v;
    let mask = (1u64 << bits) - 1;
    for i in 0..n {
        while used < bits {
            acc |= (b[at] as u64) << used;
            used += 8;
            at += 1;
        }
        let x = (acc & mask) as i64;
        acc >>= bits;
        used -= bits;
        values.push(if b[i / 8] & (1 << (i % 8)) != 0 {
            Some(Scalar::Int(x))
        } else {
            None
        });
    }
    ensure!(acc == 0, "noncanonical prefix padding");
    if n % 8 != 0 {
        ensure!(b[v - 1] >> (n % 8) == 0, "validity padding");
    }
    Ok(Column::from_values(values.into_iter()))
}
fn prefix_root(t: &Trusted, f: &str, bits: usize, b: &[u8]) -> Result<String> {
    let mut h = Sha256::new();
    h.update(serde_json::to_vec(&(
        "complete-prefix/v1",
        &t.source,
        t.n,
        f,
        bits,
        b.len(),
    ))?);
    h.update(b);
    Ok(format!("{:x}", h.finalize()))
}
fn tree(rows: &[Trade], page: usize) -> Result<Vec<Vec<[u8; 32]>>> {
    let cap = rows.len().div_ceil(page).next_power_of_two();
    let mut leaves = Vec::with_capacity(cap);
    for (i, chunk) in rows.chunks(page).enumerate() {
        let b = chunk.iter().flat_map(raw).collect::<Vec<_>>();
        leaves.push(hash_leaf(page, i, &b)?);
    }
    for i in leaves.len()..cap {
        leaves.push(hash_leaf(page, i, b"EMPTY")?);
    }
    let mut levels = vec![leaves];
    while levels.last().unwrap().len() > 1 {
        let next = levels
            .last()
            .unwrap()
            .chunks_exact(2)
            .map(|p| hash_node(&p[0], &p[1]))
            .collect();
        levels.push(next);
    }
    Ok(levels)
}
fn hex(x: &[u8; 32]) -> String {
    x.iter().map(|v| format!("{v:02x}")).collect()
}
fn proof(levels: &[Vec<[u8; 32]>], ids: &[usize]) -> Vec<u8> {
    let mut active = ids.iter().copied().collect::<BTreeSet<_>>();
    let mut b = Vec::new();
    for level in &levels[..levels.len() - 1] {
        for &i in &active {
            if !active.contains(&(i ^ 1)) {
                b.extend(level[i ^ 1]);
            }
        }
        active = active.into_iter().map(|i| i / 2).collect();
    }
    b
}
fn check_proof(
    mut active: BTreeMap<usize, [u8; 32]>,
    b: &[u8],
    height: usize,
    root: &str,
) -> Result<()> {
    ensure!(b.len() % 32 == 0, "proof framing");
    if active.is_empty() {
        ensure!(b.is_empty(), "empty domain proof");
        return Ok(());
    }
    let mut p = 0;
    for _ in 0..height {
        let mut next = BTreeMap::new();
        for (&i, v) in &active {
            let sibling = if let Some(s) = active.get(&(i ^ 1)) {
                *s
            } else {
                take(b, &mut p, 32)?.try_into()?
            };
            let parent = if i % 2 == 0 {
                hash_node(v, &sibling)
            } else {
                hash_node(&sibling, v)
            };
            if let Some(old) = next.insert(i / 2, parent) {
                ensure!(old == parent, "proof parent");
            }
        }
        active = next;
    }
    ensure!(
        p == b.len() && active.len() == 1 && hex(&active[&0]) == root,
        "exact page root mismatch"
    );
    Ok(())
}
fn page_ids(mask: &[bool], page: usize) -> Vec<usize> {
    mask.iter()
        .enumerate()
        .filter(|(_, v)| **v)
        .map(|(i, _)| i / page)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}
fn build_packet(
    rows: &[Trade],
    t: &Trusted,
    pref: &Prefix,
    levels: &[Vec<[u8; 32]>],
    q: &QuerySpec,
    plan: &KeyPlan,
    bits: usize,
    page: usize,
    mask: &[bool],
    answers: &[RowAnswer],
) -> Result<PreparedPacket> {
    let ids = page_ids(mask, page);
    let opened = ids
        .iter()
        .flat_map(|i| i * page..((i + 1) * page).min(rows.len()))
        .collect::<Vec<_>>();
    let head = Header {
        version: "prefix-structure-packet/v1".into(),
        trusted: columns::digest(&serde_json::to_vec(t)?),
        query: columns::digest(&serde_json::to_vec(q)?),
        bits,
        page,
        fields: plan.fields.iter().cloned().collect(),
        answers: answers.to_vec(),
    };
    let h = serde_json::to_vec(&head)?;
    let mut p = Vec::new();
    for f in &head.fields {
        let blob = &pref.blobs[f];
        put(&mut p, blob.len());
        p.extend(blob);
    }
    let mut d = Vec::new();
    put(&mut d, ids.len());
    let mut last = 0;
    for &i in &ids {
        var(&mut d, i - last);
        last = i;
    }
    let mut field_sizes = BTreeMap::new();
    for f in FIELDS {
        let c = Column::from_values(opened.iter().map(|&i| Some(value(&rows[i], f))));
        let blob = c.encode();
        field_sizes.insert(f, blob.len());
        put(&mut d, blob.len());
        d.extend(blob);
    }
    let a = proof(levels, &ids);
    let proof_bytes = a.len();
    let sections = [h, p, d, a];
    let mut wire = b"PFXSTRUCT1".to_vec();
    let mut sizes = Vec::new();
    for s in &sections {
        let zip = crate::compress(s)?;
        sizes.push(json!({"raw":s.len(),"wire":zip.len()}));
        put(&mut wire, zip.len());
        wire.extend(zip);
    }
    Ok(PreparedPacket {
        wire,
        parts: json!({"header_answers":sizes[0],"complete_prefixes":sizes[1],
        "exact_openings":sizes[2],"merkle_hashes":sizes[3],"exact_field_raw_bytes":field_sizes,"framing":42}),
        candidates: mask.iter().filter(|v| **v).count(),
        opened: opened.len(),
        proof_bytes,
    })
}
fn sections(wire: &[u8]) -> Result<Vec<Vec<u8>>> {
    ensure!(wire.starts_with(b"PFXSTRUCT1"), "prefix packet magic");
    let mut p = b"PFXSTRUCT1".len();
    let mut out = Vec::new();
    for _ in 0..4 {
        let n = read(wire, &mut p)?;
        out.push(crate::decompress(take(wire, &mut p, n)?)?);
    }
    ensure!(p == wire.len(), "trailing packet bytes");
    Ok(out)
}
fn int(c: &Column, i: usize) -> Result<i64> {
    match c.get(i) {
        Some(Scalar::Int(v)) => Ok(*v),
        _ => anyhow::bail!("exact int field"),
    }
}
fn check(wire: &[u8], t: &Trusted, q: &QuerySpec, mode: &str) -> Result<Value> {
    ensure!(["count", "replay", "both"].contains(&mode), "client mode");
    let start = Instant::now();
    let s = sections(wire)?;
    let head: Header = serde_json::from_slice(&s[0])?;
    ensure!(
        head.version == "prefix-structure-packet/v1"
            && head.trusted == columns::digest(&serde_json::to_vec(t)?)
            && head.query == columns::digest(&serde_json::to_vec(q)?),
        "prefix epoch/query binding"
    );
    ensure!(
        WIDTHS.contains(&head.bits) && PAGES.contains(&head.page),
        "sealed precision/page"
    );
    let plan = KeyPlan::compile(q)?;
    ensure!(
        head.fields == plan.fields.iter().cloned().collect::<Vec<_>>(),
        "complete structural field envelope"
    );
    let mut p = 0;
    let mut coarse = BTreeMap::new();
    for f in &head.fields {
        let n = read(&s[1], &mut p)?;
        let blob = take(&s[1], &mut p, n)?;
        ensure!(
            prefix_root(t, f, head.bits, blob)? == t.prefix_roots[&format!("{}:{f}", head.bits)],
            "complete prefix authentication"
        );
        coarse.insert(f.clone(), Arc::new(unpack(blob, t.n, head.bits)?));
    }
    ensure!(p == s[1].len(), "prefix trailing bytes");
    let prefix_auth_s = start.elapsed().as_secs_f64();
    let timer = Instant::now();
    let (mask, coarse_cache) = plan.execute(&coarse, t.n)?;
    let coarse_layouts = coarse_cache.projections.len();
    drop(coarse_cache);
    drop(coarse);
    let coarse_filter_s = timer.elapsed().as_secs_f64();
    let timer = Instant::now();
    let mut p = 0;
    let nopen = read(&s[2], &mut p)?;
    ensure!(nopen <= t.n.div_ceil(head.page), "page count");
    let mut ids = Vec::with_capacity(nopen);
    let mut last = 0usize;
    for i in 0..nopen {
        last = last
            .checked_add(unvar(&s[2], &mut p)?)
            .context("page overflow")?;
        if i > 0 {
            ensure!(last > ids[i - 1], "duplicate page");
        }
        ids.push(last);
    }
    ensure!(
        ids == page_ids(&mask, head.page),
        "incomplete coarse candidate opening"
    );
    let nrows = ids
        .iter()
        .map(|i| (t.n - i * head.page).min(head.page))
        .sum::<usize>();
    let mut cols = BTreeMap::new();
    for f in FIELDS {
        let n = read(&s[2], &mut p)?;
        cols.insert(
            f.to_string(),
            Arc::new(Column::decode(take(&s[2], &mut p, n)?, nrows)?),
        );
    }
    ensure!(p == s[2].len(), "exact fields trailing bytes");
    let mut leaves = BTreeMap::new();
    let mut at = 0;
    let mut positions = Vec::new();
    let mut globals = Vec::new();
    let mut orders = Vec::new();
    for &page in &ids {
        let mut bytes = Vec::with_capacity(head.page * 37);
        for global in page * head.page..((page + 1) * head.page).min(t.n) {
            let order = u64::try_from(int(&cols["order_seq"], at)?)?;
            let time = u64::try_from(int(&cols["block_ts"], at)?)?;
            let tx = match cols["tx_hash"].get(at) {
                Some(Scalar::Text(v)) => v
                    .strip_prefix("tx:")
                    .context("tx framing")?
                    .parse::<u32>()?,
                _ => anyhow::bail!("tx field"),
            };
            let r = Trade {
                order,
                time,
                tx,
                src: u32::try_from(int(&cols["move_from"], at)?)?,
                dst: u32::try_from(int(&cols["move_to"], at)?)?,
                nft: u32::try_from(int(&cols["nft"], at)?)?,
                collection: u32::try_from(int(&cols["collection_contract"], at)?)?,
            };
            bytes.extend(raw(&r));
            if mask[global] {
                globals.push(global);
                positions.push(at);
                orders.push(order);
            }
            at += 1;
        }
        leaves.insert(page, hash_leaf(head.page, page, &bytes)?);
    }
    let height = t.n.div_ceil(head.page).next_power_of_two().trailing_zeros() as usize;
    check_proof(leaves, &s[3], height, &t.page_roots[&head.page])?;
    let exact_auth_s = timer.elapsed().as_secs_f64();
    let timer = Instant::now();
    let globals = Arc::new(globals);
    let positions = Arc::new(positions);
    let mut view = cols
        .iter()
        .map(|(f, c)| Ok((f.clone(), Arc::new(c.project(positions.clone())?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    view.insert(
        "event_id".into(),
        Arc::new(Column::from_values(
            orders
                .iter()
                .map(|o| Some(Scalar::Text(format!("order:{o}")))),
        )),
    );
    view.insert(
        "event_type".into(),
        Arc::new(Column::from_values(
            orders.iter().map(|_| Some(Scalar::Text("NFTTrade".into()))),
        )),
    );
    let tables = columns::sparse_view("NFTTrade", view, globals.clone())?;
    let rows = columns::rows(&tables)?;
    let engine = Engine::new(&rows, q, &BTreeSet::from(["NFTTrade".into()]))?;
    let exact_compile_s = timer.elapsed().as_secs_f64();
    let common_s = start.elapsed().as_secs_f64();
    let (mut count_s, mut sound_s, mut replay_s) = (0., 0., 0.);
    if mode != "replay" {
        let timer = Instant::now();
        let (n, _) = engine.count()?;
        count_s = timer.elapsed().as_secs_f64();
        let timer = Instant::now();
        engine.sound_unique(&head.answers)?;
        ensure!(
            n == num_bigint::BigUint::from(head.answers.len()),
            "cardinality seal mismatch"
        );
        sound_s = timer.elapsed().as_secs_f64();
    }
    if mode != "count" {
        let timer = Instant::now();
        let actual = engine.replay_rows()?.0;
        let mut claims = head.answers.clone();
        claims.sort();
        ensure!(claims.windows(2).all(|w| w[0] != w[1]), "duplicate answer");
        ensure!(actual == claims, "full binding replay mismatch");
        replay_s = timer.elapsed().as_secs_f64();
    }
    Ok(
        json!({"verified":true,"answers":head.answers.len(),"candidate_rows":globals.len(),"opened_rows":nrows,"mode":mode,
        "prefix_auth_decode_s":prefix_auth_s,"coarse_filter_s":coarse_filter_s,"exact_row_decode_auth_s":exact_auth_s,
        "exact_compile_s":exact_compile_s,"count_s":count_s,"sound_unique_s":sound_s,"replay_s":replay_s,
        "common_s":common_s,"client_count_s":common_s+count_s+sound_s,"client_replay_s":common_s+replay_s,
        "client_internal_s":start.elapsed().as_secs_f64(),"coarse_layouts":coarse_layouts,"coarse_indexes_reused_as_exact":0,
        "peak_rss_bytes":crate::memory::peak_bytes(),"wire_bytes":wire.len()}),
    )
}
pub fn client(args: &[String]) -> Result<()> {
    ensure!(
        args.len() == 8,
        "prefix-client QUERY_DOC COMPILED TRUSTED PACKET NEW_RESULT MODE"
    );
    let q: QuerySpec = serde_json::from_slice(&fs::read(&args[3])?)?;
    ensure!(
        q == crate::load_query(Path::new(&args[2]))?,
        "QuerySpec/document mismatch"
    );
    let trusted: Trusted = serde_json::from_slice(&fs::read(&args[4])?)?;
    let wire = fs::read(&args[5])?;
    let result = check(&wire, &trusted, &q, &args[7])?;
    crate::save(Path::new(&args[6]), &result)?;
    Ok(())
}
fn answer_check(root: &Path, qi: usize, rows: &[Trade], answers: &[RowAnswer]) -> Result<()> {
    let reference: Value = serde_json::from_slice(&fs::read(root.join(format!(
        "FunnelQ_ScopeFactor_Feasibility/results/q{qi}_reference.json"
    )))?)?;
    let mut wanted = serde_json::from_value::<Vec<Vec<u64>>>(reference["answers"].clone())?;
    let mut actual = answers
        .iter()
        .map(|a| a.rows.iter().map(|r| rows[r.1].order).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    wanted.sort();
    actual.sort();
    ensure!(wanted == actual, "independent full-domain oracle mismatch");
    Ok(())
}
fn server(full: &columns::Seal, q: &QuerySpec) -> Result<(Vec<RowAnswer>, Vec<bool>)> {
    let plan = KeyPlan::compile(q)?;
    let base = &full.tables[&plan.event_type];
    let (mask, cache) = plan.execute(&base.columns, base.n)?;
    let ids = Arc::new((0..base.n).filter(|&i| mask[i]).collect::<Vec<_>>());
    let view = columns::project_table(&plan.event_type, base, ids)?;
    let rows = columns::rows(&view)?;
    let engine =
        Engine::with_projections(&rows, q, &BTreeSet::from(["NFTTrade".into()]), Some(&cache))?;
    Ok((engine.replay_rows()?.0, mask))
}
fn launch(exe: &Path, argv: &[String], result: &Path) -> Result<(f64, Value)> {
    let timer = Instant::now();
    let output = Command::new(exe).args(argv).output()?;
    let wall = timer.elapsed().as_secs_f64();
    ensure!(
        output.status.success(),
        "isolated client rejected: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok((wall, serde_json::from_slice(&fs::read(result)?)?))
}
pub fn run(args: &[String]) -> Result<()> {
    ensure!(args.len() == 5, "prefix-structure ROOT NEW_OUTPUT ROUNDS");
    let root = Path::new(&args[2]);
    let out = Path::new(&args[3]);
    let rounds = args[4].parse::<usize>()?;
    ensure!(rounds > 0, "measured rounds");
    fs::create_dir(out)?;
    let timer = Instant::now();
    let rows = data(root)?;
    let (es, source) = crate::events(root, None)?;
    let full = columns::seal(&es, source.clone())?;
    drop(es);
    ensure!(full.tables["NFTTrade"].n == rows.len(), "source row layout");
    let mut trusted = Trusted {
        version: "prefix-structure-seal/v1".into(),
        source,
        n: rows.len(),
        prefix_roots: BTreeMap::new(),
        page_roots: BTreeMap::new(),
    };
    let mut prefix_bank = BTreeMap::new();
    let mut hashes = HashMap::<Scalar, u32>::new();
    let mut hashed_dicts = BTreeMap::new();
    for f in KEYS {
        let c = &full.tables["NFTTrade"].columns[f];
        let mut d = Vec::new();
        for x in c.dict.iter() {
            let v = if let Some(v) = hashes.get(x) {
                *v
            } else {
                let v = fingerprint(&trusted.source, x)?;
                hashes.insert(x.clone(), v);
                v
            };
            d.push(v);
        }
        hashed_dicts.insert(f.to_string(), d);
    }
    drop(hashes);
    for bits in WIDTHS {
        let mut pref = Prefix {
            cols: BTreeMap::new(),
            blobs: BTreeMap::new(),
        };
        for f in KEYS {
            let mut c = full.tables["NFTTrade"].columns[f].as_ref().clone();
            c.dict = Arc::new(
                hashed_dicts[f]
                    .iter()
                    .map(|v| Scalar::Int((v >> (32 - bits)) as i64))
                    .collect(),
            );
            let b = pack(&c, bits)?;
            trusted
                .prefix_roots
                .insert(format!("{bits}:{f}"), prefix_root(&trusted, f, bits, &b)?);
            pref.blobs.insert(f.into(), b);
            pref.cols.insert(f.into(), Arc::new(c));
        }
        prefix_bank.insert(bits, pref);
    }
    let mut trees = BTreeMap::new();
    for page in PAGES {
        let levels = tree(&rows, page)?;
        trusted
            .page_roots
            .insert(page, hex(&levels.last().unwrap()[0]));
        trees.insert(page, levels);
    }
    let offline_s = timer.elapsed().as_secs_f64();
    crate::save(&out.join("trusted_prefix_seal.json"), &trusted)?;
    crate::save(&out.join("trusted_manifest.json"), &full.manifest)?;
                                                                                                               
    let baseline_tree = hybrid_live_server::tree(&rows)?;
    crate::save(
        &out.join("trusted_row_root.json"),
        &json!({"nrows":rows.len(),"block_rows":16,"root":hex(&baseline_tree.last().unwrap()[0]),"height":baseline_tree.len()-1}),
    )?;
    let mut paths = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|x| x.map(|v| v.path()))
        .collect::<std::io::Result<Vec<_>>>()?;
    paths.sort();
    ensure!(paths.len() == 5, "five formal queries");
    let mut grid = Vec::<Value>::new();
    for (qi, path) in paths.iter().enumerate() {
        let qi = qi + 1;
        let q = crate::load_query(path)?;
        let plan = KeyPlan::compile(&q)?;
        ensure!(
            plan.fields.iter().all(|f| KEYS.contains(&f.as_str())),
            "this input's structural fields"
        );
        let timer = Instant::now();
        let (answers, exact_mask) = server(&full, &q)?;
        let server_query_s = timer.elapsed().as_secs_f64();
        answer_check(root, qi, &rows, &answers)?;
        for bits in WIDTHS {
            let timer = Instant::now();
            let (mask, coarse) = plan.execute(&prefix_bank[&bits].cols, rows.len())?;
            drop(coarse);
            let server_coarse_s = timer.elapsed().as_secs_f64();
            ensure!(
                exact_mask.iter().zip(&mask).all(|(e, c)| !e || *c),
                "false negative coarse mask"
            );
            for page in PAGES {
                let timer = Instant::now();
                let pack = build_packet(
                    &rows,
                    &trusted,
                    &prefix_bank[&bits],
                    &trees[&page],
                    &q,
                    &plan,
                    bits,
                    page,
                    &mask,
                    &answers,
                )?;
                let packet_s = timer.elapsed().as_secs_f64();
                let verified = check(&pack.wire, &trusted, &q, "both")?;
                let record = json!({"query":qi,"bits":bits,"page":page,"answers":answers.len(),"FP":0,"FN":0,
                    "coarse_false_negative_events":0,"exact_candidates":exact_mask.iter().filter(|v|**v).count(),"coarse_candidates":pack.candidates,
                    "candidate_inflation":pack.candidates as f64/exact_mask.iter().filter(|v|**v).count() as f64,"opened_rows":pack.opened,
                    "wire_bytes":pack.wire.len(),"proof_bytes":pack.proof_bytes,"parts":pack.parts,"server_query_s":server_query_s,
                    "server_coarse_filter_s":server_coarse_s,"packet_s":packet_s,"client":verified,
                    "count_compute_s":server_query_s+server_coarse_s+packet_s+verified["client_count_s"].as_f64().unwrap(),
                    "timing_scope":"single-pass in-process structural screen; RSS is whole harness, count/replay share preparation"});
                eprintln!("STRUCT Q{qi} bits={bits} page={page} candidates={} wire={:.3}MiB count={:.3}s FP=FN=0",pack.candidates,pack.wire.len() as f64/1048576.,record["count_compute_s"].as_f64().unwrap());
                crate::save(
                    &out.join(format!("q{qi}_b{bits}_p{page}_screen.json")),
                    &record,
                )?;
                grid.push(record);
            }
        }
    }
    crate::save(
        &out.join("grid.json"),
        &json!({"input_rows":rows.len(),"offline_build_s":offline_s,"records":grid,
        "schema_roots_count":trusted.prefix_roots.len()+trusted.page_roots.len(),"tree_storage_bytes":trees.values().flat_map(|v|v.iter()).map(|v|v.len()*32).sum::<usize>(),
        "prefix_storage_bytes":prefix_bank.values().flat_map(|v|v.blobs.values()).map(Vec::len).sum::<usize>(),"trusted_sealer":true,"on_chain_deployed":false}),
    )?;
                                                                                 
                                                                              
    let (bits, page) = WIDTHS
        .into_iter()
        .flat_map(|b| PAGES.into_iter().map(move |p| (b, p)))
        .min_by_key(|(b, p)| {
            grid.iter()
                .filter(|r| r["bits"] == *b && r["page"] == *p)
                .map(|r| r["wire_bytes"].as_u64().unwrap())
                .sum::<u64>()
        })
        .unwrap();
    eprintln!("UNIFORM_CONFIG bits={bits} page={page}; selection by total wire, no per-Q branch");
    let exe = std::env::current_exe()?;
    let mut paired = Vec::new();
    for rep in 0..=rounds {
        for (index, path) in paths.iter().enumerate() {
            let qi = index + 1;
            let q = crate::load_query(path)?;
            let plan = KeyPlan::compile(&q)?;
            for pref_mode in if (rep + index) % 2 == 0 {
                [true, false]
            } else {
                [false, true]
            } {
                let name = if pref_mode { "prefix" } else { "hybrid" };
                let dir = out.join(format!("q{qi}_{name}_r{rep}"));
                fs::create_dir(&dir)?;
                let timer = Instant::now();
                let (answers, exact) = server(&full, &q)?;
                let query_s = timer.elapsed().as_secs_f64();
                answer_check(root, qi, &rows, &answers)?;
                let timer = Instant::now();
                let (wire, parts, candidates, opened, proof_bytes) = if pref_mode {
                    let (mask, coarse) = plan.execute(&prefix_bank[&bits].cols, rows.len())?;
                    drop(coarse);
                    let p = build_packet(
                        &rows,
                        &trusted,
                        &prefix_bank[&bits],
                        &trees[&page],
                        &q,
                        &plan,
                        bits,
                        page,
                        &mask,
                        &answers,
                    )?;
                    (p.wire, p.parts, p.candidates, p.opened, p.proof_bytes)
                } else {
                    let ids = (0..rows.len()).filter(|&i| exact[i]).collect::<Vec<_>>();
                    let (wire, opened, proof) = hybrid_live_server::packet(
                        &rows,
                        &full,
                        &baseline_tree,
                        &q,
                        &ids,
                        &plan.fields,
                        &answers,
                    )?;
                    (wire, Value::Null, ids.len(), opened, proof)
                };
                let packet_s = timer.elapsed().as_secs_f64();
                write_new(&dir.join("response.bin"), &wire)?;
                crate::save(&dir.join("query.json"), &q)?;
                let mut modes = serde_json::Map::new();
                for mode in if rep % 2 == 0 {
                    ["count", "replay"]
                } else {
                    ["replay", "count"]
                } {
                    let result = dir.join(format!("{mode}.json"));
                    let argv = if pref_mode {
                        vec![
                            "prefix-client".into(),
                            path.display().to_string(),
                            dir.join("query.json").display().to_string(),
                            out.join("trusted_prefix_seal.json").display().to_string(),
                            dir.join("response.bin").display().to_string(),
                            result.display().to_string(),
                            mode.into(),
                        ]
                    } else {
                        vec![
                            "hybrid-client".into(),
                            path.display().to_string(),
                            dir.join("query.json").display().to_string(),
                            out.join("trusted_manifest.json").display().to_string(),
                            out.join("trusted_row_root.json").display().to_string(),
                            dir.join("response.bin").display().to_string(),
                            result.display().to_string(),
                            mode.into(),
                        ]
                    };
                    let (wall, details) = launch(&exe, &argv, &result)?;
                    ensure!(details["verified"] == true, "isolated validation");
                    modes.insert(mode.into(),json!({"client_wall_s":wall,"compute_e2e_s":query_s+packet_s+wall,"details":details}));
                }
                let record = json!({"query":qi,"backend":name,"round":rep,"warmup":rep==0,"bits":bits,"page":page,
                    "wire_bytes":wire.len(),"proof_bytes":proof_bytes,"parts":parts,"candidate_rows":candidates,"opened_rows":opened,
                    "server_query_s":query_s,"server_packet_s":packet_s,"FP":0,"FN":0,"answers":answers.len(),"modes":modes});
                eprintln!(
                    "PAIRED Q{qi} {name} r{rep} count={:.3}s wire={:.3}MiB",
                    record["modes"]["count"]["compute_e2e_s"].as_f64().unwrap(),
                    wire.len() as f64 / 1048576.
                );
                crate::save(&dir.join("record.json"), &record)?;
                paired.push(record);
            }
        }
    }
    crate::save(
        &out.join("paired.json"),
        &json!({"records":paired,"uniform_bits":bits,"uniform_page":page,"rounds":rounds,
        "input_rows":rows.len(),"network_included":false,"on_chain_deployed":false,"FP":0,"FN":0,
        "timing":"same binary interleaved backends; isolated modes; includes client read/process lifecycle; excludes offline seal, network, server artifact writes, oracle"}),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn structure_sections_roundtrip() -> Result<()> {
        let sections_in = [b"header".to_vec(), vec![0, 1, 2], vec![], vec![9; 32]];
        let mut packet = b"PFXSTRUCT1".to_vec();
        for part in &sections_in {
            let zip = crate::compress(part)?;
            put(&mut packet, zip.len());
            packet.extend(zip);
        }
        assert_eq!(sections(&packet)?, sections_in.to_vec());
        packet.push(0);
        assert!(sections(&packet).is_err());
        Ok(())
    }
    #[test]
    fn prefix_types_missing_and_collision_are_safe() -> Result<()> {
        ensure!(
            fingerprint("epoch", &Scalar::Int(1))?
                != fingerprint("epoch", &Scalar::Text("1".into()))?,
            "type separation"
        );
        let c = Column::from_values(
            [
                Some(Scalar::Int(0)),
                None,
                Some(Scalar::Int(15)),
                Some(Scalar::Int(0)),
            ]
            .into_iter(),
        );
        for width in [4, 8, 12, 16, 20, 32] {
            let b = pack(&c, width)?;
            let d = unpack(&b, 4, width)?;
            for i in 0..4 {
                assert_eq!(c.get(i), d.get(i));
            }
            assert!(unpack(&b[..b.len() - 1], 4, width).is_err());
        }
                                                                                      
        let mut seen = HashMap::new();
        let mut collision = None;
        for v in 0..100 {
            let p = fingerprint("epoch", &Scalar::Int(v))? >> 28;
            if let Some(old) = seen.insert(p, v) {
                collision = Some((old, v, p));
                break;
            }
        }
        assert!(collision.is_some());
        Ok(())
    }
    #[test]
    fn exact_pages_reject_omission_mutation_and_bad_root() -> Result<()> {
        let rows = (0..19)
            .map(|i| Trade {
                order: i,
                time: i,
                tx: i as u32,
                src: 1,
                dst: 2,
                nft: 3,
                collection: 4,
            })
            .collect::<Vec<_>>();
        for page in PAGES {
            let t = tree(&rows, page)?;
            let ids = vec![0, rows.len().div_ceil(page) - 1]
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            let active = ids
                .iter()
                .map(|&i| (i, t[0][i]))
                .collect::<BTreeMap<_, _>>();
            let p = proof(&t, &ids);
            let root = hex(&t.last().unwrap()[0]);
            check_proof(active.clone(), &p, t.len() - 1, &root)?;
            let mut bad = active.clone();
            bad.values_mut().next().unwrap()[0] ^= 1;
            assert!(check_proof(bad, &p, t.len() - 1, &root).is_err());
            assert!(check_proof(active, &p, t.len() - 1, &"00".repeat(32)).is_err());
        }
        Ok(())
    }
}
