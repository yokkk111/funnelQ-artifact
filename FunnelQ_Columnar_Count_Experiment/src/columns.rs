                                                                             
                                                                            
use crate::{
    model::{CanonicalEvent, Scalar},
    query::*,
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
pub fn digest(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
pub fn canonical(f: &str) -> &str {
    match f {
        "src" | "src_addr" => "move_from",
        "dst" | "dst_addr" => "move_to",
        "collection" => "collection_contract",
        "block_time_epoch" => "block_ts",
        "tx_id" => "tx_hash",
        _ => f,
    }
}
pub const FIXED: [&str; 6] = [
    "event_id",
    "event_type",
    "order_seq",
    "block_ts",
    "tx_hash",
    "chain_id",
];
#[derive(Clone, Debug)]
pub struct Column {
    pub dict: Arc<Vec<Scalar>>,
    pub codes: Arc<Vec<u32>>,
    selection: Option<Arc<Vec<usize>>>,
}
impl Column {
    pub fn from_values(v: impl Iterator<Item = Option<Scalar>>) -> Self {
        let (mut dict, mut codes, mut ids) = (vec![], vec![], HashMap::new());
        for x in v {
            let c = match x {
                None => 0,
                Some(x) => *ids.entry(x.clone()).or_insert_with(|| {
                    dict.push(x);
                    dict.len() as u32
                }),
            };
            codes.push(c)
        }
        Self {
            dict: Arc::new(dict),
            codes: Arc::new(codes),
            selection: None,
        }
    }
    pub fn len(&self) -> usize {
        self.selection
            .as_ref()
            .map_or(self.codes.len(), |s| s.len())
    }
    pub fn same_storage(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.dict, &other.dict) && Arc::ptr_eq(&self.codes, &other.codes)
    }
    pub fn maps_source_rows(&self, source: &Option<Arc<Vec<usize>>>) -> bool {
        match (&self.selection, source) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b) || a == b,
            _ => false,
        }
    }
    pub fn project(&self, ids: Arc<Vec<usize>>) -> Result<Self> {
        ensure!(ids.iter().all(|&i| i < self.len()), "projection bounds");
        let selection = if let Some(parent) = &self.selection {
            Arc::new(ids.iter().map(|&i| parent[i]).collect())
        } else {
            ids
        };
        Ok(Self {
            dict: self.dict.clone(),
            codes: self.codes.clone(),
            selection: Some(selection),
        })
    }
    pub fn code(&self, i: usize) -> Option<u32> {
        let at = self
            .selection
            .as_ref()
            .map_or(Some(i), |s| s.get(i).copied())?;
        self.codes.get(at).copied()
    }
    pub fn get(&self, i: usize) -> Option<&Scalar> {
        let c = self.code(i)?;
        if c == 0 {
            None
        } else {
            self.dict.get(c as usize - 1)
        }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut v = vec![];
        let mut previous_int = 0i64;
        put(&mut v, self.dict.len() as u64);
        for x in self.dict.iter() {
            match x {
                Scalar::Int(x) => {
                    v.push(1);
                    var(&mut v, zig(x.wrapping_sub(previous_int)));
                    previous_int = *x;
                }
                Scalar::Text(x) => {
                    v.push(2);
                    var(&mut v, x.len() as u64);
                    v.extend(x.as_bytes())
                }
                Scalar::Bool(x) => {
                    v.push(3);
                    v.push(*x as u8)
                }
            }
        }
        put(&mut v, self.len() as u64);
        let mut last = 0i64;
        for i in 0..self.len() {
            let c = self.code(i).unwrap();
            var(&mut v, zig(c as i64 - last));
            last = c as i64;
        }
        v
    }
    pub fn decode(mut b: &[u8], n: usize) -> Result<Self> {
        let d = read(&mut b)? as usize;
        ensure!(d <= b.len(), "dictionary length");
        let mut dict = Vec::with_capacity(d);
        let mut previous_int = 0i64;
        for _ in 0..d {
            let tag = take(&mut b, 1)?[0];
            dict.push(match tag {
                1 => {
                    previous_int = previous_int.wrapping_add(unzig(unvar(&mut b)?));
                    Scalar::Int(previous_int)
                }
                2 => {
                    let l = unvar(&mut b)? as usize;
                    Scalar::Text(String::from_utf8(take(&mut b, l)?.to_vec())?)
                }
                3 => {
                    let x = take(&mut b, 1)?[0];
                    ensure!(x <= 1, "bool encoding");
                    Scalar::Bool(x == 1)
                }
                _ => anyhow::bail!("scalar tag"),
            })
        }
        ensure!(
            read(&mut b)? == n as u64 && n <= b.len(),
            "complete column length"
        );
        let mut codes = Vec::with_capacity(n);
        let mut last = 0i64;
        for _ in 0..n {
            last = last
                .checked_add(unzig(unvar(&mut b)?))
                .context("code overflow")?;
            ensure!(last >= 0 && last as usize <= dict.len(), "dictionary code");
            codes.push(u32::try_from(last)?);
        }
        ensure!(b.is_empty(), "trailing column bytes");
        Ok(Self {
            dict: Arc::new(dict),
            codes: Arc::new(codes),
            selection: None,
        })
    }
}
fn zig(x: i64) -> u64 {
    ((x as u64) << 1) ^ ((x >> 63) as u64)
}
fn unzig(x: u64) -> i64 {
    ((x >> 1) as i64) ^ -((x & 1) as i64)
}
fn var(b: &mut Vec<u8>, mut x: u64) {
    while x >= 128 {
        b.push((x as u8) | 128);
        x >>= 7;
    }
    b.push(x as u8)
}
fn unvar(b: &mut &[u8]) -> Result<u64> {
    let mut x = 0u64;
    for shift in (0..=63).step_by(7) {
        let v = take(b, 1)?[0];
        ensure!(shift < 63 || v <= 1, "varint overflow");
        x |= ((v & 127) as u64) << shift;
        if v < 128 {
            return Ok(x);
        }
    }
    anyhow::bail!("invalid varint")
}
pub fn put(v: &mut Vec<u8>, x: u64) {
    v.extend(x.to_le_bytes())
}
pub fn take<'a>(b: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    ensure!(n <= b.len(), "truncated packet");
    let (x, y) = b.split_at(n);
    *b = y;
    Ok(x)
}
pub fn read(b: &mut &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(take(b, 8)?.try_into()?))
}
#[derive(Clone, Serialize, Deserialize)]
pub struct TableHeader {
    pub n: usize,
    pub layout: String,
    pub fields: BTreeMap<String, String>,
}
#[derive(Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub version: String,
    pub source_digest: String,
    pub types: BTreeMap<String, TableHeader>,
}
#[derive(Clone)]
pub struct Table {
    pub n: usize,
    pub columns: BTreeMap<String, Arc<Column>>,
    pub field_ids: Arc<BTreeMap<String, usize>>,
    pub slots: Vec<Option<Arc<Column>>>,
                                                                     
    pub source_rows: Option<Arc<Vec<usize>>>,
}
fn prepare_columns(tables: &mut BTreeMap<String, Table>) {
    let names = tables
        .values()
        .flat_map(|t| t.columns.keys().cloned())
        .collect::<BTreeSet<_>>();
    let ids = Arc::new(
        names
            .into_iter()
            .enumerate()
            .map(|(i, f)| (f, i))
            .collect::<BTreeMap<_, _>>(),
    );
    for t in tables.values_mut() {
        t.slots = vec![None; ids.len()];
        for (f, c) in &t.columns {
            t.slots[ids[f]] = Some(c.clone());
        }
        t.field_ids = ids.clone();
    }
}
pub struct Seal {
    pub manifest: Manifest,
    pub tables: BTreeMap<String, Table>,
    pub blobs: BTreeMap<(String, String), Vec<u8>>,
}
pub fn seal(es: &[CanonicalEvent], source_digest: String) -> Result<Seal> {
    let ids = es.iter().map(|e| &e.event_id).collect::<BTreeSet<_>>();
    ensure!(ids.len() == es.len(), "duplicate event identity");
    ensure!(
        es.iter().all(|e| e.value("chain_id").is_none()),
        "this experimental epoch is single-chain only"
    );
    let mut groups = BTreeMap::<String, Vec<&CanonicalEvent>>::new();
    for e in es {
        groups.entry(e.event_type.clone()).or_default().push(e)
    }
    let (mut types, mut tables, mut blobs) = (BTreeMap::new(), BTreeMap::new(), BTreeMap::new());
    for (ty, mut rows) in groups {
        rows.sort_by(|a, b| (a.order_seq, &a.event_id).cmp(&(b.order_seq, &b.event_id)));
        let n = rows.len();
        let layout = digest(&serde_json::to_vec(
            &rows.iter().map(|e| &e.event_id).collect::<Vec<_>>(),
        )?);
        let mut fields = FIXED
            .iter()
            .filter(|f| **f != "chain_id")
            .map(|f| f.to_string())
            .collect::<BTreeSet<_>>();
        for e in &rows {
            fields.extend(e.fields.keys().map(|f| canonical(f).to_string()))
        }
        let (mut columns, mut roots) = (BTreeMap::new(), BTreeMap::new());
        for f in fields {
            let c = Column::from_values(rows.iter().map(|e| e.value(&f)));
            let b = c.encode();
            roots.insert(
                f.clone(),
                column_hash(&source_digest, &ty, &layout, n, &f, &b),
            );
            blobs.insert((ty.clone(), f.clone()), b);
            columns.insert(f, Arc::new(c));
        }
        types.insert(
            ty.clone(),
            TableHeader {
                n,
                layout,
                fields: roots,
            },
        );
        tables.insert(
            ty,
            Table {
                n,
                columns,
                field_ids: Arc::new(BTreeMap::new()),
                slots: vec![],
                source_rows: None,
            },
        );
    }
    prepare_columns(&mut tables);
    Ok(Seal {
        manifest: Manifest {
            version: "complete-semantic-columns/v2".into(),
            source_digest,
            types,
        },
        tables,
        blobs,
    })
}
fn column_hash(epoch: &str, ty: &str, layout: &str, n: usize, f: &str, b: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(serde_json::to_vec(&("typed-column/v1", epoch, ty, layout, n, f, b.len())).unwrap());
    h.update(b);
    format!("{:x}", h.finalize())
}
pub struct Row<'a> {
    pub table: &'a Table,
    pub i: usize,
    pub event_type: &'a str,
    pub order_seq: i64,
    pub block_ts: i64,
}
impl Row<'_> {
    pub fn source_row(&self) -> usize {
        self.table
            .source_rows
            .as_ref()
            .map_or(self.i, |ids| ids[self.i])
    }
    pub fn get_slot(&self, slot: usize) -> Option<&Scalar> {
        self.table.slots.get(slot)?.as_ref()?.get(self.i)
    }
    pub fn field_slot(&self, f: &str) -> usize {
        self.table
            .field_ids
            .get(canonical(f))
            .copied()
            .unwrap_or(usize::MAX)
    }
    pub fn get(&self, f: &str) -> Option<&Scalar> {
        self.table.columns.get(canonical(f))?.get(self.i)
    }
    pub fn value(&self, f: &str) -> Option<Scalar> {
        self.get(f).cloned()
    }
    pub fn id(&self) -> &str {
        match self.get("event_id").unwrap() {
            Scalar::Text(s) => s,
            _ => unreachable!(),
        }
    }
}
pub fn rows(tables: &BTreeMap<String, Table>) -> Result<Vec<Row<'_>>> {
    let mut out = vec![];
    for (ty, t) in tables {
        let order = t.columns.get("order_seq").context("missing order column")?;
        let time = t.columns.get("block_ts").context("missing time column")?;
        for i in 0..t.n {
            let int = |col: &Column| -> Result<i64> {
                match col.get(i) {
                    Some(Scalar::Int(x)) => Ok(*x),
                    _ => anyhow::bail!("invalid core column"),
                }
            };
            out.push(Row {
                table: t,
                i,
                event_type: ty,
                order_seq: int(order)?,
                block_ts: int(time)?,
            })
        }
    }
    Ok(out)
}
                                                                          
                                                                          
                                                                                 
pub fn dense_execution_view(
    ty: &str,
    columns: BTreeMap<String, Arc<Column>>,
    n: usize,
) -> Result<BTreeMap<String, Table>> {
    ensure!(columns.values().all(|c| c.len() == n), "dense column lengths");
    ensure!(columns.keys().all(|f| canonical(f) == f), "canonical field names");
    let mut tables = BTreeMap::from([(ty.to_string(), Table {
        n, columns, field_ids: Arc::new(BTreeMap::new()), slots: vec![], source_rows: None,
    })]);
    prepare_columns(&mut tables);
    Ok(tables)
}
                                                                                 
                                                                    
pub fn sparse_view(
    ty: &str,
    columns: BTreeMap<String, Arc<Column>>,
    source_rows: Arc<Vec<usize>>,
) -> Result<BTreeMap<String, Table>> {
    ensure!(
        source_rows.windows(2).all(|w| w[0] < w[1]),
        "noncanonical sparse identity map"
    );
    ensure!(
        columns.values().all(|c| c.len() == source_rows.len()),
        "sparse column lengths"
    );
    let table = Table {
        n: source_rows.len(),
        columns,
        field_ids: Arc::new(BTreeMap::new()),
        slots: vec![],
        source_rows: Some(source_rows),
    };
    let mut tables = BTreeMap::from([(ty.to_string(), table)]);
    prepare_columns(&mut tables);
    Ok(tables)
}
pub fn project_table(
    ty: &str,
    table: &Table,
    ids: Arc<Vec<usize>>,
) -> Result<BTreeMap<String, Table>> {
    let cols = table
        .columns
        .iter()
        .map(|(f, c)| Ok((f.clone(), Arc::new(c.project(ids.clone())?))))
        .collect::<Result<BTreeMap<_, _>>>()?;
    let source = if let Some(parent) = &table.source_rows {
        Arc::new(ids.iter().map(|&i| parent[i]).collect())
    } else {
        ids
    };
    sparse_view(ty, cols, source)
}
pub fn required(q: &QuerySpec, m: &Manifest) -> Result<BTreeSet<(String, String)>> {
    ensure!(
        q.result_semantics == "full_event_binding_v1",
        "unsupported result semantics"
    );
    ensure!(!q.branches.is_empty(), "empty query");
    let mut branch_ids = BTreeSet::new();
    let (mut types, mut fs) = (
        BTreeSet::new(),
        FIXED
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
    );
    fn expr(v: &ValueExpr, fs: &mut BTreeSet<String>) {
        if let ValueExpr::Field(f) = v {
            fs.insert(canonical(&f.field).into());
        }
    }
    for b in &q.branches {
        ensure!(branch_ids.insert(&b.id), "duplicate branch ID");
        for a in &b.atoms {
            types.extend(a.event_types.clone());
            fs.extend(a.fields.keys().map(|f| canonical(f).into()));
            fs.extend(
                a.local_predicates
                    .iter()
                    .map(|p| canonical(&p.field).into()),
            );
        }
        for c in &b.constraints {
            match c {
                Constraint::Compare { left, right, .. } => {
                    expr(left, &mut fs);
                    expr(right, &mut fs)
                }
                Constraint::Order { field, .. } => {
                    fs.insert(canonical(field).into());
                }
                Constraint::DistinctValues { values, .. } => {
                    for v in values {
                        expr(v, &mut fs)
                    }
                }
                Constraint::NoEvent {
                    event_types,
                    fields,
                    ..
                } => {
                    if event_types.is_empty() {
                        types.extend(m.types.keys().cloned())
                    } else {
                        types.extend(event_types.clone())
                    }
                    for (f, v) in fields {
                        fs.insert(canonical(f).into());
                        expr(v, &mut fs)
                    }
                }
                _ => {}
            }
        }
    }
    Ok(types
        .into_iter()
        .flat_map(|ty| {
            let cols = m
                .types
                .get(&ty)
                .map(|t| {
                    fs.iter()
                        .filter(|f| t.fields.contains_key(*f))
                        .map(|f| (ty.clone(), f.clone()))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            cols
        })
        .collect())
}
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Answer {
    pub branch: String,
    pub events: Vec<String>,
}
                                                                          
                                                                                
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RowRef(pub String, pub usize);
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct RowAnswer {
    pub branch: String,
    pub rows: Vec<RowRef>,
}
#[derive(Serialize, Deserialize)]
struct Meta {
    version: String,
    epoch: String,
    query: String,
    columns: Vec<(String, String)>,
    answers: Vec<RowAnswer>,
}
pub fn packet_rows(s: &Seal, q: &QuerySpec, answers: Vec<RowAnswer>) -> Result<Vec<u8>> {
    let cols = required(q, &s.manifest)?.into_iter().collect::<Vec<_>>();
    let meta = Meta {
        version: "authenticated-row-bindings/v1".into(),
        epoch: digest(&serde_json::to_vec(&s.manifest)?),
        query: digest(&serde_json::to_vec(q)?),
        columns: cols.clone(),
        answers,
    };
    let b = serde_json::to_vec(&meta)?;
    let mut v = vec![];
    put(&mut v, b.len() as u64);
    v.extend(b);
    for key in cols {
        let b = &s.blobs[&key];
        put(&mut v, b.len() as u64);
        v.extend(b)
    }
    Ok(v)
}
pub fn authenticate(
    m: &Manifest,
    q: &QuerySpec,
    mut b: &[u8],
) -> Result<(BTreeMap<String, Table>, Vec<RowAnswer>)> {
    ensure!(
        m.version == "complete-semantic-columns/v2",
        "manifest version"
    );
    let n = read(&mut b)? as usize;
    let meta: Meta = serde_json::from_slice(take(&mut b, n)?)?;
    ensure!(
        meta.version == "authenticated-row-bindings/v1",
        "answer encoding version"
    );
    ensure!(
        meta.epoch == digest(&serde_json::to_vec(m)?)
            && meta.query == digest(&serde_json::to_vec(q)?),
        "epoch/query binding"
    );
    ensure!(
        meta.columns == required(q, m)?.into_iter().collect::<Vec<_>>(),
        "complete type/field envelope"
    );
    let mut tables = BTreeMap::<String, Table>::new();
    for (ty, f) in meta.columns {
        let h = &m.types[&ty];
        let n = read(&mut b)? as usize;
        let data = take(&mut b, n)?;
        ensure!(
            column_hash(&m.source_digest, &ty, &h.layout, h.n, &f, data) == h.fields[&f],
            "column authentication"
        );
        let c = Column::decode(data, h.n)?;
        tables
            .entry(ty)
            .or_insert_with(|| Table {
                n: h.n,
                columns: BTreeMap::new(),
                field_ids: Arc::new(BTreeMap::new()),
                slots: vec![],
                source_rows: None,
            })
            .columns
            .insert(f, Arc::new(c));
    }
    ensure!(b.is_empty(), "trailing bytes");
    prepare_columns(&mut tables);
    Ok((tables, meta.answers))
}
                                                                          
                                                                           
#[cfg(test)]
pub fn packet(s: &Seal, q: &QuerySpec, answers: Vec<Answer>) -> Result<Vec<u8>> {
    let rs = rows(&s.tables)?;
    let ids = rs
        .iter()
        .map(|r| (r.id(), RowRef(r.event_type.into(), r.i)))
        .collect::<HashMap<_, _>>();
    packet_rows(
        s,
        q,
        answers
            .into_iter()
            .map(|a| {
                Ok(RowAnswer {
                    branch: a.branch,
                    rows: a
                        .events
                        .iter()
                        .map(|id| ids.get(id.as_str()).cloned().context("fixture event ID"))
                        .collect::<Result<_>>()?,
                })
            })
            .collect::<Result<_>>()?,
    )
}
