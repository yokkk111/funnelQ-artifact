use anyhow::{bail, Context, Result};
use clap::Parser;
use csv::Writer;
use rayon::prelude::*;
use rusqlite::Connection;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::Path;
use std::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    db: String,
    #[arg(long, default_value = "C1")]
    query: String,
    #[arg(long = "query-json")]
    query_json: Option<String>,
    #[arg(long, default_value_t = usize::MAX)]
    limit: usize,
    #[arg(long, default_value_t = usize::MAX)]
    anchor_limit: usize,
    #[arg(long, default_value = "")]
    out_csv: String,
    #[arg(long)]
    report_json: Option<String>,
    #[arg(long, default_value = "full")]
    ablation: String,
    #[arg(long, default_value = "candidates")]
    mode: String,
    #[arg(long, default_value_t = 8)]
    threads: usize,
    #[arg(long, default_value_t = 1)]
    warmup_runs: usize,
    #[arg(long = "phase-marker")]
    phase_marker: Option<String>,
    #[arg(long = "tx-sidecar-db")]
    tx_sidecar_db: Option<String>,
}

#[derive(Clone, Copy, Debug)]
struct Edge {
    order_seq: u32,
    block_ts: i64,
    src_id: u32,
    dst_id: u32,
    nft_id: u32,
    collection_id: u32,
    event_type_id: u8,
    tx_id: u32,
}

#[derive(Clone, Copy, Debug)]
struct Summary {
    closure_id: u32,
    anchor_order: u32,
    start_ts: i64,
    end_ts: i64,
    end_order: u32,
    collection_id: u32,
    transition_count: u32,
    addr_count: u32,
    nft_count: u32,
    nft_ge2_count: u32,
    b_out_event_count: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct ExactMatch {
    root_order: u32,
    middle_order: Option<u32>,
    back_order: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum QueryKind {
    C1,
    C2,
    L3,
    L6,
    H5,
    Q1,
    Q2,
    Q3,
    Q4,
    Q5,
}

impl QueryKind {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "C1" => Ok(Self::C1),
            "C2" => Ok(Self::C2),
            "L3" | "L3_RETURN" => Ok(Self::L3),
            "L6" | "L6_NEG_RETURN_SUPPORT" => Ok(Self::L6),
            "H5" | "H5_23" => Ok(Self::H5),
            "Q1" | "Q1_RAPID_SAME_NFT_ROUNDTRIP" => Ok(Self::Q1),
            "Q2" | "Q2_CROSS_NFT_RECIPROCAL" => Ok(Self::Q2),
            "Q3" | "Q3_SAME_NFT_THREE_ACTOR_CYCLE" => Ok(Self::Q3),
            "Q4" | "Q4_THREE_NFT_THREE_ACTOR_CYCLE" => Ok(Self::Q4),
            "Q5" | "Q5_TWO_NFT_RETURN_CYCLE" => Ok(Self::Q5),
            other => bail!("unsupported compact FunnelQ NFT query: {other}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ablation {
    Full,
    NoBase,
    NoRefined,
    NoPruning,
    NoLexAnchor,
    NoAnchorDirectory,
}

impl Ablation {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "full" => Ok(Self::Full),
            "no_base" | "no-base" => Ok(Self::NoBase),
            "no_refined" | "no-refined" => Ok(Self::NoRefined),
            "no_pruning" | "no-pruning" => Ok(Self::NoPruning),
            "no_lex_anchor" | "no-lex-anchor" => Ok(Self::NoLexAnchor),
            "no_anchor" | "no-anchor" | "no_anchor_directory" | "no-anchor-directory" => {
                Ok(Self::NoAnchorDirectory)
            },
            other => bail!("unsupported ablation: {other}"),
        }
    }

    fn use_anchor_predicate(self) -> bool {
        !matches!(self, Self::NoPruning | Self::NoLexAnchor)
    }
    fn use_anchor_directory(self) -> bool {
        !matches!(
            self,
            Self::NoPruning | Self::NoLexAnchor | Self::NoAnchorDirectory
        )
    }
    fn use_base(self) -> bool {
        !matches!(self, Self::NoBase | Self::NoPruning)
    }
    fn use_refined(self) -> bool {
        !matches!(self, Self::NoRefined | Self::NoPruning)
    }
}

struct Runtime {
    edges: Vec<Edge>,
    order_to_edge: Vec<u32>,
    by_src: HashMap<u64, Vec<u32>>,
}

impl Runtime {
    fn root(&self, summary: &Summary) -> Option<&Edge> {
        let order = usize::try_from(summary.anchor_order).ok()?;
        let idx = *self.order_to_edge.get(order)?;
        if idx == u32::MAX {
            None
        } else {
            self.edges.get(idx as usize)
        }
    }

    fn src_range(
        &self,
        collection_id: u32,
        src_id: u32,
        after_order: u32,
        end_order: u32,
    ) -> &[u32] {
        let Some(posting) = self.by_src.get(&posting_key(collection_id, src_id)) else {
            return &[];
        };
        let lo = lower_bound_by_order(posting, &self.edges, after_order.saturating_add(1));
        let hi = upper_bound_by_order(posting, &self.edges, end_order);
        &posting[lo..hi]
    }
}

#[inline]
fn posting_key(collection_id: u32, src_id: u32) -> u64 {
    ((collection_id as u64) << 32) | src_id as u64
}

#[derive(Default, Serialize)]
struct Report {
    query: String,
    ablation: String,
    output_mode: String,
    rows: usize,
    exact_rows_before_dedup: usize,
    exact_duplicate_rows_removed: usize,
    selected_anchor_atom: String,
    anchor_engine: String,
    uses_anchor_directory: bool,
    anchor_scan_closures: usize,
    anchor_directory_entries_read: usize,
    anchor_engine_isolation_contract_ok: bool,
    closure_universe: usize,
    cand0_anchor_hits: usize,
    cand1_base_hits: usize,
    cand2_refined_hits: usize,
    candidate_count: usize,
    closure_count: usize,
    closure_summary_rows_scanned: usize,
    pathlet_rows_scanned: usize,
    refined_rows_scanned: usize,
    refined_source: String,
    exact_engine: String,
    uses_refined_posting: bool,
    fused_refined_exact: bool,
    exact_candidate_closures: usize,
    refined_posting_closures: usize,
    generic_scan_closures: usize,
    local_edges_examined: usize,
    edge_rows_loaded: usize,
    edge_scope_collections: usize,
    edge_storage_mode: String,
    canonical_projection_mode: String,
    threads_requested: usize,
    threads_effective: usize,
    anchor_load_s: f64,
    edge_load_s: f64,
    synopsis_load_s: f64,
    cand0_filter_s: f64,
    base_filter_s: f64,
    refined_filter_s: f64,
    local_exact_s: f64,
    query_s: f64,
    search_only_s: f64,
    search_only_direct_timer: bool,
    exact_matching_s: f64,
    csv_write_s: f64,
    warmup_runs: usize,
    runtime_load_excluded_from_query_s: bool,
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?1",
        [name],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

fn validate_layout(conn: &Connection) -> Result<()> {
    for table in [
        "nft_role_transition_edges",
        "nft_closure_summary",
        "nft_event_type_dict",
        "nft_role_value_dict",
    ] {
        if !table_exists(conn, table)? {
            bail!("compact FunnelQ store is missing required table {table}");
        }
    }
    let forbidden = [
        "nft_pathlet_index",
        "nft_atom_pair_postings",
        "nft_atom_postings",
        "nft_events",
        "nft_closure_payload",
    ];
    for table in forbidden {
        if table_exists(conn, table)? {
            bail!("store contains legacy bulky table {table}; rebuild with compact v4 builder");
        }
    }
    Ok(())
}

fn load_runtime(conn: &Connection, include_tx: bool) -> Result<(Runtime, f64)> {
    let t0 = Instant::now();
    let max_order: i64 = conn.query_row(
        "SELECT COALESCE(MAX(order_seq),-1) FROM nft_role_transition_edges",
        [],
        |r| r.get(0),
    )?;
    let mut edges = Vec::new();
    let mut order_to_edge = vec![u32::MAX; usize::try_from(max_order.max(-1) + 1).unwrap_or(0)];
    let mut by_src: HashMap<u64, Vec<u32>> = HashMap::new();
    let sql = if include_tx {
        "SELECT e.order_seq,e.block_ts,e.src_addr_id,e.dst_addr_id,e.nft_id,\
                e.collection_id,e.event_type_id,tx.tx_id \
         FROM nft_role_transition_edges e \
         JOIN txdb.nft_event_tx_ids tx ON tx.order_seq=e.order_seq \
         ORDER BY e.collection_id,e.order_seq"
    } else {
        "SELECT order_seq,block_ts,src_addr_id,dst_addr_id,nft_id,collection_id,\
                event_type_id,0 AS tx_id \
         FROM nft_role_transition_edges ORDER BY collection_id,order_seq"
    };
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| {
        Ok(Edge {
            order_seq: r.get(0)?,
            block_ts: r.get(1)?,
            src_id: r.get(2)?,
            dst_id: r.get(3)?,
            nft_id: r.get(4)?,
            collection_id: r.get(5)?,
            event_type_id: r.get(6)?,
            tx_id: r.get(7)?,
        })
    })?;
    for row in rows {
        let edge = row?;
        let idx = u32::try_from(edges.len()).context("FunnelQ edge count exceeds u32")?;
        let order = edge.order_seq as usize;
        if order < order_to_edge.len() {
            order_to_edge[order] = idx;
        }
        by_src
            .entry(posting_key(edge.collection_id, edge.src_id))
            .or_default()
            .push(idx);
        edges.push(edge);
    }
    Ok((
        Runtime {
            edges,
            order_to_edge,
            by_src,
        },
        t0.elapsed().as_secs_f64(),
    ))
}

fn load_summaries(conn: &Connection) -> Result<(Vec<Summary>, f64)> {
    let t0 = Instant::now();
    let mut out = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT closure_id,anchor_order_seq,start_ts,end_ts,end_order_seq,collection_id,\
                transition_count,addr_count,nft_count,nft_ge2_count,b_out_event_count \
         FROM nft_closure_summary ORDER BY anchor_order_seq",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(Summary {
            closure_id: r.get(0)?,
            anchor_order: r.get(1)?,
            start_ts: r.get(2)?,
            end_ts: r.get(3)?,
            end_order: r.get(4)?,
            collection_id: r.get(5)?,
            transition_count: r.get(6)?,
            addr_count: r.get(7)?,
            nft_count: r.get(8)?,
            nft_ge2_count: r.get(9)?,
            b_out_event_count: r.get(10)?,
        })
    })?;
    for row in rows {
        out.push(row?);
    }
    Ok((out, t0.elapsed().as_secs_f64()))
}

fn load_anchor_directory(conn: &Connection) -> Result<(Vec<Summary>, f64)> {
    let t0 = Instant::now();
    if !table_exists(conn, "nft_anchor_directory")? {
        bail!(
            "nft_anchor_directory is required by this ablation; rebuild the compact anchor index"
        );
    }
    let mut out = Vec::new();
    let mut stmt = conn.prepare(
        "SELECT s.closure_id,s.anchor_order_seq,s.start_ts,s.end_ts,s.end_order_seq,s.collection_id,\
                s.transition_count,s.addr_count,s.nft_count,s.nft_ge2_count,s.b_out_event_count \
         FROM nft_anchor_directory d \
         JOIN nft_closure_summary s ON s.closure_id=d.closure_id \
         ORDER BY d.anchor_order_seq",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok(Summary {
            closure_id: r.get(0)?,
            anchor_order: r.get(1)?,
            start_ts: r.get(2)?,
            end_ts: r.get(3)?,
            end_order: r.get(4)?,
            collection_id: r.get(5)?,
            transition_count: r.get(6)?,
            addr_count: r.get(7)?,
            nft_count: r.get(8)?,
            nft_ge2_count: r.get(9)?,
            b_out_event_count: r.get(10)?,
        })
    })?;
    for row in rows {
        out.push(row?);
    }
    Ok((out, t0.elapsed().as_secs_f64()))
}

fn lower_bound_by_order(posting: &[u32], edges: &[Edge], target: u32) -> usize {
    let mut lo = 0usize;
    let mut hi = posting.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if edges[posting[mid] as usize].order_seq < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn upper_bound_by_order(posting: &[u32], edges: &[Edge], target: u32) -> usize {
    let mut lo = 0usize;
    let mut hi = posting.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if edges[posting[mid] as usize].order_seq <= target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn upper_bound_collection_order(
    edges: &[Edge],
    start: usize,
    collection_id: u32,
    end_order: u32,
) -> usize {
    // edges are ordered by (collection_id, order_seq).  This binary search is
    // part of the neutral compact event stream, not the Refined role posting.
    let mut lo = start;
    let mut hi = edges.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let edge = edges[mid];
        if edge.collection_id < collection_id
            || (edge.collection_id == collection_id && edge.order_seq <= end_order)
        {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn anchor_pass(kind: QueryKind, s: &Summary) -> bool {
    match kind {
        // C1 is an aggregate community query and does not require an event
        // leaving the root receiver.  Every other supported pattern starts
        // its post-root path at the root receiver B, so an outgoing event
        // from B is a necessary (lossless) lexical anchor condition.
        QueryKind::C1 => true,
        _ => s.b_out_event_count > 0,
    }
}

fn base_pass(kind: QueryKind, s: &Summary) -> bool {
    match kind {
        QueryKind::C1 => s.transition_count >= 4 && s.addr_count >= 3 && s.nft_count >= 2,
        QueryKind::C2 => s.transition_count >= 5 && s.nft_count >= 2,
        QueryKind::L3 | QueryKind::L6 => {
            s.transition_count >= 3 && s.addr_count >= 3 && s.nft_count >= 2
        }
        QueryKind::H5 => s.transition_count >= 3 && s.addr_count >= 2 && s.nft_count >= 2,
        QueryKind::Q1 => s.transition_count >= 2,
        QueryKind::Q2 => s.transition_count >= 2 && s.nft_count >= 2,
        QueryKind::Q3 => s.transition_count >= 3 && s.addr_count >= 3,
        QueryKind::Q4 => s.transition_count >= 3 && s.addr_count >= 3 && s.nft_count >= 3,
        QueryKind::Q5 => s.transition_count >= 3 && s.addr_count >= 3 && s.nft_count >= 2,
    }
}

fn refined_selected(
    kind: QueryKind,
    rt: &Runtime,
    s: &Summary,
    root: &Edge,
    delta_seconds: i64,
) -> bool {
    const NFT_TRADE_TYPE_ID: u8 = 1;
    if root.event_type_id != NFT_TRADE_TYPE_ID {
        return false;
    }
    let deadline = root.block_ts.saturating_add(delta_seconds);
    for idx1 in rt.src_range(s.collection_id, root.dst_id, root.order_seq, s.end_order) {
        let e1 = rt.edges[*idx1 as usize];
        if e1.event_type_id != NFT_TRADE_TYPE_ID
            || e1.block_ts <= root.block_ts
            || e1.block_ts > deadline
        {
            continue;
        }
        match kind {
            QueryKind::Q1 => {
                if e1.dst_id == root.src_id && e1.nft_id == root.nft_id {
                    return true;
                }
                continue;
            }
            QueryKind::Q2 => {
                if e1.dst_id == root.src_id && e1.nft_id != root.nft_id {
                    return true;
                }
                continue;
            }
            QueryKind::Q3 if e1.nft_id != root.nft_id => continue,
            QueryKind::Q4 | QueryKind::Q5 if e1.nft_id == root.nft_id => continue,
            _ => {}
        }
        if e1.dst_id == root.src_id {
            continue;
        }
        for idx2 in rt.src_range(s.collection_id, e1.dst_id, e1.order_seq, s.end_order) {
            let e2 = rt.edges[*idx2 as usize];
            if e2.event_type_id != NFT_TRADE_TYPE_ID
                || e2.block_ts <= e1.block_ts
                || e2.block_ts > deadline
                || e2.dst_id != root.src_id
            {
                continue;
            }
            let nft_ok = match kind {
                QueryKind::Q3 => e2.nft_id == root.nft_id,
                QueryKind::Q4 => e2.nft_id != root.nft_id && e2.nft_id != e1.nft_id,
                QueryKind::Q5 => e2.nft_id == root.nft_id,
                _ => false,
            };
            if nft_ok {
                return true;
            }
        }
    }
    false
}

fn refined_c1(s: &Summary) -> bool {
    s.nft_ge2_count >= 2 && s.addr_count >= 3 && s.addr_count <= 6
}

fn refined_c2(rt: &Runtime, s: &Summary, root: &Edge) -> bool {
    let mut earliest_by_c: HashMap<u32, u32> = HashMap::new();
    for idx in rt.src_range(s.collection_id, root.dst_id, root.order_seq, s.end_order) {
        let e = rt.edges[*idx as usize];
        if e.nft_id == root.nft_id {
            continue;
        }
        earliest_by_c
            .entry(e.dst_id)
            .and_modify(|v| *v = (*v).min(e.order_seq))
            .or_insert(e.order_seq);
    }
    if earliest_by_c.len() < 2 {
        return false;
    }
    let mut return_sources: HashMap<u32, HashSet<u32>> = HashMap::new();
    for (c_id, lower) in earliest_by_c {
        for idx in rt.src_range(s.collection_id, c_id, lower, s.end_order) {
            let e = rt.edges[*idx as usize];
            let sources = return_sources.entry(e.dst_id).or_default();
            sources.insert(c_id);
            if sources.len() >= 2 {
                return true;
            }
        }
    }
    false
}

fn refined_l3(rt: &Runtime, s: &Summary, root: &Edge) -> bool {
    let mut mids: HashSet<u32> = HashSet::new();
    let mut nft_support: HashSet<u32> = HashSet::new();
    for idx1 in rt.src_range(s.collection_id, root.dst_id, root.order_seq, s.end_order) {
        let e1 = rt.edges[*idx1 as usize];
        if e1.nft_id == root.nft_id {
            continue;
        }
        for idx2 in rt.src_range(s.collection_id, e1.dst_id, e1.order_seq, s.end_order) {
            let e2 = rt.edges[*idx2 as usize];
            if e2.dst_id != root.src_id || e2.nft_id == root.nft_id {
                continue;
            }
            mids.insert(e1.dst_id);
            nft_support.insert(e1.nft_id);
            nft_support.insert(e2.nft_id);
            if mids.len() >= 2 && nft_support.len() >= 2 {
                return true;
            }
        }
    }
    false
}

fn refined_h5(rt: &Runtime, s: &Summary, root: &Edge) -> bool {
    for idx1 in rt.src_range(s.collection_id, root.dst_id, root.order_seq, s.end_order) {
        let e1 = rt.edges[*idx1 as usize];
        if e1.nft_id == root.nft_id {
            continue;
        }
        for idx2 in rt.src_range(s.collection_id, e1.dst_id, e1.order_seq, s.end_order) {
            let e2 = rt.edges[*idx2 as usize];
            if e2.nft_id == root.nft_id {
                continue;
            }
            if e2.dst_id == root.src_id {
                return true;
            }
            for idx3 in rt.src_range(s.collection_id, e2.dst_id, e2.order_seq, s.end_order) {
                let e3 = rt.edges[*idx3 as usize];
                if e3.nft_id != root.nft_id && e3.dst_id == root.src_id {
                    return true;
                }
            }
        }
    }
    false
}

fn refined_pass(kind: QueryKind, rt: &Runtime, s: &Summary, selected_delta_seconds: i64) -> bool {
    let Some(root) = rt.root(s) else {
        return false;
    };
    if root.collection_id != s.collection_id || root.block_ts != s.start_ts {
        return false;
    }
    match kind {
        QueryKind::C1 => refined_c1(s),
        QueryKind::C2 => refined_c2(rt, s, root),
        QueryKind::L3 | QueryKind::L6 => refined_l3(rt, s, root),
        QueryKind::H5 => refined_h5(rt, s, root),
        QueryKind::Q1 | QueryKind::Q2 | QueryKind::Q3 | QueryKind::Q4 | QueryKind::Q5 => {
            refined_selected(kind, rt, s, root, selected_delta_seconds)
        }
    }
}


fn selected_fused_for_closure(
    kind: QueryKind,
    rt: &Runtime,
    summary: &Summary,
    delta_seconds: i64,
) -> (bool, Vec<ExactMatch>, usize) {
    const NFT_TRADE_TYPE_ID: u8 = 1;
    let Some(root) = rt.root(summary) else {
        return (false, Vec::new(), 0);
    };
    if root.collection_id != summary.collection_id
        || root.block_ts != summary.start_ts
        || root.event_type_id != NFT_TRADE_TYPE_ID
    {
        return (false, Vec::new(), 0);
    }

    let deadline = root.block_ts.saturating_add(delta_seconds);
    let mut refined_hit = false;
    let mut examined = 0usize;
    let mut out = Vec::new();

    for idx1 in rt.src_range(
        summary.collection_id,
        root.dst_id,
        root.order_seq,
        summary.end_order,
    ) {
        examined += 1;
        let e1 = rt.edges[*idx1 as usize];
        if e1.event_type_id != NFT_TRADE_TYPE_ID
            || e1.block_ts <= root.block_ts
            || e1.block_ts > deadline
        {
            continue;
        }

        match kind {
            QueryKind::Q1 => {
                if e1.dst_id == root.src_id && e1.nft_id == root.nft_id {
                    refined_hit = true;
                    if root.tx_id != 0 && e1.tx_id != 0 && e1.tx_id != root.tx_id {
                        out.push(ExactMatch {
                            root_order: root.order_seq,
                            middle_order: None,
                            back_order: e1.order_seq,
                        });
                    }
                }
                continue;
            }
            QueryKind::Q2 => {
                if e1.dst_id == root.src_id && e1.nft_id != root.nft_id {
                    refined_hit = true;
                    if root.tx_id != 0 && e1.tx_id != 0 && e1.tx_id != root.tx_id {
                        out.push(ExactMatch {
                            root_order: root.order_seq,
                            middle_order: None,
                            back_order: e1.order_seq,
                        });
                    }
                }
                continue;
            }
            QueryKind::Q3 if e1.nft_id != root.nft_id => continue,
            QueryKind::Q4 | QueryKind::Q5 if e1.nft_id == root.nft_id => continue,
            QueryKind::Q3 | QueryKind::Q4 | QueryKind::Q5 => {}
            _ => continue,
        }

        // Every canonical edge has src != dst. Since root.src != root.dst and
        // e1.src == root.dst by construction of the posting, excluding C == A
        // is sufficient to enforce three distinct actors.
        if e1.dst_id == root.src_id {
            continue;
        }
        for idx2 in rt.src_range(
            summary.collection_id,
            e1.dst_id,
            e1.order_seq,
            summary.end_order,
        ) {
            examined += 1;
            let e2 = rt.edges[*idx2 as usize];
            if e2.event_type_id != NFT_TRADE_TYPE_ID
                || e2.block_ts <= e1.block_ts
                || e2.block_ts > deadline
                || e2.dst_id != root.src_id
            {
                continue;
            }
            let nft_ok = match kind {
                QueryKind::Q3 => e2.nft_id == root.nft_id,
                QueryKind::Q4 => {
                    e2.nft_id != root.nft_id && e2.nft_id != e1.nft_id
                }
                QueryKind::Q5 => e2.nft_id == root.nft_id,
                _ => false,
            };
            if !nft_ok {
                continue;
            }
            refined_hit = true;
            if root.tx_id != 0
                && e1.tx_id != 0
                && e2.tx_id != 0
                && e1.tx_id != root.tx_id
                && e2.tx_id != root.tx_id
                && e2.tx_id != e1.tx_id
            {
                out.push(ExactMatch {
                    root_order: root.order_seq,
                    middle_order: Some(e1.order_seq),
                    back_order: e2.order_seq,
                });
            }
        }
    }
    (refined_hit, out, examined)
}

fn selected_exact_generic_for_closure(
    kind: QueryKind,
    rt: &Runtime,
    summary: &Summary,
    delta_seconds: i64,
) -> (Vec<ExactMatch>, usize) {
    const NFT_TRADE_TYPE_ID: u8 = 1;
    let order = summary.anchor_order as usize;
    let Some(raw_root_idx) = rt.order_to_edge.get(order) else {
        return (Vec::new(), 0);
    };
    if *raw_root_idx == u32::MAX {
        return (Vec::new(), 0);
    }
    let root_idx = *raw_root_idx as usize;
    let Some(root) = rt.edges.get(root_idx).copied() else {
        return (Vec::new(), 0);
    };
    if root.collection_id != summary.collection_id
        || root.block_ts != summary.start_ts
        || root.event_type_id != NFT_TRADE_TYPE_ID
        || root.tx_id == 0
    {
        return (Vec::new(), 0);
    }

    // This path intentionally does not access Runtime.by_src. It scans the
    // complete ordered event range of each candidate closure and applies the
    // query predicates directly, which is the correct removal of Refined.
    let end_idx = upper_bound_collection_order(
        &rt.edges,
        root_idx + 1,
        summary.collection_id,
        summary.end_order,
    );

    let deadline = root.block_ts.saturating_add(delta_seconds);
    let mut examined = 0usize;
    let mut out = Vec::new();

    for i in (root_idx + 1)..end_idx {
        examined += 1;
        let e1 = rt.edges[i];
        if e1.event_type_id != NFT_TRADE_TYPE_ID
            || e1.src_id != root.dst_id
            || e1.block_ts <= root.block_ts
            || e1.block_ts > deadline
            || e1.tx_id == 0
            || e1.tx_id == root.tx_id
        {
            continue;
        }

        match kind {
            QueryKind::Q1 => {
                if e1.dst_id == root.src_id && e1.nft_id == root.nft_id {
                    out.push(ExactMatch {
                        root_order: root.order_seq,
                        middle_order: None,
                        back_order: e1.order_seq,
                    });
                }
                continue;
            }
            QueryKind::Q2 => {
                if e1.dst_id == root.src_id && e1.nft_id != root.nft_id {
                    out.push(ExactMatch {
                        root_order: root.order_seq,
                        middle_order: None,
                        back_order: e1.order_seq,
                    });
                }
                continue;
            }
            QueryKind::Q3 if e1.nft_id != root.nft_id => continue,
            QueryKind::Q4 | QueryKind::Q5 if e1.nft_id == root.nft_id => continue,
            QueryKind::Q3 | QueryKind::Q4 | QueryKind::Q5 => {}
            _ => continue,
        }

        if e1.dst_id == root.src_id {
            continue;
        }
        for j in (i + 1)..end_idx {
            examined += 1;
            let e2 = rt.edges[j];
            if e2.event_type_id != NFT_TRADE_TYPE_ID
                || e2.src_id != e1.dst_id
                || e2.block_ts <= e1.block_ts
                || e2.block_ts > deadline
                || e2.dst_id != root.src_id
                || e2.tx_id == 0
                || e2.tx_id == root.tx_id
                || e2.tx_id == e1.tx_id
            {
                continue;
            }
            let nft_ok = match kind {
                QueryKind::Q3 => e2.nft_id == root.nft_id,
                QueryKind::Q4 => {
                    e2.nft_id != root.nft_id && e2.nft_id != e1.nft_id
                }
                QueryKind::Q5 => e2.nft_id == root.nft_id,
                _ => false,
            };
            if nft_ok {
                out.push(ExactMatch {
                    root_order: root.order_seq,
                    middle_order: Some(e1.order_seq),
                    back_order: e2.order_seq,
                });
            }
        }
    }
    (out, examined)
}

fn finalize_exact_rows(mut rows: Vec<ExactMatch>, raw_rows: usize) -> (Vec<ExactMatch>, usize) {
    rows.sort_unstable();
    rows.dedup();
    (rows, raw_rows)
}

fn enumerate_selected_matches_fused(
    kind: QueryKind,
    runtime: &Runtime,
    candidates: &[Summary],
    delta_seconds: i64,
    limit: usize,
    pool: &rayon::ThreadPool,
) -> (Vec<ExactMatch>, usize, usize, usize) {
    // Formal runs use an unlimited candidate limit. Aggregate by chunks so the
    // executor does not allocate one Vec header per million closures.
    if limit >= candidates.len() {
        let parts: Vec<(Vec<ExactMatch>, usize, usize, usize)> = pool.install(|| {
            candidates
                .par_chunks(4096)
                .map(|chunk| {
                    let mut rows = Vec::new();
                    let mut raw_rows = 0usize;
                    let mut examined = 0usize;
                    let mut refined_hits = 0usize;
                    for summary in chunk {
                        let (refined_hit, mut local, local_examined) =
                            selected_fused_for_closure(kind, runtime, summary, delta_seconds);
                        examined += local_examined;
                        if refined_hit {
                            refined_hits += 1;
                            raw_rows += local.len();
                            rows.append(&mut local);
                        }
                    }
                    (rows, raw_rows, examined, refined_hits)
                })
                .collect()
        });
        let raw_rows = parts.iter().map(|(_, n, _, _)| *n).sum();
        let examined = parts.iter().map(|(_, _, n, _)| *n).sum();
        let refined_hits = parts.iter().map(|(_, _, _, n)| *n).sum();
        let mut rows = Vec::with_capacity(raw_rows);
        for (mut part, _, _, _) in parts {
            rows.append(&mut part);
        }
        let (rows, raw_rows) = finalize_exact_rows(rows, raw_rows);
        return (rows, raw_rows, examined, refined_hits);
    }

    // A bounded run must preserve the previous "first N refined
    // closures" semantics exactly, so use deterministic summary order here.
    let mut rows = Vec::new();
    let mut raw_rows = 0usize;
    let mut examined = 0usize;
    let mut refined_hits = 0usize;
    for summary in candidates {
        let (refined_hit, mut local, local_examined) =
            selected_fused_for_closure(kind, runtime, summary, delta_seconds);
        examined += local_examined;
        if !refined_hit {
            continue;
        }
        refined_hits += 1;
        raw_rows += local.len();
        rows.append(&mut local);
        if refined_hits >= limit {
            break;
        }
    }
    let (rows, raw_rows) = finalize_exact_rows(rows, raw_rows);
    (rows, raw_rows, examined, refined_hits)
}

fn enumerate_selected_matches_generic(
    kind: QueryKind,
    runtime: &Runtime,
    candidates: &[Summary],
    delta_seconds: i64,
    pool: &rayon::ThreadPool,
) -> (Vec<ExactMatch>, usize, usize) {
    let parts: Vec<(Vec<ExactMatch>, usize)> = pool.install(|| {
        candidates
            .par_chunks(4096)
            .map(|chunk| {
                let mut rows = Vec::new();
                let mut examined = 0usize;
                for summary in chunk {
                    let (mut local, local_examined) =
                        selected_exact_generic_for_closure(
                            kind,
                            runtime,
                            summary,
                            delta_seconds,
                        );
                    examined += local_examined;
                    rows.append(&mut local);
                }
                (rows, examined)
            })
            .collect()
    });
    let raw_rows = parts.iter().map(|(rows, _)| rows.len()).sum();
    let examined = parts.iter().map(|(_, n)| *n).sum();
    let mut rows = Vec::with_capacity(raw_rows);
    for (mut part, _) in parts {
        rows.append(&mut part);
    }
    let (rows, raw_rows) = finalize_exact_rows(rows, raw_rows);
    (rows, raw_rows, examined)
}

fn write_exact_matches(path: &str, rows: &[ExactMatch]) -> Result<f64> {
    let t0 = Instant::now();
    let file = File::create(path).with_context(|| format!("create {path}"))?;
    let mut w = Writer::from_writer(file);
    w.write_record(["root_order", "middle_order", "back_order"])?;
    for row in rows {
        let middle = row.middle_order.map(|value| value.to_string()).unwrap_or_default();
        w.write_record([
            row.root_order.to_string(),
            middle,
            row.back_order.to_string(),
        ])?;
    }
    w.flush()?;
    Ok(t0.elapsed().as_secs_f64())
}

fn collection_count(edges: &[Edge]) -> usize {
    let mut last = None;
    let mut n = 0usize;
    for e in edges {
        if last != Some(e.collection_id) {
            n += 1;
            last = Some(e.collection_id);
        }
    }
    n
}

fn write_candidates(path: &str, rows: &[Summary]) -> Result<f64> {
    let t0 = Instant::now();
    let file = File::create(path).with_context(|| format!("create {path}"))?;
    let mut w = Writer::from_writer(file);
    w.write_record(["closure_id", "anchor_order", "end_ts", "collection_id"])?;
    for s in rows {
        w.write_record([
            s.closure_id.to_string(),
            s.anchor_order.to_string(),
            s.end_ts.to_string(),
            s.collection_id.to_string(),
        ])?;
    }
    w.flush()?;
    Ok(t0.elapsed().as_secs_f64())
}

fn execute_candidate_prefix(
    kind: QueryKind,
    ablation: Ablation,
    summaries: &[Summary],
    anchor_directory: Option<&[Summary]>,
    anchor_limit: usize,
) -> (Vec<Summary>, usize, usize, f64, f64) {
    let anchor0 = Instant::now();
    let mut cand0: Vec<Summary> = if kind != QueryKind::C1 && ablation.use_anchor_directory() {
        anchor_directory
            .expect("anchor directory must be loaded for the full anchor path")
            .to_vec()
    } else if ablation.use_anchor_predicate() {
        summaries
            .iter()
            .copied()
            .filter(|s| anchor_pass(kind, s))
            .collect()
    } else {
        summaries.to_vec()
    };
    if cand0.len() > anchor_limit {
        cand0.truncate(anchor_limit);
    }
    let cand0_filter_s = anchor0.elapsed().as_secs_f64();
    let cand0_hits = cand0.len();

    let base0 = Instant::now();
    let cand1: Vec<Summary> = if ablation.use_base() {
        cand0
            .iter()
            .copied()
            .filter(|s| base_pass(kind, s))
            .collect()
    } else {
        cand0
    };
    let base_filter_s = base0.elapsed().as_secs_f64();
    let cand1_hits = cand1.len();
    (
        cand1,
        cand0_hits,
        cand1_hits,
        cand0_filter_s,
        base_filter_s,
    )
}

fn execute_candidate_query(
    kind: QueryKind,
    ablation: Ablation,
    runtime: &Runtime,
    summaries: &[Summary],
    anchor_directory: Option<&[Summary]>,
    selected_delta_seconds: i64,
    anchor_limit: usize,
    limit: usize,
    pool: &rayon::ThreadPool,
) -> (Vec<Summary>, usize, usize, f64, f64, f64) {
    let anchor0 = Instant::now();
    let mut cand0: Vec<Summary> = if kind != QueryKind::C1 && ablation.use_anchor_directory() {
        anchor_directory
            .expect("anchor directory must be loaded for the full anchor path")
            .to_vec()
    } else if ablation.use_anchor_predicate() {
        summaries
            .iter()
            .copied()
            .filter(|s| anchor_pass(kind, s))
            .collect()
    } else {
        summaries.to_vec()
    };
    if cand0.len() > anchor_limit {
        cand0.truncate(anchor_limit);
    }
    let cand0_filter_s = anchor0.elapsed().as_secs_f64();
    let cand0_hits = cand0.len();

    let base0 = Instant::now();
    let cand1: Vec<Summary> = if ablation.use_base() {
        cand0
            .iter()
            .copied()
            .filter(|s| base_pass(kind, s))
            .collect()
    } else {
        cand0
    };
    let base_filter_s = base0.elapsed().as_secs_f64();
    let cand1_hits = cand1.len();

    let refined0 = Instant::now();
    let mut cand2: Vec<Summary> = if ablation.use_refined() {
        pool.install(|| {
            cand1
                .par_iter()
                .copied()
                .filter(|s| refined_pass(kind, runtime, s, selected_delta_seconds))
                .collect()
        })
    } else {
        cand1
    };
    cand2.sort_by_key(|s| (s.anchor_order, s.closure_id));
    if cand2.len() > limit {
        cand2.truncate(limit);
    }
    let refined_filter_s = refined0.elapsed().as_secs_f64();
    (
        cand2,
        cand0_hits,
        cand1_hits,
        cand0_filter_s,
        base_filter_s,
        refined_filter_s,
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.mode != "candidates" && args.mode != "query" {
        bail!("compact NFT executor supports only --mode candidates/query");
    }
    let kind = QueryKind::parse(&args.query)?;
    let ablation = Ablation::parse(&args.ablation)?;
    let selected_query = matches!(
        kind,
        QueryKind::Q1 | QueryKind::Q2 | QueryKind::Q3 | QueryKind::Q4 | QueryKind::Q5
    );
    if args.mode == "query" && !selected_query {
        bail!("--mode query is available only for the versioned selected-five queries");
    }

    // The Python entry performs a fail-closed, field-by-field comparison of
    // the supplied QuerySpec against the versioned specialized Q1--Q5
    // templates before this executable is launched.  Rust intentionally reads
    // only the remaining runtime parameter here after that contract check.
    let selected_delta_seconds = if selected_query {
        let query_json = args
            .query_json
            .as_ref()
            .context("selected NFT queries require --query-json")?;
        let document: serde_json::Value = serde_json::from_slice(
            &std::fs::read(query_json).with_context(|| format!("read {query_json}"))?,
        )?;
        document
            .pointer("/full_binding_plan/delta_seconds")
            .and_then(|value| value.as_i64())
            .filter(|value| *value > 0)
            .context("selected query JSON has no positive full_binding_plan.delta_seconds")?
    } else {
        86_400
    };
    let threads = args.threads.max(1);
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(threads)
        .build()
        .context("build Rayon pool")?;

    let conn = Connection::open(&args.db).with_context(|| format!("open {}", args.db))?;
    let include_tx = args.mode == "query";
    if include_tx {
        let tx_path = args
            .tx_sidecar_db
            .as_ref()
            .context("--mode query requires --tx-sidecar-db")?;
        conn.execute("ATTACH DATABASE ?1 AS txdb", [tx_path.as_str()])
            .with_context(|| format!("attach transaction sidecar {tx_path}"))?;
        let tx_table_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM txdb.sqlite_master \
             WHERE type='table' AND name='nft_event_tx_ids'",
            [],
            |r| r.get(0),
        )?;
        if tx_table_count != 1 {
            bail!("transaction sidecar is missing nft_event_tx_ids");
        }
    }
    conn.execute_batch(
        "PRAGMA query_only=ON; PRAGMA temp_store=MEMORY; \
         PRAGMA cache_size=-65536; PRAGMA mmap_size=1073741824;",
    )?;
    validate_layout(&conn)?;

    let (runtime, edge_load_s) = load_runtime(&conn, include_tx)?;
    let (summaries, synopsis_load_s) = load_summaries(&conn)?;
    let (anchor_directory, anchor_load_s) =
        if kind != QueryKind::C1 && ablation.use_anchor_directory() {
            let (rows, seconds) = load_anchor_directory(&conn)?;
            (Some(rows), seconds)
        } else {
            (None, 0.0)
        };
    if runtime.edges.len() != summaries.len() {
        bail!(
            "compact coverage mismatch at query time: edges={} summaries={}",
            runtime.edges.len(),
            summaries.len()
        );
    }

    let universe = summaries.len();
    for _ in 0..args.warmup_runs {
        if args.mode == "query" {
            let (mut warm_candidates, _, _, _, _) = execute_candidate_prefix(
                kind,
                ablation,
                &summaries,
                anchor_directory.as_deref(),
                args.anchor_limit,
            );
            if ablation.use_refined() {
                let _ = enumerate_selected_matches_fused(
                    kind,
                    &runtime,
                    &warm_candidates,
                    selected_delta_seconds,
                    args.limit,
                    &pool,
                );
            } else {
                warm_candidates.sort_by_key(|s| (s.anchor_order, s.closure_id));
                if warm_candidates.len() > args.limit {
                    warm_candidates.truncate(args.limit);
                }
                let _ = enumerate_selected_matches_generic(
                    kind,
                    &runtime,
                    &warm_candidates,
                    selected_delta_seconds,
                    &pool,
                );
            }
        } else {
            let _ = execute_candidate_query(
                kind,
                ablation,
                &runtime,
                &summaries,
                anchor_directory.as_deref(),
                selected_delta_seconds,
                args.anchor_limit,
                args.limit,
                &pool,
            );
        }
    }
    if let Some(marker) = &args.phase_marker {
        if let Some(parent) = Path::new(marker).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(marker, b"query\n")?;
    }

    let mut cand2 = Vec::new();
    let mut cand2_count = 0usize;
    let mut cand0_hits = 0usize;
    let mut cand1_hits = 0usize;
    let mut cand0_filter_s = 0.0f64;
    let mut base_filter_s = 0.0f64;
    let mut refined_filter_s = 0.0f64;
    let mut exact_rows = Vec::new();
    let mut exact_rows_before_dedup = 0usize;
    let mut local_edges_examined = 0usize;
    let mut local_exact_s = 0.0f64;
    let query_s: f64;
    let search_only_s: f64;

    if args.mode == "query" {
        // Directly time only the query-dependent search pipeline.  This begins
        // at anchor localization and ends after exact local match enumeration.
        // It deliberately excludes exact-order CSV transfer and all final-result
        // projection/materialization performed by the Python wrapper.
        let search0 = Instant::now();
        let query0 = Instant::now();
        let (mut candidates, c0, c1, c0_s, base_s) = execute_candidate_prefix(
            kind,
            ablation,
            &summaries,
            anchor_directory.as_deref(),
            args.anchor_limit,
        );
        query_s = query0.elapsed().as_secs_f64();
        cand0_hits = c0;
        cand1_hits = c1;
        cand0_filter_s = c0_s;
        base_filter_s = base_s;

        let exact0 = Instant::now();
        if ablation.use_refined() {
            let (rows, raw_rows, examined, refined_hits) =
                enumerate_selected_matches_fused(
                    kind,
                    &runtime,
                    &candidates,
                    selected_delta_seconds,
                    args.limit,
                    &pool,
                );
            exact_rows = rows;
            exact_rows_before_dedup = raw_rows;
            local_edges_examined = examined;
            cand2_count = refined_hits;
        } else {
            candidates.sort_by_key(|s| (s.anchor_order, s.closure_id));
            if candidates.len() > args.limit {
                candidates.truncate(args.limit);
            }
            cand2_count = candidates.len();
            let (rows, raw_rows, examined) = enumerate_selected_matches_generic(
                kind,
                &runtime,
                &candidates,
                selected_delta_seconds,
                &pool,
            );
            exact_rows = rows;
            exact_rows_before_dedup = raw_rows;
            local_edges_examined = examined;
        }
        local_exact_s = exact0.elapsed().as_secs_f64();
        search_only_s = search0.elapsed().as_secs_f64();
    } else {
        let query0 = Instant::now();
        let (rows, c0, c1, c0_s, base_s, refined_s) = execute_candidate_query(
            kind,
            ablation,
            &runtime,
            &summaries,
            anchor_directory.as_deref(),
            selected_delta_seconds,
            args.anchor_limit,
            args.limit,
            &pool,
        );
        query_s = query0.elapsed().as_secs_f64();
        cand2_count = rows.len();
        cand2 = rows;
        cand0_hits = c0;
        cand1_hits = c1;
        cand0_filter_s = c0_s;
        base_filter_s = base_s;
        refined_filter_s = refined_s;
        search_only_s = query_s;
    }

    let out_csv = if args.out_csv.trim().is_empty() {
        if args.mode == "query" {
            "exact_match_orders.csv"
        } else {
            "cand2_closures.csv"
        }
    } else {
        args.out_csv.as_str()
    };
    if let Some(parent) = Path::new(out_csv).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    let csv_write_s = if args.mode == "query" {
        write_exact_matches(out_csv, &exact_rows)?
    } else {
        write_candidates(out_csv, &cand2)?
    };

    let output_rows = if args.mode == "query" {
        exact_rows.len()
    } else {
        cand2.len()
    };
    let report = Report {
        query: args.query.clone(),
        ablation: args.ablation.clone(),
        output_mode: args.mode.clone(),
        rows: output_rows,
        exact_rows_before_dedup,
        exact_duplicate_rows_removed: exact_rows_before_dedup.saturating_sub(exact_rows.len()),
        selected_anchor_atom: if ablation.use_anchor_predicate() && kind != QueryKind::C1 {
            "out_from_anchor_buyer".to_string()
        } else {
            "root".to_string()
        },
        anchor_engine: if !ablation.use_anchor_predicate() {
            "disabled".to_string()
        } else if kind != QueryKind::C1 && ablation.use_anchor_directory() {
            "anchor_directory".to_string()
        } else {
            "generic_anchor_scan".to_string()
        },
        uses_anchor_directory: kind != QueryKind::C1 && ablation.use_anchor_directory(),
        anchor_scan_closures: if ablation.use_anchor_predicate()
            && !(kind != QueryKind::C1 && ablation.use_anchor_directory())
        {
            universe
        } else {
            0
        },
        anchor_directory_entries_read: anchor_directory.as_ref().map_or(0, Vec::len),
        anchor_engine_isolation_contract_ok: true,
        closure_universe: universe,
        cand0_anchor_hits: cand0_hits,
        cand1_base_hits: cand1_hits,
        cand2_refined_hits: cand2_count,
        candidate_count: cand2_count,
        closure_count: cand2_count,
        closure_summary_rows_scanned: if anchor_directory.is_some() {
            anchor_directory.as_ref().map_or(0, Vec::len)
        } else {
            universe
        },
        pathlet_rows_scanned: 0,
        refined_rows_scanned: if ablation.use_refined() {
            cand1_hits
        } else {
            0
        },
        refined_source: if args.mode == "query" && !ablation.use_refined() {
            "disabled_by_ablation".to_string()
        } else {
            "query_time_integer_role_postings".to_string()
        },
        exact_engine: if args.mode != "query" {
            "not_applicable".to_string()
        } else if ablation.use_refined() {
            "refined_posting_fused".to_string()
        } else {
            "generic_closure_scan".to_string()
        },
        uses_refined_posting: args.mode == "query" && ablation.use_refined(),
        fused_refined_exact: args.mode == "query" && ablation.use_refined(),
        exact_candidate_closures: if args.mode == "query" {
            if ablation.use_refined() { cand1_hits } else { cand2_count }
        } else {
            0
        },
        refined_posting_closures: if args.mode == "query" && ablation.use_refined() {
            cand1_hits
        } else {
            0
        },
        generic_scan_closures: if args.mode == "query" && !ablation.use_refined() {
            cand2_count
        } else {
            0
        },
        local_edges_examined,
        edge_rows_loaded: runtime.edges.len(),
        edge_scope_collections: collection_count(&runtime.edges),
        edge_storage_mode: if include_tx {
            "compact_integer_event_stream_with_tx_projection".to_string()
        } else {
            "compact_integer_event_stream".to_string()
        },
        canonical_projection_mode: if args.mode == "query" {
            "deferred_matched_event_dictionary_projection".to_string()
        } else {
            "separate_address_nft_collection_dictionaries".to_string()
        },
        threads_requested: args.threads,
        threads_effective: threads,
        anchor_load_s,
        edge_load_s,
        synopsis_load_s,
        cand0_filter_s,
        base_filter_s,
        refined_filter_s,
        local_exact_s,
        query_s,
        search_only_s,
        search_only_direct_timer: args.mode == "query",
        exact_matching_s: local_exact_s,
        csv_write_s,
        warmup_runs: args.warmup_runs,
        runtime_load_excluded_from_query_s: true,
    };

    if let Some(path) = &args.report_json {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(path, serde_json::to_string_pretty(&report)?)?;
    }
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
