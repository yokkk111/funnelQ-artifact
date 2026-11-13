use anyhow::Result;
use clap::Parser;
use rayon::prelude::*;
use rusqlite::Connection;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::time::Instant;
use std::collections::HashMap;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    db: PathBuf,
    #[arg(long)]
    out_json: PathBuf,
    #[arg(long, value_delimiter = ',', default_value = "60,300,1800,3600,21600,86400")]
    deltas: Vec<i64>,
    #[arg(long, default_value = "compact")]
    layout: String,
    #[arg(long)]
    require_distinct_tx: bool,
    #[arg(long)]
    require_later_timestamp: bool,
}

#[derive(Clone, Copy)]
struct Edge {
    order: i64,
    ts: i64,
    event_type: i64,
    src: i64,
    dst: i64,
    nft: i64,
    collection: i64,
    tx: u32,
}

#[derive(Serialize)]
struct CountRow {
    pattern: &'static str,
    first_type: &'static str,
    second_type: &'static str,
    third_type: Option<&'static str>,
    delta_seconds: i64,
    full_binding_count: u64,
}

#[derive(Serialize)]
struct Report {
    mode: &'static str,
    db: String,
    edge_count: usize,
    layout: String,
    distinct_tx_required: bool,
    later_timestamp_required: bool,
    load_s: f64,
    index_build_s: f64,
    count_s: f64,
    rows: Vec<CountRow>,
}

fn type_name(value: i64) -> &'static str {
    match value {
        0 => "Any",
        1 => "NFTTrade",
        2 => "NFTTransfer",
        _ => "Unknown",
    }
}

fn type_pass(edge: &Edge, wanted: i64) -> bool {
    wanted == 0 || edge.event_type == wanted
}

fn lower_nft(index: &[usize], edges: &[Edge], coll: i64, nft: i64, ts: i64) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.nft, e.ts, e.order) < (coll, nft, ts, i64::MIN)
    })
}

fn upper_nft(index: &[usize], edges: &[Edge], coll: i64, nft: i64, ts: i64) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.nft, e.ts, e.order) <= (coll, nft, ts, i64::MAX)
    })
}

fn lower_pair(
    index: &[usize],
    edges: &[Edge],
    coll: i64,
    src: i64,
    dst: i64,
    ts: i64,
) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.src, e.dst, e.ts, e.order)
            < (coll, src, dst, ts, i64::MIN)
    })
}

fn upper_pair(
    index: &[usize],
    edges: &[Edge],
    coll: i64,
    src: i64,
    dst: i64,
    ts: i64,
) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.src, e.dst, e.ts, e.order)
            <= (coll, src, dst, ts, i64::MAX)
    })
}

fn lower_src(index: &[usize], edges: &[Edge], coll: i64, src: i64, ts: i64) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.src, e.ts, e.order) < (coll, src, ts, i64::MIN)
    })
}

fn upper_src(index: &[usize], edges: &[Edge], coll: i64, src: i64, ts: i64) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.src, e.ts, e.order) <= (coll, src, ts, i64::MAX)
    })
}

fn lower_dst(index: &[usize], edges: &[Edge], coll: i64, dst: i64, ts: i64) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.dst, e.ts, e.order) < (coll, dst, ts, i64::MIN)
    })
}

fn upper_dst(index: &[usize], edges: &[Edge], coll: i64, dst: i64, ts: i64) -> usize {
    index.partition_point(|i| {
        let e = edges[*i];
        (e.collection, e.dst, e.ts, e.order) <= (coll, dst, ts, i64::MAX)
    })
}

fn count_same_nft(
    edges: &[Edge],
    by_nft: &[usize],
    delta: i64,
    first_type: i64,
    second_type: i64,
    return_to_sender: bool,
    require_distinct_tx: bool,
    require_later_timestamp: bool,
) -> u64 {
    edges
        .par_iter()
        .map(|e1| {
            if !type_pass(e1, first_type) {
                return 0u64;
            }
            let lo = lower_nft(by_nft, edges, e1.collection, e1.nft, e1.ts);
            let hi = upper_nft(by_nft, edges, e1.collection, e1.nft, e1.ts + delta);
            by_nft[lo..hi]
                .iter()
                .filter(|i| {
                    let e2 = edges[**i];
                    e2.order > e1.order
                        && (!require_distinct_tx || e2.tx != e1.tx)
                        && (!require_later_timestamp || e2.ts > e1.ts)
                        && type_pass(&e2, second_type)
                        && e2.src == e1.dst
                        && if return_to_sender {
                            e2.dst == e1.src
                        } else {
                            e2.dst != e1.src
                        }
                })
                .count() as u64
        })
        .sum()
}

fn count_cross_nft_pair(
    edges: &[Edge],
    by_pair: &[usize],
    delta: i64,
    first_type: i64,
    second_type: i64,
    reverse: bool,
    require_distinct_tx: bool,
    require_later_timestamp: bool,
) -> u64 {
    edges
        .par_iter()
        .map(|e1| {
            if !type_pass(e1, first_type) {
                return 0u64;
            }
            let (src, dst) = if reverse {
                (e1.dst, e1.src)
            } else {
                (e1.src, e1.dst)
            };
            let lo = lower_pair(by_pair, edges, e1.collection, src, dst, e1.ts);
            let hi = upper_pair(by_pair, edges, e1.collection, src, dst, e1.ts + delta);
            by_pair[lo..hi]
                .iter()
                .filter(|i| {
                    let e2 = edges[**i];
                    e2.order > e1.order
                        && (!require_distinct_tx || e2.tx != e1.tx)
                        && (!require_later_timestamp || e2.ts > e1.ts)
                        && type_pass(&e2, second_type)
                        && e2.nft != e1.nft
                })
                .count() as u64
        })
        .sum()
}

fn count_triangle(
    edges: &[Edge],
    by_src: &[usize],
    by_pair: &[usize],
    delta: i64,
    first_type: i64,
    second_type: i64,
    third_type: i64,
    nft_mode: i64,
) -> u64 {
    edges
        .par_iter()
        .map(|e1| {
            if !type_pass(e1, first_type) {
                return 0u64;
            }
            let lo2 = lower_src(by_src, edges, e1.collection, e1.dst, e1.ts + 1);
            let hi2 = upper_src(by_src, edges, e1.collection, e1.dst, e1.ts + delta);
            let mut count = 0u64;
            for i2 in &by_src[lo2..hi2] {
                let e2 = edges[*i2];
                if e2.order <= e1.order
                    || e2.tx == e1.tx
                    || !type_pass(&e2, second_type)
                    || e2.dst == e1.src
                    || (nft_mode == 0 && e2.nft != e1.nft)
                    || (nft_mode == 1 && e2.nft == e1.nft)
                    || (nft_mode == 2 && e2.nft != e1.nft)
                    || (nft_mode == 3 && e2.nft == e1.nft)
                {
                    continue;
                }
                let lo3 = lower_pair(
                    by_pair,
                    edges,
                    e1.collection,
                    e2.dst,
                    e1.src,
                    e2.ts + 1,
                );
                let hi3 = upper_pair(
                    by_pair,
                    edges,
                    e1.collection,
                    e2.dst,
                    e1.src,
                    e1.ts + delta,
                );
                count += by_pair[lo3..hi3]
                    .iter()
                    .filter(|i3| {
                        let e3 = edges[**i3];
                        e3.order > e2.order
                            && e3.tx != e1.tx
                            && e3.tx != e2.tx
                            && type_pass(&e3, third_type)
                            && match nft_mode {
                                0 => e3.nft == e1.nft,
                                1 => e3.nft != e1.nft && e3.nft != e2.nft,
                                2 => e3.nft != e1.nft,
                                3 => e3.nft == e1.nft,
                                _ => false,
                            }
                    })
                    .count() as u64;
            }
            count
        })
        .sum()
}

fn count_two_leg(
    edges: &[Edge],
    index: &[usize],
    delta: i64,
    root_type: i64,
    leg_type: i64,
    fanout: bool,
) -> u64 {
    edges
        .par_iter()
        .map(|root| {
            if !type_pass(root, root_type) {
                return 0u64;
            }
            let (lo, hi) = if fanout {
                (
                    lower_src(index, edges, root.collection, root.dst, root.ts + 1),
                    upper_src(index, edges, root.collection, root.dst, root.ts + delta),
                )
            } else {
                (
                    lower_dst(index, edges, root.collection, root.src, root.ts + 1),
                    upper_dst(index, edges, root.collection, root.src, root.ts + delta),
                )
            };
            let legs: Vec<Edge> = index[lo..hi]
                .iter()
                .map(|i| edges[*i])
                .filter(|e| {
                    e.order > root.order
                        && e.tx != root.tx
                        && type_pass(e, leg_type)
                        && e.nft != root.nft
                        && if fanout {
                            e.dst != root.src
                        } else {
                            e.src != root.dst
                        }
                })
                .collect();
            let mut count = 0u64;
            for i in 0..legs.len() {
                for j in (i + 1)..legs.len() {
                    let a = legs[i];
                    let b = legs[j];
                    let distinct_actor = if fanout { a.dst != b.dst } else { a.src != b.src };
                    if distinct_actor && a.nft != b.nft && a.tx != b.tx {
                        count += 1;
                    }
                }
            }
            count
        })
        .sum()
}

fn main() -> Result<()> {
    let args = Args::parse();
    let load0 = Instant::now();
    let conn = Connection::open_with_flags(
        &args.db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut edges = Vec::new();
    if args.layout == "compact" {
        if args.require_distinct_tx {
            anyhow::bail!("compact layout does not retain tx_hash; use --layout hgmatch");
        }
        let mut stmt = conn.prepare(
            "SELECT order_seq,block_ts,event_type_id,src_addr_id,dst_addr_id,nft_id,collection_id \
             FROM nft_role_transition_edges",
        )?;
        let mapped = stmt.query_map([], |r| {
            let order: i64 = r.get(0)?;
            Ok(Edge {
                order,
                ts: r.get(1)?,
                event_type: r.get(2)?,
                src: r.get(3)?,
                dst: r.get(4)?,
                nft: r.get(5)?,
                collection: r.get(6)?,
                tx: order as u32,
            })
        })?;
        for row in mapped {
            edges.push(row?);
        }
    } else if args.layout == "hgmatch" {
        let mut tx_ids: HashMap<String, u32> = HashMap::new();
        let mut stmt = conn.prepare(
            "SELECT order_seq,block_ts,event_type_id,src_id,dst_id,nft_id,collection_id,tx_hash \
             FROM hg_hyperedges",
        )?;
        let mapped = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, i64>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, i64>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, String>(7)?,
            ))
        })?;
        for row in mapped {
            let (order, ts, event_type, src, dst, nft, collection, tx_hash) = row?;
            let next = tx_ids.len() as u32;
            let tx = *tx_ids.entry(tx_hash).or_insert(next);
            edges.push(Edge { order, ts, event_type, src, dst, nft, collection, tx });
        }
    } else {
        anyhow::bail!("unsupported layout: {}", args.layout);
    }
    let load_s = load0.elapsed().as_secs_f64();

    let index0 = Instant::now();
    let mut by_nft: Vec<usize> = (0..edges.len()).collect();
    by_nft.par_sort_unstable_by_key(|i| {
        let e = edges[*i];
        (e.collection, e.nft, e.ts, e.order)
    });
    let mut by_pair: Vec<usize> = (0..edges.len()).collect();
    by_pair.par_sort_unstable_by_key(|i| {
        let e = edges[*i];
        (e.collection, e.src, e.dst, e.ts, e.order)
    });
    let mut by_src: Vec<usize> = (0..edges.len()).collect();
    by_src.par_sort_unstable_by_key(|i| {
        let e = edges[*i];
        (e.collection, e.src, e.ts, e.order)
    });
    let mut by_dst: Vec<usize> = (0..edges.len()).collect();
    by_dst.par_sort_unstable_by_key(|i| {
        let e = edges[*i];
        (e.collection, e.dst, e.ts, e.order)
    });
    let index_build_s = index0.elapsed().as_secs_f64();

    let count0 = Instant::now();
    let mut rows = Vec::new();
    let type_pairs = [(0, 0), (1, 1), (1, 2), (2, 1), (2, 2)];
    for delta in args.deltas {
        for (first_type, second_type) in type_pairs {
            rows.push(CountRow {
                pattern: "same_nft_return",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: None,
                delta_seconds: delta,
                full_binding_count: count_same_nft(
                    &edges,
                    &by_nft,
                    delta,
                    first_type,
                    second_type,
                    true,
                    args.require_distinct_tx,
                    args.require_later_timestamp,
                ),
            });
            rows.push(CountRow {
                pattern: "same_nft_forward",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: None,
                delta_seconds: delta,
                full_binding_count: count_same_nft(
                    &edges,
                    &by_nft,
                    delta,
                    first_type,
                    second_type,
                    false,
                    args.require_distinct_tx,
                    args.require_later_timestamp,
                ),
            });
            rows.push(CountRow {
                pattern: "cross_nft_reciprocal",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: None,
                delta_seconds: delta,
                full_binding_count: count_cross_nft_pair(
                    &edges,
                    &by_pair,
                    delta,
                    first_type,
                    second_type,
                    true,
                    args.require_distinct_tx,
                    args.require_later_timestamp,
                ),
            });
            rows.push(CountRow {
                pattern: "cross_nft_repeat_pair",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: None,
                delta_seconds: delta,
                full_binding_count: count_cross_nft_pair(
                    &edges,
                    &by_pair,
                    delta,
                    first_type,
                    second_type,
                    false,
                    args.require_distinct_tx,
                    args.require_later_timestamp,
                ),
            });
        }
        for (first_type, second_type, third_type) in [
            (1, 1, 1),
            (1, 2, 2),
            (2, 1, 1),
            (2, 2, 1),
            (2, 2, 2),
        ] {
            rows.push(CountRow {
                pattern: "same_nft_three_actor_cycle",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: Some(type_name(third_type)),
                delta_seconds: delta,
                full_binding_count: count_triangle(
                    &edges,
                    &by_src,
                    &by_pair,
                    delta,
                    first_type,
                    second_type,
                    third_type,
                    0,
                ),
            });
            rows.push(CountRow {
                pattern: "three_nft_three_actor_cycle",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: Some(type_name(third_type)),
                delta_seconds: delta,
                full_binding_count: count_triangle(
                    &edges,
                    &by_src,
                    &by_pair,
                    delta,
                    first_type,
                    second_type,
                    third_type,
                    1,
                ),
            });
            rows.push(CountRow {
                pattern: "two_nft_cycle_forward_same",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: Some(type_name(third_type)),
                delta_seconds: delta,
                full_binding_count: count_triangle(
                    &edges,
                    &by_src,
                    &by_pair,
                    delta,
                    first_type,
                    second_type,
                    third_type,
                    2,
                ),
            });
            rows.push(CountRow {
                pattern: "two_nft_cycle_return_same",
                first_type: type_name(first_type),
                second_type: type_name(second_type),
                third_type: Some(type_name(third_type)),
                delta_seconds: delta,
                full_binding_count: count_triangle(
                    &edges,
                    &by_src,
                    &by_pair,
                    delta,
                    first_type,
                    second_type,
                    third_type,
                    3,
                ),
            });
        }
        for leg_type in [1, 2] {
            rows.push(CountRow {
                pattern: "buyer_two_nft_fanout",
                first_type: type_name(1),
                second_type: type_name(leg_type),
                third_type: Some(type_name(leg_type)),
                delta_seconds: delta,
                full_binding_count: count_two_leg(
                    &edges,
                    &by_src,
                    delta,
                    1,
                    leg_type,
                    true,
                ),
            });
            rows.push(CountRow {
                pattern: "seller_two_nft_convergence",
                first_type: type_name(1),
                second_type: type_name(leg_type),
                third_type: Some(type_name(leg_type)),
                delta_seconds: delta,
                full_binding_count: count_two_leg(
                    &edges,
                    &by_dst,
                    delta,
                    1,
                    leg_type,
                    false,
                ),
            });
        }
    }
    let report = Report {
        mode: "NFT-semantic-pattern-cardinality-audit-v1",
        db: args.db.display().to_string(),
        edge_count: edges.len(),
        layout: args.layout,
        distinct_tx_required: args.require_distinct_tx,
        later_timestamp_required: args.require_later_timestamp,
        load_s,
        index_build_s,
        count_s: count0.elapsed().as_secs_f64(),
        rows,
    };
    if let Some(parent) = args.out_json.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.out_json, serde_json::to_vec_pretty(&report)?)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
