                                                                               
fn main() -> Result<()> {
    use std::io::{BufRead, Write};
    let args = Args::parse();
    let started = Instant::now();
    let conn = Connection::open_with_flags(&args.db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let tx = args.tx_sidecar_db.as_ref().context("transaction DB required")?;
                                                                       
    conn.execute_batch("PRAGMA query_only=ON; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-65536;")?;
    conn.execute("ATTACH DATABASE ?1 AS txdb", [tx])?;
    validate_layout(&conn)?;
    let duplicate: i64 = conn.query_row("SELECT COUNT(*) FROM (SELECT event_id FROM nft_role_transition_edges GROUP BY event_id HAVING COUNT(*)>1 OR event_id='')", [], |r| r.get(0))?;
    if duplicate != 0 { bail!("source event_id must be nonempty and globally unique"); }
    let (runtime, _) = load_runtime(&conn, true)?;
    let (summaries, _) = load_summaries(&conn)?;
    let (anchors, _) = load_anchor_directory(&conn)?;
    if runtime.edges.len() != summaries.len() { bail!("summary coverage mismatch"); }
    let pool = rayon::ThreadPoolBuilder::new().num_threads(args.threads).build()?;
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    writeln!(out, "{}", serde_json::json!({"frame":"ready","load_count":1,"source_events":runtime.edges.len(),"startup_seconds":started.elapsed().as_secs_f64()}))?;
    out.flush()?;
    for line in std::io::stdin().lock().lines() {
        let req: serde_json::Value = serde_json::from_str(&line?)?;
        if req["op"] == "close" { break; }
        let kind = QueryKind::parse(req["query_id"].as_str().context("query_id")?)?;
        if !matches!(kind, QueryKind::Q1|QueryKind::Q2|QueryKind::Q3|QueryKind::Q4|QueryKind::Q5) { bail!("selected5 only"); }
        let delta = req["delta_seconds"].as_i64().filter(|n| *n>0).context("delta")?;
        let t = Instant::now();
        let (candidates, cand0, cand1, _, _) = execute_candidate_prefix(kind, Ablation::Full, &summaries, Some(&anchors), usize::MAX);
        let (rows, raw, examined, cand2) = enumerate_selected_matches_fused(kind, &runtime, &candidates, delta, usize::MAX, &pool);
        let kernel = t.elapsed().as_secs_f64();
        let orders: Vec<_> = rows.iter().map(|r| (r.root_order,r.middle_order,r.back_order)).collect();
        writeln!(out, "{}", serde_json::json!({"frame":"result","matches":orders,"raw_rows":raw,"cand0":cand0,"cand1":cand1,"cand2":cand2,"examined":examined,"kernel_seconds":kernel}))?;
        out.flush()?;
    }
    Ok(())
}
