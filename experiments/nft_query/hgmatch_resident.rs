                                                                  
fn main() -> Result<()> {
    use std::io::{BufRead, Write};
    let args = Args::parse();
    rayon::ThreadPoolBuilder::new().num_threads(args.threads).build_global()?;
    let t = Instant::now();
    let index = load_index(&args.db)?;
    let ids: HashSet<_> = index.edges.iter().map(|e| &e.event_id).collect();
    if ids.len()!=index.edges.len() || ids.iter().any(|id| id.is_empty()) { anyhow::bail!("duplicate/empty event_id"); }
    drop(ids);
    let mut out = std::io::BufWriter::new(std::io::stdout().lock());
    writeln!(out,"{}",json!({"frame":"ready","load_count":1,"source_events":index.edges.len(),"startup_seconds":t.elapsed().as_secs_f64()}))?;
    out.flush()?;
    for line in std::io::stdin().lock().lines() {
        let raw=line?;
        if serde_json::from_str::<Value>(&raw)?["op"]=="close" { break; }
        let doc: QueryDocument=serde_json::from_str(&raw)?;
        if doc.full_binding_plan.result_semantics!="full_event_binding_v1" { anyhow::bail!("semantics"); }
        let (rows,duplicates,_,matching,canonical)=execute_query(&index,&doc.full_binding_plan,0);
        writeln!(out,"{}",json!({"frame":"result","rows":rows,"duplicate_count":duplicates,"matching_seconds":matching,"canonical_seconds":canonical}))?;
        out.flush()?;
    }
    Ok(())
}
