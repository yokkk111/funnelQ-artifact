                                                                            
                                                                       
use super::*;
use std::path::PathBuf;

const VARIANTS: [&str; 4] = ["shared", "complete", "single_max", "independent"];

#[derive(Serialize, Deserialize)]
struct Entry {
    directory: String,
    manifest: Manifest,
}
#[derive(Serialize, Deserialize)]
struct Bundle {
    version: String,
    variant: String,
    full_rows: usize,
    entries: Vec<Entry>,
}
impl Bundle {
    fn validate(&self) -> Result<()> {
        ensure!(self.version == "scc-mechanism-ablation/v1", "bundle version");
        ensure!(VARIANTS.contains(&self.variant.as_str()), "variant");
        let first = self.entries.first().context("empty bundle")?;
        for e in &self.entries {
            e.manifest.validate()?;
            ensure!(e.manifest.source_sha256 == first.manifest.source_sha256,
                "bundle source mismatch");
        }
        if self.variant == "independent" {
            ensure!(self.entries.len() == SCALES.len() + 1, "independent scale coverage");
            for (e, h) in self.entries[..SCALES.len()].iter().zip(SCALES) {
                ensure!(e.manifest.domain_only && e.manifest.scales == [h]
                    && e.manifest.prefix_counts == [e.manifest.nrows], "independent index layout");
            }
            let m = &self.entries.last().unwrap().manifest;
            ensure!(!m.domain_only && m.scales.is_empty() && m.nrows == self.full_rows,
                "independent full-input fallback");
        } else {
            ensure!(self.entries.len() == 1, "single index bundle");
            let m = &first.manifest;
            let expected = match self.variant.as_str() {
                "shared" => SCALES.to_vec(),
                "single_max" => vec![*SCALES.last().unwrap()],
                _ => vec![],
            };
            ensure!(!m.domain_only && m.scales == expected && m.nrows == self.full_rows,
                "variant index layout");
        }
        Ok(())
    }
    fn select(&self, q: &QuerySpec) -> Result<usize> {
        self.validate()?;
        if self.variant != "independent" {
            plan(q, &self.entries[0].manifest)?;
            return Ok(0);
        }
                                                                          
                                                                             
        let mut descriptor = self.entries.last().unwrap().manifest.clone();
        descriptor.scales = SCALES.to_vec();
        descriptor.prefix_counts = self.entries[..SCALES.len()].iter()
            .map(|e| e.manifest.nrows).collect();
        Ok(plan(q, &descriptor)?.level.unwrap_or(SCALES.len()))
    }
}

fn persist(dir: &Path, ix: &Index) -> Result<Value> {
    fs::create_dir(dir)?;
    let t = Instant::now();
    crate::save(&dir.join("manifest.json"), &ix.manifest)?;
    write_new(&dir.join("records.bin"), &ix.records.iter().flat_map(raw).collect::<Vec<_>>())?;
    write_new(&dir.join("hashes.bin"), &ix.tree.iter().flatten()
        .flat_map(|h| h.iter().copied()).collect::<Vec<_>>())?;
    Ok(json!({"write_seconds":t.elapsed().as_secs_f64(),
        "event_rows":ix.records.len(),"event_bytes":fs::metadata(dir.join("records.bin"))?.len(),
        "hash_bytes":fs::metadata(dir.join("hashes.bin"))?.len(),
        "manifest_bytes":fs::metadata(dir.join("manifest.json"))?.len(),"build":ix.build}))
}
fn load_index(dir: &Path, m: &Manifest) -> Result<Index> {
    m.validate()?;
    let bytes = fs::read(dir.join("records.bin"))?;
    ensure!(bytes.len() == m.nrows * 38, "persisted record count");
    let records = bytes.chunks_exact(38).map(parse_raw).collect::<Result<Vec<_>>>()?;
    let hashes = fs::read(dir.join("hashes.bin"))?;
    let mut bytes = hashes.as_slice();
    let mut tree = vec![];
    let mut n = m.nrows.div_ceil(PAGE).max(1).next_power_of_two();
    loop {
        tree.push(columns::take(&mut bytes, n*32)?.chunks_exact(32)
            .map(|h| h.try_into().unwrap()).collect::<Vec<Hash>>());
        if n == 1 { break; }
        n /= 2;
    }
    ensure!(bytes.is_empty() && tree.last().unwrap()[0] == m.root, "persisted Merkle root");
    Ok(Index {manifest:m.clone(), records, tree, build:Value::Null})
}

fn seal(args: &[String]) -> Result<()> {
    ensure!(args.len() == 5, "scc-ablation-seal ROOT NEW_OUTPUT VARIANT");
    let root = Path::new(&args[2]);
    let out = Path::new(&args[3]);
    let variant = args[4].as_str();
    ensure!(VARIANTS.contains(&variant), "variant");
    fs::create_dir(out)?;
    let t = Instant::now();
    let data = hybrid_key_audit::data(root)?;
    let sha = columns::digest(&fs::read(root.join(
        "FunnelQ_CSR/artifacts/nft_csr_profit_screen_stage6_block_b16_t1800/canonical_events_collection_order_v3.bin"))?);
    let read_seconds = t.elapsed().as_secs_f64();
    let mut entries = vec![];
    let mut storage = vec![];
    let mut compute_seconds = 0.0;
    let mut store = |name: String, ix: Index| -> Result<()> {
        storage.push(persist(&out.join(&name), &ix)?);
        entries.push(Entry {directory:name, manifest:ix.manifest});
        Ok(())
    };
    if variant == "independent" {
        let t = Instant::now();
        let mut collections = BTreeMap::<u32, Vec<usize>>::new();
        for (i,r) in data.iter().enumerate() { collections.entry(r.collection).or_default().push(i); }
        compute_seconds += t.elapsed().as_secs_f64();
        for h in SCALES {
            let t = Instant::now();
            let keep = retained(&data, &collections, h);
            let records: Vec<_> = data.iter().zip(&keep).filter(|(_, k)| **k)
                .map(|(r,_)| Record {trade:*r,birth:0}).collect();
            let n = records.len();
            let m = Manifest {version:VERSION.into(), source_sha256:sha.clone(),
                schema:"independent SCC domain; full original fields; canonical-order; same covers and collection partition".into(),
                trust:"local trusted sealer; complete-input fallback separately sealed".into(),
                scales:vec![h],prefix_counts:vec![n],nrows:n,page_rows:PAGE,root:[0;32],domain_only:true};
            let mut ix = seal_records(records,m)?;
            let seconds = t.elapsed().as_secs_f64();
            compute_seconds += seconds;
            ix.build = json!({"h":h,"rows":n,"build_seconds":seconds});
            store(format!("scale_{h}"),ix)?;
        }
        let t = Instant::now();
        let ix = build_scales(&data,sha.clone(),&[])?;
        compute_seconds += t.elapsed().as_secs_f64();
        store("fallback".into(),ix)?;
    } else {
        let scales = match variant { "shared" => SCALES.to_vec(), "single_max" => vec![65536], _ => vec![] };
        let t = Instant::now();
        let ix = build_scales(&data,sha,&scales)?;
        compute_seconds = t.elapsed().as_secs_f64();
        store("index".into(),ix)?;
    }
    let bundle = Bundle {version:"scc-mechanism-ablation/v1".into(),variant:variant.into(),
        full_rows:data.len(),entries};
    bundle.validate()?;
    crate::save(&out.join("bundle.json"),&bundle)?;
    let sum = |field: &str| storage.iter().map(|v|v[field].as_u64().unwrap()).sum::<u64>();
    crate::save(&out.join("offline.json"),&json!({"variant":variant,"canonical_rows":data.len(),
        "source_read_hash_seconds":read_seconds,"build_seconds_excluding_write":compute_seconds,
        "write_seconds":storage.iter().map(|v|v["write_seconds"].as_f64().unwrap()).sum::<f64>(),
        "event_rows_stored":sum("event_rows"),"event_bytes":sum("event_bytes"),
        "hash_bytes":sum("hash_bytes"),"metadata_bytes":sum("manifest_bytes")+fs::metadata(out.join("bundle.json"))?.len(),
        "total_index_bytes":sum("event_bytes")+sum("hash_bytes")+sum("manifest_bytes")+fs::metadata(out.join("bundle.json"))?.len(),
        "complete_fallback_included":true,"query_files_read_before_sealing":false,
        "sealer_peak_rss_bytes":crate::memory::peak_bytes(),"indexes":storage}))
}

fn read_bundle(path: &Path) -> Result<Bundle> {
    let b: Bundle = serde_json::from_slice(&fs::read(path)?)?;
    b.validate()?;
    Ok(b)
}
fn client(args: &[String]) -> Result<()> {
    ensure!(args.len() == 7,"scc-ablation-client BUNDLE QUERY PACKET OUTPUT MODE");
    let t = Instant::now();
    let b = read_bundle(Path::new(&args[2]))?;
    let q: QuerySpec = serde_json::from_slice(&fs::read(&args[3])?)?;
    let selected = b.select(&q)?;
    let wire = fs::read(&args[4])?;
    let mut report = verify(&b.entries[selected].manifest,&q,&wire,&args[6])?;
    report["client_read_verify_seconds"] = json!(t.elapsed().as_secs_f64());
    report["selected_index"] = json!(selected);
    crate::save(Path::new(&args[5]),&report)
}
fn invoke(command: &str, args: &[String], logfile: &Path) -> Result<f64> {
    let t = Instant::now();
    let result = Command::new(std::env::current_exe()?).arg(command).args(args).output()?;
    write_new(logfile,&result.stderr)?;
    ensure!(result.status.success(),"{} failed: {}",command,String::from_utf8_lossy(&result.stderr));
    Ok(t.elapsed().as_secs_f64())
}

fn case(args: &[String]) -> Result<()> {
    ensure!(args.len() == 7,"scc-ablation-case BUNDLE QUERY EXPECTED_OR_DASH NEW_OUTPUT ROUNDS");
    let bundle_path = Path::new(&args[2]);
    let b = read_bundle(bundle_path)?;
    let q: QuerySpec = serde_json::from_slice(&fs::read(&args[3])?)?;
    let out = Path::new(&args[5]);
    let rounds: usize = args[6].parse()?;
    ensure!(rounds > 0,"rounds");
    fs::create_dir(out)?;
    let load = Instant::now();
    let selected = b.select(&q)?;
    let ix = load_index(&bundle_path.parent().unwrap().join(&b.entries[selected].directory),&b.entries[selected].manifest)?;
    let load_seconds = load.elapsed().as_secs_f64();
    let mut expected: Option<Vec<Answer>> = if args[4] == "-" {None} else {
        Some(serde_json::from_slice(&fs::read(&args[4])?)?)
    };
    ensure!(expected.is_some() || b.variant == "complete","only complete input can establish runtime reference");
    let h = header(&ix,&q)?;
    let mut runs = vec![];
    for rep in 0..=rounds {
        let t = Instant::now();
        let sealed = table(&ix.records,h.cut)?;
        let typed = columns::rows(&sealed.tables)?;
        let engine = Engine::new(&typed,&q,&BTreeSet::from(["NFTTrade".into()]))?;
        let (answers,_) = engine.replay_rows()?;
        let query_seconds = t.elapsed().as_secs_f64();
        let actual = engine.canonical_answers(&answers)?;
        if expected.is_none() { expected = Some(actual.clone()); }
        let expected = expected.as_ref().unwrap();
                                                                       
        let a: BTreeSet<_> = actual.iter().cloned().collect();
        let e: BTreeSet<_> = expected.iter().cloned().collect();
        let fp = a.difference(&e).count();
        let fn_ = e.difference(&a).count();
        ensure!(fp == 0 && fn_ == 0 && actual.len() == a.len(),"binding mismatch FP={fp} FN={fn_}");
        drop(engine); drop(typed); drop(sealed); drop(a); drop(e);
        let t = Instant::now();
        let fresh = header(&ix,&q)?;
        let (wire,sizes) = pack(&fresh,&ix.records[..fresh.opened],&q,&answers)?;
        let packet_seconds = t.elapsed().as_secs_f64();
        let packet = out.join(format!("rep{rep}.packet"));
        write_new(&packet,&wire)?;
        let mut clients = vec![];
        for mode in if rep%2 == 0 { ["count","replay"] } else { ["replay","count"] } {
            let dest = out.join(format!("rep{rep}_{mode}.json"));
            let wall = invoke("scc-ablation-client",&[
                args[2].clone(),args[3].clone(),packet.display().to_string(),dest.display().to_string(),mode.into()],
                &out.join(format!("rep{rep}_{mode}.log")))?;
            let mut v: Value = serde_json::from_slice(&fs::read(dest)?)?;
            v["client_process_wall_seconds"] = json!(wall);
            v["e2e_compute_seconds"] = json!(query_seconds+packet_seconds+wall);
            clients.push(v);
        }
        runs.push(json!({"rep":rep,"warmup":rep==0,"FP":fp,"FN":fn_,"answers":answers.len(),
            "query_seconds":query_seconds,"proof_packet_generation_seconds":packet_seconds,
            "sizes":sizes,"clients":clients}));
    }
    crate::save(&out.join("reference.json"),expected.as_ref().unwrap())?;
    crate::save(&out.join("summary.json"),&json!({"variant":b.variant,"query":q.query_id,
        "query_sha256":query_hash(&q),"domain_rows":h.cut,"opened_rows":h.opened,"plan":h.plan,
        "source_rows":b.full_rows,"index_load_seconds_excluded_from_steady_query":load_seconds,
        "server_process_peak_rss_including_oracle_comparison":crate::memory::peak_bytes(),
        "network_included":false,"os_cache_flushed":false,"server_index_resident":true,
        "client_fresh_process_each_run":true,"runs":runs}))
}

fn median(mut xs: Vec<f64>) -> f64 {
    xs.sort_by(f64::total_cmp);
    if xs.len()%2 == 0 { (xs[xs.len()/2-1]+xs[xs.len()/2])/2.0 } else {xs[xs.len()/2]}
}
fn summarize(case: &Value) -> Value {
    let runs: Vec<_> = case["runs"].as_array().unwrap().iter().filter(|r|r["warmup"]==false).collect();
    let mid = |f: &str| median(runs.iter().map(|r|r[f].as_f64().unwrap()).collect());
    let mut modes = serde_json::Map::new();
    for mode in ["count","replay"] {
        let cs: Vec<_> = runs.iter().map(|r|r["clients"].as_array().unwrap().iter().find(|c|c["mode"]==mode).unwrap()).collect();
        let mut obj = serde_json::Map::new();
        for f in ["authentication_decode_seconds","table_prepare_seconds","sound_unique_seconds",
            "count_or_replay_seconds","client_read_verify_seconds","client_process_wall_seconds","e2e_compute_seconds","client_peak_rss_bytes"] {
            obj.insert(f.into(),json!(median(cs.iter().map(|c|c[f].as_f64().unwrap()).collect())));
        }
        modes.insert(mode.into(),Value::Object(obj));
    }
    json!({"variant":case["variant"],"query":case["query"],"domain_rows":case["domain_rows"],
        "opened_rows":case["opened_rows"],"answers":runs[0]["answers"],"FP":0,"FN":0,
        "response_bytes":runs[0]["sizes"]["response_bytes"],"sizes":runs[0]["sizes"],
        "query_seconds":mid("query_seconds"),"proof_packet_generation_seconds":mid("proof_packet_generation_seconds"),
        "clients":modes,"measured_rounds":runs.len()})
}

fn suite(args: &[String]) -> Result<()> {
    ensure!(args.len() >= 4,"scc-ablation ROOT NEW_OUTPUT [rounds=3] [windows=300,900,1800,3600,7200]");
    let root = fs::canonicalize(&args[2])?;
    let out = PathBuf::from(&args[3]);
    fs::create_dir(&out)?;
    let out = fs::canonicalize(out)?;
    let rounds = args.get(4).map(|s|s.parse::<usize>()).transpose()?.unwrap_or(3);
    ensure!(rounds > 0,"rounds");
    let mut windows = args.get(5).map(String::as_str).unwrap_or("300,900,1800,3600,7200")
        .split(',').map(|s|s.parse::<i64>()).collect::<std::result::Result<Vec<_>,_>>()?;
    ensure!(windows.iter().all(|w|*w>0 && *w<=65536),"window range");
    windows.push(1800); windows.sort(); windows.dedup();
                                                             
    let mut offline = vec![];
    for variant in VARIANTS {
        eprintln!("SEAL {variant}");
        let dir = out.join(format!("sealed_{variant}"));
        let wall = invoke("scc-ablation-seal",&[root.display().to_string(),dir.display().to_string(),variant.into()],
            &out.join(format!("seal_{variant}.log")))?;
        let mut report: Value = serde_json::from_slice(&fs::read(dir.join("offline.json"))?)?;
        report["isolated_sealer_process_wall_seconds"] = json!(wall);
        offline.push(report);
    }
    crate::save(&out.join("offline_summary.json"),&offline)?;
    let mut files = fs::read_dir(root.join("FunnelQ_CSR/nft/queries/nft/selected5"))?
        .map(|e|e.map(|x|x.path())).collect::<std::io::Result<Vec<_>>>()?;
    files.sort();
    let mut results = vec![];
    for (qi,file) in files.iter().enumerate() {
        let original = crate::load_query(file)?;
        for &window in &windows {
            let mut q = original.clone();
            if let ScopeSpec::Episode {window_seconds,..} = &mut q.scope { *window_seconds = window; }
                                                                           
                                                                                
            for branch in &mut q.branches {
                for c in &mut branch.constraints {
                    if let Constraint::Within {delta_seconds,..} = c {
                        ensure!(*delta_seconds == 1800,"unexpected independent temporal parameter");
                        *delta_seconds = window;
                    }
                }
            }
            if window != 1800 { q.query_id = format!("{}__window_{window}",q.query_id); }
            let label = format!("q{}_w{window}",qi+1);
            let query_path = out.join(format!("{label}.query.json"));
            crate::save(&query_path,&q)?;
            let mut reference_path = out.join(format!("{label}.expected.json"));
            let original_reference = window == 1800;
            if original_reference { crate::save(&reference_path,&truth(&root,qi,&q)?)?; }
                                                                              
                                                                                  
            let mut variants = vec!["shared","single_max"];
            if window == 1800 { variants.push("independent"); }
            let rotation = qi % variants.len(); variants.rotate_left(rotation);
            variants.insert(0,"complete");
            for variant in variants {
                eprintln!("CASE {label} {variant}");
                let dir = out.join(format!("{label}_{variant}"));
                let expected = if !original_reference && variant=="complete" {"-".into()}
                    else {reference_path.display().to_string()};
                invoke("scc-ablation-case",&[
                    out.join(format!("sealed_{variant}/bundle.json")).display().to_string(),
                    query_path.display().to_string(),expected,dir.display().to_string(),rounds.to_string()],
                    &out.join(format!("{label}_{variant}.log")))?;
                if variant=="complete" && !original_reference {reference_path=dir.join("reference.json");}
                let case: Value = serde_json::from_slice(&fs::read(dir.join("summary.json"))?)?;
                let mut compact = summarize(&case);
                compact["window_seconds"] = json!(window);
                compact["formal_query"] = json!(window==1800);
                compact["reference_kind"] = json!(if original_reference {"existing full-domain Generic Residual binding set"}
                    else {"fresh complete-input exact engine; not an independent implementation"});
                if variant == "independent" {
                    let shared = results.iter().find(|v: &&Value|v["query"]==q.query_id && v["variant"]=="shared");
                    if let Some(s) = shared { ensure!(s["domain_rows"]==compact["domain_rows"],"storage ablation changed domain"); }
                }
                eprintln!("DONE {label} {variant}: E2E={:.3}s bytes={} FP=0 FN=0",
                    compact["clients"]["count"]["e2e_compute_seconds"].as_f64().unwrap(),compact["response_bytes"]);
                crate::save(&out.join(format!("{label}_{variant}.median.json")),&compact)?;
                results.push(compact);
            }
        }
    }
    let report = json!({"version":"scc-mechanism-ablation/v1","full_three_month":true,
        "rounds":rounds,"warmup_per_case":1,"windows":windows,"offline":offline,"results":results,
        "no_network":true,"os_cache_flushed":false,"trusted_sealer_required":true,
        "formal_system_modified":false,"no_query_id_dispatch":true,
        "timing":"server query + packet generation + fresh client process wall; server index load separately recorded; oracle, packet disk write, offline build and network excluded"});
    crate::save(&out.join("summary.json"),&report)?;
    let mut md = String::from("# SCC mechanism ablation\n\nAll sizes are decimal MB. One warmup, measured medians. Same packet codec and exact engine. Trusted sealer; no blockchain deployment. No network; OS caches not flushed. Index load and offline work are separate, not silently included in online medians.\n\n## Offline (complete fallback included)\n\n|Variant|Build s|Write s|Stored rows|Index MB|Peak MB|\n|---|---:|---:|---:|---:|---:|\n");
    for v in report["offline"].as_array().unwrap() { md += &format!("|{}|{:.3}|{:.3}|{}|{:.3}|{:.1}|\n",
        v["variant"].as_str().unwrap(),v["build_seconds_excluding_write"].as_f64().unwrap(),v["write_seconds"].as_f64().unwrap(),
        v["event_rows_stored"],v["total_index_bytes"].as_f64().unwrap()/1e6,v["sealer_peak_rss_bytes"].as_f64().unwrap()/1e6); }
    md += "\n## Online\n\n|Query|Window s|Variant|Domain rows|Response MB|Query s|Packet s|Client count path s|E2E count s|E2E replay s|\n|---|---:|---|---:|---:|---:|---:|---:|---:|---:|\n";
    for v in report["results"].as_array().unwrap() { md += &format!("|{}|{}|{}|{}|{:.3}|{:.3}|{:.3}|{:.3}|{:.3}|{:.3}|\n",
        v["query"].as_str().unwrap(),v["window_seconds"],v["variant"].as_str().unwrap(),v["domain_rows"],v["response_bytes"].as_f64().unwrap()/1e6,
        v["query_seconds"].as_f64().unwrap(),v["proof_packet_generation_seconds"].as_f64().unwrap(),
        v["clients"]["count"]["client_process_wall_seconds"].as_f64().unwrap(),v["clients"]["count"]["e2e_compute_seconds"].as_f64().unwrap(),
        v["clients"]["replay"]["e2e_compute_seconds"].as_f64().unwrap()); }
    md += "\n## Interpretation limits\n\nSCC removal measures domain reduction; single maximum scale measures scale selection; independent scales measure storage sharing, not an assumed online speedup. Count must include soundness/uniqueness when compared with replay. Formal 1800s results use existing Generic Residual full-binding references. Changed windows use fresh full-input exact evaluation plus small brute-force tests, not a second independent full-data implementation. All full binding comparisons passed with FP/FN=0. Cached trusted bundle bytes are listed in offline metadata; per-response bytes exclude this reusable trust anchor.\n";
    write_new(&out.join("report.md"),md.as_bytes())
}

pub fn dispatch(args: &[String]) -> Result<()> {
    match args[1].as_str() {
        "scc-ablation" => suite(args),
        "scc-ablation-seal" => seal(args),
        "scc-ablation-case" => case(args),
        "scc-ablation-client" => client(args),
        _ => anyhow::bail!("unknown ablation command"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::{AtomSpec, BranchSpec};
    fn query(window: i64, closed: bool) -> QuerySpec {
        QuerySpec {
            query_id:"never_registered".into(),result_semantics:"full_event_binding_v1".into(),
            scope:ScopeSpec::Episode {anchor_atom:"a".into(),window_seconds:window},
            branches:vec![BranchSpec {id:"branch".into(),event_injective:true,
                atoms:[("a","x","y"),("b","y",if closed {"x"} else {"z"})].into_iter()
                    .map(|(id,src,dst)|AtomSpec {id:id.into(),event_types:BTreeSet::from(["NFTTrade".into()]),
                        fields:BTreeMap::from([("src".into(),Term::Variable(src.into())),
                            ("dst".into(),Term::Variable(dst.into())),("collection".into(),Term::Variable("c".into()))]),
                        local_predicates:vec![]}).collect(),
                constraints:vec![Constraint::Order {earlier:"a".into(),later:"b".into(),field:"order_seq".into()},
                    Constraint::Order {earlier:"a".into(),later:"b".into(),field:"block_ts".into()},
                    Constraint::Within {earlier:"a".into(),later:"b".into(),delta_seconds:window}]}],
        }
    }
    fn data() -> Vec<Trade> {
        [(1,511,1,2),(2,600,2,1),(3,601,2,1),(4,2000,3,4),
            (5,3800,4,3),(6,8000,7,8),(7,8001,8,9)]
            .into_iter().map(|(order,time,src,dst)|Trade {order,time,src,dst,nft:1,collection:5,tx:order as u32}).collect()
    }
    fn independent(data: &[Trade]) -> Result<Vec<Index>> {
        let mut indexes = vec![];
        for h in SCALES {
            let ix = build_scales(data,"test".into(),&[h])?;
            let n = ix.manifest.prefix_counts[0];
            let mut m = ix.manifest.clone(); m.nrows=n; m.domain_only=true;
            indexes.push(seal_records(ix.records[..n].to_vec(),m)?);
        }
        indexes.push(build_scales(data,"test".into(),&[])?);
        Ok(indexes)
    }
    #[test]
    fn all_mechanisms_match_independent_bruteforce_and_reject_tampering() -> Result<()> {
        let data = data();
        let shared = build(&data,"test".into())?;
        let full = build_scales(&data,"test".into(),&[])?;
        let maximum = build_scales(&data,"test".into(),&[65536])?;
        let indexes = independent(&data)?;
        let bundle = Bundle {version:"scc-mechanism-ablation/v1".into(),variant:"independent".into(),
            full_rows:data.len(),entries:indexes.iter().enumerate().map(|(i,ix)|Entry {
                directory:i.to_string(),manifest:ix.manifest.clone()}).collect()};
        for (window,closed) in [(89,true),(90,true),(300,true),(900,true),(1800,true),(7200,true),(1800,false),(70000,true)] {
            let q = query(window,closed);
            let expected = crate::reference(&data.iter().map(event).collect::<Result<Vec<_>>>()?,&q)?;
            let independent = &indexes[bundle.select(&q)?];
            for ix in [&shared,&full,&maximum,independent] {
                let h = header(ix,&q)?;
                let s = table(&ix.records,h.cut)?;
                let rs = columns::rows(&s.tables)?;
                let e = Engine::new(&rs,&q,&BTreeSet::from(["NFTTrade".into()]))?;
                let (answers,_) = e.replay_rows()?;
                assert_eq!(e.canonical_answers(&answers)?,expected);
                let (wire,_) = pack(&h,&ix.records[..h.opened],&q,&answers)?;
                verify(&ix.manifest,&q,&wire,"count")?;
                verify(&ix.manifest,&q,&wire,"replay")?;
                let mut h_bad = h.clone(); h_bad.cut += 1;
                let (wire,_) = pack(&h_bad,&ix.records[..h.opened],&q,&answers)?;
                assert!(verify(&ix.manifest,&q,&wire,"count").is_err());
            }
        }
        Ok(())
    }
    #[test]
    fn partial_index_cannot_be_used_as_complete_input_and_scale_cannot_be_chosen_by_server() -> Result<()> {
        let indexes = independent(&data())?;
        assert!(plan(&query(1800,false),&indexes[2].manifest).is_err());
        assert!(plan(&query(70000,true),&indexes[2].manifest).is_err());
        let mut bundle = Bundle {version:"scc-mechanism-ablation/v1".into(),variant:"independent".into(),
            full_rows:data().len(),entries:indexes.iter().enumerate().map(|(i,ix)|Entry {
                directory:i.to_string(),manifest:ix.manifest.clone()}).collect()};
        assert_eq!(bundle.select(&query(1800,true))?,2);
        assert_eq!(bundle.select(&query(1800,false))?,8);
        assert_eq!(bundle.select(&query(70000,true))?,8);
        bundle.entries.swap(0,2);
        assert!(bundle.validate().is_err());
        Ok(())
    }
    #[test]
    fn legacy_manifest_omits_new_field_and_roundtrips() -> Result<()> {
        let ix = build(&data(),"test".into())?;
        let bytes = serde_json::to_vec(&ix.manifest)?;
        assert!(!String::from_utf8_lossy(&bytes).contains("domain_only"));
        let m: Manifest = serde_json::from_slice(&bytes)?;
        assert_eq!(m.context(),ix.manifest.context());
        Ok(())
    }
}
