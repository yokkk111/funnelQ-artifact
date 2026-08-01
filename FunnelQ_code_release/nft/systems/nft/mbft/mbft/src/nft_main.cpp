#include "adapt.h"
#include "nft_loader.h"
#include "persist.h"
#include "nlohmann/json.hpp"

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>

using nlohmann::json;
namespace fs = std::filesystem;

namespace {

unordered_map<string,string> parseArgs(int argc, char** argv, int start) {
    unordered_map<string,string> args;
    for (int i=start;i<argc;++i) {
        string key=argv[i];
        if (key.rfind("--",0)!=0) throw invalid_argument("expected option: "+key);
        if (i+1>=argc) throw invalid_argument("missing value for "+key);
        args[key.substr(2)] = argv[++i];
    }
    return args;
}

string need(const unordered_map<string,string>& a,const string& k) {
    auto it=a.find(k);
    if(it==a.end()) throw invalid_argument("missing --"+k);
    return it->second;
}

template<typename T>T optNum(const unordered_map<string,string>& a,const string& k,T d) {
    auto it=a.find(k);
    if(it==a.end()) return d;
    if constexpr(std::is_integral_v<T>) return static_cast<T>(stoull(it->second));
    else return static_cast<T>(stod(it->second));
}

vector<string> split(const string& s) {
    vector<string> out;
    string cur;
    for(char c:s){
        if(c==','){
            if(!cur.empty()) out.push_back(cur);
            cur.clear();
        }else cur.push_back(c);
    }
    if(!cur.empty()) out.push_back(cur);
    return out;
}

void usage() {
    cerr<<"MBFT-NFT-Adapt (original MergeBloom/Hint core)\n"
        <<"  build-store --events CSV --store FILE --metrics FILE [--order-offset 0] [--combine 0] [--fp-rate .001]\n"
        <<"  query-store --store FILE --outdir DIR [--types nfttrade,nfttransfer]\n"
        <<"  prove-store --store FILE --outdir DIR [--types nfttrade,nfttransfer]\n"
        <<"  run-store   --store FILE --outdir DIR [--types nfttrade,nfttransfer]\n"
        <<"  query-only  --events CSV --outdir DIR [--types nfttrade,nfttransfer] [--combine 0] [--fp-rate .001]\n"
        <<"  run-all     --events CSV --outdir DIR [--types nfttrade,nfttransfer] [--combine 0] [--fp-rate .001]\n"
        <<"  verify      --proof FILE --commitments FILE --out FILE --metrics FILE --event-type TYPE\n";
}

json loadBuild(const NftLoadMetrics& m,double loadUs,double buildUs,double ads,double avg,
               unsigned combine,double fp,unsigned seed,unsigned tweak,uint64_t orderOffset) {
    return {
        {"format","MBFT-NFT-Adapt-MergeBloom-v1"},
        {"source","original mbft-main plus adapter files"},
        {"event_count",m.eventCount},{"block_count",m.blockCount},
        {"in_scope_root_closures",m.inScopeRootCount},
        {"zero_move_from",m.zeroMoveFromCount},{"zero_move_to",m.zeroMoveToCount},
        {"fixed_keywords_per_event",16},{"keyword_count",m.keywordCount},
        {"canonical_payload_bytes",m.payloadBytes},{"csv_load_time_us",loadUs},
        {"mbft_build_time_us",buildUs},{"total_ads_bytes",ads},
        {"average_block_ads_bytes",avg},{"combine_cycle",combine},{"fp_rate",fp},
        {"seed",seed},{"tweak",tweak},{"order_offset",orderOffset},
        {"hash","original MurmurHash3_x86_32"},
        {"vo","original MergeBloom genHint/reconstruct"}
    };
}

json persistentMetricsJson(const PersistentStoreMetrics& m) {
    return {
        {"blocks",m.blocks},
        {"transactions",m.transactions},
        {"tree_nodes",m.treeNodes},
        {"bloom_bytes",m.bloomBytes},
        {"hint_bytes",m.hintBytes},
        {"payload_bytes",m.payloadBytes},
        {"file_bytes",m.fileBytes}
    };
}

unique_ptr<Chain> buildChainFromEvents(const unordered_map<string,string>& args,
                                       NftLoadMetrics& lm, json& build) {
    const string events=need(args,"events");
    const unsigned seed=optNum<unsigned>(args,"seed",0U);
    const unsigned tweak=optNum<unsigned>(args,"tweak",0U);
    const unsigned combine=optNum<unsigned>(args,"combine",0U);
    const double fp=optNum<double>(args,"fp-rate",0.001);
    const uint64_t orderOffset=optNum<uint64_t>(args,"order-offset",0ULL);

    auto t0=high_resolution_clock::now();
    TxnPool pool=loadNftEventPool(events,seed,&lm,orderOffset);
    const double loadUs=duration<double,micro>(high_resolution_clock::now()-t0).count();

    ParamBF param(0,0,fp,tweak);
    auto chain=make_unique<Chain>(param,seed,set<AggType>{MAX},combine,false,MBFT);
    t0=high_resolution_clock::now();
    chain->buildChain(pool);
    const double buildUs=duration<double,micro>(high_resolution_clock::now()-t0).count();
    vector<Transaction>().swap(pool.pool);
    vector<int>().swap(pool.blkIndex);

    build=loadBuild(lm,loadUs,buildUs,chain->getTotalSizeADS(),chain->getsizeADS(),
                    combine,fp,seed,tweak,orderOffset);
    return chain;
}

int queryChain(Chain& chain,const fs::path& outdir,const vector<string>& types,
               bool withProof,bool verifyGeneratedProof,const json& build,unsigned warmupRuns,
               const fs::path& phaseMarker) {
    fs::create_directories(outdir);
    ofstream(outdir/"build_metrics.json")<<build.dump(2)<<'\n';
    json summary={{"build",build},{"query_only",json::object()}};

    fs::create_directories(outdir/"server_candidates");
    const auto warm0=high_resolution_clock::now();
    for(unsigned run=0;run<warmupRuns;++run){
        for(const string& type:types){
            AtomicSelectQuery q;
            q.lowerBound=0;
            q.upperBound=UINT32_MAX;
            q.keywordsCNF={{"type="+type}};
            auto warm=selectAllChainNoProof(chain,q);
            (void)warm;
        }
    }
    const double warmupUs=duration<double,micro>(high_resolution_clock::now()-warm0).count();
    summary["warmup_runs"]=warmupRuns;
    summary["warmup_time_us"]=warmupUs;
    for(const string& type:types){
        AtomicSelectQuery q;
        q.lowerBound=0;
        q.upperBound=UINT32_MAX;
        q.keywordsCNF={{"type="+type}};
        if(!phaseMarker.empty()){
            fs::create_directories(phaseMarker.parent_path());
            ofstream(phaseMarker)<<"query\n";
        }
        QueryOnlySelection sel=selectAllChainNoProof(chain,q);
        fs::path payloadPath=outdir/"server_candidates"/(type+".jsonl");
        const auto write0=high_resolution_clock::now();
        writePayloadsJsonl(sel.payloads,payloadPath.string());
        const double candidateWriteUs=duration<double,micro>(high_resolution_clock::now()-write0).count();
        if(!phaseMarker.empty() && fs::exists(phaseMarker)) fs::remove(phaseMarker);
        writeSelectionMetricsJson(sel.metrics,(outdir/"server_candidates"/(type+"_metrics.json")).string(),type);
        summary["query_only"][type]={
            {"events",sel.payloads.size()},{"query_time_us",sel.metrics.queryMicros},
            {"candidate_write_time_us",candidateWriteUs},
            {"opened_leaves",sel.metrics.openedLeaves},{"false_positive_leaves",sel.metrics.falsePositiveLeaves},
            {"payload_bytes",sel.metrics.payloadBytes},{"path",payloadPath.string()}
        };
    }

    if(withProof){
        fs::create_directories(outdir/"proofs");
        fs::create_directories(outdir/"verified_candidates");
        vector<RootCommitment> reference;
        summary["proofs"]=json::object();
        summary["verification"]=json::object();

        for(const string& type:types){
            AtomicSelectQuery q;
            q.lowerBound=0;
            q.upperBound=UINT32_MAX;
            q.keywordsCNF={{"type="+type}};
            SelectionMetrics sm;
            vector<RootCommitment> commitments;
            AtomicSelectionProof proof=selectAllChain(chain,q,&commitments,&sm);
            fs::path proofPath=outdir/"proofs"/(type+".mbfhint.proof");
            const auto proofWrite0=high_resolution_clock::now();
            saveAtomicSelectionProof(proof,proofPath.string());
            const double proofWriteUs=duration<double,micro>(high_resolution_clock::now()-proofWrite0).count();
            sm.proofBytes=fs::file_size(proofPath);
            writeSelectionMetricsJson(sm,(outdir/"proofs"/(type+"_server_metrics.json")).string(),type);
            if(reference.empty()){
                reference=commitments;
                saveCommitments(reference,(outdir/"commitments.json").string());
            }else if(reference.size()!=commitments.size()){
                throw runtime_error("commitment schedule changed");
            }
            summary["proofs"][type]={
                {"proof",proofPath.string()},{"proof_bytes",fs::file_size(proofPath)},
                {"vo_generation_time_us",sm.queryMicros},{"proof_write_time_us",proofWriteUs},
                {"hint_bytes",sm.hintBytes},
                {"pruned_bloom_bytes",sm.prunedBloomBytes},{"opened_payload_bytes",sm.payloadBytes}
            };
        }

        if(verifyGeneratedProof){
            // Independent client phase: verification reads only proof + commitments.
            for(const string& type:types){
                fs::path proofPath=outdir/"proofs"/(type+".mbfhint.proof");
                const auto proofLoad0=high_resolution_clock::now();
                AtomicSelectionProof proof=loadAtomicSelectionProof(proofPath.string());
                const double proofLoadUs=duration<double,micro>(high_resolution_clock::now()-proofLoad0).count();
                const auto commitmentLoad0=high_resolution_clock::now();
                vector<RootCommitment> commitments=loadCommitments((outdir/"commitments.json").string());
                const double commitmentLoadUs=duration<double,micro>(high_resolution_clock::now()-commitmentLoad0).count();
                VerifiedSelection verified=verifyAtomicSelection(proof,commitments);
                verified.metrics.proofBytes=fs::file_size(proofPath);
                fs::path outPath=outdir/"verified_candidates"/(type+".jsonl");
                const auto verifiedWrite0=high_resolution_clock::now();
                writePayloadsJsonl(verified.payloads,outPath.string());
                const double verifiedWriteUs=duration<double,micro>(high_resolution_clock::now()-verifiedWrite0).count();
                writeSelectionMetricsJson(verified.metrics,(outdir/"verified_candidates"/(type+"_client_metrics.json")).string(),type);
                summary["verification"][type]={
                    {"valid",verified.valid},{"events",verified.payloads.size()},
                    {"proof_load_time_us",proofLoadUs},{"commitment_load_time_us",commitmentLoadUs},
                    {"verify_time_us",verified.metrics.verifyMicros},
                    {"candidate_write_time_us",verifiedWriteUs},{"proof_bytes",verified.metrics.proofBytes},
                    {"false_positive_leaves",verified.metrics.falsePositiveLeaves},{"path",outPath.string()}
                };
            }
        }
    }

    ofstream(outdir/"atomic_selection_summary.json")<<summary.dump(2)<<'\n';
    if(!phaseMarker.empty() && fs::exists(phaseMarker)) fs::remove(phaseMarker);
    cout<<summary.dump(2)<<endl;
    return 0;
}

int executeEvents(const unordered_map<string,string>& args,bool withProof) {
    const fs::path outdir=need(args,"outdir");
    const vector<string> types=split(args.count("types")?args.at("types"):"nfttrade,nfttransfer");
    NftLoadMetrics lm;
    json build;
    auto chain=buildChainFromEvents(args,lm,build);
    return queryChain(*chain,outdir,types,withProof,withProof,build,
                      optNum<unsigned>(args,"warmup-runs",1),
                      args.count("phase-marker")?fs::path(args.at("phase-marker")):fs::path());
}

int buildStoreCmd(const unordered_map<string,string>& args) {
    const string store=need(args,"store");
    const string metricsPath=need(args,"metrics");
    NftLoadMetrics lm;
    json build;
    auto chain=buildChainFromEvents(args,lm,build);

    PersistentStoreMetrics pm;
    const auto t0=high_resolution_clock::now();
    saveMBFTChain(*chain,store,&pm);
    const double saveUs=duration<double,micro>(high_resolution_clock::now()-t0).count();

    json report={
        {"format","MBFT-NFT-Persistent-Store-v1"},
        {"store",fs::absolute(store).string()},
        {"store_save_time_us",saveUs},
        {"build",build},
        {"persistent",persistentMetricsJson(pm)},
        {"query_time_excludes_csv_load_build_and_store_save",true}
    };
    fs::create_directories(fs::path(metricsPath).parent_path());
    ofstream(metricsPath)<<report.dump(2)<<'\n';
    cout<<report.dump(2)<<endl;
    return 0;
}

int executeStore(const unordered_map<string,string>& args,bool withProof,bool verifyGeneratedProof) {
    const string store=need(args,"store");
    const fs::path outdir=need(args,"outdir");
    const vector<string> types=split(args.count("types")?args.at("types"):"nfttrade,nfttransfer");

    PersistentStoreMetrics pm;
    const auto t0=high_resolution_clock::now();
    auto chain=loadMBFTChain(store,&pm);
    const double loadUs=duration<double,micro>(high_resolution_clock::now()-t0).count();
    json build={
        {"format","MBFT-NFT-Persistent-Store-v1"},
        {"store",fs::absolute(store).string()},
        {"store_bytes",fs::file_size(store)},
        {"store_load_time_us",loadUs},
        {"persistent",persistentMetricsJson(pm)},
        {"query_time_excludes_store_load",true},
        {"hash","original MurmurHash3_x86_32"},
        {"vo","original MergeBloom genHint/reconstruct"}
    };
    return queryChain(*chain,outdir,types,withProof,verifyGeneratedProof,build,
                      optNum<unsigned>(args,"warmup-runs",1),
                      args.count("phase-marker")?fs::path(args.at("phase-marker")):fs::path());
}

int verifyCmd(const unordered_map<string,string>& args){
    const string typ=need(args,"event-type");
    const auto proofLoad0=high_resolution_clock::now();
    AtomicSelectionProof p=loadAtomicSelectionProof(need(args,"proof"));
    const double proofLoadUs=duration<double,micro>(high_resolution_clock::now()-proofLoad0).count();
    const string expectedKeyword="type="+typ;
    if(p.query.lowerBound!=0 || p.query.upperBound!=UINT32_MAX ||
       p.query.keywordsCNF.size()!=1 || p.query.keywordsCNF[0].size()!=1 ||
       p.query.keywordsCNF[0][0]!=expectedKeyword){
        throw runtime_error("proof query does not match the expected atomic event-type predicate");
    }
    const auto commitmentLoad0=high_resolution_clock::now();
    vector<RootCommitment> c=loadCommitments(need(args,"commitments"));
    const double commitmentLoadUs=duration<double,micro>(high_resolution_clock::now()-commitmentLoad0).count();
    VerifiedSelection v=verifyAtomicSelection(p,c);
    const string out=need(args,"out");
    const string metrics=need(args,"metrics");
    v.metrics.proofBytes=fs::file_size(need(args,"proof"));
    const auto write0=high_resolution_clock::now();
    writePayloadsJsonl(v.payloads,out);
    const double writeUs=duration<double,micro>(high_resolution_clock::now()-write0).count();
    writeSelectionMetricsJson(v.metrics,metrics,typ);
    cout<<json{{"valid",v.valid},{"events",v.payloads.size()},
               {"proof_load_time_us",proofLoadUs},{"commitment_load_time_us",commitmentLoadUs},
               {"verify_time_us",v.metrics.verifyMicros},{"candidate_write_time_us",writeUs},
               {"proof_bytes",v.metrics.proofBytes}}.dump(2)<<endl;
    return v.valid?0:2;
}

} // namespace

int main(int argc,char**argv){
    try{
        if(argc<2){usage();return 2;}
        string cmd=argv[1];
        auto args=parseArgs(argc,argv,2);
        if(cmd=="build-store") return buildStoreCmd(args);
        if(cmd=="query-store") return executeStore(args,false,false);
        if(cmd=="prove-store") return executeStore(args,true,false);
        if(cmd=="run-store") return executeStore(args,true,true);
        if(cmd=="query-only") return executeEvents(args,false);
        if(cmd=="run-all") return executeEvents(args,true);
        if(cmd=="verify") return verifyCmd(args);
        usage();
        return 2;
    }catch(const exception&e){
        cerr<<"error: "<<e.what()<<endl;
        return 1;
    }
}
