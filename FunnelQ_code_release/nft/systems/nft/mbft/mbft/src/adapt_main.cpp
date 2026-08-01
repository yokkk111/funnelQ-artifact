#include "adapt.h"
#include "nlohmann/json.hpp"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <unordered_map>

using nlohmann::json;
namespace fs = std::filesystem;

namespace {

unordered_map<string,string> parseArgs(int argc, char** argv, int start) {
    unordered_map<string,string> args;
    for (int i = start; i < argc; ++i) {
        string key = argv[i];
        if (key.rfind("--", 0) != 0) throw invalid_argument("expected option, got: " + key);
        if (i + 1 >= argc) throw invalid_argument("missing value for " + key);
        args[key.substr(2)] = argv[++i];
    }
    return args;
}

string need(const unordered_map<string,string>& args, const string& key) {
    auto it = args.find(key);
    if (it == args.end()) throw invalid_argument("missing --" + key);
    return it->second;
}

template <typename T>
T optionalNumber(const unordered_map<string,string>& args, const string& key, T fallback) {
    auto it = args.find(key);
    if (it == args.end()) return fallback;
    if constexpr (is_integral_v<T>) return static_cast<T>(stoull(it->second));
    else return static_cast<T>(stod(it->second));
}

void printUsage() {
    cerr << "MBFT-Adapt with original MergeBloom Hint VO\n\n"
         << "Commands:\n"
         << "  run --events event_atoms.csv --outdir DIR [--combine 0] [--fp-rate 0.001]\n"
         << "      [--seed 0] [--tweak 0] [--types flashloan,swap,borrow]\n"
         << "  query-only-select --events event_atoms.csv --outdir DIR [--combine 0]\n"
         << "      [--fp-rate 0.001] [--seed 0] [--tweak 0]\n"
         << "      [--types flashloan,swap,borrow]\n"
         << "  verify --proof FILE --commitments FILE --out FILE --metrics FILE --event-type TYPE\n";
}

vector<string> splitTypes(const string& value) {
    vector<string> out; string current;
    for (char c : value) {
        if (c == ',') { if (!current.empty()) out.push_back(current); current.clear(); }
        else current.push_back(c);
    }
    if (!current.empty()) out.push_back(current);
    return out;
}

int runCommand(const unordered_map<string,string>& args) {
    const string events = need(args, "events");
    const fs::path outdir = need(args, "outdir");
    fs::create_directories(outdir);
    const unsigned int seed = optionalNumber<unsigned int>(args, "seed", 0U);
    const unsigned int tweak = optionalNumber<unsigned int>(args, "tweak", 0U);
    const unsigned int combine = optionalNumber<unsigned int>(args, "combine", 0U);
    const double fpRate = optionalNumber<double>(args, "fp-rate", 0.001);
    const vector<string> types = splitTypes(args.count("types") ? args.at("types") : "flashloan,swap,borrow");

    json build;
    json selections = json::object();
    vector<RootCommitment> referenceCommitments;

    // Server phase. The chain and its event payloads are destroyed before the
    // proof files are loaded by the client phase below.
    {
        DefiLoadMetrics loadMetrics;
        cerr << "[adapt] loading CSV...\n";
        const auto loadStart = high_resolution_clock::now();
        TxnPool pool = loadDefiEventPool(events, seed, &loadMetrics);
        const double loadUs = duration<double, micro>(high_resolution_clock::now() - loadStart).count();
        cerr << "[adapt] loaded " << pool.pool.size() << " events in " << loadMetrics.blockCount << " blocks\n";

        ParamBF param(0, 0, fpRate, tweak);
        Chain chain(param, seed, {MAX}, combine, false, MBFT);
        cerr << "[adapt] building MBFT chain...\n";
        const auto buildStart = high_resolution_clock::now();
        chain.buildChain(pool);
        const double buildUs = duration<double, micro>(high_resolution_clock::now() - buildStart).count();
        cerr << "[adapt] built chain with " << chain.getSize() << " blocks\n";
        // Chain transaction vectors share immutable event material. The loader
        // pool is no longer needed after construction.
        vector<Transaction>().swap(pool.pool);
        vector<int>().swap(pool.blkIndex);

        cerr << "[adapt] computing ADS sizes...\n";
        const double totalAds = chain.getTotalSizeADS();
        const double avgAds = chain.getsizeADS();
        build = {
            {"format", "MBFT-Adapt-MergeBloom-v2"},
            {"event_count", loadMetrics.eventCount}, {"block_count", loadMetrics.blockCount},
            {"fixed_keywords_per_event", 16}, {"keyword_count", loadMetrics.keywordCount},
            {"canonical_payload_bytes", loadMetrics.payloadBytes},
            {"csv_load_time_us", loadUs}, {"mbft_build_time_us", buildUs},
            {"total_ads_bytes", totalAds}, {"average_block_ads_bytes", avgAds},
            {"combine_cycle", combine}, {"fp_rate", fpRate}, {"seed", seed}, {"tweak", tweak},
            {"hash", "original MurmurHash3_x86_32"}, {"vo", "original MergeBloom genHint/reconstruct"}
        };
        ofstream(outdir / "build_metrics.json") << build.dump(2) << '\n';

        cerr << "[adapt] generating MergeBloom Hint proofs...\n";
        for (const string& type : types) {
            cerr << "[adapt] selecting " << type << "...\n";
            AtomicSelectQuery query;
            query.lowerBound = 0;
            query.upperBound = numeric_limits<uint32_t>::max();
            query.keywordsCNF = {{"type=" + type}};

            SelectionMetrics serverMetrics;
            vector<RootCommitment> commitments;
            AtomicSelectionProof proof = selectAllChain(chain, query, &commitments, &serverMetrics);
            const fs::path proofPath = outdir / (type + ".mbfhint.proof");
            saveAtomicSelectionProof(proof, proofPath.string());
            serverMetrics.proofBytes = fs::file_size(proofPath);
            writeSelectionMetricsJson(serverMetrics, (outdir / (type + "_server_metrics.json")).string(), type);

            if (referenceCommitments.empty()) {
                referenceCommitments = commitments;
                saveCommitments(referenceCommitments, (outdir / "commitments.json").string());
            } else if (commitments.size() != referenceCommitments.size()) {
                throw runtime_error("commitment schedule changed between atomic selections");
            }
            selections[type] = {
                {"proof", proofPath.string()}, {"proof_bytes", fs::file_size(proofPath)},
                {"query_time_us", serverMetrics.queryMicros}, {"hint_bytes", serverMetrics.hintBytes},
                {"pruned_bloom_bytes", serverMetrics.prunedBloomBytes},
                {"opened_payload_bytes", serverMetrics.payloadBytes},
                {"server_opened_leaves", serverMetrics.openedLeaves}
            };
        }
        cerr << "[adapt] server phase complete; releasing MBFT chain before client verification...\n";
    }

    // Client phase: only commitments and serialized VO files are available.
    for (const string& type : types) {
        cerr << "[adapt] independently verifying " << type << "...\n";
        const fs::path proofPath = outdir / (type + ".mbfhint.proof");
        AtomicSelectionProof loadedProof = loadAtomicSelectionProof(proofPath.string());
        vector<RootCommitment> loadedCommitments = loadCommitments((outdir / "commitments.json").string());
        VerifiedSelection verified = verifyAtomicSelection(loadedProof, loadedCommitments);
        // Release the proof before retaining the output candidate payloads.
        loadedProof.roots.clear(); loadedProof.roots.shrink_to_fit();
        verified.metrics.proofBytes = fs::file_size(proofPath);
        const fs::path verifiedPath = outdir / (type + ".verified.jsonl");
        writePayloadsJsonl(verified.payloads, verifiedPath.string());
        writeSelectionMetricsJson(verified.metrics, (outdir / (type + "_client_metrics.json")).string(), type);

        selections[type]["verified_events"] = verifiedPath.string();
        selections[type]["unique_events"] = verified.payloads.size();
        selections[type]["verify_time_us"] = verified.metrics.verifyMicros;
        selections[type]["false_positive_leaves"] = verified.metrics.falsePositiveLeaves;
        selections[type]["raw_matched_leaves"] = verified.metrics.rawMatchedLeaves;
        cerr << type << ": verified " << verified.payloads.size() << " unique events, proof "
             << fs::file_size(proofPath) << " bytes\n";
    }

    json summary = {{"build", build}, {"selections", selections}};
    ofstream(outdir / "atomic_selection_summary.json") << summary.dump(2) << '\n';
    cout << summary.dump(2) << endl;
    return 0;
}

int verifyCommand(const unordered_map<string,string>& args) {
    const string proofPath = need(args, "proof");
    const string commitmentPath = need(args, "commitments");
    const string outPath = need(args, "out");
    const string metricsPath = need(args, "metrics");
    const string eventType = need(args, "event-type");
    AtomicSelectionProof proof = loadAtomicSelectionProof(proofPath);
    vector<RootCommitment> commitments = loadCommitments(commitmentPath);
    VerifiedSelection verified = verifyAtomicSelection(proof, commitments);
    verified.metrics.proofBytes = fs::file_size(proofPath);
    writePayloadsJsonl(verified.payloads, outPath);
    writeSelectionMetricsJson(verified.metrics, metricsPath, eventType);
    cout << json{{"valid", verified.valid}, {"unique_events", verified.payloads.size()},
                 {"proof_bytes", verified.metrics.proofBytes}, {"verify_time_us", verified.metrics.verifyMicros}}.dump(2) << endl;
    return 0;
}

int queryOnlySelectCommand(const unordered_map<string,string>& args) {
    const string events = need(args, "events");
    const fs::path outdir = need(args, "outdir");
    fs::create_directories(outdir);
    const unsigned int seed = optionalNumber<unsigned int>(args, "seed", 0U);
    const unsigned int tweak = optionalNumber<unsigned int>(args, "tweak", 0U);
    const unsigned int combine = optionalNumber<unsigned int>(args, "combine", 0U);
    const double fpRate = optionalNumber<double>(args, "fp-rate", 0.001);
    const vector<string> types = splitTypes(args.count("types") ? args.at("types") : "flashloan,swap,borrow");

    DefiLoadMetrics loadMetrics;
    const auto loadStart = high_resolution_clock::now();
    TxnPool pool = loadDefiEventPool(events, seed, &loadMetrics);
    const double loadUs = duration<double, micro>(high_resolution_clock::now() - loadStart).count();

    ParamBF param(0, 0, fpRate, tweak);
    Chain chain(param, seed, {MAX}, combine, false, MBFT);
    const auto buildStart = high_resolution_clock::now();
    chain.buildChain(pool);
    const double buildUs = duration<double, micro>(high_resolution_clock::now() - buildStart).count();
    vector<Transaction>().swap(pool.pool);
    vector<int>().swap(pool.blkIndex);

    json selections = json::object();
    for (const string& type : types) {
        AtomicSelectQuery query;
        query.lowerBound = 0;
        query.upperBound = numeric_limits<uint32_t>::max();
        query.keywordsCNF = {{"type=" + type}};

        QueryOnlySelection selected = selectAllChainNoProof(chain, query);
        const fs::path outputPath = outdir / (type + ".query_only.jsonl");
        const auto writeStart = high_resolution_clock::now();
        writePayloadsJsonl(selected.payloads, outputPath.string());
        const double writeUs = duration<double, micro>(high_resolution_clock::now() - writeStart).count();
        const uint64_t outputBytes = fs::file_size(outputPath);

        json metrics = {
            {"mode", "query-only-no-vo"},
            {"event_type", type},
            {"roots", selected.metrics.roots},
            {"pruned_nodes", selected.metrics.prunedNodes},
            {"opened_leaves", selected.metrics.openedLeaves},
            {"false_positive_leaves", selected.metrics.falsePositiveLeaves},
            {"raw_matched_leaves", selected.metrics.rawMatchedLeaves},
            {"unique_events", selected.payloads.size()},
            {"candidate_payload_bytes", selected.metrics.payloadBytes},
            {"mbft_tree_query_time_us", selected.metrics.queryMicros},
            {"candidate_write_time_us", writeUs},
            {"candidate_file_bytes", outputBytes},
            {"candidate_file", outputPath.string()}
        };
        ofstream(outdir / (type + "_query_only_metrics.json")) << metrics.dump(2) << '\n';
        selections[type] = metrics;
        cerr << "[query-only] " << type << ": " << selected.payloads.size()
             << " events, tree query " << selected.metrics.queryMicros / 1000.0 << " ms\n";
    }

    json summary = {
        {"format", "MBFT-Adapt-MergeBloom-query-only-v3"},
        {"mode", "query-only-no-vo"},
        {"event_count", loadMetrics.eventCount},
        {"block_count", loadMetrics.blockCount},
        {"csv_load_time_us", loadUs},
        {"mbft_build_time_us", buildUs},
        {"combine_cycle", combine},
        {"fp_rate", fpRate},
        {"seed", seed},
        {"tweak", tweak},
        {"hash", "original MurmurHash3_x86_32"},
        {"index", "original MBFT MergeBloom trees"},
        {"proof_generation", false},
        {"verification", false},
        {"selections", selections}
    };
    ofstream(outdir / "query_only_atomic_summary.json") << summary.dump(2) << '\n';
    cout << summary.dump(2) << endl;
    return 0;
}

} // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) { printUsage(); return 2; }
        const string command = argv[1];
        const auto args = parseArgs(argc, argv, 2);
        if (command == "run") return runCommand(args);
        if (command == "query-only-select") return queryOnlySelectCommand(args);
        if (command == "verify") return verifyCommand(args);
        printUsage();
        return 2;
    } catch (const exception& e) {
        cerr << "error: " << e.what() << endl;
        return 1;
    }
}
