                                                                              
                                                                              
#include "adapt.h"
#include "nlohmann/json.hpp"
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <unordered_map>

using nlohmann::json;
using Clock = std::chrono::steady_clock;
double elapsed_seconds(Clock::time_point start) {
    return std::chrono::duration<double>(Clock::now() - start).count();
}

int main(int argc, char** argv) {
                                                                         
                                                                              
    std::ostream wire(std::cout.rdbuf());
    std::cout.rdbuf(std::cerr.rdbuf());
    try {
        if (argc != 2) throw std::invalid_argument("usage: mbft_resident SHARD_MANIFEST.json");
        std::ifstream input(argv[1]);
        if (!input) throw std::runtime_error("cannot open shard manifest");
        json manifest; input >> manifest;
        if (!manifest.is_object() || manifest.size() != 2 || !manifest.contains("shards") ||
            !manifest.contains("expected_source_rows") || !manifest.at("shards").is_array() || manifest.at("shards").empty())
            throw std::invalid_argument("manifest must contain only shards and expected_source_rows");
        const auto init = Clock::now();
        std::vector<std::unique_ptr<Chain>> chains;
        std::unordered_map<std::string, std::string> identities;
        std::set<std::string> shardPaths;
        uint64_t sourceRows = 0;
        for (const auto& pathJson : manifest.at("shards")) {
            const auto path = pathJson.get<std::string>();
            if (!shardPaths.insert(path).second) throw std::invalid_argument("duplicate shard path");
            DefiLoadMetrics metrics;
            TxnPool pool = loadDefiEventPool(path, 0U, &metrics);
                                                                                
                                                                            
            for (const auto& txn : pool.pool) {
                const auto& payload = txn.getPayload();
                const auto id = json::parse(payload).at("event_id").get<std::string>();
                if (id.empty()) throw std::runtime_error("empty source event_id");
                auto inserted = identities.emplace(id, payload);
                if (!inserted.second && inserted.first->second != payload)
                    throw std::runtime_error("conflicting source event_id: " + id);
            }
            ParamBF param(0, 0, 0.001, 0U);
            auto chain = std::make_unique<Chain>(param, 0U, std::set<AggType>{MAX}, 0U, false, MBFT);
            chain->buildChain(pool);
            chains.push_back(std::move(chain));
            sourceRows += metrics.eventCount;
            std::cerr << "[resident-load] shards=" << chains.size() << " events=" << sourceRows << '\n';
        }
        if (sourceRows != manifest.at("expected_source_rows").get<uint64_t>())
            throw std::runtime_error("source row-count mismatch");
        const auto uniqueRows = identities.size();
        identities.clear(); identities.rehash(0);
        wire << json{{"frame", "ready"}, {"protocol", "mbft_resident_jsonl_v1"},
            {"source_rows", sourceRows}, {"unique_source_events", uniqueRows},
            {"shards", chains.size()}, {"load_count", 1},
            {"startup_seconds", elapsed_seconds(init)}, {"startup_includes_tree_build", true}}.dump() << std::endl;
        std::string line;
        uint64_t previous = 0;
        while (std::getline(std::cin, line)) {
            auto request = json::parse(line);
            if (request == json{{"op", "close"}}) break;
            if (request.size() != 2 || !request.contains("request_id") || !request.contains("types"))
                throw std::invalid_argument("request fields");
            const auto id = request.at("request_id").get<uint64_t>();
            if (id != previous + 1) throw std::invalid_argument("request sequence");
            previous = id;
            const auto types = request.at("types").get<std::vector<std::string>>();
            std::set<std::string> distinct;
            if (types.empty()) throw std::invalid_argument("empty event types");
            for (const auto& type : types) {
                if ((type != "flashloan" && type != "borrow" && type != "swap") || !distinct.insert(type).second)
                    throw std::invalid_argument("unsupported/duplicate event type");
            }
            const auto started = Clock::now();
            double retrieval = 0, transfer = 0;
            uint64_t count = 0, selections = 0;
            for (size_t shard = 0; shard < chains.size(); ++shard) {
                for (const auto& type : types) {
                    AtomicSelectQuery query;
                    query.lowerBound = 0;
                    query.upperBound = UINT32_MAX;
                    query.keywordsCNF = {{"type=" + type}};
                                                                                   
                    auto selected = selectAllChainNoProof(*chains[shard], query);
                    retrieval += selected.metrics.queryMicros / 1e6;
                    const auto sending = Clock::now();
                    wire << json{{"frame", "selection"}, {"request_id", id}, {"shard", shard},
                        {"type", type}, {"rows", selected.payloads.size()}}.dump() << '\n';
                    for (const auto& payload : selected.payloads) wire << payload << '\n';
                    wire.flush();
                    if (!wire) throw std::runtime_error("candidate pipe broken");
                    transfer += elapsed_seconds(sending);
                    count += selected.payloads.size();
                    ++selections;
                }
            }
            wire << json{{"frame", "end"}, {"request_id", id}, {"rows", count},
                {"selections", selections}, {"native_retrieval_seconds", retrieval},
                {"native_transfer_seconds", transfer}, {"native_request_seconds", elapsed_seconds(started)},
                {"retrieval_executed", true}, {"candidate_cache_used", false}}.dump() << std::endl;
        }
        return 0;
    } catch (const std::exception& e) {
        wire << json{{"frame", "error"}, {"error", e.what()}}.dump() << std::endl;
        std::cerr << e.what() << std::endl;
        return 1;
    }
}
