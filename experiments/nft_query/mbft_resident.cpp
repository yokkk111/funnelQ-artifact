                                                                            
#include "adapt.h"
#include "persist.h"
#include "nlohmann/json.hpp"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <set>
using nlohmann::json;
int main(int argc,char**argv) {
    std::ostream wire(std::cout.rdbuf()); std::cout.rdbuf(std::cerr.rdbuf());
    try {
        if(argc!=2) throw std::runtime_error("manifest required");
        std::ifstream f(argv[1]); json manifest; f>>manifest;
        std::vector<std::unique_ptr<Chain>> chains;
        uint64_t events=0;
        for(const auto& p:manifest.at("shards")) {
            PersistentStoreMetrics metrics;
            chains.push_back(loadMBFTChain(p.get<std::string>(),&metrics));
            events+=metrics.transactions;
            std::cerr<<"loaded shards "<<chains.size()<<std::endl;
        }
        if(events!=manifest.at("expected_source_rows").get<uint64_t>()) throw std::runtime_error("source count");
        wire<<json{{"frame","ready"},{"load_count",1},{"source_events",events},{"shards",chains.size()}}.dump()<<std::endl;
        std::string line;
        while(std::getline(std::cin,line)) {
            auto req=json::parse(line);
            if(req.value("op","")=="close") break;
            const auto path=req.at("candidate_path").get<std::string>();
            if(std::filesystem::exists(path)) throw std::runtime_error("refusing existing candidate file");
            std::ofstream dest(path); uint64_t count=0;
            std::set<std::string> seen;
            for(const auto& typ:req.at("types")) {
                auto type=typ.get<std::string>();
                if((type!="nfttrade" && type!="nfttransfer")||!seen.insert(type).second) throw std::runtime_error("atomic type");
                for(const auto& chain:chains) {
                    AtomicSelectQuery query; query.lowerBound=0; query.upperBound=UINT32_MAX;
                    query.keywordsCNF={{"type="+type}};
                    auto selected=selectAllChainNoProof(*chain,query);
                    for(const auto& payload:selected.payloads) dest<<payload<<'\n';
                    count+=selected.payloads.size();
                }
            }
            dest.close(); if(!dest) throw std::runtime_error("candidate write failed");
            wire<<json{{"frame","result"},{"events",count},{"candidate_cache_used",false}}.dump()<<std::endl;
        }
        return 0;
    } catch(const std::exception& e) {
        wire<<json{{"frame","error"},{"error",e.what()}}.dump()<<std::endl; return 1;
    }
}
