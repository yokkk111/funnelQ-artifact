#ifndef MBFT_PERSIST_H
#define MBFT_PERSIST_H

#include "blockchain.h"

#include <memory>
#include <string>

struct PersistentStoreMetrics {
    uint64_t blocks = 0;
    uint64_t transactions = 0;
    uint64_t treeNodes = 0;
    uint64_t bloomBytes = 0;
    uint64_t hintBytes = 0;
    uint64_t payloadBytes = 0;
    uint64_t fileBytes = 0;
};

void saveMBFTChain(const Chain& chain, const std::string& path,
                   PersistentStoreMetrics* metrics = nullptr);

std::unique_ptr<Chain> loadMBFTChain(const std::string& path,
                                     PersistentStoreMetrics* metrics = nullptr);

#endif
