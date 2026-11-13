#ifndef MBFT_NFT_LOADER_H
#define MBFT_NFT_LOADER_H

#include "adapt.h"

struct NftLoadMetrics {
    uint64_t eventCount = 0;
    uint64_t blockCount = 0;
    uint64_t keywordCount = 0;
    uint64_t payloadBytes = 0;
    uint64_t inScopeRootCount = 0;
    uint64_t zeroMoveFromCount = 0;
    uint64_t zeroMoveToCount = 0;
};

TxnPool loadNftEventPool(const string& csvPath, unsigned int seed,
                         NftLoadMetrics* metrics = nullptr,
                         uint64_t orderOffset = 0);

#endif
