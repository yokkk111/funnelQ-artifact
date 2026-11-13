#include "persist.h"

#include "bf.h"
#include "merkle.h"
#include "transaction.h"

#include <array>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <stdexcept>
#include <type_traits>

namespace {

constexpr std::array<char, 8> MAGIC{{'M','B','F','T','I','D','X','2'}};
constexpr uint32_t VERSION = 1;

template <typename T>
void writePod(std::ostream& out, const T& value) {
    static_assert(std::is_trivially_copyable_v<T>);
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    if (!out) throw std::runtime_error("failed to write MBFT persistent store");
}

template <typename T>
T readPod(std::istream& in) {
    static_assert(std::is_trivially_copyable_v<T>);
    T value{};
    in.read(reinterpret_cast<char*>(&value), sizeof(T));
    if (!in) throw std::runtime_error("truncated MBFT persistent store");
    return value;
}

void writeString(std::ostream& out, const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("MBFT persistent string too large");
    writePod(out, static_cast<uint32_t>(value.size()));
    out.write(value.data(), static_cast<std::streamsize>(value.size()));
    if (!out) throw std::runtime_error("failed to write MBFT persistent string");
}

std::string readString(std::istream& in) {
    const uint32_t n = readPod<uint32_t>(in);
    std::string value(n, '\0');
    in.read(value.data(), static_cast<std::streamsize>(n));
    if (!in) throw std::runtime_error("truncated MBFT persistent string");
    return value;
}

void writeBytes(std::ostream& out, const std::vector<unsigned char>& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("MBFT persistent byte vector too large");
    writePod(out, static_cast<uint32_t>(value.size()));
    if (!value.empty())
        out.write(reinterpret_cast<const char*>(value.data()),
                  static_cast<std::streamsize>(value.size()));
    if (!out) throw std::runtime_error("failed to write MBFT persistent bytes");
}

std::vector<unsigned char> readBytes(std::istream& in) {
    const uint32_t n = readPod<uint32_t>(in);
    std::vector<unsigned char> value(n);
    if (n)
        in.read(reinterpret_cast<char*>(value.data()), static_cast<std::streamsize>(n));
    if (!in) throw std::runtime_error("truncated MBFT persistent bytes");
    return value;
}

void writeParam(std::ostream& out, const ParamBF& p) {
    writePod(out, p.nCurrentItems);
    writePod(out, p.nItems);
    writePod(out, p.nFPRate);
    writePod(out, p.nTweak);
}

ParamBF readParam(std::istream& in) {
    ParamBF p;
    p.nCurrentItems = readPod<unsigned int>(in);
    p.nItems = readPod<unsigned int>(in);
    p.nFPRate = readPod<double>(in);
    p.nTweak = readPod<unsigned int>(in);
    return p;
}

void writeTransaction(std::ostream& out, const Transaction& txn,
                      PersistentStoreMetrics* metrics) {
    writePod(out, txn.getTxid());
    writePod(out, txn.getTimestamp());
    writePod(out, txn.getAmount());

    const auto& addresses = txn.getAddressesRef();
    if (addresses.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("too many transaction keywords");
    writePod(out, static_cast<uint32_t>(addresses.size()));
    for (const auto& v : addresses) writeString(out, v);

    const auto sketches = txn.getSketches();
    if (sketches.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("too many transaction sketches");
    writePod(out, static_cast<uint32_t>(sketches.size()));
    for (double v : sketches) writePod(out, v);

    writeString(out, txn.getPayload());
    if (metrics) {
        metrics->transactions++;
        metrics->payloadBytes += txn.getPayload().size();
    }
}

Transaction readTransaction(std::istream& in, PersistentStoreMetrics* metrics) {
    const uint32_t txid = readPod<uint32_t>(in);
    const uint32_t timestamp = readPod<uint32_t>(in);
    const uint32_t amount = readPod<uint32_t>(in);

    const uint32_t na = readPod<uint32_t>(in);
    std::vector<std::string> addresses;
    addresses.reserve(na);
    for (uint32_t i = 0; i < na; ++i) addresses.emplace_back(readString(in));

    const uint32_t ns = readPod<uint32_t>(in);
    std::vector<double> sketches;
    sketches.reserve(ns);
    for (uint32_t i = 0; i < ns; ++i) sketches.emplace_back(readPod<double>(in));

    std::string payload = readString(in);
    if (metrics) {
        metrics->transactions++;
        metrics->payloadBytes += payload.size();
    }
    return Transaction(txid, timestamp, amount, std::move(addresses),
                       std::move(sketches), std::move(payload));
}

void writeTransactions(std::ostream& out, const std::vector<Transaction>& txns,
                       PersistentStoreMetrics* metrics) {
    if (txns.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("too many transactions in MBFT block");
    writePod(out, static_cast<uint32_t>(txns.size()));
    for (const auto& txn : txns) writeTransaction(out, txn, metrics);
}

std::vector<Transaction> readTransactions(std::istream& in,
                                          PersistentStoreMetrics* metrics) {
    const uint32_t n = readPod<uint32_t>(in);
    std::vector<Transaction> txns;
    txns.reserve(n);
    for (uint32_t i = 0; i < n; ++i) txns.emplace_back(readTransaction(in, metrics));
    return txns;
}

void writeBloom(std::ostream& out, BloomFilter* bf,
                PersistentStoreMetrics* metrics) {
    const auto* mbf = dynamic_cast<MergeBloomFilter*>(bf);
    writePod<uint8_t>(out, mbf ? 1U : 0U);
    ParamBF p(bf->nCurrentItems, bf->nItems, bf->nFPRate, bf->nTweak);
    writeParam(out, p);
    writeBytes(out, bf->vData);
    if (mbf) writeBytes(out, mbf->hint);
    if (metrics) {
        metrics->bloomBytes += bf->vData.size();
        if (mbf) metrics->hintBytes += mbf->hint.size();
    }
}

BloomFilter* readBloom(std::istream& in, PersistentStoreMetrics* metrics) {
    const bool isMerge = readPod<uint8_t>(in) != 0;
    const ParamBF p = readParam(in);
    auto data = readBytes(in);
    BloomFilter* bf = nullptr;
    if (isMerge) {
        auto* mbf = new MergeBloomFilter(p, data);
        mbf->hint = readBytes(in);
        if (metrics) metrics->hintBytes += mbf->hint.size();
        bf = mbf;
    } else {
        bf = new BloomFilter(p, data);
    }
    if (metrics) metrics->bloomBytes += data.size();
    return bf;
}

void writeNode(std::ostream& out, MerkleBFNode* node,
               PersistentStoreMetrics* metrics) {
    writePod<uint8_t>(out, node ? 1U : 0U);
    if (!node) return;
    if (metrics) metrics->treeNodes++;

    writePod(out, node->getHash());
    writePod(out, node->getLowerBound());
    writePod(out, node->getUpperBound());
    writePod(out, node->getValue());
    writePod(out, node->getLeafTxnIndex());
    writeBloom(out, node->getBF(), metrics);

    writeNode(out, static_cast<MerkleBFNode*>(node->getLeftChild()), metrics);
    writeNode(out, static_cast<MerkleBFNode*>(node->getRightChild()), metrics);
}

MerkleBFNode* readNode(std::istream& in, unsigned int seed, bool earlyStop,
                       ADSType adsType, PersistentStoreMetrics* metrics) {
    if (readPod<uint8_t>(in) == 0) return nullptr;
    if (metrics) metrics->treeNodes++;

    const uint32_t hash = readPod<uint32_t>(in);
    const uint32_t lower = readPod<uint32_t>(in);
    const uint32_t upper = readPod<uint32_t>(in);
    const double value = readPod<double>(in);
    const int leafTxnIndex = readPod<int>(in);
    BloomFilter* bf = readBloom(in, metrics);
    MerkleBFNode* left = nullptr;
    MerkleBFNode* right = nullptr;
    try {
        left = readNode(in, seed, earlyStop, adsType, metrics);
        right = readNode(in, seed, earlyStop, adsType, metrics);
        return new MerkleBFNode(hash, bf, seed, lower, upper, value,
                                earlyStop, adsType, leafTxnIndex, left, right);
    } catch (...) {
        delete left;
        delete right;
        delete bf;
        throw;
    }
}

void writeBlock(std::ostream& out, const Block& block,
                PersistentStoreMetrics* metrics) {
    writePod(out, block.timestamp);
    writeParam(out, block.paramBF);
    writeParam(out, block.paramBFCombine);
    writePod(out, block.nSeed);
    writePod<uint8_t>(out, block.earlyStop ? 1U : 0U);
    writePod(out, static_cast<uint32_t>(block.adsType));
    writeTransactions(out, block.vTransactions, metrics);
    writeTransactions(out, block.vTransactionsCombine, metrics);

    if (block.vRoots.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("too many MBFT roots");
    writePod(out, static_cast<uint32_t>(block.vRoots.size()));
    for (auto* root : block.vRoots) writeNode(out, root, metrics);
    if (metrics) metrics->blocks++;
}

Block* readBlock(std::istream& in, PersistentStoreMetrics* metrics) {
    auto* block = new Block();
    try {
        block->timestamp = readPod<uint32_t>(in);
        block->paramBF = readParam(in);
        block->paramBFCombine = readParam(in);
        block->nSeed = readPod<unsigned int>(in);
        block->earlyStop = readPod<uint8_t>(in) != 0;
        block->adsType = static_cast<ADSType>(readPod<uint32_t>(in));
        block->vTransactions = readTransactions(in, metrics);
        block->vTransactionsCombine = readTransactions(in, metrics);
        block->rootMHT = nullptr;

        const uint32_t nr = readPod<uint32_t>(in);
        block->vRoots.assign(nr, nullptr);
        for (uint32_t i = 0; i < nr; ++i)
            block->vRoots[i] = readNode(in, block->nSeed, block->earlyStop,
                                        block->adsType, metrics);
        if (metrics) metrics->blocks++;
        return block;
    } catch (...) {
        delete block;
        throw;
    }
}

}             

void saveMBFTChain(const Chain& chain, const std::string& path,
                   PersistentStoreMetrics* metrics) {
    PersistentStoreMetrics local;
    PersistentStoreMetrics* m = metrics ? metrics : &local;
    *m = PersistentStoreMetrics{};

    const std::filesystem::path output(path);
    if (!output.parent_path().empty())
        std::filesystem::create_directories(output.parent_path());
    const std::filesystem::path tmp = output.string() + ".tmp";

    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) throw std::runtime_error("cannot create MBFT persistent store: " + tmp.string());
    out.write(MAGIC.data(), static_cast<std::streamsize>(MAGIC.size()));
    writePod(out, VERSION);
    writeParam(out, chain.paramBF);
    writePod(out, chain.nSeed);
    writePod(out, chain.combineCycle);
    writePod<uint8_t>(out, chain.earlyStop ? 1U : 0U);
    writePod(out, static_cast<uint32_t>(chain.adsType));

    writePod(out, static_cast<uint32_t>(chain.supportAggs.size()));
    for (AggType agg : chain.supportAggs) writePod(out, static_cast<uint32_t>(agg));

    if (chain.chain.size() > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("too many MBFT blocks");
    writePod(out, static_cast<uint32_t>(chain.chain.size()));
    for (const Block* block : chain.chain) writeBlock(out, *block, m);
    out.flush();
    if (!out) throw std::runtime_error("failed to flush MBFT persistent store");
    out.close();

    std::filesystem::rename(tmp, output);
    m->fileBytes = std::filesystem::file_size(output);
}

std::unique_ptr<Chain> loadMBFTChain(const std::string& path,
                                     PersistentStoreMetrics* metrics) {
    PersistentStoreMetrics local;
    PersistentStoreMetrics* m = metrics ? metrics : &local;
    *m = PersistentStoreMetrics{};

    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open MBFT persistent store: " + path);
    std::array<char, 8> magic{};
    in.read(magic.data(), static_cast<std::streamsize>(magic.size()));
    if (!in || magic != MAGIC) throw std::runtime_error("invalid MBFT persistent store magic");
    const uint32_t version = readPod<uint32_t>(in);
    if (version != VERSION) throw std::runtime_error("unsupported MBFT persistent store version");

    auto chain = std::make_unique<Chain>();
    chain->paramBF = readParam(in);
    chain->nSeed = readPod<unsigned int>(in);
    chain->combineCycle = readPod<unsigned int>(in);
    chain->earlyStop = readPod<uint8_t>(in) != 0;
    chain->adsType = static_cast<ADSType>(readPod<uint32_t>(in));

    const uint32_t na = readPod<uint32_t>(in);
    for (uint32_t i = 0; i < na; ++i)
        chain->supportAggs.insert(static_cast<AggType>(readPod<uint32_t>(in)));

    const uint32_t nb = readPod<uint32_t>(in);
    chain->chain.reserve(nb);
    for (uint32_t i = 0; i < nb; ++i) chain->chain.emplace_back(readBlock(in, m));

    const int trailing = in.peek();
    if (trailing != std::char_traits<char>::eof())
        throw std::runtime_error("unexpected trailing bytes in MBFT persistent store");
    m->fileBytes = std::filesystem::file_size(path);
    return chain;
}
