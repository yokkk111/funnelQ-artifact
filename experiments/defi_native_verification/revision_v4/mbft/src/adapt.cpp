#include "adapt.h"

#include "hash.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <chrono>
#include <cctype>
#include <ctime>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

using nlohmann::json;

namespace {

string lowerAscii(string value) {
    transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(tolower(c));
    });
    return value;
}

vector<string> parseCsvLine(const string& line) {
    vector<string> fields;
    string current;
    bool quoted = false;
    for (size_t i = 0; i < line.size(); ++i) {
        const char c = line[i];
        if (quoted) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    current.push_back('"');
                    ++i;
                } else {
                    quoted = false;
                }
            } else {
                current.push_back(c);
            }
        } else if (c == ',') {
            fields.emplace_back(move(current));
            current.clear();
        } else if (c == '"') {
            quoted = true;
        } else if (c != '\r') {
            current.push_back(c);
        }
    }
    if (quoted) throw runtime_error("unterminated quoted CSV field");
    fields.emplace_back(move(current));
    return fields;
}

uint64_t parseUtcTimestamp(const string& value) {
    if (value.empty()) return 0;
    tm t{};
    istringstream ss(value.substr(0, 19));
    ss >> get_time(&t, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) throw runtime_error("failed to parse timestamp: " + value);
#if defined(_WIN32)
    const time_t out = _mkgmtime(&t);
#else
    const time_t out = timegm(&t);
#endif
    if (out < 0) throw runtime_error("invalid timestamp: " + value);
    return static_cast<uint64_t>(out);
}

json parseObjectOrEmpty(const string& text) {
    if (text.empty()) return json::object();
    json obj = json::parse(text);
    return obj.is_object() ? obj : json::object();
}

string jsonString(const json& obj, const string& key) {
    if (!obj.contains(key) || obj.at(key).is_null()) return "";
    if (obj.at(key).is_string()) return obj.at(key).get<string>();
    return obj.at(key).dump();
}

string roleValue(const json& roles, const string& field) {
    if (!roles.contains(field) || roles.at(field).is_null()) return "";
    if (roles.at(field).is_string()) return lowerAscii(roles.at(field).get<string>());
    return lowerAscii(roles.at(field).dump());
}

string kw(const string& prefix, const string& value) {
    return prefix + (value.empty() ? "<nil>" : lowerAscii(value));
}

vector<string> fixedKeywords(const json& obj) {
    const json roles = obj.value("roles", json::object());
    vector<string> out {
        kw("type=", obj.value("event_type", "")),
        kw("protocol=", obj.value("protocol", "")),
        kw("contract=", obj.value("contract", "")),
        kw("tx=", obj.value("tx_hash", "")),
        kw("closure=", obj.value("closure_id", "")),
        kw("role.recipient=", roleValue(roles, "recipient")),
        kw("role.asset=", roleValue(roles, "asset")),
        kw("role.pool=", roleValue(roles, "pool")),
        kw("role.taker=", roleValue(roles, "taker")),
        kw("role.maker=", roleValue(roles, "maker")),
        kw("role.token_sold=", roleValue(roles, "token_sold")),
        kw("role.token_bought=", roleValue(roles, "token_bought")),
        kw("role.borrower=", roleValue(roles, "borrower")),
        kw("version=", obj.value("version", "")),
        kw("raw_source=", obj.value("raw_source", "")),
        kw("src_rule=", obj.value("src_rule_id", ""))
    };
                                                                           
                                                                               
                                                                             
    if (out.size() != 16) throw logic_error("fixed keyword schema must contain 16 entries");
    return out;
}


bool exactMatchWords(const vector<string>& words, const vector<vector<string>>& cnf) {
    for (const auto& clause : cnf) {
        if (clause.empty()) continue;
        bool any = false;
        for (const auto& wanted : clause) {
            if (find(words.begin(), words.end(), wanted) != words.end()) { any = true; break; }
        }
        if (!any) return false;
    }
    return true;
}

bool queryOverlap(const AtomicSelectQuery& query, uint32_t lower, uint32_t upper) {
    return lower <= query.upperBound && query.lowerBound <= upper;
}

bool nodeMayMatch(MerkleBFNode* node, const AtomicSelectQuery& query) {
    return queryOverlap(query, node->getLowerBound(), node->getUpperBound())
        && node->getBF()->batchContain(query.keywordsCNF);
}

uint32_t hashInternal(uint32_t leftHash, uint32_t rightHash, uint32_t lower,
                      uint32_t upper, double value, BloomFilter* bf, unsigned int seed) {
    uint32_t hash = 0;
    stringstream ss;
    ss << leftHash << rightHash << bf->getStrBF() << lower << upper << value << endl;
    const string str = ss.str();
    vector<unsigned char> bytes(str.begin(), str.end());
    MurmurHash3_x86_32(bytes.data(), bytes.size(), seed, &hash);
    return hash;
}

template <typename T>
void writePod(ostream& out, const T& value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(T));
    if (!out) throw runtime_error("failed to write MBFT-Adapt proof");
}

template <typename T>
T readPod(istream& in) {
    T value{};
    in.read(reinterpret_cast<char*>(&value), sizeof(T));
    if (!in) throw runtime_error("failed to read MBFT-Adapt proof");
    return value;
}

void writeBytes(ostream& out, const vector<unsigned char>& data) {
    if (data.size() > numeric_limits<uint32_t>::max()) throw runtime_error("proof field too large");
    writePod(out, static_cast<uint32_t>(data.size()));
    out.write(reinterpret_cast<const char*>(data.data()), static_cast<streamsize>(data.size()));
    if (!out) throw runtime_error("failed to write proof bytes");
}

vector<unsigned char> readBytes(istream& in) {
    const uint32_t size = readPod<uint32_t>(in);
    vector<unsigned char> data(size);
    in.read(reinterpret_cast<char*>(data.data()), static_cast<streamsize>(size));
    if (!in) throw runtime_error("failed to read proof bytes");
    return data;
}

void writeString(ostream& out, const string& value) {
    if (value.size() > numeric_limits<uint32_t>::max()) throw runtime_error("proof string too large");
    writePod(out, static_cast<uint32_t>(value.size()));
    out.write(value.data(), static_cast<streamsize>(value.size()));
    if (!out) throw runtime_error("failed to write proof string");
}

string readString(istream& in) {
    const uint32_t size = readPod<uint32_t>(in);
    string value(size, '\0');
    in.read(value.data(), static_cast<streamsize>(size));
    if (!in) throw runtime_error("failed to read proof string");
    return value;
}

string eventIdFromPayload(const string& payload) {
    try { return json::parse(payload).value("event_id", payload); }
    catch (...) { return payload; }
}

struct ReconstructedNode {
    unique_ptr<BloomFilter> bf;
    uint32_t hash = 0;
    uint32_t lower = 0;
    uint32_t upper = 0;
    double value = 0;
};

}             

uint32_t computePayloadHash(const string& payload, unsigned int seed) {
    uint32_t hash = 0;
    vector<unsigned char> data(payload.begin(), payload.end());
    MurmurHash3_x86_32(data.data(), data.size(), seed, &hash);
    return hash;
}

vector<string> keywordsFromPayload(const string& payload) {
    return fixedKeywords(json::parse(payload));
}

Transaction transactionFromPayload(const string& payload, unsigned int seed) {
    const json obj = json::parse(payload);
    const uint32_t block = obj.value("mbft_block", 0U);
    const uint32_t amount = obj.value("mbft_amount", 1U);
    vector<double> sketches(3, 0.0);
    return Transaction(computePayloadHash(payload, seed), block, amount,
                       fixedKeywords(obj), sketches, payload);
}

TxnPool loadDefiEventPool(const string& csvPath, unsigned int seed, DefiLoadMetrics* metrics) {
    ifstream in(csvPath);
    if (!in) throw runtime_error("cannot open canonical event CSV: " + csvPath);
    string headerLine;
    if (!getline(in, headerLine)) throw runtime_error("empty canonical event CSV");
    const vector<string> headers = parseCsvLine(headerLine);
    unordered_map<string, size_t> column;
    for (size_t i = 0; i < headers.size(); ++i) column[headers[i]] = i;

    auto required = [&](const string& name) {
        if (!column.count(name)) throw runtime_error("missing CSV column: " + name);
    };
    for (const string& name : {"event_id","closure_id","tx_hash","block_number","block_time","evt_index",
                               "event_type","protocol","version","contract","roles_json","attrs_json",
                               "raw_source","raw_row_id","src_rule_id","seq_index"}) required(name);

    struct RawEvent { json obj; uint64_t blockNumber; int64_t seq; int64_t evt; string eventId; };
    vector<RawEvent> rows;
    string line;
    while (getline(in, line)) {
        if (line.empty()) continue;
        const vector<string> fields = parseCsvLine(line);
        if (fields.size() != headers.size()) throw runtime_error("malformed CSV row");
        auto get = [&](const string& name) -> string { return fields[column.at(name)]; };
        json obj = json::object();
        obj["event_id"] = get("event_id");
        obj["closure_id"] = get("closure_id");
        obj["tx_hash"] = lowerAscii(get("tx_hash"));
        obj["block_number"] = get("block_number").empty() ? 0ULL : stoull(get("block_number"));
        obj["block_time"] = get("block_time");
        obj["block_time_epoch"] = parseUtcTimestamp(get("block_time"));
        obj["evt_index"] = get("evt_index").empty() ? 0LL : stoll(get("evt_index"));
        obj["event_type"] = lowerAscii(get("event_type"));
        obj["protocol"] = lowerAscii(get("protocol"));
        obj["version"] = get("version");
        obj["contract"] = lowerAscii(get("contract"));
        obj["roles"] = parseObjectOrEmpty(get("roles_json"));
        obj["attrs"] = parseObjectOrEmpty(get("attrs_json"));
        obj["raw_source"] = get("raw_source");
        obj["raw_row_id"] = get("raw_row_id");
        obj["src_rule_id"] = get("src_rule_id");
        obj["seq_index"] = get("seq_index").empty() ? 0LL : stoll(get("seq_index"));
        rows.push_back({move(obj), stoull(get("block_number")),
                        get("seq_index").empty() ? 0LL : stoll(get("seq_index")),
                        get("evt_index").empty() ? 0LL : stoll(get("evt_index")), get("event_id")});
    }

    stable_sort(rows.begin(), rows.end(), [](const RawEvent& a, const RawEvent& b) {
        if (a.blockNumber != b.blockNumber) return a.blockNumber < b.blockNumber;
        const string atx = a.obj.value("tx_hash", ""), btx = b.obj.value("tx_hash", "");
        if (atx != btx) return atx < btx;
        if (a.seq != b.seq) return a.seq < b.seq;
        if (a.evt != b.evt) return a.evt < b.evt;
        return a.eventId < b.eventId;
    });

    TxnPool pool;
    pool.nSeed = seed;
    pool.supportAggs = {MAX};
    uint64_t lastBlock = numeric_limits<uint64_t>::max();
    uint32_t blockOrdinal = 0;
    for (auto& row : rows) {
        if (row.blockNumber != lastBlock) {
            lastBlock = row.blockNumber;
            ++blockOrdinal;
            pool.blkIndex.emplace_back(static_cast<int>(pool.pool.size()));
        }
        row.obj["mbft_block"] = blockOrdinal;
        row.obj["mbft_amount"] = 1U;
        const string payload = row.obj.dump();
        vector<string> words = fixedKeywords(row.obj);
        vector<double> sketches(3, 0.0);
        pool.pool.emplace_back(computePayloadHash(payload, seed), blockOrdinal, 1U,
                               move(words), move(sketches), payload);
        if (metrics) {
            metrics->eventCount++;
            metrics->keywordCount += 16;
            metrics->payloadBytes += payload.size();
        }
    }
    if (metrics) metrics->blockCount = blockOrdinal;
    return pool;
}

size_t SelectVONode::serializedSize() const {
                                                                                                        
    return sizeof(uint8_t) + sizeof(hash) + sizeof(nCurrentItems) + sizeof(uint32_t) + data.size()
         + sizeof(lowerBound) + sizeof(upperBound) + sizeof(value) + sizeof(uint32_t) + payload.size();
}

RootSelectionProof selectAllSingleRoot(Block* block, RootType rootType,
                                       const AtomicSelectQuery& query,
                                       SelectionMetrics* metrics) {
    RootSelectionProof proof;
    proof.rootType = rootType;
    stack<MerkleBFNode*> pending;
    stack<SelectVONode> emitted;
    MerkleBFNode* root = block->getRoot(rootType);
    if (!root) throw runtime_error("requested MBFT root is missing");
    pending.push(root);

    while (!pending.empty()) {
        MerkleBFNode* current = pending.top();
        pending.pop();
        if (!nodeMayMatch(current, query)) {
            SelectVONode node;
            node.flag = SELECT_PRUNED;
            node.hash = current->getHash();
            node.nCurrentItems = current->getBF()->nCurrentItems;
            node.data = current->getBF()->getVecBF();
            node.lowerBound = current->getLowerBound();
            node.upperBound = current->getUpperBound();
            node.value = current->getValue();
            emitted.push(move(node));
            if (metrics) { metrics->prunedNodes++; metrics->prunedBloomBytes += emitted.top().data.size(); }
        } else if (current->hasChildren()) {
            SelectVONode hint;
            hint.flag = SELECT_HINT;
                                                                             
                                                                             
            hint.hash = current->getHash();
            hint.data = current->getBF()->getHint();
            hint.lowerBound = current->getLowerBound();
            hint.upperBound = current->getUpperBound();
            hint.value = current->getValue();
            emitted.push(move(hint));
            if (metrics) { metrics->hintNodes++; metrics->hintBytes += emitted.top().data.size(); }
                                                                                       
                                                                                    
            pending.push(static_cast<MerkleBFNode*>(current->getLeftChild()));
            pending.push(static_cast<MerkleBFNode*>(current->getRightChild()));
        } else {
            const int index = current->getLeafTxnIndex();
            if (index < 0) throw runtime_error("opened leaf has no transaction index");
            const Transaction txn = (rootType % 2 == 0) ? block->getTxn(index) : block->getTxnCombine(index);
            if (!txn.hasPayload()) throw runtime_error("MBFT-Adapt leaf is missing canonical payload");
            SelectVONode leaf;
            leaf.flag = SELECT_OPEN_LEAF;
            leaf.payload = txn.getPayload();
            emitted.push(move(leaf));
            if (metrics) {
                metrics->openedLeaves++;
                metrics->payloadBytes += txn.getPayload().size();
                const bool exact = queryOverlap(query, txn.getAmount(), txn.getAmount())
                    && exactMatchWords(txn.getAddressesRef(), query.keywordsCNF);
                if (exact) metrics->matchedLeaves++; else metrics->falsePositiveLeaves++;
            }
        }
    }

    proof.nodes.reserve(emitted.size());
    while (!emitted.empty()) {
        proof.nodes.emplace_back(move(emitted.top()));
        emitted.pop();
    }
    if (metrics) {
        metrics->voNodes += proof.nodes.size();
        for (const auto& node : proof.nodes) metrics->proofBytes += node.serializedSize();
    }
    return proof;
}

AtomicSelectionProof selectAllChain(const Chain& chain, const AtomicSelectQuery& query,
                                    vector<RootCommitment>* commitments,
                                    SelectionMetrics* metrics) {
    const auto started = high_resolution_clock::now();
    AtomicSelectionProof proof;
    proof.query = query;
    uint32_t current = 1;
    const uint32_t end = chain.getSize();
    const uint32_t cycle = chain.getCombineCycle();
    while (current <= end) {
        uint32_t startBlock = current;
        RootType type = AMOUNT_ADS;
        if (cycle && current % cycle == 1 && current + cycle - 1 <= end) {
            current += cycle - 1;
            type = AMOUNT_COMBINE_ADS;
        }
        Block* block = chain.getBlock(current);
        RootSelectionProof rootProof = selectAllSingleRoot(block, type, query, metrics);
        rootProof.startBlock = startBlock;
        rootProof.endBlock = current;
        proof.roots.emplace_back(move(rootProof));
        if (commitments) {
            RootCommitment commitment;
            commitment.startBlock = startBlock;
            commitment.endBlock = current;
            commitment.rootType = type;
            commitment.rootHash = block->getRoot(type)->getHash();
            commitment.seed = block->getSeed();
            commitment.paramBF = (type % 2 == 0) ? block->getParamBF() : block->getParamBFCombine();
            commitments->emplace_back(commitment);
        }
        if (metrics) metrics->roots++;
        ++current;
    }
    if (metrics) metrics->queryMicros = duration<double, micro>(high_resolution_clock::now() - started).count();
    return proof;
}

QueryOnlySelection selectAllChainNoProof(const Chain& chain,
                                         const AtomicSelectQuery& query) {
    const auto started = high_resolution_clock::now();
    QueryOnlySelection result;
    unordered_map<string, string> uniquePayloads;

    uint32_t current = 1;
    const uint32_t end = chain.getSize();
    const uint32_t cycle = chain.getCombineCycle();
    while (current <= end) {
        RootType type = AMOUNT_ADS;
        if (cycle && current % cycle == 1 && current + cycle - 1 <= end) {
            current += cycle - 1;
            type = AMOUNT_COMBINE_ADS;
        }
        Block* block = chain.getBlock(current);
        MerkleBFNode* root = block->getRoot(type);
        if (!root) throw runtime_error("requested MBFT root is missing");
        result.metrics.roots++;

        stack<MerkleBFNode*> pending;
        pending.push(root);
        while (!pending.empty()) {
            MerkleBFNode* node = pending.top();
            pending.pop();
            if (!nodeMayMatch(node, query)) {
                result.metrics.prunedNodes++;
                continue;
            }
            if (node->hasChildren()) {
                                                                              
                                                                              
                                             
                pending.push(static_cast<MerkleBFNode*>(node->getLeftChild()));
                pending.push(static_cast<MerkleBFNode*>(node->getRightChild()));
                continue;
            }

            const int index = node->getLeafTxnIndex();
            if (index < 0) throw runtime_error("opened leaf has no transaction index");
            const Transaction txn = (type % 2 == 0) ? block->getTxn(index) : block->getTxnCombine(index);
            if (!txn.hasPayload()) throw runtime_error("MBFT-Adapt leaf is missing canonical payload");
            result.metrics.openedLeaves++;

            const bool exact = queryOverlap(query, txn.getAmount(), txn.getAmount())
                && exactMatchWords(txn.getAddressesRef(), query.keywordsCNF);
            if (!exact) {
                result.metrics.falsePositiveLeaves++;
                continue;
            }
            result.metrics.matchedLeaves++;
            result.metrics.rawMatchedLeaves++;
            const string& payload = txn.getPayload();
            uniquePayloads.emplace(eventIdFromPayload(payload), payload);
        }
        ++current;
    }

    vector<pair<string, string>> ordered;
    ordered.reserve(uniquePayloads.size());
    for (auto& item : uniquePayloads) ordered.emplace_back(move(item.first), move(item.second));
    sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    result.payloads.reserve(ordered.size());
    for (auto& item : ordered) {
        result.metrics.payloadBytes += item.second.size();
        result.payloads.emplace_back(move(item.second));
    }
    result.metrics.uniqueMatchedEvents = result.payloads.size();
    result.metrics.queryMicros = duration<double, micro>(high_resolution_clock::now() - started).count();
    return result;
}

VerifiedSelection verifyAtomicSelection(const AtomicSelectionProof& proof,
                                        const vector<RootCommitment>& commitments) {
    const auto started = high_resolution_clock::now();
    if (proof.roots.size() != commitments.size()) throw runtime_error("proof/commitment root count mismatch");
    VerifiedSelection result;
    unordered_map<string, string> uniquePayloads;

    for (size_t rootIndex = 0; rootIndex < proof.roots.size(); ++rootIndex) {
        const RootSelectionProof& rootProof = proof.roots[rootIndex];
        const RootCommitment& commitment = commitments[rootIndex];
        if (rootProof.startBlock != commitment.startBlock || rootProof.endBlock != commitment.endBlock
            || rootProof.rootType != commitment.rootType) {
            throw runtime_error("proof root identifier does not match trusted commitment");
        }
        vector<ReconstructedNode> stackNodes;
        vector<unique_ptr<BloomFilter>> gc;
        stackNodes.reserve(rootProof.nodes.size());

        for (const SelectVONode& node : rootProof.nodes) {
            result.metrics.voNodes++;
            result.metrics.proofBytes += node.serializedSize();
            if (node.flag == SELECT_HINT) {
                if (stackNodes.size() < 2) throw runtime_error("malformed hint VO");
                ReconstructedNode right = move(stackNodes.back()); stackNodes.pop_back();
                ReconstructedNode left = move(stackNodes.back()); stackNodes.pop_back();
                const auto reconStart = high_resolution_clock::now();
                unique_ptr<BloomFilter> parent(left.bf->reconstruct(right.bf.get(), node.data));
                result.metrics.reconstructMicros += duration<double, micro>(high_resolution_clock::now() - reconStart).count();
                const uint32_t lower = min(left.lower, right.lower);
                const uint32_t upper = max(left.upper, right.upper);
                const double value = max(left.value, right.value);
                if (lower != node.lowerBound || upper != node.upperBound || value != node.value)
                    throw runtime_error("hint metadata mismatch");
                const uint32_t hash = hashInternal(left.hash, right.hash, lower, upper, value,
                                                   parent.get(), commitment.seed);
                if (node.hash != 0 && hash != node.hash) {
                    throw runtime_error(
                        "reconstructed MBFT internal node mismatch at proof root "
                        + to_string(rootIndex) + ": reconstructed=" + to_string(hash)
                        + " serialized=" + to_string(node.hash)
                    );
                }
                ReconstructedNode merged;
                merged.bf = move(parent); merged.hash = hash; merged.lower = lower; merged.upper = upper; merged.value = value;
                stackNodes.emplace_back(move(merged));
                result.metrics.hintNodes++;
                result.metrics.hintBytes += node.data.size();
            } else if (node.flag == SELECT_PRUNED) {
                ParamBF param = commitment.paramBF;
                param.nCurrentItems = node.nCurrentItems;
                unique_ptr<BloomFilter> bf(new MergeBloomFilter(param, node.data));
                if (queryOverlap(proof.query, node.lowerBound, node.upperBound)
                    && bf->batchContain(proof.query.keywordsCNF)) {
                    throw runtime_error("invalid pruned node: summary may satisfy atomic query");
                }
                ReconstructedNode rebuilt;
                rebuilt.bf = move(bf); rebuilt.hash = node.hash; rebuilt.lower = node.lowerBound;
                rebuilt.upper = node.upperBound; rebuilt.value = node.value;
                stackNodes.emplace_back(move(rebuilt));
                result.metrics.prunedNodes++;
                result.metrics.prunedBloomBytes += node.data.size();
            } else if (node.flag == SELECT_OPEN_LEAF) {
                                                                          
                const json obj = json::parse(node.payload);
                Transaction txn(computePayloadHash(node.payload, commitment.seed),
                                obj.value("mbft_block", 0U), obj.value("mbft_amount", 1U),
                                fixedKeywords(obj), vector<double>(3, 0.0), node.payload);
                ParamBF param = commitment.paramBF;
                param.nCurrentItems = txn.getAddressesRef().size();
                unique_ptr<BloomFilter> bf(new MergeBloomFilter(param));
                for (const string& word : txn.getAddressesRef()) bf->insert(word);
                const bool bloomCandidate = queryOverlap(proof.query, txn.getAmount(), txn.getAmount())
                    && bf->batchContain(proof.query.keywordsCNF);
                if (!bloomCandidate) throw runtime_error("opened leaf was not a Bloom/range candidate");
                const bool exact = exactMatchWords(txn.getAddressesRef(), proof.query.keywordsCNF);
                if (exact) {
                    result.metrics.rawMatchedLeaves++;
                    const string id = obj.value("event_id", node.payload);
                    const auto inserted = uniquePayloads.emplace(id, node.payload);
                    if (!inserted.second && inserted.first->second != node.payload)
                        throw runtime_error("conflicting authenticated event identity");
                    result.metrics.matchedLeaves++;
                } else {
                    result.metrics.falsePositiveLeaves++;
                }
                ReconstructedNode rebuilt;
                rebuilt.bf = move(bf); rebuilt.hash = txn.getTxid(); rebuilt.lower = txn.getAmount();
                rebuilt.upper = txn.getAmount(); rebuilt.value = txn.getValue(MAX);
                stackNodes.emplace_back(move(rebuilt));
                result.metrics.openedLeaves++;
                result.metrics.payloadBytes += node.payload.size();
            } else {
                throw runtime_error("unknown selection VO flag");
            }
        }
        if (stackNodes.size() != 1) throw runtime_error("selection VO did not reconstruct one root");
        if (stackNodes.back().hash != commitment.rootHash) {
            throw runtime_error(
                "reconstructed MBFT root mismatch at proof root " + to_string(rootIndex)
                + " blocks " + to_string(rootProof.startBlock) + "-" + to_string(rootProof.endBlock)
                + ": reconstructed=" + to_string(stackNodes.back().hash)
                + " committed=" + to_string(commitment.rootHash)
            );
        }
        result.metrics.roots++;
    }

    vector<pair<string,string>> ordered(uniquePayloads.begin(), uniquePayloads.end());
    sort(ordered.begin(), ordered.end(), [](const auto& a, const auto& b){ return a.first < b.first; });
    result.payloads.reserve(ordered.size());
    for (auto& item : ordered) result.payloads.emplace_back(move(item.second));
    result.metrics.uniqueMatchedEvents = result.payloads.size();
    result.metrics.verifyMicros = duration<double, micro>(high_resolution_clock::now() - started).count();
    result.valid = true;
    return result;
}

void saveAtomicSelectionProof(const AtomicSelectionProof& proof, const string& path) {
    ofstream out(path, ios::binary);
    if (!out) throw runtime_error("cannot create proof file: " + path);
    const string magic("MBFSA2\0", 7);
    out.write(magic.data(), static_cast<streamsize>(magic.size()));
    writePod(out, proof.query.lowerBound); writePod(out, proof.query.upperBound);
    writePod(out, static_cast<uint32_t>(proof.query.keywordsCNF.size()));
    for (const auto& clause : proof.query.keywordsCNF) {
        writePod(out, static_cast<uint32_t>(clause.size()));
        for (const string& word : clause) writeString(out, word);
    }
    writePod(out, static_cast<uint32_t>(proof.roots.size()));
    for (const auto& root : proof.roots) {
        writePod(out, root.startBlock); writePod(out, root.endBlock);
        writePod(out, static_cast<uint32_t>(root.rootType));
        writePod(out, static_cast<uint64_t>(root.nodes.size()));
        for (const auto& node : root.nodes) {
            writePod(out, static_cast<uint8_t>(node.flag)); writePod(out, node.hash);
            writePod(out, node.nCurrentItems); writeBytes(out, node.data);
            writePod(out, node.lowerBound); writePod(out, node.upperBound); writePod(out, node.value);
            writeString(out, node.payload);
        }
    }
}

AtomicSelectionProof loadAtomicSelectionProof(const string& path) {
    ifstream in(path, ios::binary);
    if (!in) throw runtime_error("cannot open proof file: " + path);
    char magic[7]{}; in.read(magic, 7);
    if (!in || string(magic, 7) != string("MBFSA2\0", 7)) throw runtime_error("invalid proof magic");
    AtomicSelectionProof proof;
    proof.query.lowerBound = readPod<uint32_t>(in); proof.query.upperBound = readPod<uint32_t>(in);
    const uint32_t clauses = readPod<uint32_t>(in);
    proof.query.keywordsCNF.resize(clauses);
    for (auto& clause : proof.query.keywordsCNF) {
        const uint32_t words = readPod<uint32_t>(in);
        clause.reserve(words);
        for (uint32_t i = 0; i < words; ++i) clause.emplace_back(readString(in));
    }
    const uint32_t roots = readPod<uint32_t>(in);
    proof.roots.resize(roots);
    for (auto& root : proof.roots) {
        root.startBlock = readPod<uint32_t>(in); root.endBlock = readPod<uint32_t>(in);
        root.rootType = static_cast<RootType>(readPod<uint32_t>(in));
        const uint64_t nodes = readPod<uint64_t>(in);
        root.nodes.resize(nodes);
        for (auto& node : root.nodes) {
            node.flag = static_cast<SelectFlag>(readPod<uint8_t>(in)); node.hash = readPod<uint32_t>(in);
            node.nCurrentItems = readPod<unsigned int>(in); node.data = readBytes(in);
            node.lowerBound = readPod<uint32_t>(in); node.upperBound = readPod<uint32_t>(in);
            node.value = readPod<double>(in); node.payload = readString(in);
        }
    }
    return proof;
}

void saveCommitments(const vector<RootCommitment>& commitments, const string& path) {
    json root = json::array();
    for (const auto& c : commitments) {
        root.push_back({
            {"start_block", c.startBlock}, {"end_block", c.endBlock},
            {"root_type", static_cast<uint32_t>(c.rootType)}, {"root_hash", c.rootHash},
            {"seed", c.seed}, {"n_items", c.paramBF.nItems},
            {"fp_rate", c.paramBF.nFPRate}, {"tweak", c.paramBF.nTweak}
        });
    }
    ofstream out(path); if (!out) throw runtime_error("cannot create commitment file");
    out << json{{"format", "MBFT-Adapt-MergeBloom-v2"}, {"roots", root}}.dump(2) << '\n';
}

vector<RootCommitment> loadCommitments(const string& path) {
    ifstream in(path); if (!in) throw runtime_error("cannot open commitment file");
    json obj; in >> obj;
    vector<RootCommitment> out;
    for (const auto& item : obj.at("roots")) {
        RootCommitment c;
        c.startBlock = item.at("start_block").get<uint32_t>(); c.endBlock = item.at("end_block").get<uint32_t>();
        c.rootType = static_cast<RootType>(item.at("root_type").get<uint32_t>());
        c.rootHash = item.at("root_hash").get<uint32_t>(); c.seed = item.at("seed").get<unsigned int>();
        c.paramBF = ParamBF(0, item.at("n_items").get<unsigned int>(), item.at("fp_rate").get<double>(),
                            item.at("tweak").get<unsigned int>());
        out.emplace_back(c);
    }
    return out;
}

void writePayloadsJsonl(const vector<string>& payloads, const string& path) {
    ofstream out(path); if (!out) throw runtime_error("cannot create verified event file: " + path);
    for (const string& payload : payloads) out << payload << '\n';
}

void writeSelectionMetricsJson(const SelectionMetrics& m, const string& path, const string& eventType) {
    json obj = {
        {"event_type", eventType}, {"roots", m.roots}, {"vo_nodes", m.voNodes},
        {"hint_nodes", m.hintNodes}, {"pruned_nodes", m.prunedNodes},
        {"opened_leaves", m.openedLeaves}, {"matched_leaves", m.matchedLeaves},
        {"false_positive_leaves", m.falsePositiveLeaves}, {"raw_matched_leaves", m.rawMatchedLeaves},
        {"unique_matched_events", m.uniqueMatchedEvents}, {"hint_bytes", m.hintBytes},
        {"pruned_bloom_bytes", m.prunedBloomBytes}, {"candidate_payload_bytes", m.payloadBytes},
        {"proof_bytes_in_memory", m.proofBytes}, {"query_time_us", m.queryMicros},
        {"verify_time_us", m.verifyMicros}, {"reconstruct_time_us", m.reconstructMicros}
    };
    ofstream out(path); if (!out) throw runtime_error("cannot create metrics file: " + path);
    out << obj.dump(2) << '\n';
}
