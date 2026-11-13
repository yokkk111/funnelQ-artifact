#ifndef MBFT_ADAPT_H
#define MBFT_ADAPT_H

#include "blockchain.h"
#include "query.h"
#include "transaction.h"

#include <cstdint>
#include <string>
#include <vector>

struct DefiLoadMetrics {
    uint64_t eventCount = 0;
    uint64_t blockCount = 0;
    uint64_t keywordCount = 0;
    uint64_t payloadBytes = 0;
};

                                                                              
                                                                              
                                                      
TxnPool loadDefiEventPool(const string& csvPath, unsigned int seed, DefiLoadMetrics* metrics = nullptr);

uint32_t computePayloadHash(const string& payload, unsigned int seed);
Transaction transactionFromPayload(const string& payload, unsigned int seed);
vector<string> keywordsFromPayload(const string& payload);

struct AtomicSelectQuery {
    uint32_t lowerBound = 0;
    uint32_t upperBound = UINT32_MAX;
    vector<vector<string>> keywordsCNF;
};

enum SelectFlag : uint8_t { SELECT_PRUNED = 0, SELECT_OPEN_LEAF = 1, SELECT_HINT = 2 };

struct SelectVONode {
    SelectFlag flag = SELECT_PRUNED;
    uint32_t hash = 0;
    unsigned int nCurrentItems = 0;
    vector<unsigned char> data;                                                              
    uint32_t lowerBound = 0;
    uint32_t upperBound = 0;
    double value = 0;
    string payload;                                         

    size_t serializedSize() const;
};

struct RootCommitment {
    uint32_t startBlock = 0;
    uint32_t endBlock = 0;
    RootType rootType = AMOUNT_ADS;
    uint32_t rootHash = 0;
    unsigned int seed = 0;
    ParamBF paramBF;
};

struct RootSelectionProof {
    uint32_t startBlock = 0;
    uint32_t endBlock = 0;
    RootType rootType = AMOUNT_ADS;
    vector<SelectVONode> nodes;                             
};

struct SelectionMetrics {
    uint64_t roots = 0;
    uint64_t voNodes = 0;
    uint64_t hintNodes = 0;
    uint64_t prunedNodes = 0;
    uint64_t openedLeaves = 0;
    uint64_t matchedLeaves = 0;
    uint64_t falsePositiveLeaves = 0;
    uint64_t rawMatchedLeaves = 0;
    uint64_t uniqueMatchedEvents = 0;
    uint64_t hintBytes = 0;
    uint64_t prunedBloomBytes = 0;
    uint64_t payloadBytes = 0;
    uint64_t proofBytes = 0;
    double queryMicros = 0;
    double verifyMicros = 0;
    double reconstructMicros = 0;
};

struct AtomicSelectionProof {
    AtomicSelectQuery query;
    vector<RootSelectionProof> roots;
};

struct VerifiedSelection {
    bool valid = false;
    vector<string> payloads;
    SelectionMetrics metrics;
};

                                                                           
                                                                               
                                                                            
                                                                         
                         
struct QueryOnlySelection {
    vector<string> payloads;
    SelectionMetrics metrics;
};

                                                                          
                                                                           
                                                                              
                                                                     
struct SerializedMbftIndexMetrics {
    uint64_t fileBytes = 0;
    uint64_t blocks = 0;
    uint64_t events = 0;
    uint64_t roots = 0;
    uint64_t uniqueNodes = 0;
    uint64_t bloomBytes = 0;
    uint64_t payloadBytes = 0;
};

RootSelectionProof selectAllSingleRoot(Block* block, RootType rootType,
                                       const AtomicSelectQuery& query,
                                       SelectionMetrics* metrics = nullptr);

AtomicSelectionProof selectAllChain(const Chain& chain, const AtomicSelectQuery& query,
                                    vector<RootCommitment>* commitments,
                                    SelectionMetrics* metrics = nullptr);

VerifiedSelection verifyAtomicSelection(const AtomicSelectionProof& proof,
                                        const vector<RootCommitment>& commitments);

QueryOnlySelection selectAllChainNoProof(const Chain& chain,
                                         const AtomicSelectQuery& query);

SerializedMbftIndexMetrics saveSerializedMbftIndex(const Chain& chain,
                                                    const string& path);
SerializedMbftIndexMetrics inspectSerializedMbftIndex(const string& path);

void saveAtomicSelectionProof(const AtomicSelectionProof& proof, const string& path);
AtomicSelectionProof loadAtomicSelectionProof(const string& path);
void saveCommitments(const vector<RootCommitment>& commitments, const string& path);
vector<RootCommitment> loadCommitments(const string& path);

void writePayloadsJsonl(const vector<string>& payloads, const string& path);
void writeSelectionMetricsJson(const SelectionMetrics& metrics, const string& path,
                               const string& eventType = "");

#endif                
