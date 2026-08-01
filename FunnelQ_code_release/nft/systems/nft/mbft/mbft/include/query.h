#ifndef QUERY_H
#define QUERY_H

//#include <cstdint>
//#include <set>
//#include <utility>
#include <variant>

#include "utils.h"
#include "merkle.h"
#include "blockchain.h"

enum MyFlag {LEFT, RIGHT, ROOT, RES, HINT, ASTRAY};

/** Verify Result
 * FAIL_VERIFY: Verification fails.
 * CONTAIN: It's verified that it contains the demanding result.
 * CONTAIN_ASTRAY: It's verified that it contains the demanding result, and VO path has astray.
 * NOT_CONTAIN: It's verified that it doesn't contain the demanding result.
**/
enum VerifyRes {FAIL_VERIFY, CONTAIN, CONTAIN_ASTRAY, NOT_CONTAIN};

class Query {
public:
    Query(AggType agg, uint32_t beginBlock, uint32_t endBlock, uint32_t lowerBound, uint32_t upperBound,
          vector<vector<string>> addresses, bool earlyStop);

    bool overlap(const uint32_t& lBound, const uint32_t& uBound, const double& value) const;

    AggType agg;
    uint32_t beginBlock;
    uint32_t endBlock;
    uint32_t lowerBound;
    uint32_t upperBound;
    vector<vector<string>> addresses;
    double earlyValue;

    void printAddresses() const;

    bool earlyStop;
};

struct VONode {
    VONode() = default;
    VONode(MyFlag myFlag, uint32_t hash, unsigned int nCurrentItems, vector<unsigned char> vData,
           uint32_t lowerBound, uint32_t upperBound, double value, int txnIndex=-1);

    size_t getSizeADS() const;
    size_t getSizeBF() const;

    MyFlag myFlag;
    uint32_t hash;
    unsigned int nCurrentItems;
    vector<unsigned char> vData;
    uint32_t lowerBound;
    uint32_t upperBound;
    double value;
    // MerkleBFNode* ptrBack;
    int txnIndex;
};

struct ReVONode {
    ReVONode() = default;
    ReVONode(MyFlag myFlag, BloomFilter* bf, uint32_t hash, uint32_t lowerBound, uint32_t upperBound, double value);

    MyFlag myFlag;
    BloomFilter* bf;
    uint32_t hash;
    uint32_t lowerBound;
    uint32_t upperBound;
    double value;
};

// query and verify

// stack<tuple<MyFlag, uint32_t, uint32_t, vector<unsigned char>, MerkleBFNode*>> mbfMaxQuery(MerkleBFNode * root, uint32_t qid); // select max() from block where txid==qid
// VerifyRes mbfVerify(uint32_t qid, stack<tuple<MyFlag, uint32_t, uint32_t, vector<unsigned char>, MerkleBFNode*>> sMerklePath, uint32_t nRootHash, unsigned int nSeed, unsigned int nItems, double nFPRate, unsigned int nTweak);

using varVO = std::variant<
    queue<stack<VONode>>,
    queue<vector<Transaction>>
>;

// pair<double, stack<VONode>> singleQuery(MerkleBFNode * root, const Query& query);
pair<double, stack<VONode>> singleQuery(Block* blk, RootType rootType, const Query& query);
double singleVerify(const Query& query, stack<VONode> sMerklePath, Block* blk, RootType rootType, ExperimentMetrics& metrics);

pair<double, queue<stack<VONode>>> chainQueryMBFT(const Chain& chain, Query query);
bool chainVerifyMBFT(Query query, double res, queue<stack<VONode>> vVO, const Chain& chain, ExperimentMetrics& metrics);

pair<double, queue<vector<Transaction>>> chainQueryMHT(const Chain& chain, Query query);
bool chainVerifyMHT(Query query, double res, queue<vector<Transaction>> vVO, const Chain& chain);

pair<double, queue<vector<Transaction>>> chainQueryVChain(const Chain& chain, Query query);
bool chainVerifyVChain(Query query, double res, queue<vector<Transaction>> vVO, const Chain& chain);

pair<double, varVO> chainQuery(const Chain& chain, Query query);
bool chainVerify(Query query, double res, varVO vVO, const Chain& chain, ExperimentMetrics& metrics);

#endif //QUERY_H
