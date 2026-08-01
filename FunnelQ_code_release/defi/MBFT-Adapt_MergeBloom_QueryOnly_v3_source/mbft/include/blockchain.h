#ifndef BLOCKCHAIN_H
#define BLOCKCHAIN_H

//#include <cstdint>
//#include <vector>

#include "utils.h"
#include "merkle.h"
#include "transaction.h"

class TxnPool {
public:
    TxnPool() = default;
    TxnPool(unsigned int nSeed, set<AggType> supportAggs, string inPath);

    string getAddr(unsigned int posTxn, unsigned int posAddr) const;
    int getSize() const;
    uint32_t ComputeTxnId(string strTxn);

    // Property
    unsigned int nSeed;
    set<AggType> supportAggs;
    vector<Transaction> pool;
    vector<int> blkIndex;
};

class Block {
public:
    Block() = default;
    Block(uint32_t timestamp, const vector<Transaction>& vTxns, unsigned int nSeed, ADSType adsType); // MHT
    Block(uint32_t timestamp, const vector<vector<Transaction>>& vvTxns, queue<RootType> qTypes, ParamBF paramBF,
          unsigned int nSeed, bool earlyStop, ADSType adsType, unsigned int nItemsCombine=0);

    ~Block();

    void display() const;

    MerkleBFNode* getRoot(RootType rootType) const;

    MerkleNode* getRootMHT() const;
    Transaction getTxn(uint32_t index) const;
    vector<Transaction> getTxns() const;
    Transaction getTxnCombine(uint32_t index) const;
    vector<Transaction> getTxnsCombine() const;

    double getsizeADS() const;

    unsigned int getSeed() const;
    ParamBF getParamBF() const;
    ParamBF getParamBFCombine() const;

    // Properties
    ParamBF paramBF;
    ParamBF paramBFCombine;
    unsigned int nSeed;

    uint32_t timestamp;
    vector<Transaction> vTransactions;
    vector<Transaction> vTransactionsCombine;

    vector<MerkleBFNode*> vRoots;

    MerkleNode* rootMHT;
    ADSType adsType;

    bool earlyStop;
};

class Chain {
public:
    Chain() = default;

    /*
     * supportAggs: txn, block & chain are constructed according to different aggregations: max, count, sum, count distinct.
     *  - chain: record set of supported aggregation
     *  - block: construct different MBF-tree root
     *  - txn: construct different sketches
     * */
    Chain(ParamBF paramBF, unsigned int nSeed, set<AggType> supportAggs, unsigned int combineCycle, bool earlyStop, ADSType adsType);

    ~Chain();

    void buildChain(const TxnPool& txnPool);
    void buildMBFTChain(const TxnPool& txnPool);
    void buildMHTChain(const TxnPool& txnPool);

    bool support(AggType agg) const;

    Block* getBlock(uint32_t timestamp) const;
    uint32_t getSize() const;
    uint32_t getCombineCycle() const;

    double getsizeADS() const;
    double getTotalSizeADS() const;

    void display() const;

    // Property
    ParamBF paramBF;
    unsigned int nSeed;
    unsigned int combineCycle;
    vector<Block*> chain;
    set<AggType> supportAggs;
    bool earlyStop;
    ADSType adsType;
};


#endif //BLOCKCHAIN_H