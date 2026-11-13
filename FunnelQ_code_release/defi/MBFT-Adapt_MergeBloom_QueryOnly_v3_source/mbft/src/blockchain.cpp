#include "blockchain.h"

#include <utility>
#include <iostream>

bool ascendAmount(const Transaction& a, const Transaction& b) {return a.amount < b.amount;}
bool ascendCount(const Transaction& a, const Transaction& b) {return a.sketches[0] < b.sketches[0];}
bool ascendSum(const Transaction& a, const Transaction& b) {return a.sketches[1] < b.sketches[1];}
bool ascendCountDistinct(const Transaction& a, const Transaction& b) {return a.sketches[2] < b.sketches[2];}


  
                                             
     

TxnPool::TxnPool(unsigned int nSeedIn, set<AggType> supportAggsIn, string inPath):
    nSeed(nSeedIn),
    supportAggs(move(supportAggsIn))
{
    ifstream infile(inPath);
    string sTxn, tmp;
    regex reg(R"(^(.*)\s\[(.*)\]\s\{(.*)\}$)");
    regex regAddr(R"(0x[0-9a-f]+)");
    smatch m, ma;
    uint32_t txid;
    uint32_t timestamp;
    uint32_t amount;
    vector<string> addresses;

    double varRand;
    vector<double> sketches(3);                                  
    uniform_real_distribution<double> uniformDist(0,1);                                    
    int currentBlk = 0;

    if (infile.is_open()) {
        while (getline(infile, sTxn)) {
                   
            txid = ComputeTxnId(sTxn);
            regex_match(sTxn, m, reg);

                        
            timestamp = stoi(m[1].str());
                     
            amount = stoi(m[2].str());
                        
            assert(addresses.empty());
            string addrs = m[3].str();

            auto pos = addrs.cbegin();
            auto end = addrs.cend();
            for (; std::regex_search(pos, end, ma, regAddr); pos = ma.suffix().first)
            {
                addresses.emplace_back(ma.str());
            }
            if (addresses.size() == 1) {
                tmp = addresses[0];
                addresses.emplace_back(tmp);
            }

                       
            if (supportAggs.count(COUNT) || supportAggs.count(SUM)) {
                default_random_engine engTxid(txid);
                varRand = uniformDist(engTxid);
                if (supportAggs.count(COUNT)) {                 
                    sketches[0] = varRand;
                }
                if (supportAggs.count(SUM)) {                 
                                                   
                    sketches[1] = ceil(-log2(1 - pow(varRand, 1.0/amount)));
                }
            }
            if (supportAggs.count(COUNT_DISTINCT)) {                     
                default_random_engine engValue(amount);
                sketches[2] = uniformDist(engValue);
            }
            
            if (timestamp != currentBlk) {
                currentBlk = timestamp;
                blkIndex.emplace_back(pool.size());
            }

            pool.emplace_back(Transaction(txid, timestamp, amount, addresses, sketches));
            addresses.clear();
        }
    }
}


string TxnPool::getAddr(unsigned int posTxn, unsigned int posAddr) const
{
    return pool[posTxn].getAddressesRef().at(posAddr);
}


int TxnPool::getSize() const
{
    return pool.size();
}


uint32_t TxnPool::ComputeTxnId(string strTxn)
{
    uint32_t hash;
    vector<unsigned char> data(strTxn.begin(), strTxn.end());
    MurmurHash3_x86_32(data.data(), data.size(), nSeed, &hash);
    return hash;
}


  
                                           
     

          
Block::Block(uint32_t timestampIn, const vector<Transaction>& vTxns, unsigned int nSeedIn, ADSType adsTypeIn):
    timestamp(timestampIn),
    nSeed(nSeedIn),
    vRoots(0),
    earlyStop(false),
    adsType(adsTypeIn)
{
    assert(adsType == MHT || adsType == VCHAIN);
    vTransactions.assign(vTxns.begin(), vTxns.end());
    rootMHT = new MerkleNode(vTxns, nSeedIn);
}

           
Block::Block(uint32_t timestampIn, const vector<vector<Transaction>>& vvTxns, queue<RootType> qTypes, ParamBF paramBFIn,
             unsigned int nSeedIn, bool earlyStopIn, ADSType adsTypeIn, unsigned int nItemsCombineIn):
    timestamp(timestampIn),
    paramBF(paramBFIn),
    paramBFCombine(paramBFIn),
    nSeed(nSeedIn),
    vRoots(8),
    earlyStop(earlyStopIn),
    rootMHT(nullptr),
    adsType(adsTypeIn)
{
    assert(adsType == MBFT || adsType == MBFT_BF);
    assert(vvTxns.size() == qTypes.size());
    paramBFCombine.nItems = nItemsCombineIn;

    for (auto vTxns : vvTxns) {
        if (vTransactions.empty() && (qTypes.front() & 1) == 0) {                                                                   
            vTransactions.assign(vTxns.begin(), vTxns.end());
        }
        if (vTransactionsCombine.empty() && (qTypes.front() & 1) == 1) {
            vTransactionsCombine.assign(vTxns.begin(), vTxns.end());
        }

        if (qTypes.front() & 1) {                                                                              
            assert(nItemsCombineIn);
            vRoots[qTypes.front()] = new MerkleBFNode(vTxns, paramBFCombine, nSeed, (AggType)(qTypes.front()/2), earlyStop, adsType);

        } else {                                                                                             
            vRoots[qTypes.front()] = new MerkleBFNode(vTxns, paramBF, nSeed, (AggType)(qTypes.front()/2), earlyStop, adsType);
        }
        qTypes.pop();
    }
}

Block::~Block()
{
    for (MerkleBFNode* root : vRoots) {
        if (root != nullptr) {
            delete root;
        }
    }
    if (rootMHT != nullptr) {
        delete rootMHT;
    }
}

double Block::getsizeADS() const
{
    double sizeADS = 0;
    int counter = 0;
    if (adsType == MHT || adsType == VCHAIN) {
        sizeADS = rootMHT->getSizeADS();
        counter++;
    }
    else {
        for (MerkleBFNode* root : vRoots) {
            if (root != nullptr) {
                sizeADS += root->getSizeADS();
                counter++;
            }
        }
    }
    return sizeADS / counter;
}

MerkleBFNode* Block::getRoot(RootType rootType) const
{
    return vRoots[rootType];
}

unsigned int Block::getSeed() const
{
    return nSeed;
}

ParamBF Block::getParamBF() const
{
    return paramBF;
}

ParamBF Block::getParamBFCombine() const
{
    return paramBFCombine;
}

MerkleNode* Block::getRootMHT() const
{
    assert(adsType == MHT || adsType == VCHAIN);
    return rootMHT;
}

Transaction Block::getTxn(uint32_t index) const
{
    return vTransactions[index];
}

vector<Transaction> Block::getTxns() const
{
    return vTransactions;
}

Transaction Block::getTxnCombine(uint32_t index) const
{
    return vTransactionsCombine[index];
}

vector<Transaction> Block::getTxnsCombine() const
{
    return vTransactionsCombine;
}

void Block::display() const
{
    cout << "======== Block transaction list ========" << endl;
    cout << "timestamp: " << timestamp << endl;
    cout << "vTransactions: " << endl;
    for (const Transaction& transaction : vTransactions) {
        transaction.display();
    }
}

  
                                           
     

Chain::Chain(ParamBF paramBFIn, unsigned int nSeedIn, set<AggType> supportAggsIn, unsigned int combineCycleIn, bool earlyStopIn, ADSType adsTypeIn):
    paramBF(paramBFIn),
    nSeed(nSeedIn),
    supportAggs(std::move(supportAggsIn)),
    combineCycle(combineCycleIn),
    earlyStop(earlyStopIn),
    adsType(adsTypeIn)
{
}

Chain::~Chain()
{
    for (Block* block : chain) {
        delete block;
    }
}

void Chain::buildChain(const TxnPool& txnPool)
{
    switch (adsType) {
        case MHT:
        case VCHAIN:
            buildMHTChain(txnPool);
            break;
        case MBFT:
        case MBFT_BF:
            buildMBFTChain(txnPool);
            break;
    }
}

void Chain::buildMHTChain(const TxnPool& txnPool)
{
    uint32_t timestamp;
    uint32_t lastTimestamp = 1;
    vector<Transaction> txns(0);
    Block* block;

    for (const Transaction& txn : txnPool.pool) {
        timestamp = txn.getTimestamp();

        if (timestamp != lastTimestamp) {
            block = new Block(lastTimestamp, txns, nSeed, adsType);
            chain.emplace_back(block);

            lastTimestamp = timestamp;
            txns.clear();
        }

        txns.emplace_back(txn);
    }
    if (!txns.empty()) {
        block = new Block(lastTimestamp, txns, nSeed, adsType);
        chain.emplace_back(block);

        lastTimestamp = timestamp;
        txns.clear();
    }
}

void Chain::buildMBFTChain(const TxnPool& txnPool)
{
    if (txnPool.pool.empty()) return;

    bool (*comps[4])(const Transaction&, const Transaction&) = {
        ascendAmount, ascendCount, ascendSum, ascendCountDistinct
    };
    vector<vector<Transaction>> combinedByAgg(4);
    unsigned int combinedItemCount = 0;

    size_t pos = 0;
    while (pos < txnPool.pool.size()) {
        const uint32_t timestamp = txnPool.pool[pos].getTimestamp();
        vector<Transaction> blockTxns;
        unsigned int blockItemCount = 0;
        while (pos < txnPool.pool.size() && txnPool.pool[pos].getTimestamp() == timestamp) {
            blockItemCount += txnPool.pool[pos].getAddressesRef().size();
            blockTxns.emplace_back(txnPool.pool[pos]);
            ++pos;
        }

        ParamBF blockParam = paramBF;
        blockParam.nItems = blockItemCount;
        vector<vector<Transaction>> vvTxns;
        queue<RootType> qTypes;

        for (auto agg : supportAggs) {
            vector<Transaction> sorted = blockTxns;
            stable_sort(sorted.begin(), sorted.end(), comps[agg]);
            vvTxns.emplace_back(sorted);
            qTypes.emplace((RootType)(agg * 2));

            if (combineCycle) {
                vector<Transaction> merged(combinedByAgg[agg].size() + sorted.size());
                merge(combinedByAgg[agg].begin(), combinedByAgg[agg].end(),
                      sorted.begin(), sorted.end(), merged.begin(), comps[agg]);
                combinedByAgg[agg].swap(merged);
            }
        }
        if (combineCycle) combinedItemCount += blockItemCount;

        const bool closesCombinedGroup = combineCycle && (timestamp % combineCycle == 0);
        if (closesCombinedGroup) {
            for (auto agg : supportAggs) {
                vvTxns.emplace_back(combinedByAgg[agg]);
                qTypes.emplace((RootType)(agg * 2 + 1));
                combinedByAgg[agg].clear();
            }
            chain.emplace_back(new Block(timestamp, vvTxns, qTypes, blockParam, nSeed,
                                         earlyStop, adsType, combinedItemCount));
            combinedItemCount = 0;
        } else {
            chain.emplace_back(new Block(timestamp, vvTxns, qTypes, blockParam, nSeed,
                                         earlyStop, adsType));
        }
    }
}

bool Chain::support(AggType agg) const
{
    return supportAggs.count(agg);
}

Block* Chain::getBlock(uint32_t timestamp) const
{
    return chain[timestamp - 1];
}

uint32_t Chain::getSize() const
{
    return chain.size();
}

uint32_t Chain::getCombineCycle() const
{
    return combineCycle;
}

double Chain::getsizeADS() const
{
    if (chain.empty()) return 0;
    return getTotalSizeADS() / chain.size();
}

double Chain::getTotalSizeADS() const
{
    double sizeADS = 0;
    for (auto block : chain) sizeADS += block->getsizeADS();
    return sizeADS;
}

void Chain::display() const
{
    cout << "======== Chain block list ========" << endl;
    for (Block* block : chain) {
        block->display();
    }
}