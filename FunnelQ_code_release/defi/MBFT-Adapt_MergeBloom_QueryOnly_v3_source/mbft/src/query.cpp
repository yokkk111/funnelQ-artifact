#include "query.h"

#include <utility>
#include <sstream>
#include <iostream>

Query::Query(AggType aggIn, uint32_t beginBlockIn, uint32_t endBlockIn, uint32_t lowerBoundIn, uint32_t upperBoundIn,
             vector<vector<string>> addressesIn, bool earlyStopIn):
    agg(aggIn),
    beginBlock(beginBlockIn),
    endBlock(endBlockIn),
    lowerBound(lowerBoundIn),
    upperBound(upperBoundIn),
    addresses(move(addressesIn)),
    earlyValue(0),
    earlyStop(earlyStopIn)
{
}

bool Query::overlap(const uint32_t& lBoundIn, const uint32_t& uBoundIn, const double& valueIn) const
{

    if (earlyStop) {
        bool res = (valueIn > earlyValue) && (lBoundIn <= upperBound) && (lowerBound <= uBoundIn);
        // cout << "res: " << res << endl;
        return res;
    }
    bool res = (lBoundIn <= upperBound) && (lowerBound <= uBoundIn);
    // cout << "res: " << res << endl;
    return res;
}

void Query::printAddresses() const {
    for (const auto& vaddr : addresses) {
        cout << "{";
        for (const auto& addr : vaddr) {
            cout << addr << " ";
        }
        cout << "}" << endl;
    }
}

VONode::VONode(MyFlag myFlagIn, uint32_t hashIn, unsigned int nCurrentItemsIn, vector<unsigned char> vDataIn,
               uint32_t lowerBoundIn, uint32_t upperBoundIn, double valueIn, int txnIndexIn):
    myFlag(myFlagIn),
    hash(hashIn),
    nCurrentItems(nCurrentItemsIn),
    vData(move(vDataIn)),
    lowerBound(lowerBoundIn),
    upperBound(upperBoundIn),
    value(valueIn),
    txnIndex(txnIndexIn)
{
}

size_t VONode::getSizeADS() const
{
    size_t sizeADS = sizeof(myFlag) + sizeof(hash) + sizeof(nCurrentItems);
    sizeADS += sizeof(lowerBound) + sizeof(upperBound) + sizeof(value);
    sizeADS += vData.size() * sizeof(unsigned char);
    return sizeADS;
}

size_t VONode::getSizeBF() const
{
    size_t sizeBF = vData.size() * sizeof(unsigned char);
    return sizeBF;
}

ReVONode::ReVONode(MyFlag myFlagIn, BloomFilter* bfIn, uint32_t hashIn, uint32_t lowerBoundIn, uint32_t upperBoundIn, double valueIn):
    myFlag(myFlagIn),
    bf(bfIn),
    hash(hashIn),
    lowerBound(lowerBoundIn),
    upperBound(upperBoundIn),
    value(valueIn)
{
}

// Hash

uint32_t Hash(const uint32_t hash1, const uint32_t hash2, uint32_t lBound, uint32_t uBound, double value, BloomFilter * pBF,
              unsigned int nSeed)
{
    uint32_t hash;
    stringstream ss;
    string strBF = pBF->getStrBF();
    ss << hash1 << hash2 << strBF << lBound << uBound << value << endl;

    string str = ss.str();
    vector<unsigned char> vec(str.begin(), str.end());
    MurmurHash3_x86_32(vec.data(), vec.size(), nSeed, &hash);

    return hash;
}

// DNF boolean

bool match(const vector<string>& addrs, const vector<vector<string>>& wordsCNF)
{
    bool resDNF;
    for (const vector<string>& wordsDNF : wordsCNF) {
        if (wordsDNF.empty()) {
            return true;
        }
        resDNF = false;
        for (const string& word : wordsDNF) {
            if (count(addrs.begin(), addrs.end(), word)) {
                resDNF = true;
                break;
            }
        }
        if (!resDNF)
            return false;
    }
    return true;
}

// Merge BF Query & Verify ------------------------------

//pair<double, stack<VONode>> singleQuery(MerkleBFNode * root, const Query& query) {
pair<double, stack<VONode>> singleQuery(Block* blk, RootType rootType, const Query& query) {
    stack<pair<MyFlag, MerkleBFNode*>> sNode;
    stack<VONode> sVO;
    bool isFound = false;
    MyFlag myFlag;
    MerkleBFNode* current;
    double result = -1;

    MerkleBFNode* root = blk->getRoot(rootType);


    sNode.emplace(pair<MyFlag, MerkleBFNode*>(ROOT, root));

    while (!sNode.empty()) {
        tie(myFlag,current) = sNode.top();
        sNode.pop();

        // If already found result, assert remaining are LEFT
        if (isFound) {
            assert(myFlag == LEFT);
            sVO.emplace(VONode(LEFT, current->getHash(), current->getBF()->nCurrentItems, current->getBF()->getVecBF(),
                               current->getLowerBound(), current->getUpperBound(), current->getValue()));
        }
        // check if satisfy
        else if (current->satisfy(query.addresses, query.lowerBound, query.upperBound, query.earlyValue)) {
            // Internal node matches, get hint, push children
            if (current->hasChildren()) {
                sVO.emplace(VONode(HINT, 0, 0, current->getBF()->getHint(),
                                   current->getLowerBound(), current->getUpperBound(), current->getValue()));
                sNode.emplace(pair<MyFlag, MerkleBFNode*>(LEFT, (MerkleBFNode*)current->getLeftChild()));
                sNode.emplace(pair<MyFlag, MerkleBFNode*>(RIGHT, (MerkleBFNode*)current->getRightChild()));
            }
            // Leaf node matches, it's the result
            else {
                assert(current->getLowerBound() == current->getUpperBound());
                assert(current->getLeafTxnIndex() != -1);
                
                Transaction txn = (rootType % 2 == 0) ? blk->getTxn(current->getLeafTxnIndex()) : blk->getTxnCombine(current->getLeafTxnIndex());

                if (match(txn.getAddresses(), query.addresses)) {
                    // cout << "match leaf" << endl;
                    result = current->getValue();
                    sVO.emplace(VONode(RES, current->getHash(), current->getBF()->nCurrentItems, current->getBF()->getVecBF(),
                                   current->getLowerBound(), current->getUpperBound(), current->getValue()));
                    isFound = true;
                }
                else {
                    if (myFlag == LEFT) {
                        myFlag = ASTRAY;
                    }
                    // cout << "astray leaf txn index: " << current->getLeafTxnIndex() << endl;
                    sVO.emplace(VONode(myFlag, current->getHash(), current->getBF()->nCurrentItems, current->getBF()->getVecBF(),
                               current->getLowerBound(), current->getUpperBound(), current->getValue(), current->getLeafTxnIndex()));
                }                
            }
        }
        // Not satisfy, push node's BF. RIGHT stay RIGHT, LEFT turns ASTRAY
        else {
            if (myFlag == LEFT) {
                // cout << "left astray" << endl;
                myFlag = ASTRAY;
            }

            sVO.emplace(VONode(myFlag, current->getHash(), current->getBF()->nCurrentItems, current->getBF()->getVecBF(),
                               current->getLowerBound(), current->getUpperBound(), current->getValue()));
        }
    }
    return pair<double, stack<VONode>>{result, sVO};
}

// return result value;
double singleVerify(const Query& query, stack<VONode> sVO, Block* blk, RootType rootType, ExperimentMetrics& metrics) {
    time_point<system_clock> start, end, startP, endP;
    startP = system_clock::now();
    duration<double, micro> dur(0);
    
    uint32_t nRootHash = blk->getRoot(rootType)->getHash();
    unsigned int nSeed = blk->getSeed();
    ParamBF paramBF = (rootType % 2 == 0) ? blk->getParamBF() : blk->getParamBFCombine();

    stack<ReVONode> sReVO;
    ReVONode left, right;
    VONode hint;
    double resVal = -1;

    vector<BloomFilter*> bfGC;

    MyFlag myFlag;
    BloomFilter* bf;
    uint32_t hash, lowerBound, upperBound;
    double value;
    int txnIndex;

    int nodeSize = sVO.size();
    int bfSize = 0;

    while(!sVO.empty()) {
        myFlag = sVO.top().myFlag;
        bfSize += sVO.top().getSizeBF();
        // Hint: pop 2 children to reconstruct
        if (myFlag == HINT) {
            start = system_clock::now();

            hint = sVO.top();
            right = sReVO.top();

            sReVO.pop();
            left = sReVO.top();

            sReVO.pop();

            // Reconstruct
            bf = left.bf->reconstruct(right.bf, hint.vData);

            bfGC.emplace_back(bf);
            lowerBound = min(left.lowerBound, right.lowerBound);
            upperBound = max(left.upperBound, right.upperBound);
            assert(lowerBound == hint.lowerBound);
            assert(upperBound == hint.upperBound);
            value = max(left.value, right.value);
            assert(value == hint.value);

            metrics.reconstNum += 1;
            metrics.hintSize += hint.vData.size() * sizeof(unsigned char);
            metrics.astryReconstNum += left.myFlag == ASTRAY ? 1 : 0;

            hash = Hash(left.hash, right.hash, lowerBound, upperBound, value, bf, nSeed);

            myFlag = left.myFlag == ASTRAY ? ASTRAY : RES;
            sReVO.emplace(ReVONode(myFlag, bf, hash, lowerBound, upperBound, value));

            end = system_clock::now();
            dur = duration_cast<microseconds>(end - start);
            metrics.reconstTime += dur.count();
        }
        // Other
        else {
            start = system_clock::now();
            paramBF.nCurrentItems = sVO.top().nCurrentItems;
            if (blk->adsType == MBFT) {
                bf = new MergeBloomFilter(paramBF, sVO.top().vData);
            }
            else {
                bf = new BloomFilter(paramBF.nItems, paramBF.nFPRate, paramBF.nTweak, sVO.top().vData);
            }
            bfGC.emplace_back(bf);
            hash = sVO.top().hash;
            lowerBound = sVO.top().lowerBound;
            upperBound = sVO.top().upperBound;
            value = sVO.top().value;
            txnIndex = sVO.top().txnIndex;
            // Root: verify integrity & non-membership
            switch (myFlag) {
                case ROOT:      // ROOT: Check non-membership
                    assert(sVO.size() == 1);
                case RIGHT:     // RIGHT: check non-membership
                case ASTRAY:    // ASTRAY: check non-membership
                    // non-membership
                    if (txnIndex == -1) {
                        assert(!(query.overlap(lowerBound, upperBound, value) && bf->batchContain(query.addresses)));
                    } else { // leaf astray
                        Transaction txn = (rootType % 2 == 0) ? blk->getTxn(txnIndex) : blk->getTxnCombine(txnIndex);
                        assert(!(query.overlap(lowerBound, upperBound, value) && bf->batchContain(query.addresses) && match(txn.getAddresses(), query.addresses)));
                    }

                    sReVO.emplace(ReVONode(myFlag, bf, hash, lowerBound, upperBound, value));
                    break;

                case LEFT:      // LEFT: check nothing
                    sReVO.emplace(ReVONode(myFlag, bf, hash, lowerBound, upperBound, value));
                    break;

                case RES:       // RES: check membership
                    assert(query.overlap(lowerBound, upperBound, value) && bf->batchContain(query.addresses));
                    resVal = value;

                    sReVO.emplace(ReVONode(myFlag, bf, hash, lowerBound, upperBound, value));
                    break;

                default:
                    throw exception();
                    break;
            }
            end = system_clock::now();
            dur = duration_cast<microseconds>(end - start);
            metrics.otherTime += dur.count();
        }
        sVO.pop();
    }
    assert(sReVO.size() == 1);
    hash = sReVO.top().hash;
    myFlag = sReVO.top().myFlag;

    assert(hash == nRootHash);

    endP = system_clock::now();
    dur = duration_cast<microseconds>(endP -startP);
    metrics.pureTime += dur.count();

    while(!sReVO.empty()) {
        ReVONode reVO = sReVO.top();
        sReVO.pop();

        if (reVO.myFlag == ROOT) { // ROOT: check non-membership
            metrics.rootNum += 1;
            end = system_clock::now();
            dur = duration_cast<microseconds>(end - startP);
            metrics.rootTime += dur.count();
            metrics.rootSize += reVO.bf->getSize() * sizeof(unsigned char);
            start = system_clock::now();
        } else if (reVO.myFlag == RES) { // RES
            metrics.resNum += 1;
            metrics.resNodeNum += nodeSize; // Assuming nodeSize applies here? Need clarification. Maybe totalNodeNum is better.
            end = system_clock::now();
            dur = duration_cast<microseconds>(end - startP);
            metrics.resTime += dur.count();
            metrics.resSize += reVO.bf->getSize() * sizeof(unsigned char);
            start = system_clock::now();
        } else if (reVO.myFlag == ASTRAY || reVO.myFlag == RIGHT || reVO.myFlag == LEFT) { // ASTRAY / RIGHT / LEFT
            metrics.astrayNum += 1;
            metrics.astrayNodeNum += nodeSize; // Assuming nodeSize applies here? Need clarification. Maybe totalNodeNum is better.
            end = system_clock::now();
            dur = duration_cast<microseconds>(end - startP);
            metrics.astrayTime += dur.count();
            metrics.astraySize += reVO.bf->getSize() * sizeof(unsigned char);
            start = system_clock::now();
        }
        // Update total counts regardless of type
        metrics.totalNum += 1;
        metrics.totalNodeNum += nodeSize; // Assuming nodeSize refers to nodes in sVO processed
    }

    for (auto ptr: bfGC)
        delete ptr;

    return resVal;
}

pair<double, queue<stack<VONode>>> chainQueryMBFT(const Chain& chain, Query query)
{
    queue<stack<VONode>> qVO;
    double res = 0;
    stack<VONode> VO;
    double resTmp;

    uint32_t currentBlk = query.beginBlock;
    uint32_t endBlk = query.endBlock;
    assert(1 <= currentBlk && currentBlk <= endBlk && endBlk <= chain.getSize());

    uint32_t combineCycle = chain.getCombineCycle();
    // MerkleBFNode* root;
    Block* blk;

    RootType rootType;

    // Check if the chain is constructed to support the queried aggregation.
    assert(chain.support(query.agg));

    while (currentBlk <= endBlk) {
        rootType = (RootType)(2 * query.agg); // single root type
        if (combineCycle && currentBlk % combineCycle == 1 && currentBlk + combineCycle - 1 <= endBlk) {    // Search Combined MBF-tree
            currentBlk += combineCycle - 1;
            rootType = (RootType)(2 * query.agg + 1); // combine root type
        }
        blk = chain.getBlock(currentBlk);
        tie(resTmp, VO) = singleQuery(blk, rootType, query);

        // Update res & vVO
        if (resTmp > res) {
            res = resTmp;
            query.earlyValue = res;
        }
        qVO.emplace(VO);

        currentBlk += 1;
    }

    return pair<double, queue<stack<VONode>>>{res, qVO};
}

bool chainVerifyMBFT(Query query, double res, queue<stack<VONode>> vVO, const Chain& chain, ExperimentMetrics& metrics) {
    Block* blk;
    uint32_t nRootHash;
    unsigned int nSeed;
    ParamBF paramBF;
    VerifyRes verify;
    double resV;
    VONode topNode;

    uint32_t currentBlk = query.beginBlock;
    uint32_t endBlk = query.endBlock;
    assert(1 <= currentBlk && currentBlk <= endBlk && endBlk <= chain.getSize());

    uint32_t combineCycle = chain.getCombineCycle();

    RootType rootType;

    // Check if the chain is constructed to support the queried aggregation.
    assert(chain.support(query.agg));

    while (currentBlk <= endBlk) {
        rootType = (RootType)(2 * query.agg); // single root type
        if (combineCycle && currentBlk % combineCycle == 1 && currentBlk + combineCycle - 1 <= endBlk) {    // Search Combined MBF-tree
            currentBlk += combineCycle - 1;
            rootType = (RootType)(2 * query.agg + 1); // combine root type
        }
        blk = chain.getBlock(currentBlk);

        // verify = singleVerify(query, vVO.front(), n RootHash, nSeed, paramBF);
        resV = singleVerify(query, vVO.front(), blk, rootType, metrics);


        if (resV > query.earlyValue) {
            query.earlyValue = resV;
        }

        vVO.pop();
        currentBlk += 1;
    }

    assert(vVO.empty());
    assert(query.earlyValue == res);
    return true;
}

pair<double, queue<vector<Transaction>>> chainQueryMHT(const Chain& chain, Query query)
{
    queue<vector<Transaction>> qVO;
    double res = 0;
    double amount;
    vector<string> addrs;
    set<double> distinctPool;

    uint32_t currentBlk = query.beginBlock;
    uint32_t endBlk = query.endBlock;
    assert(1 <= currentBlk && currentBlk <= endBlk && endBlk <= chain.getSize());

    Block* block;
    vector<Transaction> vTxns;

    // Check if it's MHT
    assert(chain.adsType == MHT);

    while (currentBlk <= endBlk) {
        block = chain.getBlock(currentBlk);

        for (Transaction txn : block->getTxns()) {
            amount = txn.getAmount();
            addrs = txn.getAddresses();
            if (amount <= query.upperBound && amount >= query.lowerBound && match(addrs, query.addresses)) {
                switch (query.agg) {
                    case MAX:
                        if (amount > res) {
                            res = amount;
                        }
                        break;
                    case COUNT:
                        res += 1;
                        break;
                    case SUM:
                        res += amount;
                        break;
                    case COUNT_DISTINCT:
                        if (!distinctPool.count(amount)) {
                            distinctPool.emplace(amount);
                        }
                        break;
                }
            }
            vTxns.emplace_back(txn);
        }
        if (query.agg == COUNT_DISTINCT) {
            res = distinctPool.size();
        }

        qVO.emplace(vTxns);
        vTxns.clear();

        currentBlk += 1;
    }

    return pair<double, queue<vector<Transaction>>>{res, qVO};
}

bool chainVerifyMHT(Query query, double resIn, queue<vector<Transaction>> vVO, const Chain& chain)
{
    queue<pair<vector<Transaction>, uint32_t>> qVO;
    double res = 0;
    double amount;
    vector<string> addrs;
    set<double> distinctPool;

    uint32_t currentBlk = query.beginBlock;
    uint32_t endBlk = query.endBlock;
    assert(1 <= currentBlk && currentBlk <= endBlk && endBlk <= chain.getSize());

    Block* block;
    uint32_t hash;
    vector<Transaction> vTxns;
    MerkleNode* root;

    // Check if it's MHT
    assert(chain.adsType == MHT);

    while (currentBlk <= endBlk) {
        block = chain.getBlock(currentBlk);
        hash = block->getRootMHT()->getHash();

        vTxns = vVO.front();
        // Check hash
        root = new MerkleNode(vTxns, chain.nSeed);
        assert(root->getHash() == hash);

        // Update result
        for (Transaction txn : vTxns) {
            amount = txn.getAmount();
            addrs = txn.getAddresses();
            if (amount <= query.upperBound && amount >= query.lowerBound && match(addrs, query.addresses)) {
                switch (query.agg) {
                    case MAX:
                        if (amount > res) {
                            res = amount;
                        }
                        break;
                    case COUNT:
                        res += 1;
                        break;
                    case SUM:
                        res += amount;
                        break;
                    case COUNT_DISTINCT:
                        if (!distinctPool.count(amount)) {
                            distinctPool.emplace(amount);
                        }
                        break;
                }
            }
        }

        vVO.pop();
        vTxns.clear();
        delete root;

        currentBlk += 1;
    }
    if (query.agg == COUNT_DISTINCT) {
        res = distinctPool.size();
    }

    assert(vVO.empty());
    assert(resIn == res);
    return true;
}

pair<double, queue<vector<Transaction>>> chainQueryVChain(const Chain& chain, Query query)
{
    queue<vector<Transaction>> qVO;
    double res = 0;
    double amount;
    vector<string> addrs;
    set<double> distinctPool;

    uint32_t currentBlk = query.beginBlock;
    uint32_t endBlk = query.endBlock;
    assert(1 <= currentBlk && currentBlk <= endBlk && endBlk <= chain.getSize());

    Block* block;
    vector<Transaction> vTxns;

    // Check if it's MHT
    assert(chain.adsType == VCHAIN);

    while (currentBlk <= endBlk) {
        block = chain.getBlock(currentBlk);

        for (Transaction txn : block->getTxns()) {
            amount = txn.getAmount();
            addrs = txn.getAddresses();
            if (amount <= query.upperBound && amount >= query.lowerBound && match(addrs, query.addresses)) {
                switch (query.agg) {
                    case MAX:
                        if (amount > res) {
                            res = amount;
                        }
                        break;
                    case COUNT:
                        res += 1;
                        break;
                    case SUM:
                        res += amount;
                        break;
                    case COUNT_DISTINCT:
                        if (!distinctPool.count(amount)) {
                            distinctPool.emplace(amount);
                        }
                        break;
                }
                vTxns.emplace_back(txn);
            }
        }
        if (query.agg == COUNT_DISTINCT) {
            res = distinctPool.size();
        }
        if (!vTxns.empty()) {
            qVO.emplace(vTxns);
            vTxns.clear();
        }

        currentBlk += 1;
    }

    return pair<double, queue<vector<Transaction>>>{res, qVO};
}

bool chainVerifyVChain(Query query, double resIn, queue<vector<Transaction>> vVO, const Chain& chain)
{
    queue<pair<vector<Transaction>, uint32_t>> qVO;
    double res = 0;
    double amount;
    vector<string> addrs;
    set<double> distinctPool;

    uint32_t currentBlk = query.beginBlock;
    uint32_t endBlk = query.endBlock;
    assert(1 <= currentBlk && currentBlk <= endBlk && endBlk <= chain.getSize());

    Block* block;
    uint32_t hash;
    vector<Transaction> vTxns;
    MerkleNode* root;

    // Check if it's MHT
    assert(chain.adsType == VCHAIN);

    while (!vVO.empty()) {
        vTxns = vVO.front();

        // Update result
        for (Transaction txn : vTxns) {
            amount = txn.getAmount();
            addrs = txn.getAddresses();
            assert (amount <= query.upperBound && amount >= query.lowerBound && match(addrs, query.addresses));
            switch (query.agg) {
                case MAX:
                    if (amount > res) {
                        res = amount;
                    }
                    break;
                case COUNT:
                    res += 1;
                    break;
                case SUM:
                    res += amount;
                    break;
                case COUNT_DISTINCT:
                    if (!distinctPool.count(amount)) {
                        distinctPool.emplace(amount);
                    }
                    break;
            }
        }

        vVO.pop();
        vTxns.clear();
    }
    if (query.agg == COUNT_DISTINCT) {
        res = distinctPool.size();
    }

    assert(vVO.empty());
    assert(resIn == res);
    return true;
}

pair<double, varVO> chainQuery(const Chain& chain, Query query)
{
    switch (chain.adsType) {
        case MBFT: {
            auto [res, qVO] = chainQueryMBFT(chain, query);
            varVO variant_result;
            variant_result.emplace<0>(qVO);
            return std::make_pair(res, variant_result);
        }
        case MBFT_BF: {
            auto [res, qVO] = chainQueryMBFT(chain, query);
            varVO variant_result;
            variant_result.emplace<0>(qVO);
            return std::make_pair(res, variant_result);
        }
        case MHT: {
            auto [res, qVO] = chainQueryMHT(chain, query);
            varVO variant_result;
            variant_result.emplace<1>(qVO);
            return std::make_pair(res, variant_result);
        }
        case VCHAIN: {
            auto [res, qVO] = chainQueryVChain(chain, query);
            varVO variant_result;
            variant_result.emplace<1>(qVO);
            return std::make_pair(res, variant_result);
        }
    }
    throw std::invalid_argument("Invalid chain type");
}

bool chainVerify(Query query, double res, varVO vVO, const Chain& chain, ExperimentMetrics& metrics)
{
    switch (chain.adsType) {
        case MBFT:
            return chainVerifyMBFT(query, res, std::get<0>(vVO), chain, metrics);
        case MBFT_BF:
            return chainVerifyMBFT(query, res, std::get<0>(vVO), chain, metrics);
        case MHT:
            return chainVerifyMHT(query, res, std::get<1>(vVO), chain);
        case VCHAIN:
            return chainVerifyVChain(query, res, std::get<1>(vVO), chain);
    }
    throw std::invalid_argument("Invalid chain type");
}
