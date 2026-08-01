#include "merkle.h"

#include <string>
#include <queue>
#include <sstream>
#include <iostream>
#include <assert.h>
#include <algorithm>

// Merkle Node -------------------------------------------------------------

uint32_t MerkleNode::Hash(const uint32_t hash1, const uint32_t hash2) const
{
	string str1 = to_string(hash1);
	string str2 = to_string(hash2);
	string str = str1 + str2;
	vector<unsigned char> vec(str.begin(), str.end());
	uint32_t hash;
    MurmurHash3_x86_32(vec.data(), vec.size(), nSeed, &hash);
    return hash;
}

MerkleNode* MerkleNode::getLeftChild(){
	return pLeftChild;
}

MerkleNode* MerkleNode::getRightChild(){
	return pRightChild;
}

void MerkleNode::setChildrenNull()
{
	pLeftChild = nullptr;
	pRightChild = nullptr;
}

MerkleNode::MerkleNode(const uint32_t hashIn, const unsigned int nSeedIn):
	hash(hashIn),
	nSeed(nSeedIn),
	pLeftChild(nullptr),
	pRightChild(nullptr)
{
}

MerkleNode::MerkleNode(const uint32_t hashIn, const unsigned int nSeedIn, MerkleNode * pLeftChild, MerkleNode * pRightChild):
	hash(hashIn),
	nSeed(nSeedIn),
	pLeftChild(pLeftChild),
	pRightChild(pRightChild)
{
}

vector<MerkleNode *> MerkleNode::Merge(vector<MerkleNode *>& vChildNodes)
{
	assert(!vChildNodes.empty());
	while(vChildNodes.size() > 1){
		if (vChildNodes.size() & 1) {	// is odd
			vChildNodes.push_back(vChildNodes.back());
		}
		vector<MerkleNode *> vParentNodes(vChildNodes.size() / 2);
		for (int i = 0; i < vParentNodes.size(); i++) {
			uint32_t lHash = vChildNodes[2 * i]->getHash();
			uint32_t rHash = vChildNodes[2 * i + 1]->getHash();
			uint32_t hash = Hash(lHash, rHash);
			vParentNodes[i] = new MerkleNode(hash, nSeed, vChildNodes[2 * i], vChildNodes[2 * i + 1]);
		}

		return Merge(vParentNodes);
	}
	return vChildNodes;
}

MerkleNode::MerkleNode(const vector<uint32_t>& vHashes, const unsigned int nSeedIn):
	nSeed(nSeedIn)
{
	vector<MerkleNode *> leaves(vHashes.size());
	for (int i = 0; i < vHashes.size(); i++) {
		leaves[i] = new MerkleNode(vHashes[i], nSeed);
	}

	MerkleNode * pRoot = Merge(leaves)[0];

	hash = pRoot->getHash();
	pLeftChild = pRoot->getLeftChild();
	pRightChild = pRoot->getRightChild();

	pRoot->setChildrenNull();
	delete pRoot;
}

MerkleNode::MerkleNode(const vector<Transaction>& vTxns, unsigned int nSeedIn):
    nSeed(nSeedIn)
{
    vector<MerkleNode *> leaves(vTxns.size());
    for (int i = 0; i < vTxns.size(); i++) {
        leaves[i] = new MerkleNode(vTxns[i].getTxid(), nSeed);
    }

    MerkleNode * pRoot = Merge(leaves)[0];

    hash = pRoot->getHash();
    pLeftChild = pRoot->getLeftChild();
    pRightChild = pRoot->getRightChild();

    pRoot->setChildrenNull();
    delete pRoot;
}

MerkleNode::~MerkleNode()
{
	if (pLeftChild != nullptr)
		delete pLeftChild;
	if (pLeftChild != pRightChild && pRightChild != nullptr)
		delete pRightChild;
}

uint32_t MerkleNode::getHash() {
	return hash;
}

size_t MerkleNode::getSizeADS() {
    size_t sizeADS = sizeof(hash) + sizeof(nSeed);
    if (pRightChild != nullptr) {
        sizeADS += pRightChild->getSizeADS();
    }
    if (pLeftChild != nullptr && pLeftChild != pRightChild) {
        sizeADS += pLeftChild->getSizeADS();
    }
    return sizeADS;
}

void MerkleNode::printBFS()
{
	queue<MerkleNode *> unvisited;

	unvisited.push(this); // push root node
	unsigned int cNodes = 1;
	unsigned int cPower = 1;

	while (!unvisited.empty()) {
	    MerkleNode * current = (unvisited.front());
	    if (current->pLeftChild != nullptr)
	        unvisited.push(current->pLeftChild);
	    if (current->pRightChild != nullptr)
	        unvisited.push(current->pRightChild);
	    cout << current->getHash() << endl;
	    unvisited.pop();
	    cNodes++;
	    if (cNodes >> cPower){
	    	cout << "============" << endl;
	    	cPower++;
	    }
	}
}

// Merkle BF node -------------------------------------------------------------

uint32_t MerkleBFNode::Hash(const uint32_t hash1, const uint32_t hash2, uint32_t lBound, uint32_t uBound, double value, BloomFilter * pBF) const
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


MerkleBFNode::MerkleBFNode(const Transaction& txn, ParamBF param, unsigned int nSeed, AggType aggType, bool earlyStopIn, ADSType adsTypeIn, int leafTxnIndexIn):
    MerkleNode(txn.getTxid(), nSeed),
    bf(adsTypeIn == MBFT ? new MergeBloomFilter(param) : new BloomFilter(param.nItems, param.nFPRate, param.nTweak)),
    lowerBound(txn.getAmount()),
    upperBound(txn.getAmount()),
    value(txn.getValue(aggType)),
    earlyStop(earlyStopIn),
    adsType(adsTypeIn),
    leafTxnIndex(leafTxnIndexIn)
{
    assert(param.nCurrentItems == txn.getAddresses().size());
    assert(value);
    for (string word : txn.getAddresses()){
        bf->insert(word);
    }
}

MerkleBFNode::MerkleBFNode(const uint32_t hashIn, BloomFilter* bfIn, const unsigned int nSeedIn, uint32_t lBoundIn,
                           uint32_t uBoundIn, double valueIn, MerkleBFNode * pLeftChild, MerkleBFNode * pRightChild):
	MerkleNode(hashIn, nSeedIn, pLeftChild, pRightChild),
	bf(bfIn),
    lowerBound(lBoundIn),
    upperBound(uBoundIn),
    value(valueIn),
    earlyStop(pLeftChild->isEarlyStop()),
    adsType(pLeftChild->adsType)
{
    assert(pLeftChild->isEarlyStop() == pRightChild->isEarlyStop());
    assert(pLeftChild->adsType == pRightChild->adsType);
}

MerkleBFNode::MerkleBFNode(const uint32_t hashIn, BloomFilter* bfIn, const unsigned int nSeedIn,
                           uint32_t lBoundIn, uint32_t uBoundIn, double valueIn,
                           bool earlyStopIn, ADSType adsTypeIn, int leafTxnIndexIn,
                           MerkleBFNode* pLeftChild, MerkleBFNode* pRightChild):
    MerkleNode(hashIn, nSeedIn, pLeftChild, pRightChild),
    bf(bfIn),
    lowerBound(lBoundIn),
    upperBound(uBoundIn),
    value(valueIn),
    earlyStop(earlyStopIn),
    adsType(adsTypeIn),
    leafTxnIndex(leafTxnIndexIn)
{
}

MerkleBFNode::MerkleBFNode(const vector<Transaction>& vTxns, ParamBF paramBFIn, unsigned int nSeed, AggType aggType, bool earlyStopIn, ADSType adsTypeIn):
        MerkleNode(0, nSeed),
        bf(nullptr),
        earlyStop(earlyStopIn),
        adsType(adsTypeIn)
//    pLeftChild(nullptr),
//    pRightChild(nullptr)
{
    vector<MerkleBFNode *> leaves(vTxns.size());
    ParamBF paramBF = paramBFIn;
    for (int i = 0; i < vTxns.size(); i++) {
        paramBF.nCurrentItems = vTxns[i].getAddresses().size();
        leaves[i] = new MerkleBFNode(vTxns[i], paramBF, nSeed, aggType, earlyStop, adsType, i);
    }

    MerkleBFNode * pRoot = Merge(leaves)[0];

    hash = pRoot->getHash();
    pLeftChild = pRoot->getLeftChild();
    pRightChild = pRoot->getRightChild();
    lowerBound = pRoot->getLowerBound();
    upperBound = pRoot->getUpperBound();

    bf = pRoot->getBF();
    value = pRoot->getValue();
    // Preserve the leaf index when a block contains exactly one transaction.
    // The original MAX workload never needed to open this wrapper as a report
    // leaf, but select-all must recover its canonical event payload.
    leafTxnIndex = pRoot->getLeafTxnIndex();
    // The final root is never merged again after construction.
    vector<uint32_t>().swap(bf->vHash);

    pRoot->setPtrNull();

    delete pRoot;
}

vector<MerkleBFNode *> MerkleBFNode::Merge(vector<MerkleBFNode *>& vChildNodes)
{
	assert(!vChildNodes.empty());
    MerkleBFNode* left;
    MerkleBFNode* right;
    BloomFilter* parentBF;
	while(vChildNodes.size() > 1){
		if (vChildNodes.size() & 1) {	// is odd
			vChildNodes.push_back(vChildNodes.back());
		}
		vector<MerkleBFNode *> vParentNodes(vChildNodes.size() / 2);
		for (int i = 0; i < vParentNodes.size(); i++) {
			left = vChildNodes[2 * i];
			right = vChildNodes[2 * i + 1];

			// merge bf using the original MergeBloom construction. Once the
            // parent has copied both hash-position lists, the child build-only
            // vHash vectors are no longer required by query generation or hint
            // reconstruction. Releasing them preserves identical vData, hints,
            // and node hashes while avoiding O(n log n) retained build memory.
			parentBF = left->bf->merge(right->bf);
            vector<uint32_t>().swap(left->bf->vHash);
            if (right != left) vector<uint32_t>().swap(right->bf->vHash);

            // compute hash
			uint32_t lHash = left->getHash();
			uint32_t rHash = right->getHash();
            uint32_t lBound = min(left->getLowerBound(), right->getLowerBound());
            uint32_t uBound = max(left->getUpperBound(), right->getUpperBound());
            double value = max(left->getValue(), right->getValue());
			uint32_t hash = Hash(lHash, rHash, lBound, uBound, value, parentBF);
			vParentNodes[i] = new MerkleBFNode(hash, parentBF, nSeed, lBound, uBound, value, left, right);
		}

		return Merge(vParentNodes);
	}
	return vChildNodes;
}

MerkleBFNode::~MerkleBFNode()
{
    if (bf != nullptr)
        delete bf;
}


BloomFilter* MerkleBFNode::getBF()
{
	return bf;
}

bool MerkleBFNode::isEarlyStop() const
{
    return earlyStop;
}

double MerkleBFNode::getValue() const
{
    return value;
}

uint32_t MerkleBFNode::getLowerBound() const
{
    return lowerBound;
}

uint32_t MerkleBFNode::getUpperBound() const
{
    return upperBound;
}

int MerkleBFNode::getLeafTxnIndex() const {
    return leafTxnIndex;
}

size_t MerkleBFNode::getSizeADS() const {
    size_t sizeADS = sizeof(hash) + sizeof(nSeed);
    sizeADS += sizeof(lowerBound) + sizeof(upperBound) + sizeof(value);
    sizeADS += bf->vData.size() * sizeof(unsigned char);
    if (pRightChild != nullptr) {
        sizeADS += ((MerkleBFNode*)pRightChild)->getSizeADS();
    }
    if (pLeftChild != nullptr && pLeftChild != pRightChild) {
        sizeADS += ((MerkleBFNode*)pLeftChild)->getSizeADS();
    }
    return sizeADS;
}

void MerkleBFNode::setPtrNull()
{
	pLeftChild = nullptr;
	pRightChild = nullptr;
    bf = nullptr;
}

void MerkleBFNode::printBFS()
{
	queue<MerkleNode *> unvisited;

	unvisited.push(this); // push root node
	unsigned int cNodes = 1;
	unsigned int cPower = 1;

	while (!unvisited.empty()) {
	    MerkleBFNode * current = (MerkleBFNode*)unvisited.front();
	    if (current->getLeftChild() != nullptr)
	        unvisited.push(current->getLeftChild());
	    if (current->getRightChild() != nullptr)
	        unvisited.push(current->getRightChild());
	    // cout << current->getHash() << " " << current->getBF().getBinBF()<< endl;
        cout << current->getHash()<< endl;
		cout << "nCurrentItems: " << current->getBF()->nCurrentItems << endl;
	    unvisited.pop();
	    cNodes++;
	    if (cNodes >> cPower){
	    	cout << "============" << endl;
	    	cPower++;
	    }
	}
}


bool MerkleBFNode::contain(const string& word) const
{
    return bf->contain(word);
}

bool MerkleBFNode::batchContain(const vector<vector<string>>& wordsCNF) const
{
    return bf->batchContain(wordsCNF);
}

bool MerkleBFNode::overlap(const uint32_t& lBound, const uint32_t& uBound) const
{
    return (lBound <= upperBound) && (lowerBound <= uBound);
}


bool MerkleBFNode::satisfy(const vector<vector<string>>& words, const uint32_t& lBound, const uint32_t& uBound, const double& valueIn) const
{
    if (earlyStop) {
        return (value > valueIn) && overlap(lBound, uBound) && batchContain(words);
    }
    return overlap(lBound, uBound) && batchContain(words);
}

bool MerkleBFNode::hasChildren()
{
	return pLeftChild != nullptr && pRightChild != nullptr;
}