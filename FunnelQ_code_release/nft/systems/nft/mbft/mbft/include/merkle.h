#ifndef MERKLE_H
#define MERKLE_H

//#include <vector>
//#include <stack>
//#include <tuple>
//#include <cstdint>

#include "utils.h"
#include "bf.h"
#include "hash.h"
#include "transaction.h"

// Bloom filter type.

class MerkleNode
{
public:
	MerkleNode() = default;
	MerkleNode(uint32_t hash, unsigned int nSeed);
	MerkleNode(uint32_t hash, unsigned int nSeed, MerkleNode * pLeftChild, MerkleNode * pRightChild);
	MerkleNode(const vector<uint32_t>& vHashes, unsigned int nSeed);
    MerkleNode(const vector<Transaction>& vTxns, unsigned int nSeed);

	~MerkleNode();

	uint32_t Hash(uint32_t hash1, uint32_t hash2) const;

	uint32_t getHash();

    virtual size_t getSizeADS();

//	virtual MerkleNode* getLeftChild();
//	virtual MerkleNode* getRightChild();
    MerkleNode* getLeftChild();
    MerkleNode* getRightChild();
	
	// virtual void setChildrenNull();
    void setChildrenNull();

	// virtual void printBFS();
    virtual void printBFS();

protected:
    virtual vector<MerkleNode *> Merge(vector<MerkleNode *>& vChildNodes);

    // Property
	uint32_t hash;	// Current implementation uses MurmurHash.
	unsigned int nSeed;

	MerkleNode* pLeftChild;
	MerkleNode* pRightChild;
};

class MerkleBFNode: public MerkleNode
{
public:
    MerkleBFNode() = default;
	// MerkleBFNode(uint32_t hash, uint32_t lBound, uint32_t uBound, BloomFilter* bf, unsigned int nSeed); // for verify reconstruct
    MerkleBFNode(const Transaction& txn, ParamBF param, unsigned int nSeed, AggType aggType, bool earlyStop, ADSType adsType, int leafTxnIndex); // leaf
    MerkleBFNode(uint32_t hash, BloomFilter* bf, unsigned int nSeed, uint32_t lBound, uint32_t uBound, double value,
                 MerkleBFNode * pLeftChild, MerkleBFNode * pRightChild);                                                // non-leaf
    // Persistence-only constructor. It restores either a leaf or an internal
    // node without re-running MergeBloom construction.
    MerkleBFNode(uint32_t hash, BloomFilter* bf, unsigned int nSeed, uint32_t lBound, uint32_t uBound,
                 double value, bool earlyStop, ADSType adsType, int leafTxnIndex,
                 MerkleBFNode* pLeftChild, MerkleBFNode* pRightChild);
	// MerkleBFNode(const vector<uint32_t>& vHashes, const vector<uint32_t>& vAmounts, const vector<BloomFilter*>& vBFs, unsigned int nSeed);
    MerkleBFNode(const vector<Transaction>& vTxns, ParamBF paramBF, unsigned int nSeed, AggType aggType, bool earlyStop, ADSType adsType);

	~MerkleBFNode();

	uint32_t Hash(uint32_t hash1, uint32_t hash2, uint32_t lBound, uint32_t uBound, double value, BloomFilter * pBF) const;

	BloomFilter* getBF();

    bool isEarlyStop() const;
    double getValue() const;
    uint32_t getLowerBound() const;
    uint32_t getUpperBound() const;

    size_t getSizeADS() const;

    int getLeafTxnIndex() const;

//	MerkleBFNode* getLeftChild();
//	MerkleBFNode* getRightChild();

	bool hasChildren();
	// bool contain(const uint32_t& word) const;
    bool contain(const string& word) const;
    bool batchContain(const vector<vector<string>>& wordsCNF) const;
    bool overlap(const uint32_t& lBound, const uint32_t& uBound) const;
    // Both contain and overlap returns ture
    bool satisfy(const vector<vector<string>>& words, const uint32_t& lBound, const uint32_t& uBound, const double& value) const;

//	void setChildrenNull();
    void setPtrNull();

    void printBFS() override;

//  void updateHash(uint32_t hash);
//  void updateBF(BloomFilter bf);

protected:
    vector<MerkleBFNode *> Merge(vector<MerkleBFNode *>& vChildNodes);

    // Property
	BloomFilter* bf;
    uint32_t lowerBound;
    uint32_t upperBound;

    double value;
    bool earlyStop;
    ADSType adsType;

    int leafTxnIndex = -1;
};

#endif
