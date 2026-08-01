#ifndef BF_H
#define BF_H

//#include <vector>
//#include <set>
//#include <string>
//#include <cstdint>

#include "utils.h"
#include "hash.h"

//! 20,000 items with fp rate < 0.1% or 10,000 items and <0.0001%
static const unsigned int MAX_BLOOM_FILTER_SIZE = 32768; // bytes, 2^15
static const unsigned int MAX_HASH_FUNCS = 50;

struct ParamBF{
    ParamBF() = default;
    ParamBF(unsigned int nCurrentItems, unsigned int nItems, double nFPRate, unsigned int nTweak);
    unsigned int nCurrentItems;
    unsigned int nItems;
    double nFPRate;
    unsigned int nTweak;
};


class BloomFilter
{
public:
	BloomFilter() = default;
    ~BloomFilter() = default;
    // For standard BF
	BloomFilter(unsigned int nItems, double nFPRate, unsigned int nTweak);
    // For mergeBF, since it requires a 2-bit counter for each bit.
    BloomFilter(unsigned int nCurrentItems, unsigned int nItems, double nFPRate, unsigned int nTweak);
    explicit BloomFilter(ParamBF paramBF);

    // Usage: when clients receive vData, used to reconstruct the bloom filter.
	BloomFilter(unsigned int nItems, double nFPRate, unsigned int nTweak, const vector<unsigned char>& vData);  // for standard BF
    BloomFilter(unsigned int nCurrentItems, unsigned int nItems, double nFPRate, unsigned int nTweak, const vector<unsigned char>& vData); // for MBF
    BloomFilter(ParamBF paramBF, const vector<unsigned char>& vData); // for MBF
	// BloomFilter(const BloomFilter& obj);
	
	virtual void insert(const vector<unsigned char>& vKey);
	// virtual void insert(const uint32_t& word);
    virtual void insert(const string& word);

	virtual bool contain(const vector<unsigned char>& vKey) const;
	// virtual bool contain(const uint32_t& word) const;
    virtual bool contain(const string& word) const;
    // virtual bool batchContain(const set<uint32_t>& words) const;
    virtual bool batchContain(const vector<vector<string>>& wordsCNF) const;

	bool checkConsistency(unsigned int nItems, double nFPRate, unsigned int nTweak) const;

	virtual BloomFilter* merge(BloomFilter* other);
    virtual BloomFilter* reconstruct(BloomFilter* other, const vector<unsigned char>& hint);

	string getStrBF() const;
	vector<unsigned char> getVecBF() const;
    string getBinBF() const;

    size_t getSize() const;

    virtual vector<unsigned char> getHint();

// protected:
	vector<unsigned char> vData;
	double nFPRate;
	unsigned int nItems;
	unsigned int nHashFuncs;
    unsigned int nTweak;

    unsigned int nCurrentItems;
    vector<uint32_t> vHash;
    // vector<unsigned int> vCapacity;

	virtual unsigned int Hash(unsigned int nHashNum, const vector<unsigned char>& vDataToHash) const;
};


class MergeBloomFilter: public BloomFilter
{
public:
    MergeBloomFilter() = default;
    MergeBloomFilter(unsigned int nCurrentItems, unsigned int nItems, double nFPRate, unsigned int nTweak);
    // Initialize with given parameters.
    explicit MergeBloomFilter(ParamBF paramBF);
    MergeBloomFilter(unsigned int nCurrentItems, unsigned int nItems, double nFPRate, unsigned int nTweak, const vector<unsigned char>& vData);
    // Initialized with raw data to reconstruct the MBF.
    MergeBloomFilter(ParamBF paramBF, const vector<unsigned char>& vData);

    void insert(const vector<unsigned char>& vKey) override;
    // virtual void insert(const uint32_t& word);
    void insert(const string& word) override;

    bool contain(const vector<unsigned char>& vKey) const override;
    // virtual bool contain(const uint32_t& word) const;
    bool contain(const string& word) const override;
    // virtual bool batchContain(const set<uint32_t>& words) const;
    bool batchContain(const vector<vector<string>>& wordsCNF) const override;

    MergeBloomFilter* merge(BloomFilter* other) override;
    MergeBloomFilter* reconstruct(BloomFilter* other,  const vector<unsigned char>& hint) override;

    void genHint();
    vector<unsigned char> getHint() override;

    bool checkNumVHash() const;

// private:
    unsigned int nTotalSize;

    vector<unsigned char> hint;

    unsigned int Hash(unsigned int nHashNum, const vector<unsigned char>& vDataToHash) const override;
};

class DynamicBloomFilter: public BloomFilter
{
public:
    DynamicBloomFilter() = default;
    DynamicBloomFilter(unsigned int nBFs, unsigned int nItems, double nFPRate, unsigned int nTweak);
    DynamicBloomFilter(unsigned int nBFs, unsigned int nItems, double nFPRate, unsigned int nTweak, const vector<unsigned char>& vData, const vector<unsigned int>& nCurrentItemsIn);

    virtual void insert(const vector<unsigned char>& vKey);
    virtual void insert(const uint32_t& txid);

    virtual bool contain(const vector<unsigned char>& vKey) const;
    virtual bool contain(const uint32_t& txid) const;

    virtual DynamicBloomFilter* merge(BloomFilter* other);

    virtual unsigned int Hash(unsigned int nHashNum, const vector<unsigned char>& vDataToHash) const;

    unsigned int singleSize;
    unsigned int singleCapacity;
    unsigned int nBFs;

};

#endif