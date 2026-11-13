#include "bf.h"

#include <iostream>
#include <sstream>
#include <bitset>
#include <math.h>
#include <assert.h>

#define LN2SQUARED 0.4804530139182014246671025263266649717305529515945455
#define LN2 0.6931471805599453094172321214581765680755001343602552

                                                                                                                             
                                            
                                                                                    
static inline uint32_t FastMod(uint32_t x, size_t n) {
    return ((uint64_t)x * (uint64_t)n) >> 32;
}


ParamBF::ParamBF(unsigned int nCurrentItemsIn, unsigned int nItemsIn, double nFPRateIn, unsigned int nTweakIn):
    nCurrentItems(nCurrentItemsIn),
    nItems(nItemsIn),
    nFPRate(nFPRateIn),
    nTweak(nTweakIn)
{
}

                                                                             

BloomFilter::BloomFilter(const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn):
	                                                                   
	vData(min((unsigned int)floor(-1  / LN2SQUARED * nItemsIn * log(nFPRateIn)), MAX_BLOOM_FILTER_SIZE * 8) / 8),
	nHashFuncs(min((unsigned int)ceil(-log(nFPRateIn) / LN2), MAX_HASH_FUNCS)),
	nFPRate(nFPRateIn),
	nItems(nItemsIn),
	nTweak(nTweakIn),
    nCurrentItems(0),
    vHash()
                  
{
}

BloomFilter::BloomFilter(const unsigned int nCurrentItemsIn, const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn):
                                                                                            
    vData(min((unsigned int)1 << (int)floor(log2(-1  / LN2SQUARED * nCurrentItemsIn * log(nFPRateIn))), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
    nHashFuncs(min((unsigned int)ceil(-log(nFPRateIn) / LN2), MAX_HASH_FUNCS)),
    nFPRate(nFPRateIn),
    nItems(nItemsIn),
    nTweak(nTweakIn),
    nCurrentItems(nCurrentItemsIn),
    vHash()
                  
{
}

BloomFilter::BloomFilter(ParamBF paramBF):
                                                                                        
        vData(min((unsigned int)1 << (int)floor(log2(-1  / LN2SQUARED * paramBF.nCurrentItems * log(paramBF.nFPRate))), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
        nHashFuncs(min((unsigned int)ceil(-log(paramBF.nFPRate) / LN2), MAX_HASH_FUNCS)),
        nFPRate(paramBF.nFPRate),
        nItems(paramBF.nItems),
        nTweak(paramBF.nTweak),
        nCurrentItems(paramBF.nCurrentItems),
        vHash()
                      
{
}

BloomFilter::BloomFilter(const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn, const vector<unsigned char>& vDataIn):
	                                                                   
	vData(min((unsigned int)floor(-1  / LN2SQUARED * nItemsIn * log(nFPRateIn)), MAX_BLOOM_FILTER_SIZE * 8) / 8),
	nHashFuncs(min((unsigned int)ceil(-log(nFPRateIn) / LN2), MAX_HASH_FUNCS)),
	nFPRate(nFPRateIn),
	nItems(nItemsIn),
	nTweak(nTweakIn),
    nCurrentItems(0),
    vHash()
                  
{
	assert(vDataIn.size() == vData.size());
	vData.assign(vDataIn.begin(), vDataIn.end());
}

BloomFilter::BloomFilter(const unsigned int nCurrentItemsIn, const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn, const vector<unsigned char>& vDataIn):
                                                                   
    vData(min((unsigned int)1 << (int)floor(log2(-1  / LN2SQUARED * nCurrentItemsIn * log(nFPRateIn))), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
    nHashFuncs(min((unsigned int)ceil(-log(nFPRateIn) / LN2), MAX_HASH_FUNCS)),
    nFPRate(nFPRateIn),
    nItems(nItemsIn),
    nTweak(nTweakIn),
    nCurrentItems(nCurrentItemsIn),
    vHash()
                  
{
    assert(vDataIn.size() == vData.size());
    vData.assign(vDataIn.begin(), vDataIn.end());
}

BloomFilter::BloomFilter(ParamBF paramBF, const vector<unsigned char>& vDataIn):
                                                                   
        vData(min((unsigned int)1 << (int)floor(log2(-1  / LN2SQUARED * paramBF.nCurrentItems * log(paramBF.nFPRate))), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
        nHashFuncs(min((unsigned int)ceil(-log(paramBF.nFPRate) / LN2), MAX_HASH_FUNCS)),
        nFPRate(paramBF.nFPRate),
        nItems(paramBF.nItems),
        nTweak(paramBF.nTweak),
        nCurrentItems(paramBF.nCurrentItems),
        vHash()
                      
{
    assert(vDataIn.size() == vData.size());
    vData.assign(vDataIn.begin(), vDataIn.end());
}

                                                   
    
                      
                          
                        
                                
                        
    

unsigned int BloomFilter::Hash(unsigned int nHashNum, const vector<unsigned char>& vDataToHash) const
{
    unsigned int hash;
                                                                                              
    MurmurHash3_x86_32(vDataToHash.data(), vDataToHash.size(), nHashNum * 0xFBA4C795 + nTweak, &hash);
    return hash % (vData.size() * 8);
}

void BloomFilter::insert(const vector<unsigned char>& vKey)
{
	if (vData.empty())                                        
        return;
    for (unsigned int i = 0; i < nHashFuncs; i++) {
        unsigned int nIndex = Hash(i, vKey);
                                   
        vData[nIndex >> 3] |= (1 << (7 & nIndex));
    }
}


void BloomFilter::insert(const string& word)
{
    vector<unsigned char> data(word.begin(), word.end());
    insert(data);
}

bool BloomFilter::contain(const vector<unsigned char>& vKey) const
{
    if (vData.empty())                                        
        return true;
    for (unsigned int i = 0; i < nHashFuncs; i++) {
        unsigned int nIndex = Hash(i, vKey);
                                     
        if (!(vData[nIndex >> 3] & (1 << (7 & nIndex))))
            return false;
    }
    return true;
}

bool BloomFilter::contain(const string& word) const
{
    vector<unsigned char> data(word.begin(), word.end());
    return contain(data);
}

bool BloomFilter::batchContain(const vector<vector<string>>& wordsCNF) const
{
    bool resDNF;
    for (const vector<string>& wordsDNF : wordsCNF) {
        resDNF = false;
        if (wordsDNF.empty()) {
            resDNF = true;
        }
        else {
            for (const string& word : wordsDNF) {
                if (contain(word)) {
                    resDNF = true;
                    break;
                }
            }
        }
        if (!resDNF)
            return false;
    }
    return true;
}

bool BloomFilter::checkConsistency(const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn) const
{
	return nItems == nItemsIn && nFPRate == nFPRateIn && nTweak == nTweakIn;
}

BloomFilter* BloomFilter::merge(BloomFilter* other)
{
	                                           
	assert(other->checkConsistency(nItems, nFPRate, nTweak));

	BloomFilter* result = new BloomFilter(nItems, nFPRate, nTweak);
	for (int i = 0; i < vData.size(); i++) {
		result->vData[i] = vData[i] | other->vData[i];
	}

	return result;
}

BloomFilter* BloomFilter::reconstruct(BloomFilter* other, const vector<unsigned char>& hint)
{
    return merge(other);
}

string BloomFilter::getStrBF() const {
	string str(vData.begin(), vData.end());
	return str;
}

vector<unsigned char> BloomFilter::getVecBF() const {
	return vData;
}

string BloomFilter::getBinBF() const {
                  
    stringstream ss;
    for (int i = 0; i < vData.size(); i++) {
        ss << std::bitset<8>(vData[i]) << '|';
    }
    return ss.str();
}

size_t BloomFilter::getSize() const {
    return vData.size();
}

vector<unsigned char> BloomFilter::getHint()
{
    return {};
}


                                                                                   

MergeBloomFilter::MergeBloomFilter(const unsigned int nCurrentItemsIn, const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn):
    BloomFilter(nCurrentItemsIn, nItemsIn, nFPRateIn, nTweakIn),
               
                                      
    nTotalSize(min((unsigned int)floor(-1  / LN2SQUARED * nItemsIn * log(nFPRateIn)), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
    hint(0)
{
}

MergeBloomFilter::MergeBloomFilter(ParamBF paramBF):
    BloomFilter(paramBF),
               
                                      
    nTotalSize(min((unsigned int)floor(-1  / LN2SQUARED * paramBF.nItems * log(paramBF.nFPRate)), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
    hint(0)
{
}

MergeBloomFilter::MergeBloomFilter(const unsigned int nCurrentItemsIn, const unsigned int nItemsIn, const double nFPRateIn, const unsigned int nTweakIn, const vector<unsigned char>& vDataIn):
    BloomFilter(nCurrentItemsIn, nItemsIn, nFPRateIn, nTweakIn, vDataIn),
               
                                      
    nTotalSize(min((unsigned int)floor(-1  / LN2SQUARED * nItemsIn * log(nFPRateIn)), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
    hint(0)
{
}

MergeBloomFilter::MergeBloomFilter(ParamBF paramBF, const vector<unsigned char>& vDataIn):
    BloomFilter(paramBF, vDataIn),
               
                                      
    nTotalSize(min((unsigned int)floor(-1  / LN2SQUARED * paramBF.nItems * log(paramBF.nFPRate)), MAX_BLOOM_FILTER_SIZE * 8) * 2 / 8),
    hint(0)
{
}

unsigned int MergeBloomFilter::Hash(unsigned int nHashNum, const vector<unsigned char>& vDataToHash) const
{
    unsigned int hash;
                                                                                              
    MurmurHash3_x86_32(vDataToHash.data(), vDataToHash.size(), nHashNum * 0xFBA4C795 + nTweak, &hash);
    return hash % (nTotalSize * 8);
}

void MergeBloomFilter::insert(const vector<unsigned char>& vKey)
{
    if (vData.empty())                                        
        return;

    unsigned int indexLen = log2(vData.size() * 8 / 2);

    for (unsigned int i = 0; i < nHashFuncs; i++) {
        unsigned int nHash = Hash(i, vKey);                             
        unsigned int nIndex = nHash & (1 << indexLen) - 1;
                                                 
        unsigned int nFlag = nHash & (1 << indexLen) ? 2 : 1;

                          
        vHash.push_back(nHash);
                                     
        vData[nIndex >> 2] |= (nFlag << (3 & nIndex) * 2);
    }
}


void MergeBloomFilter::insert(const string& word)
{
    vector<unsigned char> data(word.begin(), word.end());
    insert(data);
}

bool MergeBloomFilter::contain(const vector<unsigned char>& vKey) const
{
    if (vData.empty())                                        
        return true;

    unsigned int indexLen = log2(vData.size() * 8 / 2);

    for (unsigned int i = 0; i < nHashFuncs; i++) {
        unsigned int nHash = Hash(i, vKey);
        unsigned int nIndex = nHash & (1 << indexLen) - 1;

                                     
                                                                    
        if (((vData[nIndex >> 2] >> (3 & nIndex) * 2) & 3) == 0) {
            return false;
        }
    }
    return true;
}

bool MergeBloomFilter::contain(const string& word) const
{
    if (word.empty()) {
        return true;
    }
    vector<unsigned char> data(word.begin(), word.end());
    return contain(data);
}

bool MergeBloomFilter::batchContain(const vector<vector<string>>& wordsCNF) const
{
    bool resDNF;
    for (const vector<string>& wordsDNF : wordsCNF) {
        resDNF = false;
        if (wordsDNF.empty()) {
            resDNF = true;
        }
        else {
            for (const string& word : wordsDNF) {
                if (contain(word)) {
                    resDNF = true;
                    break;
                }
            }
        }
        if (!resDNF)
            return false;
    }
    return true;
}

inline bool MergeBloomFilter::checkNumVHash() const
{
    return vHash.size() == nCurrentItems * nHashFuncs;
}

MergeBloomFilter* MergeBloomFilter::merge(BloomFilter* other)
{
                                               
    assert(other->checkConsistency(nItems, nFPRate, nTweak));
    assert(checkNumVHash());
                                      

    unsigned int resCurrentItems = nCurrentItems + other->nCurrentItems;
    MergeBloomFilter* result = new MergeBloomFilter(resCurrentItems, nItems, nFPRate, nTweak);

    result->vHash.insert(result->vHash.end(),vHash.begin(),vHash.end());
    result->vHash.insert(result->vHash.end(),other->vHash.begin(),other->vHash.end());

    unsigned int indexLen = log2(result->vData.size() * 8 / 2);

    for (const auto& nHash : result->vHash) {
        unsigned int nIndex = nHash & (1 << indexLen) - 1;
                                                 
        unsigned int nFlag = nHash & (1 << indexLen) ? 2 : 1;

                                     
        result->vData[nIndex >> 2] |= (nFlag << (3 & nIndex) * 2);
    }

    result->genHint();
    return result;
}



MergeBloomFilter* MergeBloomFilter::reconstruct(BloomFilter* other,  const vector<unsigned char>& hint)
{
    assert(other->checkConsistency(nItems, nFPRate, nTweak));

    unsigned int resCurrentItems = nCurrentItems + other->nCurrentItems;
    MergeBloomFilter* result = new MergeBloomFilter(resCurrentItems, nItems, nFPRate, nTweak);
                                                                              

    assert(vData.size() == other->vData.size());
    if (vData.size() != other->vData.size()) {
        delete result;
        throw runtime_error("MergeBloom reconstruction received unequal child filters");
    }
                                                             
                                                                             
                                                                            
                                                                              
                                                                               
                                                                              
                                                        
    if (result->vData.size() == vData.size()) {
        for (size_t i = 0; i < vData.size(); ++i) {
            result->vData[i] = vData[i] | other->vData[i];
        }
        return result;
    }
    if (result->vData.size() != 2 * vData.size()) {
        delete result;
        throw runtime_error("unsupported MergeBloom reconstruction growth geometry");
    }

    vector<unsigned char> vec1(vData.size());
    vector<unsigned char> vec2(other->vData.size());
    vector<unsigned char> vec3(result->vData.size() / 2);

    unsigned int len1 = vec1.size() * 8 / 2;
    unsigned int len2 = vec2.size() * 8 / 2;

    for (unsigned int i = 0; i < vData.size() * 8 / 2; i++) {
        unsigned int nFlag = (vData[i >> 2] >> (3 & i) * 2) & 3;
        switch (nFlag) {
            case 0:
                break;
            case 1:
                vec1[i >> 3] |= (1 << (7 & i));
                break;
            case 2:
                if (len1 > 4) {
                    vec1[(i | len1) >> 3] |= (1 << (7 & i));
                } else{
                    vec1[(i | len1) >> 3] |= (1 << (7 & i) << 4);
                }
                break;
            case 3:
                vec1[i >> 3] |= (1 << (7 & i));
                if (len1 > 4) {
                    vec1[(i | len1) >> 3] |= (1 << (7 & i));
                } else{
                    vec1[(i | len1) >> 3] |= (1 << (7 & i) << 4);
                }
                break;
        }
    }

    for (unsigned int i = 0; i < other->vData.size() * 8 / 2; i++) {
        unsigned int nFlag = (other->vData[i >> 2] >> (3 & i) * 2) & 3;
        switch (nFlag) {
            case 0:
                break;
            case 1:
                vec2[i >> 3] |= (1 << (7 & i));
                break;
            case 2:
                if (len2 > 4) {
                    vec2[(i | len2) >> 3] |= (1 << (7 & i));
                } else{
                    vec2[(i | len2) >> 3] |= (1 << (7 & i) << 4);
                }
                break;
            case 3:
                vec2[i >> 3] |= (1 << (7 & i));
                if (len2 > 4) {
                    vec2[(i | len2) >> 3] |= (1 << (7 & i));
                } else{
                    vec2[(i | len2) >> 3] |= (1 << (7 & i) << 4);
                }
                break;
        }
    }


    for (unsigned int i = 0; i < vec3.size(); i++) {
        vec3[i] = vec1[i] | vec2[i];
    }

    unsigned int counter = 0;
    for (unsigned int i = 0; i < vec3.size() * 8; i++) {
        unsigned int nBit = vec3[i >> 3] & (1 << (7 & i));
        if (nBit) {     
            unsigned nFlag = (hint[counter >> 2] >> (3 & counter) * 2) & 3;
            counter++;
            switch (nFlag) {
                case 0:
                    break;
                case 1:
                case 2:
                case 3:
                    result->vData[i >> 2] |= (nFlag << (3 & i) * 2);
                    break;
            }
        }
    }

    return result;
}

   
       
             
              
              
                 
   
void MergeBloomFilter::genHint()
{
    unsigned char byteHint = 0;
    unsigned int counter = 0;
    for (unsigned int i = 0; i < vData.size() * 8 / 2; i++) {
        unsigned int nFlag = (vData[i >> 2] >> (3 & i) * 2) & 3;
        switch (nFlag) {
            case 0:
                break;
            case 1:
            case 2:
            case 3:
                byteHint |= nFlag << counter * 2;
                if (++counter == 4) {
                    hint.push_back(byteHint);
                    byteHint = 0;
                    counter = 0;
                }
        }
    }
    if (counter)
        hint.push_back(byteHint);
}

vector<unsigned char> MergeBloomFilter::getHint()
{
    return hint;
}
