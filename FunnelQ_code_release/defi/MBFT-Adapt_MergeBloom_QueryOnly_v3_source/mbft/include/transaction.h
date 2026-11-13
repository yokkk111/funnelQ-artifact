#ifndef TRANSACTION_H
#define TRANSACTION_H

#include "utils.h"
#include <memory>

class Transaction{
public:
    Transaction();
    Transaction(uint32_t txid, uint32_t timestamp, uint32_t amount, vector<string> addresses);
    Transaction(uint32_t txid, uint32_t timestamp, uint32_t amount, vector<string> addresses, vector<double> sketches);
    Transaction(uint32_t txid, uint32_t timestamp, uint32_t amount, vector<string> addresses,
                vector<double> sketches, string payload);

    void display() const;

    uint32_t getTxid() const;
    uint32_t getTimestamp() const;
    double getValue(AggType aggType) const;
    uint32_t getAmount() const;
    vector<string> getAddresses() const;
    const vector<string>& getAddressesRef() const;
    vector<double> getSketches() const;
    const string& getPayload() const;
    bool hasPayload() const;

    size_t getSizeADS() const;

    uint32_t txid = 0;
    uint32_t timestamp = 0;
    uint32_t amount = 0;
    vector<double> sketches;

private:
                                                                               
                                                                               
                                                                                
    shared_ptr<const vector<string>> addressesData;
    shared_ptr<const string> payloadData;
};

#endif                
