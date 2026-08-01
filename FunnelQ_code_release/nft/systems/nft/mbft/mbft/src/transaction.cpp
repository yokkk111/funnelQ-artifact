#include "transaction.h"

#include <iostream>
#include <utility>

namespace {
const vector<string>& emptyAddresses() { static const vector<string> value; return value; }
const string& emptyPayload() { static const string value; return value; }
}

Transaction::Transaction():
    sketches(), addressesData(make_shared<const vector<string>>()), payloadData(make_shared<const string>())
{
}

Transaction::Transaction(uint32_t txidIn, uint32_t timestampIn, uint32_t amountIn, vector<string> addressesIn):
    txid(txidIn), timestamp(timestampIn), amount(amountIn), sketches(),
    addressesData(make_shared<const vector<string>>(move(addressesIn))), payloadData(make_shared<const string>())
{
}

Transaction::Transaction(uint32_t txidIn, uint32_t timestampIn, uint32_t amountIn, vector<string> addressesIn, vector<double> sketchesIn):
    txid(txidIn), timestamp(timestampIn), amount(amountIn), sketches(move(sketchesIn)),
    addressesData(make_shared<const vector<string>>(move(addressesIn))), payloadData(make_shared<const string>())
{
}

Transaction::Transaction(uint32_t txidIn, uint32_t timestampIn, uint32_t amountIn, vector<string> addressesIn,
                         vector<double> sketchesIn, string payloadIn):
    txid(txidIn), timestamp(timestampIn), amount(amountIn), sketches(move(sketchesIn)),
    addressesData(make_shared<const vector<string>>(move(addressesIn))),
    payloadData(make_shared<const string>(move(payloadIn)))
{
}

void Transaction::display() const
{
    cout << "------ Transaction::display() ------" << endl;
    cout << "txid: " << txid << endl;
    cout << "timestamp: " << timestamp << endl;
    cout << "amount: " << amount << endl;
    cout << "addresses: ";
    for (const string& address : getAddressesRef()) cout << address << " ";
    cout << endl;
    cout << "sketches: ";
    for (const double& sketch : sketches) cout << sketch << " ";
    cout << endl;
    if (hasPayload()) cout << "payload bytes: " << getPayload().size() << endl;
}

uint32_t Transaction::getTxid() const { return txid; }
uint32_t Transaction::getTimestamp() const { return timestamp; }

double Transaction::getValue(AggType aggType) const
{
    if (aggType) return sketches.at(aggType - 1);
    return amount;
}

uint32_t Transaction::getAmount() const { return amount; }
vector<string> Transaction::getAddresses() const { return getAddressesRef(); }
const vector<string>& Transaction::getAddressesRef() const { return addressesData ? *addressesData : emptyAddresses(); }
vector<double> Transaction::getSketches() const { return sketches; }
const string& Transaction::getPayload() const { return payloadData ? *payloadData : emptyPayload(); }
bool Transaction::hasPayload() const { return payloadData && !payloadData->empty(); }

size_t Transaction::getSizeADS() const {
    size_t sizeADS = sizeof(txid) + sizeof(timestamp) + sizeof(amount);
    for (const string& addr : getAddressesRef()) sizeADS += addr.size();
    sizeADS += getPayload().size();
    sizeADS += sketches.size() * sizeof(double);
    return sizeADS;
}
