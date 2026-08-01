#include "nft_loader.h"
#include "nlohmann/json.hpp"

#include <algorithm>
#include <cctype>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <unordered_map>

using nlohmann::json;

namespace {

string lowerAscii(string value) {
    transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(tolower(c));
    });
    return value;
}

vector<string> parseCsvLine(const string& line) {
    vector<string> fields;
    string current;
    bool quoted = false;
    for (size_t i = 0; i < line.size(); ++i) {
        const char c = line[i];
        if (quoted) {
            if (c == '"') {
                if (i + 1 < line.size() && line[i + 1] == '"') {
                    current.push_back('"'); ++i;
                } else quoted = false;
            } else current.push_back(c);
        } else if (c == ',') {
            fields.emplace_back(move(current)); current.clear();
        } else if (c == '"') quoted = true;
        else if (c != '\r') current.push_back(c);
    }
    if (quoted) throw runtime_error("unterminated quoted CSV field");
    fields.emplace_back(move(current));
    return fields;
}


bool csvRecordComplete(const string& record) {
    bool quoted = false;

    for (size_t i = 0; i < record.size(); ++i) {
        if (record[i] != '"') continue;

        if (quoted &&
            i + 1 < record.size() &&
            record[i + 1] == '"') {
            ++i;
            continue;
        }

        quoted = !quoted;
    }

    return !quoted;
}

bool readCsvRecord(istream& in, string& record) {
    record.clear();

    string line;
    bool sawAny = false;

    while (getline(in, line)) {
        if (sawAny) record.push_back('\n');
        record += line;
        sawAny = true;

        if (csvRecordComplete(record)) {
            return true;
        }
    }

    if (!sawAny) return false;

    throw runtime_error("unterminated quoted CSV record");
}

uint64_t parseUintLoose(const string& value) {
    if (value.empty()) return 0;
    try {
        size_t pos = 0;
        const unsigned long long v = stoull(value, &pos);
        if (pos == value.size()) return static_cast<uint64_t>(v);
    } catch (...) {}
    return static_cast<uint64_t>(stold(value));
}

int64_t parseIntLoose(const string& value) {
    if (value.empty()) return 0;
    try {
        size_t pos = 0;
        const long long v = stoll(value, &pos);
        if (pos == value.size()) return static_cast<int64_t>(v);
    } catch (...) {}
    return static_cast<int64_t>(stold(value));
}

uint64_t parseUtcTimestamp(const string& value) {
    if (value.empty()) return 0;
    tm t{};
    istringstream ss(value.substr(0, 19));
    ss >> get_time(&t, "%Y-%m-%d %H:%M:%S");
    if (ss.fail()) throw runtime_error("failed to parse timestamp: " + value);
#if defined(_WIN32)
    const time_t out = _mkgmtime(&t);
#else
    const time_t out = timegm(&t);
#endif
    if (out < 0) throw runtime_error("invalid timestamp: " + value);
    return static_cast<uint64_t>(out);
}

bool isZeroAddress(const string& value) {
    const string v = lowerAscii(value);
    return v.empty() || v == "0x0000000000000000000000000000000000000000";
}

}

TxnPool loadNftEventPool(const string& csvPath, unsigned int seed,
                         NftLoadMetrics* metrics, uint64_t orderOffset) {
    ifstream in(csvPath);
    if (!in) throw runtime_error("cannot open NFT canonical event CSV: " + csvPath);
    string headerLine;
    if (!readCsvRecord(in, headerLine)) throw runtime_error("empty NFT canonical event CSV");
    const vector<string> headers = parseCsvLine(headerLine);
    unordered_map<string,size_t> col;
    for (size_t i = 0; i < headers.size(); ++i) col[headers[i]] = i;
    auto required = [&](const string& name) {
        if (!col.count(name)) throw runtime_error("missing CSV column: " + name);
    };
    for (const string& name : {
        "event_id","event_type","block_time","block_number","tx_hash","local_order",
        "nft","seller","buyer","from_address","to_address","source_table","source_id",
        "evt_index"
    }) required(name);

    struct RawEvent {
        json obj;
        uint64_t blockTs = 0;
        uint64_t blockNumber = 0;
        string txHash;
        int64_t localOrder = 0;
        string eventId;
    };
    vector<RawEvent> rows;
    string line;
    while (readCsvRecord(in,line)) {
        if (line.empty()) continue;
        const vector<string> f = parseCsvLine(line);
        if (f.size() != headers.size()) throw runtime_error("malformed NFT CSV row");
        auto get = [&](const string& name)->string {
            auto it = col.find(name); return it == col.end() ? string() : f[it->second];
        };
        const string eventTypeRaw = get("event_type");
        const string eventType = lowerAscii(eventTypeRaw);
        const string nft = lowerAscii(get("nft"));
        const string collection = nft.find(':') == string::npos ? string() : nft.substr(0,nft.find(':'));
        const string seller = lowerAscii(get("seller"));
        const string buyer = lowerAscii(get("buyer"));
        const string fromAddr = lowerAscii(get("from_address"));
        const string toAddr = lowerAscii(get("to_address"));
        const string moveFrom = eventType == "nfttrade" ? seller : fromAddr;
        const string moveTo = eventType == "nfttrade" ? buyer : toAddr;
        const uint64_t blockTs = parseUtcTimestamp(get("block_time"));
        const uint64_t blockNumber = parseUintLoose(get("block_number"));
        const int64_t localOrder = parseIntLoose(get("local_order"));

        json obj = json::object();
        obj["event_id"] = get("event_id");
        obj["event_type"] = eventType;
        obj["block_time"] = get("block_time");
        obj["block_time_epoch"] = blockTs;
        obj["block_number"] = blockNumber;
        obj["tx_hash"] = lowerAscii(get("tx_hash"));
        obj["local_order"] = localOrder;
        obj["evt_index"] = parseIntLoose(get("evt_index"));
        obj["nft"] = nft;
        obj["collection_contract"] = collection;
        obj["seller"] = seller;
        obj["buyer"] = buyer;
        obj["from_address"] = fromAddr;
        obj["to_address"] = toAddr;
        obj["move_from"] = moveFrom;
        obj["move_to"] = moveTo;
        obj["source_table"] = get("source_table");
        obj["source_id"] = get("source_id");
        obj["protocol"] = "nft";
        obj["version"] = "";
        obj["contract"] = collection;
        obj["raw_source"] = get("source_table");
        obj["raw_row_id"] = get("source_id");
        obj["src_rule_id"] = "nft_canonical_v1";
        obj["attrs"] = json::object();
        obj["roles"] = {
            {"recipient", moveTo}, {"asset", nft}, {"pool", collection},
            {"taker", moveFrom}, {"maker", moveTo}, {"token_sold", nft},
            {"token_bought", nft}, {"borrower", moveFrom}
        };
        rows.push_back({move(obj), blockTs, blockNumber, lowerAscii(get("tx_hash")),
                        localOrder, get("event_id")});
    }

    stable_sort(rows.begin(), rows.end(), [](const RawEvent& a, const RawEvent& b) {
        if (a.blockTs != b.blockTs) return a.blockTs < b.blockTs;
        if (a.blockNumber != b.blockNumber) return a.blockNumber < b.blockNumber;
        if (a.txHash != b.txHash) return a.txHash < b.txHash;
        if (a.localOrder != b.localOrder) return a.localOrder < b.localOrder;
        return a.eventId < b.eventId;
    });

    TxnPool pool;
    pool.nSeed = seed;
    pool.supportAggs = {MAX};
    uint64_t lastBlock = numeric_limits<uint64_t>::max();
    uint32_t blockOrdinal = 0;
    uint64_t orderSeq = orderOffset;
    for (auto& row : rows) {
        if (row.blockNumber != lastBlock) {
            lastBlock = row.blockNumber;
            ++blockOrdinal;
            pool.blkIndex.emplace_back(static_cast<int>(pool.pool.size()));
        }
        row.obj["order_seq"] = orderSeq;
        row.obj["closure_id"] = to_string(orderSeq + 1);
        row.obj["mbft_block"] = blockOrdinal;
        row.obj["mbft_amount"] = 1U;
        const string payload = row.obj.dump();
        pool.pool.emplace_back(transactionFromPayload(payload, seed));
        if (metrics) {
            metrics->eventCount++;
            metrics->keywordCount += 16;
            metrics->payloadBytes += payload.size();
            const string mf = row.obj.value("move_from","");
            const string mt = row.obj.value("move_to","");
            if (isZeroAddress(mf)) metrics->zeroMoveFromCount++;
            else if (isZeroAddress(mt)) metrics->zeroMoveToCount++;
            else metrics->inScopeRootCount++;
        }
        ++orderSeq;
    }
    if (metrics) metrics->blockCount = blockOrdinal;
    return pool;
}
