#include <ctime>
#include <unistd.h>
#include <sys/stat.h>
#include <filesystem>
#include <system_error>
#include <tuple>
#include <chrono>
#include "nlohmann/json.hpp" // Assuming nlohmann/json library is available
#include "utils.h"
#include "hash.h"
#include "bf.h"
#include "merkle.h"
#include "transaction.h"
#include "blockchain.h"
#include "query.h"

#define MAX_AMOUNT 8000000
// #define MAX_BLOCK 20000

// Use nlohmann/json for JSON parsing
using json = nlohmann::json;

enum QueryType {AND, OR, RANGE, AND_RANGE, OR_RANGE};

string AggTypeString[] = {"MAX", "COUNT", "SUM", "COUNT_DISTINCT"};

struct Params{
    Params() = default;
    Params(const cmdline::parser& argParser);

    // Property:
    unsigned int nSeed;
    ParamBF paramBF;
    set<AggType> supportAggs;
    string inPath;
    string outPath;
    string queryPath;

    ADSType adsType;

    bool earlyStop;
    unsigned int combineCycle;
    int repeats;
    double valueRange;
    // Query construction
    AggType agg;
};

Params::Params(const cmdline::parser& argParser)
{
    // Parameter initialization
    // nSeed
    nSeed = argParser.get<int>("seed");
    if (nSeed == 0) {
        nSeed = time(0);
    }

    // paramBF
    unsigned int nCurrentItems = 0;
    unsigned int nItems = 0;
    double nFPRate = argParser.get<double>("FPRate");
    unsigned int nTweak = nSeed;
    paramBF = ParamBF(nCurrentItems, nItems, nFPRate, nTweak);

    // supportAggs
    int addAggs = argParser.get<int>("supportAggs");
    supportAggs = {};
    for (int i = 0; i < 4; i++) {
        if ((addAggs >> i) & 1) {
            supportAggs.emplace((AggType)i);
        }
    }

    // input
    inPath = argParser.get<string>("inputFile");
    outPath = argParser.get<string>("outputDir");
    queryPath = argParser.get<string>("queryDir");

    // MHT or MBFT
    adsType = (ADSType)argParser.get<int>("ADSType");
    // Optimization
    earlyStop = argParser.exist("earlyStop");
    combineCycle = argParser.get<unsigned int>("combineCycle");

    repeats = argParser.get<int>("repeats");
    valueRange = argParser.get<double>("valueRange");

    // Query construction
    agg = (AggType)argParser.get<int>("queryAgg");
    // print agg and supportAggs
    assert(supportAggs.count(agg) || adsType == MHT || adsType == VCHAIN);
}

// Helper function to convert aggregation string to AggType enum
AggType stringToAggType(const std::string& aggStr) {
    if (aggStr == "COUNT") return AggType::COUNT;
    if (aggStr == "SUM") return AggType::SUM;
    if (aggStr == "MAX") return AggType::MAX;
    // if (aggStr == "MIN") return AggType::MIN;

    // Add other types if necessary
    std::cerr << "Warning: Unknown aggregation type string '" << aggStr << "'. Defaulting to COUNT." << std::endl;
    return AggType::COUNT; // Default or throw error
}


Query genQuery(const Params& params, AggType agg, int window, double v, int s, QueryType& queryType, TxnPool& txnPool, int maxBlock) {
    uint32_t beginBlock, endBlock;
    uint32_t lowerBound, upperBound;
    string addr;
    vector<string> addrsDNF {};
    vector<vector<string>> addrsCNF {};
    unsigned int randPos;

    
    // Use high-resolution clock for a more precise seed
    auto seed = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    default_random_engine randEng(seed);

    int blockRange = maxBlock - window;
    int valueRange = (int)(MAX_AMOUNT * v);
    
    // Random time window
    beginBlock = randEng() % blockRange + 1;
    endBlock = beginBlock + window - 1;

    // Random value range
    if (queryType == OR || queryType == AND) { // No Numerical Range
        lowerBound = 0;
        upperBound = MAX_AMOUNT;
    }
    else { // Numerical Range
        lowerBound = randEng() % (MAX_AMOUNT - valueRange);
        upperBound = lowerBound + valueRange;
    }

    // Random addresses
    if (queryType == OR || queryType == OR_RANGE) {
        for (int l = 0; l < s; l++) {
            randPos = randEng() % (txnPool.blkIndex[endBlock] - txnPool.blkIndex[beginBlock]);
            addr = txnPool.getAddr(txnPool.blkIndex[beginBlock] + randPos, randPos & 1);
            addrsDNF.emplace_back(addr);
        }
        addrsCNF.emplace_back(addrsDNF);
    }
    else if (queryType == AND || queryType == AND_RANGE) {
        for (int l = 0; l < s; l++) {
            randPos = randEng() % (txnPool.blkIndex[endBlock] - txnPool.blkIndex[beginBlock]);
            addr = txnPool.getAddr(txnPool.blkIndex[beginBlock] + randPos, randPos & 1);
            addrsDNF.emplace_back(addr);
            addrsCNF.emplace_back(addrsDNF);
            addrsDNF.clear();
        }
    }
    return Query(agg, beginBlock, endBlock, lowerBound, upperBound, addrsCNF, params.earlyStop);
}


pair<size_t, size_t> computeVO(varVO qVO, const ADSType& adsType) {
    size_t currentSize = 0;
    size_t currentBFSize = 0;
    switch (adsType) {
        case MHT:
        case VCHAIN:
        {
            queue<vector<Transaction>> temp_qVOMHT = get<1>(qVO); // Copy for size calculation
            while (!temp_qVOMHT.empty()) {
                for (const Transaction& txn : temp_qVOMHT.front()) {
                    currentSize += txn.getSizeADS();
                }
                temp_qVOMHT.pop();
            }
            break;
        }
        case MBFT:
        {
            // cout << "=============MBFT=============" << endl;
            queue<stack<VONode>> temp_qVO = get<0>(qVO); // Copy for size calculation
            while (!temp_qVO.empty()) {
                stack<VONode> sVO = temp_qVO.front();
                while (!sVO.empty()) {
                    VONode VO = sVO.top();
                    // cout << "VO----------------" << endl;
                    // cout << "VO.myFlag: " << VO.myFlag << endl;
                    // cout << "VO.getSizeADS(): " << VO.getSizeADS() << endl;
                    // cout << "VO.getSizeBF(): " << VO.getSizeBF() << endl;
                    currentSize += VO.getSizeADS();
                    currentBFSize += VO.getSizeBF();
                    sVO.pop();
                    if (sVO.empty()) {
                        if (VO.myFlag == ROOT) { // Root need not opcode to restore MBF
                            currentSize -= VO.getSizeBF() / 2;
                            currentBFSize -= VO.getSizeBF() / 2;
                        }
                        else if (VO.myFlag == HINT) {
                            currentSize -= VO.getSizeBF();
                            currentBFSize -= VO.getSizeBF();
                        }
                    }
                }
                temp_qVO.pop();
            }
            break;
        }
        case MBFT_BF:
        {
            // cout << "=============MBFT_BF=============" << endl;
            queue<stack<VONode>> temp_qVO = get<0>(qVO); // Copy for size calculation
            while (!temp_qVO.empty()) {
                stack<VONode> sVO = temp_qVO.front();
                while (!sVO.empty()) {
                    VONode VO = sVO.top();
                    // cout << "VO----------------" << endl;
                    // cout << "VO.myFlag: " << VO.myFlag << endl;
                    // cout << "VO.getSizeADS(): " << VO.getSizeADS() << endl;
                    // cout << "VO.getSizeBF(): " << VO.getSizeBF() << endl;
                    currentSize += VO.getSizeADS();
                    currentBFSize += VO.getSizeBF();
                    sVO.pop();
                }
                temp_qVO.pop();
            }
            break;
        }
    }
    return make_pair(currentSize, currentBFSize);
}


// 1. Single shot:  Build a chain, do single query & verification
void runSingleShot(const Params& params)
{
    bool verify;
    // clock_t start, end;
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    ExperimentMetrics metrics;

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    ofstream outfile;
    outfile.open(params.outPath + "/runSingleShot.csv", ios::out);
    ExperimentMetrics::writeHeader(outfile, "False Positive");

    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);

    Chain chain(params.paramBF, params.nSeed, params.supportAggs,
                params.combineCycle, params.earlyStop, params.adsType);

    // Chain construction
    cout << "Chain construction..." << endl;
    start = system_clock::now();
    chain.buildChain(txnPool);
    end = system_clock::now();
    dur = duration_cast<microseconds>(end -start);
    cout << "construction time: " << dur.count() << "us" << endl;

    // chain.getBlock(1)->getRoot(AMOUNT_ADS)->printBFS();

    // Query construction
    uint32_t beginBlock = 7332, endBlock = 8132;
    uint32_t lowerBound = 3800585, upperBound = 7790085;
    vector<string> addrsDNF {
        "0x28c69cd429f785954ae4b0263db4d9871c033ca7",
        "0x706f3c6299772a7f923e9d152398a6ac306708ba"
    };
    // vector<string> addrsDNF {};
    vector<vector<string>> addrsCNF {addrsDNF};
    Query query(params.agg, beginBlock, endBlock, lowerBound, upperBound, addrsCNF, params.earlyStop);

    start = system_clock::now();
    auto [res, qVO] = chainQuery(chain, query);
    end = system_clock::now();
    dur = duration_cast<microseconds>(end -start);
    metrics.queryTime = dur.count();
    cout << "Query time: " << dur.count() << "us" << endl;

    // Verify
    start = system_clock::now();
    verify = chainVerify(query, res, qVO, chain, metrics);
    end = system_clock::now();
    dur = duration_cast<microseconds>(end -start);
    metrics.verifyTime = dur.count();
    cout << "Verify time: " << dur.count() << "us" << endl;

    assert(verify);
    cout << "Verified result: " << res << endl;

    tie(metrics.voSize, metrics.bfSize) = computeVO(qVO, chain.adsType);
    metrics.writeDataRow(outfile, "0", 0);
}


// 2. Construct:    Test i) construction time & ii) ADS size for MHT, MHT_BF, MBFT, MBFT-comb
void testConstruct(const Params& params)
{
    string outputName = "Construct";
    // Parameters for compared methods
    vector<ADSType> vAdsType {MHT, MBFT_BF, MBFT, MBFT};
    vector<int> blk_sizes = {32, 64, 128, 256, 512};

    ParamBF paramBF = params.paramBF;
    vector<bool> vEarlyStop {0, 0, 0, 0};
    vector<unsigned int> vCombineCycle {0, 0, 0, 8};

    double time = 0;
    double size = 0;

    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    string resPath = params.outPath + "/" + outputName + ".csv";
    ofstream outfile(resPath, ios::out);
    outfile << "Block Size,\tADS,\ttime(us),\tsize(B)" << endl;

    // Chain construction
    vector<vector<Chain*>> chains(blk_sizes.size(), vector<Chain*>(vAdsType.size()));
    // Create chain
    cout << ">>> Creating chain..." << endl;
    for (int i = 0; i < blk_sizes.size(); i++) {
        // Create pool
        string inPath = "../data/eth-" + to_string(blk_sizes[i]) + ".dat";
        TxnPool txnPool(params.nSeed, params.supportAggs, inPath);
        for (int j = 0; j < vAdsType.size(); j++) {
            start = system_clock::now();
            chains[i][j] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[j], vEarlyStop[j], vAdsType[j]);
            chains[i][j]->buildChain(txnPool);
            end = system_clock::now();
            dur = duration_cast<microseconds>(end -start);

            time = dur.count() / chains[i][j]->getSize();
            size = chains[i][j]->getsizeADS();
            outfile << blk_sizes[i] << ",\t" << j << ",\t" << time << ",\t" << size << endl;
        }
    }
    cout << "<<< Chain created" << endl;
    outfile.close();

    for (int i = 0; i < blk_sizes.size(); i++) {
        for (int j = 0; j < vAdsType.size(); j++) {
            delete chains[i][j];
        }
    }
}


// 3. Compare:      Test i) query time, ii) VO size, iii) verify time for different `query type` & `time window` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es
void testCompare(const Params& params)
{
    string outputName = "window";
    vector<QueryType> queryTypes {AND_RANGE, OR_RANGE};
    vector<string> strQueryTypes {"and", "or"};

    // Parameters for compared methods: MHT, MBFT, MBFT-es, MBFT-mc, MBFT-all
    vector<ADSType> vAdsType {MHT, MBFT, MBFT, MBFT, MBFT};
    // vector<ADSType> vAdsType {MBFT, MBFT};
    vector<int> windows = {400, 800, 1200, 1600, 2000};
    // vector<int> windows = {400, 800, 1200};
    
    ParamBF paramBF = params.paramBF;

    vector<bool> vEarlyStop {0, 0, 1, 0, 1};
    // vector<bool> vEarlyStop {0, 1};
    vector<unsigned int> vCombineCycle {0, 0, 0, 8, 8};
    // vector<unsigned int> vCombineCycle {0, 8};

    // Chain construction
    vector<Chain*> chains(vAdsType.size());
    // Create pool
    cout << "Creating pool... " << params.inPath << endl;
    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);
    // Create chain
    cout << "Creating chain..." << endl;
    for (int i = 0; i < vAdsType.size(); i++) {
        chains[i] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[i], vEarlyStop[i], vAdsType[i]);
        chains[i]->buildChain(txnPool);
        cout << "ADS Chain:" << i << " built" << endl;
    }

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(windows.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    vector<ofstream> outfiles(queryTypes.size());
    for (int i = 0; i < queryTypes.size(); i++) {
        outfiles[i].open(params.outPath + "/" + outputName + "_" + strQueryTypes[i] + ".csv", ios::out);
        ExperimentMetrics::writeHeader(outfiles[i], outputName);
    }

    // Query construction
    // int x = 128;
    int n = params.repeats;
    int w;
    double v = params.valueRange;
    string boolean_op;
    int s = 2;
    AggType agg = params.agg;
    vector<string> queryPaths(queryTypes.size());
    for (int i = 0; i < queryTypes.size(); i++) {
        queryPaths[i] = params.queryPath + "/" + outputName + "_" + strQueryTypes[i];
    }

    // test different epsilons
    for (int i = 0; i < queryTypes.size(); i++) {
        boolean_op = strQueryTypes[i];
        // test different time window
        for (int j = 0; j < windows.size(); j++) {
            cout << ">>> Testing time window: " << windows[j] << endl;
            // reset last queryType i
            for(int k=0; k < vAdsType.size(); ++k) {
                metricsAcc[j][k].reset();
            }

            w = windows[j];
            // vector<Query> queries = readQueriesFromFile(queryPaths[i], x, n, w, v, boolean_op, s, agg);
            // test different ADS types
            for (int k = 0; k < vAdsType.size(); k++) {
                cout << ">>> Testing ADS type: " << k << endl;
                // metricsAcc[i][j].reset();
                // for (int l = 0; l < 2; l++) { 
                for (int l = 0; l < params.repeats; l++) {
                    ExperimentMetrics currentMetrics; // Metrics for this single run
                    currentMetrics.reset();

                    // Query query = queries[l];
                    // cout << "query_tmp: ";
                    // query_tmp.printAddresses();
                    Query query = genQuery(params, agg, w, v, s, queryTypes[i], txnPool, chains[k]->getSize());
                    // cout << "query: ";
                    // query.printAddresses();
                    // query.addresses = query_tmp.addresses;
                    
                    query.earlyStop = vEarlyStop[k];

                    // Query Process
                    start = system_clock::now();
                    auto [res, qVO] = chainQuery(*chains[k], query);
                    end = system_clock::now();
                    dur = duration_cast<microseconds>(end -start);
                    currentMetrics.queryTime = dur.count(); // Store query time
                    // cout << "result: " << res << endl;

                    // Verify
                    start = system_clock::now();
                    verify = chainVerify(query, res, qVO, *chains[k], currentMetrics);
                    end = system_clock::now();
                    dur = duration_cast<microseconds>(end -start);
                    currentMetrics.verifyTime = dur.count(); // Store verify time
                    assert(verify);

                    // Size calculation
                    tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[k]->adsType);
                    metricsAcc[j][k].accumulate(currentMetrics);
                } // End repeats loop (l)
                metricsAcc[j][k].average(params.repeats);
                metricsAcc[j][k].writeDataRow(outfiles[i], std::to_string(windows[j]), k);
            } // End ADS type loop (k)
        } // End time window loop (j)
        outfiles[i].close();
    } // End query type loop (i)
    
    for (auto chain : chains) {
        delete chain;
    }
}


// 4. Range:        Test i) query time, ii) VO size, iii) verify time for different `value range` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es
void testRange(const Params& params)
{
    string outputName = "value_range";

    // Parameters for compared methods: MHT, MBFT, MBFT-es, MBFT-mc, MBFT-all
    vector<ADSType> vAdsType {MHT, MBFT, MBFT, MBFT, MBFT};
    // vector<ADSType> vAdsType {MBFT, MBFT};
    vector<double> valRanges = {0.1, 0.3, 0.5, 0.7, 0.9};
    
    ParamBF paramBF = params.paramBF;

    vector<bool> vEarlyStop {0, 0, 1, 0, 1};
    // vector<bool> vEarlyStop {0, 1};
    vector<unsigned int> vCombineCycle {0, 0, 0, 8, 8};
    // vector<unsigned int> vCombineCycle {0, 10};

    // Chain construction
    vector<Chain*> chains(vAdsType.size());
    // Create pool
    cout << "Creating pool... " << params.inPath << endl;
    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);
    // Create chain
    cout << "Creating chain..." << endl;
    for (int i = 0; i < vAdsType.size(); i++) {
        chains[i] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[i], vEarlyStop[i], vAdsType[i]);
        chains[i]->buildChain(txnPool);
        cout << "ADS Chain:" << i << " built" << endl;
    }

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(valRanges.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    ofstream outfile(params.outPath + "/" + outputName + ".csv", ios::out);
    ExperimentMetrics::writeHeader(outfile, outputName);

    // Query construction
    int x = 128;
    int n = params.repeats;
    int w = 800;
    double v;
    string boolean_op = "or";
    int s = 2;
    AggType agg = params.agg;
    string queryPath = params.queryPath + "/" + outputName;
    QueryType queryType = OR_RANGE;

    // test different epsilons
    for (int j = 0; j < valRanges.size(); j++) {
        cout << ">>> Testing value range: " << valRanges[j] << endl;
        // reset last queryType i
        for(int k=0; k < vAdsType.size(); ++k) {
            metricsAcc[j][k].reset();
        }
        
        v = valRanges[j];

        // vector<Query> queries = readQueriesFromFile(queryPaths[i], x, n, w, v, boolean_op, s, agg);
        // test different ADS types
        for (int k = 0; k < vAdsType.size(); k++) {
            cout << ">>> Testing ADS type: " << k << endl;
            // metricsAcc[i][j].reset();
            // for (int l = 0; l < 2; l++) { 
            for (int l = 0; l < params.repeats; l++) {
                ExperimentMetrics currentMetrics; // Metrics for this single run
                currentMetrics.reset();

                // Query query = queries[l];
                // cout << "query_tmp: ";
                // query_tmp.printAddresses();
                Query query = genQuery(params, agg, w, v, s, queryType, txnPool, chains[k]->getSize());
                // cout << "query: ";
                // query.printAddresses();
                // query.addresses = query_tmp.addresses;
                
                query.earlyStop = vEarlyStop[k];

                // Query Process
                start = system_clock::now();
                auto [res, qVO] = chainQuery(*chains[k], query);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.queryTime = dur.count(); // Store query time
                // cout << "result: " << res << endl;

                // Verify
                start = system_clock::now();
                verify = chainVerify(query, res, qVO, *chains[k], currentMetrics);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.verifyTime = dur.count(); // Store verify time
                assert(verify);

                // Size calculation
                tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[k]->adsType);
                metricsAcc[j][k].accumulate(currentMetrics);
            } // End repeats loop (l)
            metricsAcc[j][k].average(params.repeats);
            metricsAcc[j][k].writeDataRow(outfile, std::to_string(valRanges[j]), k);
        } // End ADS type loop (k)
    } // End time window loop (j)
    outfile.close();
    
    for (auto chain : chains) {
        delete chain;
    }
}


// 5. Selectivity:  Test i) query time, ii) VO size, iii) verify time for different `keyword selectivity` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es
void testSelectivity(const Params& params)
{
    string outputName = "keyword";

    // Parameters for compared methods: MHT, MBFT, MBFT-es, MBFT-mc, MBFT-all
    vector<ADSType> vAdsType {MHT, MBFT, MBFT, MBFT, MBFT};
    // vector<ADSType> vAdsType {MBFT, MBFT, MBFT, MBFT};
    vector<double> keywords = {1, 2, 4, 8, 16};
    
    ParamBF paramBF = params.paramBF;

    vector<bool> vEarlyStop {0, 0, 1, 0, 1};
    // vector<bool> vEarlyStop {0, 1, 0, 1};
    vector<unsigned int> vCombineCycle {0, 0, 0, 8, 8};
    // vector<unsigned int> vCombineCycle {0, 0, 10, 10};

    // Chain construction
    vector<Chain*> chains(vAdsType.size());
    // Create pool
    cout << "Creating pool... " << params.inPath << endl;
    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);
    // Create chain
    cout << "Creating chain..." << endl;
    for (int i = 0; i < vAdsType.size(); i++) {
        chains[i] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[i], vEarlyStop[i], vAdsType[i]);
        chains[i]->buildChain(txnPool);
        cout << "ADS Chain:" << i << " built" << endl;
    }

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(keywords.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    ofstream outfile(params.outPath + "/" + outputName + ".csv", ios::out);
    ExperimentMetrics::writeHeader(outfile, outputName);

    // Query construction
    int x = 128;
    int n = params.repeats;
    int w = 800;
    double v = 0.5;
    string boolean_op = "or";
    int s;
    AggType agg = params.agg;
    string queryPath = params.queryPath + "/" + outputName;
    QueryType queryType = OR_RANGE;

    // test different epsilons
    for (int j = 0; j < keywords.size(); j++) {
        cout << ">>> Testing keywords selectivity: " << keywords[j] << endl;
        // reset last queryType i
        for(int k=0; k < vAdsType.size(); ++k) {
            metricsAcc[j][k].reset();
        }
        
        s = keywords[j];

        // vector<Query> queries = readQueriesFromFile(queryPaths[i], x, n, w, v, boolean_op, s, agg);
        // test different ADS types
        for (int k = 0; k < vAdsType.size(); k++) {
            cout << ">>> Testing ADS type: " << k << endl;
            // metricsAcc[i][j].reset();
            // for (int l = 0; l < 2; l++) { 
            for (int l = 0; l < params.repeats; l++) {
                ExperimentMetrics currentMetrics; // Metrics for this single run
                currentMetrics.reset();

                // Query query = queries[l];
                // cout << "query_tmp: ";
                // query_tmp.printAddresses();
                Query query = genQuery(params, agg, w, v, s, queryType, txnPool, chains[k]->getSize());
                // cout << "query: ";
                // query.printAddresses();
                // query.addresses = query_tmp.addresses;
                
                query.earlyStop = vEarlyStop[k];

                // Query Process
                start = system_clock::now();
                auto [res, qVO] = chainQuery(*chains[k], query);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.queryTime = dur.count(); // Store query time
                // cout << "result: " << res << endl;

                // Verify
                start = system_clock::now();
                verify = chainVerify(query, res, qVO, *chains[k], currentMetrics);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.verifyTime = dur.count(); // Store verify time
                assert(verify);

                // Size calculation
                tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[k]->adsType);
                metricsAcc[j][k].accumulate(currentMetrics);
            } // End repeats loop (l)
            metricsAcc[j][k].average(params.repeats);
            metricsAcc[j][k].writeDataRow(outfile, std::to_string(keywords[j]), k);
        } // End ADS type loop (k)
    } // End time window loop (j)
    outfile.close();
    
    for (auto chain : chains) {
        delete chain;
    }
}


// 6. combineCycle: Test i) query time, ii) VO size, iii) verify time for different `combine cycle` on MBFT-comb-es
void testCombine(const Params& params)
{
    string outputName = "combine";

    // Parameters for compared methods: MBFT-mc, MBFT-all
    vector<QueryType> queryTypes {OR_RANGE, AND_RANGE};
    vector<string> queryTypesStr {"OR_RANGE", "AND_RANGE"};
    vector<ADSType> vAdsType {MBFT, MBFT};
    vector<double> combines {0, 2, 4, 8, 16};

    
    ParamBF paramBF = params.paramBF;

    vector<bool> vEarlyStop {0, 1};

    // Chain construction
    vector<vector<Chain*>> chains(combines.size(), vector<Chain*>(vAdsType.size()));
    // Create pool
    cout << "Creating pool... " << params.inPath << endl;
    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);
    // Create chain
    cout << "Creating chain..." << endl;
    for (int i = 0; i < combines.size(); i++) {
        double combine = combines[i];
        for (int j = 0; j < vAdsType.size(); j++) {
            chains[i][j] = new Chain(paramBF, params.nSeed, params.supportAggs, combine, vEarlyStop[j], vAdsType[j]);
            chains[i][j]->buildChain(txnPool);
            cout << "ADS Chain with combine cycle " << combine << " and ADS type " << j << " built" << endl;
        }
    }

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(combines.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    vector<ofstream> outfiles(queryTypes.size());
    for (int i = 0; i < queryTypes.size(); i++) {
        outfiles[i].open(params.outPath + "/" + outputName + "_" + queryTypesStr[i] + ".csv", ios::out);
        ExperimentMetrics::writeHeader(outfiles[i], outputName);
    }

    // Query construction
    int x = 128;
    int n = params.repeats;
    int w = 800;
    double v = 0.5;
    string boolean_op = "or";
    int s = 2;
    AggType agg = params.agg;
    string queryPath = params.queryPath + "/" + outputName;
    QueryType queryType = OR;

    // test different epsilons
    for (int i = 0; i < queryTypes.size(); i++) {
        Query query = genQuery(params, agg, w, v, s, queryTypes[i], txnPool, chains[0][0]->getSize());

        for (int j = 0; j < combines.size(); j++) {
            cout << ">>> Testing combine cycle: " << combines[j] << endl;
            // reset last queryType i
            for(int k=0; k < vAdsType.size(); ++k) {
                metricsAcc[j][k].reset();
            }

            // vector<Query> queries = readQueriesFromFile(queryPaths[i], x, n, w, v, boolean_op, s, agg);
            // test different ADS types
            for (int k = 0; k < vAdsType.size(); k++) {
                cout << ">>> Testing ADS type: " << k << endl;
                // metricsAcc[i][j].reset();
                // for (int l = 0; l < 2; l++) { 
                for (int l = 0; l < params.repeats; l++) {
                    ExperimentMetrics currentMetrics; // Metrics for this single run
                    currentMetrics.reset();

                    // Query query = queries[l];
                    // cout << "query_tmp: ";
                    // query_tmp.printAddresses();
                    
                    // cout << "query: ";
                    // query.printAddresses();
                    // query.addresses = query_tmp.addresses;
                    
                    query.earlyStop = vEarlyStop[k];

                    // Query Process
                    start = system_clock::now();
                    auto [res, qVO] = chainQuery(*chains[j][k], query);
                    end = system_clock::now();
                    dur = duration_cast<microseconds>(end -start);
                    currentMetrics.queryTime = dur.count(); // Store query time
                    // cout << "result: " << res << endl;

                    // Verify
                    start = system_clock::now();
                    verify = chainVerify(query, res, qVO, *chains[j][k], currentMetrics);
                    end = system_clock::now();
                    dur = duration_cast<microseconds>(end -start);
                    currentMetrics.verifyTime = dur.count(); // Store verify time
                    assert(verify);

                    // Size calculation
                    tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[j][k]->adsType);
                    metricsAcc[j][k].accumulate(currentMetrics);
                } // End repeats loop (l)
                metricsAcc[j][k].average(params.repeats);
                metricsAcc[j][k].writeDataRow(outfiles[i], std::to_string(combines[j]), k);
            } // End ADS type loop (k)
        } // End time window loop (j)
        outfiles[i].close();
    }
    
    for (auto chain : chains) {
        for (auto c : chain) {
            delete c;
        }
    }
}


// 7. test false positive rate for MBFT and MBFT-BF
void testFalsePositiveRate(const Params& params) {
    // Parameters for compared methods: MBFT-BF, MBFT
    vector<ADSType> vAdsType {MBFT_BF, MBFT};
    vector<double> epsilons = {0.01, 0.03, 0.05, 0.07, 0.09};
    // vector<double> epsilons = {0.05};
    
    ParamBF paramBF = params.paramBF;

    vector<bool> vEarlyStop {0, 0};
    vector<unsigned int> vCombineCycle {0, 0};

    // Chain construction
    vector<vector<Chain*>> chains(epsilons.size(), vector<Chain*>(vAdsType.size()));
    // Create pool
    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);
    // Create chain
    for (int i = 0; i < epsilons.size(); i++) {
        paramBF.nFPRate = epsilons[i];
        for (int j = 0; j < vAdsType.size(); j++) {
            chains[i][j] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[j], vEarlyStop[j], vAdsType[j]);
            chains[i][j]->buildChain(txnPool);
            cout << "FPRate: " << epsilons[i] << " ADS Chain:" << j << " built" << endl;
            cout << "Root BF size: " << chains[i][j]->getBlock(1)->getRoot(RootType::COUNT_ADS)->getBF()->getSize() << endl;
        }
    }

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(epsilons.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    ofstream outfile;
    outfile.open(params.outPath + "/FPRate.csv", ios::out);
    ExperimentMetrics::writeHeader(outfile, "False Positive");

    // Query construction
    int x = 128;
    int n = params.repeats;
    int w = 800;
    double v = params.valueRange;
    string boolean_op = "or";
    int s = 2;
    AggType agg = params.agg;
    string queryPath = params.queryPath + "/false_pos";

    QueryType queryType = OR_RANGE;

    // vector<Query> queries = readQueriesFromFile(queryPath, x, n, w, v, boolean_op, s, agg);

    // test different epsilons
    for (int i = 0; i < epsilons.size(); i++) {
        cout << ">>> Testing epsilon: " << epsilons[i] << endl;
        // test different ADS types
        for (int j = 0; j < vAdsType.size(); j++) {
            cout << ">>> Testing ADS type: " << j << endl;
            // metricsAcc[i][j].reset();
            for (int k = 0; k < params.repeats; k++) { 
                ExperimentMetrics currentMetrics; // Metrics for this single run
                currentMetrics.reset();

                // Query query = queries[k];
                Query query = genQuery(params, agg, w, v, s, queryType, txnPool, chains[i][j]->getSize());
                query.earlyStop = vEarlyStop[j];

                // Query Process
                start = system_clock::now();
                auto [res, qVO] = chainQuery(*chains[i][j], query);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.queryTime = dur.count(); // Store query time
                // cout << "result: " << res << endl;

                // Verify
                start = system_clock::now();
                verify = chainVerify(query, res, qVO, *chains[i][j], currentMetrics);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.verifyTime = dur.count(); // Store verify time
                assert(verify);
                

                // Size calculation
                tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[i][j]->adsType);
                // cout << "i: " << i << " j: " << j << " VO size: " << currentMetrics.voSize << " BF size: " << currentMetrics.bfSize << endl;
                metricsAcc[i][j].accumulate(currentMetrics);
            } // End repeats loop (k)
            // Average results
            // metricsAcc[i][j].print();
            metricsAcc[i][j].average(params.repeats);
            // Write averaged results using metricsAcc object
            metricsAcc[i][j].writeDataRow(outfile, std::to_string(epsilons[i]), j);
        } // End ADS type loop (j)
    } // End epsilon loop (i)
    
    outfile.close();
    for (auto chainVec : chains) {
        for (auto chain : chainVec) {
            delete chain;
        }
    } 

}


// 8. test block size for MHT, MBFT, MBFT-es, MBFT-mc, MBFT-all
void testBlockSize(const Params& params) {
    string outputName = "BlockSize";
    // Parameters for compared methods
    vector<ADSType> vAdsType {MHT, MBFT, MBFT, MBFT, MBFT};
    vector<int> blk_sizes = {32, 64, 128, 256, 512};
    // vector<int> blk_sizes = {32};

    // vector<double> epsilons = {0.05};
    ParamBF paramBF = params.paramBF;
    vector<bool> vEarlyStop {0, 0, 1, 0, 1};
    vector<unsigned int> vCombineCycle {0, 0, 0, 8, 8};

    // Chain construction
    vector<TxnPool> txnPools;
    vector<vector<Chain*>> chains(blk_sizes.size(), vector<Chain*>(vAdsType.size()));
    // Create chain
    cout << ">>> Creating chain..." << endl;
    for (int i = 0; i < blk_sizes.size(); i++) {
        // Create pool
        string inPath = "../../data/eth-" + to_string(blk_sizes[i]) + ".dat";
        TxnPool txnPool(params.nSeed, params.supportAggs, inPath);
        txnPools.push_back(txnPool);
        for (int j = 0; j < vAdsType.size(); j++) {
            chains[i][j] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[j], vEarlyStop[j], vAdsType[j]);
            chains[i][j]->buildChain(txnPool);
            cout << "Chain blk size: " << blk_sizes[i] << " ads type: " << j << " built" << endl;
        }
    }
    cout << "<<< Chain created" << endl;

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(blk_sizes.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    ofstream outfile;
    outfile.open(params.outPath + "/" + outputName + ".csv", ios::out);
    ExperimentMetrics::writeHeader(outfile, outputName);

    // Query construction
    int x = 0;
    int n = params.repeats;
    int w = 800;
    double v = params.valueRange;
    string boolean_op = "or";
    int s = 2;
    AggType agg = params.agg;

    QueryType queryType = OR_RANGE;

    string queryPath = params.queryPath + "/blk_size";

    // test different epsilons
    for (int i = 0; i < blk_sizes.size(); i++) {
        cout << ">>> Testing block size: " << blk_sizes[i] << endl;
        x = blk_sizes[i];        
        
        // vector<Query> queries = readQueriesFromFile(queryPath, x, n, w, v, boolean_op, s, agg);

        // test different ADS types
        for (int j = 0; j < vAdsType.size(); j++) {
            cout << ">>> Testing ADS type j: " << j << endl;
            // metricsAcc[i][j].reset();
            for (int k = 0; k < params.repeats; k++) {
                ExperimentMetrics currentMetrics; // Metrics for this single run
                currentMetrics.reset();

                // Query query = queries[k];
                Query query = genQuery(params, agg, w, v, s, queryType, txnPools[i], chains[i][j]->getSize());

                query.earlyStop = vEarlyStop[j];

                // Query Process
                start = system_clock::now();
                auto [res, qVO] = chainQuery(*chains[i][j], query);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.queryTime = dur.count(); // Store query time
                // cout << "result: " << res << endl;

                // Verify
                start = system_clock::now();
                verify = chainVerify(query, res, qVO, *chains[i][j], currentMetrics);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.verifyTime = dur.count(); // Store verify time
                assert(verify);
                

                // Size calculation
                tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[i][j]->adsType);
                // cout << "i: " << i << " j: " << j << " VO size: " << currentMetrics.voSize << " BF size: " << currentMetrics.bfSize << endl;
                metricsAcc[i][j].accumulate(currentMetrics);
            } // End repeats loop (k)
            // Average results
            metricsAcc[i][j].average(params.repeats);
            // Write averaged results using metricsAcc object
            metricsAcc[i][j].writeDataRow(outfile, std::to_string(blk_sizes[i]), j);
        } // End ADS type loop (j)
    } // End epsilon loop (i)
    
    outfile.close();
    for (auto chainVec : chains) {
        for (auto chain : chainVec) {
            delete chain;
        }
    } 

}


// 9. test agg
void testAgg(const Params& params)
{
    string outputName = "agg";

    // Parameters for compared methods: MHT, MBFT, MBFT-es, MBFT-mc, MBFT-all
    vector<ADSType> vAdsType {MHT, MBFT, MBFT, MBFT, MBFT};
    vector<AggType> aggTypes = {MAX, COUNT, SUM, COUNT_DISTINCT};
    
    ParamBF paramBF = params.paramBF;

    vector<bool> vEarlyStop {0, 0, 1, 0, 1};
    // vector<bool> vEarlyStop {0, 1};
    vector<unsigned int> vCombineCycle {0, 0, 0, 8, 8};
    // vector<unsigned int> vCombineCycle {0, 10};

    // Chain construction
    vector<Chain*> chains(vAdsType.size());
    // Create pool
    cout << "Creating pool... " << params.inPath << endl;
    TxnPool txnPool(params.nSeed, params.supportAggs, params.inPath);
    // Create chain
    cout << "Creating chain..." << endl;
    for (int i = 0; i < vAdsType.size(); i++) {
        chains[i] = new Chain(paramBF, params.nSeed, params.supportAggs, vCombineCycle[i], vEarlyStop[i], vAdsType[i]);
        chains[i]->buildChain(txnPool);
        cout << "ADS Chain:" << i << " built" << endl;
    }

    // res & VO
    double res;
    bool verify;
    queue<stack<VONode>> qVO;
    queue<vector<Transaction>> qVOMHT;

    // Use ExperimentMetrics for accumulation for ALL types
    vector<vector<ExperimentMetrics>> metricsAcc(aggTypes.size(), vector<ExperimentMetrics>(vAdsType.size()));
    // time
    time_point<system_clock> start, end;
    duration<double, micro> dur(0);

    // Output file
    try {
        std::filesystem::create_directories(params.outPath);
    } catch (const std::filesystem::filesystem_error& e) {
        std::cerr << "Error when creating directory '" << params.outPath << "': " << e.what() << std::endl;
        throw;
    }
    ofstream outfile(params.outPath + "/" + outputName + ".csv", ios::out);
    ExperimentMetrics::writeHeader(outfile, outputName);

    // Query construction
    int x = 128;
    int n = params.repeats;
    int w = 800;
    double v;
    string boolean_op = "or";
    int s = 2;
    AggType agg;
    string queryPath = params.queryPath + "/" + outputName;
    QueryType queryType = OR_RANGE;

    // test different epsilons
    for (int j = 0; j < aggTypes.size(); j++) {
        cout << ">>> Testing agg type: " << aggTypes[j] << endl;
        // reset last queryType i
        for(int k=0; k < vAdsType.size(); ++k) {
            metricsAcc[j][k].reset();
        }
        
        agg = aggTypes[j];

        // vector<Query> queries = readQueriesFromFile(queryPaths[i], x, n, w, v, boolean_op, s, agg);
        // test different ADS types
        for (int k = 0; k < vAdsType.size(); k++) {
            cout << ">>> Testing ADS type: " << k << endl;
            // metricsAcc[i][j].reset();
            // for (int l = 0; l < 2; l++) { 
            for (int l = 0; l < params.repeats; l++) {
                ExperimentMetrics currentMetrics; // Metrics for this single run
                currentMetrics.reset();

                // Query query = queries[l];
                // cout << "query_tmp: ";
                // query_tmp.printAddresses();
                Query query = genQuery(params, agg, w, v, s, queryType, txnPool, chains[k]->getSize());
                // cout << "query: ";
                // query.printAddresses();
                // query.addresses = query_tmp.addresses;
                
                query.earlyStop = vEarlyStop[k];

                // Query Process
                start = system_clock::now();
                auto [res, qVO] = chainQuery(*chains[k], query);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.queryTime = dur.count(); // Store query time
                // cout << "result: " << res << endl;

                // Verify
                start = system_clock::now();
                verify = chainVerify(query, res, qVO, *chains[k], currentMetrics);
                end = system_clock::now();
                dur = duration_cast<microseconds>(end -start);
                currentMetrics.verifyTime = dur.count(); // Store verify time
                assert(verify);

                // Size calculation
                tie(currentMetrics.voSize, currentMetrics.bfSize) = computeVO(qVO, chains[k]->adsType);
                metricsAcc[j][k].accumulate(currentMetrics);
            } // End repeats loop (l)
            metricsAcc[j][k].average(params.repeats);
            metricsAcc[j][k].writeDataRow(outfile, std::to_string(aggTypes[j]), k);
        } // End ADS type loop (k)
    } // End time window loop (j)
    outfile.close();
    
    for (auto chain : chains) {
        delete chain;
    }
}


int main(int argc, char *argv[])
{

    cmdline::parser argParser;

    argParser.add<int>("seed", 's', "Seed for randomness in Hash & BF", false, 0);
    argParser.add<double>("FPRate", 'f', "False positive rate for BF", false, 0.05, cmdline::range(0.0, 1.0));

    // ADS construction
    argParser.add<int>("ADSType", 'm', "Run which Merkle ADS: MHT, MBFT, MBFT-BF, VHAIN", false, MBFT);
    argParser.add<unsigned int>("combineCycle", 'c', "Enable multi-combine with given combination cycle", false, 0, cmdline::range(0, 100));
    argParser.add("earlyStop", 'e', "Enable value pruning for early stop");
    

    // Experiment
    argParser.add<string>("inputFile", 'i', "Input file", false, "../data/eth-128.dat");
    argParser.add<string>("outputDir", 'o', "Output directory", false, "../res");
    argParser.add<string>("queryDir", 'q', "Query directory", false, "../query");
    argParser.add<int>("repeats", 'r', "Repeat times",false, 1);

    // supported aggregations
    argParser.add<int>("supportAggs", 'A', "Supported aggregates 1111 (MAX, COUNT, SUM, COUNT_DISTINCT)", false, 1, cmdline::range(1, 15));
    // Query requirements
    argParser.add<int>("queryAgg", 'a', "Query aggregation type: MAX, COUNT, SUM, COUNT_DISTINCT",false, 0, cmdline::range(0, 3));
    argParser.add<double>("valueRange", 'v', "Value range",false, 0.5, cmdline::range(0.0, 1.0));

    /* Test Case
     * 1. Single shot:  Build a chain, do single query & verification
     * 2. Construct:    Test i) construction time & ii) ADS size for MHT, MBFT, MBFT-comb
     * 3. Compare:      Test i) query time, ii) VO size, iii) verify time for different `query type` & `time window` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es
     * 4. Range:        Test i) query time, ii) VO size, iii) verify time for different `value range` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es
     * 5. Selectivity:  Test i) query time, ii) VO size, iii) verify time for different `keyword selectivity` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es
     * 6. combineCycle: Test i) query time, ii) VO size, iii) verify time for different `combine cycle` on MBFT-comb-es
     * 7. FPRate:       Test i) query time, ii) VO size, iii) verify time for different `FPRate` on MBFT, MBFT-BF
     * 8. BlockSize:    Test i) query time, ii) VO size, iii) verify time for different `block size` on MHT, MBFT, MBFT-comb, MBFT-BF, VCHAIN
     * */
    argParser.add<int>("test", 't', "Test cases",false, 1, cmdline::range(0, 8));

    argParser.parse_check(argc, argv);

    // Test case
    int testCase = argParser.get<int>("test");
    Params params(argParser);

    // print start time: hour, minute, second
    auto start = chrono::high_resolution_clock::now();
    auto start_time = chrono::system_clock::to_time_t(start);
    cout << "==========Start time: " << put_time(localtime(&start_time), "%H:%M:%S") << endl;

    switch(testCase) {
        case 1: // single test
            cout << "Case 0. Single shot:  Build a chain, do single query & verification" << endl;
            runSingleShot(params);
            break;
        case 2:
            cout << "Case 2. Construct:    Test i) construction time & ii) ADS size for MHT, MBFT, MBFT-comb" << endl;
            testConstruct(params);
            break;
        case 3:
            cout << "Case 3. Compare:      Test i) query time, ii) VO size, iii) verify time for different `query type` & `time window` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es" << endl;
            testCompare(params);
            break;
        case 4:
            cout << "Case 4. Range:        Test i) query time, ii) VO size, iii) verify time for different `value range` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es" << endl;
            testRange(params);
            break;
        case 5:
            cout << "Case 5. Selectivity:  Test i) query time, ii) VO size, iii) verify time for different `keyword selectivity` on MHT, MBFT, MBFT-comb, MBFT-es, MBFT-comb-es" << endl;
            testSelectivity(params);
            break;
        case 6:
            cout << "Case 6. combineCycle: Test i) query time, ii) VO size, iii) verify time for different `combine cycle` on MBFT-comb-es" << endl;
            testCombine(params);
            break;
        case 7:
            cout << "Case 7. FPRate: Test i) query time, ii) VO size, iii) verify time for different `FPRate` on MBFT, MBFT-BF" << endl;
            testFalsePositiveRate(params);
            break;
        case 8:
            cout << "Case 8. BlockSize: Test i) query time, ii) VO size, iii) verify time for different `block size` on MHT, MBFT, MBFT-comb, MBFT-BF, VCHAIN" << endl;
            testBlockSize(params);
            break;
        case 9:
            cout << "Case 9. Agg: Test i) query time, ii) VO size, iii) verify time for different `agg type` on MHT, MBFT, MBFT-comb, MBFT-BF, VCHAIN" << endl;
            testAgg(params);
            break;
        default:
            break;
    }

    auto end = chrono::high_resolution_clock::now();
    auto end_time = chrono::system_clock::to_time_t(end);
    cout << "==========End time: " << put_time(localtime(&end_time), "%H:%M:%S") << endl;
    cout << "==========Total time: " << chrono::duration_cast<chrono::seconds>(end - start).count() << " seconds" << endl;

    return 0;
}