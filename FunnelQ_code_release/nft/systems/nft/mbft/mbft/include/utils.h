#ifndef UTILS_H
#define UTILS_H

#include <iostream>
#include <sstream>
#include <cassert>
#include <vector>
#include <stack>
#include <queue>
#include <set>
#include <string>
#include <cstdint>
#include <utility>
#include <fstream>
#include <regex>
#include <random>
#include <chrono>
#include <algorithm>
#include <cmath>

#include "cmdline.h"

using namespace std;
using namespace chrono;

// 0-MAX(mandatory), 1-COUNT, 2-SUM, 3-COUNT_DISTINCT
enum AggType {MAX, COUNT, SUM, COUNT_DISTINCT};

enum RootType {AMOUNT_ADS, AMOUNT_COMBINE_ADS, COUNT_ADS, COUNT_COMBINE_ADS,
    SUM_ADS, SUM_COMBINE_ADS, COUNT_DISTINCT_ADS, COUNT_DISTINCT_COMBINE_ADS};

enum ADSType {MHT, MBFT, MBFT_BF, VCHAIN};

// extern int astryReconstNum;
// extern int reconstNum;
// extern double hintSize;
// extern double reconstTime;
// extern double otherTime;
// extern double pureTime;

// extern double rootTime;
// extern double resTime;
// extern double astrayTime;

// extern double rootSize;
// extern double resSize;
// extern double astraySize;

// extern int rootNum;
// extern int resNum;
// extern int astrayNum;
// extern int totalNum;
// extern int resNodeNum;
// extern int astrayNodeNum;
// extern int totalNodeNum;

struct ExperimentMetrics {
    // General Performance Metrics (Previously separate Acc vectors)
    double queryTime = 0.0;   // Time for the query phase (us)
    double verifyTime = 0.0;  // Time for the verification phase (us)
    double voSize = 0.0;      // Total size of the Verification Object (B)
    double bfSize = 0.0;      // Total size of Bloom Filters within VO (B)

    // Detailed Verification Metrics (Previously global vars)
    // Counters
    double astryReconstNum = 0; // # of reconstructions involving an ASTRAY node
    double reconstNum = 0;      // Total # of reconstructions
    double rootNum = 0;         // # of ROOT nodes processed in verification
    double resNum = 0;          // # of RES nodes processed in verification
    double astrayNum = 0;       // # of ASTRAY/LEFT/RIGHT nodes processed in verification
    double totalNum = 0;        // Total # of VO nodes processed in verification
    double resNodeNum = 0;      // # RES leaf nodes
    double astrayNodeNum = 0;   // # ASTRAY/LEFT/RIGHT/ROOT nodes
    double totalNodeNum = 0;    // Total # nodes based on flags

    // Sizes (Bytes)
    double hintSize = 0.0;    // Total size of HINT data used
    double rootSize = 0.0;    // Size of ROOT node BF
    double resSize = 0.0;     // Total size of RES node BFs
    double astraySize = 0.0;  // Total size of ASTRAY/LEFT/RIGHT node BFs

    // Times (microseconds)
    double reconstTime = 0.0; // Time spent in reconstruction
    double otherTime = 0.0;   // Other verification time (pure - reconst - res - astray - root)
    double pureTime = 0.0;    // Total time for singleVerify execution
    double rootTime = 0.0;    // Time processing ROOT node
    double resTime = 0.0;     // Time processing RES nodes
    double astrayTime = 0.0;  // Time processing ASTRAY/LEFT/RIGHT nodes


    // Method to reset all metrics to zero
    void reset() {
        queryTime = 0.0;
        verifyTime = 0.0;
        voSize = 0.0;
        bfSize = 0.0;

        astryReconstNum = 0.0;
        reconstNum = 0.0;
        rootNum = 0.0;
        resNum = 0.0;
        astrayNum = 0.0;
        totalNum = 0.0;
        resNodeNum = 0.0;
        astrayNodeNum = 0.0;
        totalNodeNum = 0.0;

        hintSize = 0.0;
        rootSize = 0.0;
        resSize = 0.0;
        astraySize = 0.0;

        reconstTime = 0.0;
        otherTime = 0.0;
        pureTime = 0.0;
        rootTime = 0.0;
        resTime = 0.0;
        astrayTime = 0.0;
    }

    // Method to accumulate metrics from another instance
    void accumulate(const ExperimentMetrics& other) {
        queryTime += other.queryTime;
        verifyTime += other.verifyTime;
        voSize += other.voSize;
        bfSize += other.bfSize;

        astryReconstNum += other.astryReconstNum;
        reconstNum += other.reconstNum;
        rootNum += other.rootNum;
        resNum += other.resNum;
        astrayNum += other.astrayNum;
        totalNum += other.totalNum;
        resNodeNum += other.resNodeNum;
        astrayNodeNum += other.astrayNodeNum;
        totalNodeNum += other.totalNodeNum;

        hintSize += other.hintSize;
        rootSize += other.rootSize;
        resSize += other.resSize;
        astraySize += other.astraySize;

        reconstTime += other.reconstTime;
        otherTime += other.otherTime;
        pureTime += other.pureTime;
        rootTime += other.rootTime;
        resTime += other.resTime;
        astrayTime += other.astrayTime;
    }

    // Method to average metrics based on repeat count
    void average(int repeats) {
        if (repeats > 0) {
            double d_repeats = static_cast<double>(repeats);

            queryTime /= d_repeats;
            verifyTime /= d_repeats;
            voSize /= d_repeats;
            bfSize /= d_repeats;


            astryReconstNum /= d_repeats;
            reconstNum /= d_repeats;
            rootNum /= d_repeats;
            resNum /= d_repeats;
            astrayNum /= d_repeats;
            totalNum /= d_repeats;
            resNodeNum /= d_repeats;
            astrayNodeNum /= d_repeats;
            totalNodeNum /= d_repeats;

            hintSize /= d_repeats;
            rootSize /= d_repeats;
            resSize /= d_repeats;
            astraySize /= d_repeats;

            reconstTime /= d_repeats;
            otherTime /= d_repeats;
            pureTime /= d_repeats;
            rootTime /= d_repeats;
            resTime /= d_repeats;
            astrayTime /= d_repeats;
        }
    }

    // Static method to write CSV header
    static void writeHeader(std::ofstream& outfile, const string& firstColumnName) {
        outfile << firstColumnName << ",\tADS,\tquery time(us),\tverify time(us),\tsize(B),\tBF size(B),\t"
                << "hint size(B),\t#reconstruct,\t#astrayRecon,\t"
                << "reconst time(us),\tother time(us),\tpure time(us),\t"
                << "root time(us),\tres time(us),\tastray time(us),\t"
                << "root size(B),\tres size(B),\tastray size(B),\t"
                << "#root,\t#res,\t#astray,\t#total,\t"
                << "#resNode,\t#astrayNode,\t#totalNode" << endl;
    }

    // Member method to write data row to CSV
    // Takes the value for the first column (e.g., window size, range) as a string
    void writeDataRow(std::ofstream& outfile, const string& firstColumnValue, int adsTypeIndex) const {
        outfile << firstColumnValue << ",\t" << adsTypeIndex << ",\t"
                << queryTime << ",\t" << verifyTime << ",\t"
                << voSize << ",\t" << bfSize << ",\t"
                << hintSize << ",\t" << reconstNum << ",\t" << astryReconstNum << ",\t"
                << reconstTime << ",\t" << otherTime << ",\t" << pureTime << ",\t"
                << rootTime << ",\t" << resTime << ",\t" << astrayTime << ",\t"
                << rootSize << ",\t" << resSize << ",\t" << astraySize << ",\t"
                << rootNum << ",\t" << resNum << ",\t" << astrayNum << ",\t" << totalNum << ",\t"
                << resNodeNum << ",\t" << astrayNodeNum << ",\t" << totalNodeNum
                << endl;
    }

    void print() const {
        cout << "================================================" << endl;
        cout << "rootNum: " << rootNum << endl;
        cout << "resNum: " << resNum << endl;
        cout << "astrayNum: " << astrayNum << endl;
        cout << "totalNum: " << totalNum << endl;
        cout << "resNodeNum: " << resNodeNum << endl;
        cout << "astrayNodeNum: " << astrayNodeNum << endl;
        cout << "totalNodeNum: " << totalNodeNum << endl;
        cout << "================================================" << endl;
    }
};


#endif //UTILS_H
