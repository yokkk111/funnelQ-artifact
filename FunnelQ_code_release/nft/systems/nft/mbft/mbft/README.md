# README


## Usage

```
./mbft [-option]

optional arguments:
    -s, --seed            Seed for randomness in Hash & BF
    -f, --FPRate          False positive rate for BF
    -m, --MHT             Run a MHT instance as baseline
    -c, --combineCycle    Enable multi-combine with given combination cycle
    -e, --earlyStop       Enable value pruning for early stop
    -A, --supportAggs     Supported aggregates (MAX, COUNT, SUM, COUNT_DISTINCT)
    -i, --inputPath       Input path
    -o, --outputPath      Output path
    -r, --repeats         Repeat times
    -a, --queryAgg        Query aggregation type: MAX, COUNT, SUM, COUNT_DISTINCT
    -v, --valueRange      Value range
    -t, --testCase        Test cases
                          * 1. Single shot:  Build a chain, do single query & verification
                          * 2. Construct:    Test i) construction time & ii) ADS size
                          * 3. Compare:      Test i) query time, ii) VO size, iii) verify time for different `query type` & `time window`
                          * 4. Range:        Test i) query time, ii) VO size, iii) verify time for different `value range`
                          * 5. Selectivity:  Test i) query time, ii) VO size, iii) verify time for different `keyword selectivity`
                          * 6. combineCycle: Test i) query time, ii) VO size, iii) verify time for different `combine cycle`
                          * 7. FPRate:       Test i) query time, ii) VO size, iii) verify time for different `FPRate`
                          * 8. BlockSize:    Test i) query time, ii) VO size, iii) verify time for different `block size`
```

Example of usage:

```
./mbft -A 7 -a 2 -c 8 -r 100 -t 5 -i path_to_dataset
```