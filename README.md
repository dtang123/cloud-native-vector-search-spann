# Index Build
## Command
```
./indexbuilder -d 960 -v Float -f XVEC -i ~/gist_base.fvecs -o ~/indexes -a BKT -t 2 
```
## Build config
```
[Base]
ValueType=Float
DistCalcMethod=L2
IndexAlgoType=BKT
Dim=960
VectorPath=/home/ec2-user/GIST1M/gist_base.bin
VectorType=DEFAULT
QueryPath=/home/ec2-user/gist_query.bin
QueryType=DEFAULT
WarmupPath=/home/ec2-user/gist_query.bin
WarmupType=DEFAULT
TruthPath=/home/ec2-user/gist_groundtruth.bin
TruthType=DEFAULT
IndexDirectory=/home/ec2-user/indexes_spann

[SelectHead]
isExecute=true
TreeNumber=1
BKTKmeansK=32
BKTLeafSize=8
SamplesNumber=1000
SaveBKT=false
SelectThreshold=10
SplitFactor=6
SplitThreshold=25
Ratio=0.12
NumberOfThreads=2
BKTLambdaFactor=1.0

[BuildHead]
isExecute=true
NeighborhoodSize=32
TPTNumber=32
TPTLeafSize=50
MaxCheck=16324
MaxCheckForRefineGraph=16324
RefineIterations=3
NumberOfThreads=2
BKTLambdaFactor=-1.0

[BuildSSDIndex]
isExecute=true
BuildSsdIndex=true
InternalResultNum=64
ReplicaCount=8
PostingPageLimit=12
NumberOfThreads=2
MaxCheck=16324
TmpDir=/tmp/
```
# Experiment script
```
#!/bin/bash

# Configuration Paths
QUERY_FILE="/dev/shm/gist_query.bin"
INDEX_FOLDER="/dev/shm/sptag_metadata"
TRUTH_FILE="/dev/shm/gist_groundtruth.bin"
RESULTS_FILE="spann_results.txt"

for cache_size in 4096; do
    for nprobe in 8 16 32 64 128; do
        for num_concurrent_queries in 1 4 16; do
            echo "Starting: Cache=${cache_size}M, nprobe=${nprobe}, Threads=${num_concurrent_queries}"
            echo "${cache_size} ${nprobe} ${num_concurrent_queries}" >> "$RESULTS_FILE"
            for l in 1 2 3 4 5; do
                ./SPTAG/Release/indexsearcher \
                    --vectortype Float \
                    --valuetype Float \
                    --dimension 960 \
		    --input "$QUERY_FILE" \
                    --filetype DEFAULT \
                    --index "$INDEX_FOLDER" \
                    --threads "${num_concurrent_queries}" \
                    --truth "$TRUTH_FILE" \
                    --KNN 10 \
                    --truthKNN 10 \
                    --enablecache true \
                    --cachesize "${cache_size}M" \
                    BuildSSDIndex.SearchInternalResultNum="${nprobe}" \
                    BuildSSDIndex.MaxDistRatio=8.0 \
                    BuildSSDIndex.MaxCheck=16384 \
                    BuildSSDIndex.ResultNum=10 \
                    BuildSSDIndex.InternalResultNum=32 \
                    BuildSSDIndex.HashTableExponent=4 \
                    &>> "$RESULTS_FILE"
                wait
            done
        done
    done
done
```
