# Steps:
## Index Build
### Build executables
```
mkdir -p build
cd build
cmake ..
make indexbuilder
make indexsearcher
```
### Example buildconfig.ini file
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
### Command
Use DEFAULT for a .bin file
```
./SPTAG/Release/indexbuilder -d 960 -v Float -f DEFAULT -i ./GIST1M/gist_base.bin -o /output/folder -a SPANN -c /path/to/buildconfig.ini -t 2
```
## Set up Minio
Docker-compose.yml
```
services:
  minio:
    image: minio/minio
    container_name: minio
    # network_mode: host bypasses the docker-proxy for max NVMe/Network throughput
    network_mode: "host"
    ulimits:
      nproc: 65535
      nofile:
        soft: 65535
        hard: 65535
    environment:
      - MINIO_REGION_NAME=us-east-2
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
      - MINIO_DOMAIN=minio
      # Performance Tuning
      - MINIO_API_REQUESTS_MAX=4096
      - MINIO_API_REQUESTS_DEADLINE=30s
    volumes:
      - ./minio-data:/data
    # Note: 'ports' and 'networks' are ignored in network_mode: host. 
    # MinIO will be available on your host at 19000 and 9001 directly due to the command below.
    command: ["server", "/data", "--address", ":19000", "--console-address", ":9001"]

  mc:
    depends_on:
      - minio
    image: minio/mc
    container_name: mc
    network_mode: "host"
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-2
    entrypoint: >
      /bin/sh -c "
      until (/usr/bin/mc alias set minio http://127.0.0.1:19000 admin password) do echo '...waiting...' && sleep 1; done;
      /usr/bin/mc rm -r --force minio/warehouse;
      /usr/bin/mc mb minio/warehouse;
      /usr/bin/mc policy set public minio/warehouse;
      tail -f /dev/null
```
```
docker compose up -d
# Copy file to minio Container
~/mc cp /path/to/SPTAGFullList.bin minio-data/warehouse/spann
```
# Setup script
Sets up environment by copying metadata to ramdisk and checking minio is properly set up
```
#!/bin/bash
set -e  # exit on any error

# ─────────────────────────────────────────────────────────────────────────────
# REMOVED: S3 mount for SPTAGFullList.bin
# S3FileIO now reads SPTAGFullList.bin directly via AWS SDK byte-range GETs.
# No local mount or symlink needed for the posting list file.
# ─────────────────────────────────────────────────────────────────────────────

# 1. Environment variables for S3FileIO
# Pointed at local MinIO instance instead of real AWS S3
export SPTAG_S3_BUCKET=warehouse
export SPTAG_S3_KEY=spann/SPTAGFullList.bin
export AWS_DEFAULT_REGION=us-east-2
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password

# MinIO endpoint override — tells the AWS SDK to hit localhost instead of AWS
export MINIO_ENDPOINT=http://127.0.0.1:19000

# Make exports persistent for this session and any child processes
grep -q "SPTAG_S3_BUCKET"      ~/.bashrc || echo "export SPTAG_S3_BUCKET=warehouse"                    >> ~/.bashrc
grep -q "SPTAG_S3_KEY"         ~/.bashrc || echo "export SPTAG_S3_KEY=spann/SPTAGFullList.bin"          >> ~/.bashrc
grep -q "AWS_DEFAULT_REGION"   ~/.bashrc || echo "export AWS_DEFAULT_REGION=us-east-2"                  >> ~/.bashrc
grep -q "AWS_ACCESS_KEY_ID"    ~/.bashrc || echo "export AWS_ACCESS_KEY_ID=admin"                       >> ~/.bashrc
grep -q "AWS_SECRET_ACCESS_KEY" ~/.bashrc || echo "export AWS_SECRET_ACCESS_KEY=password"               >> ~/.bashrc
grep -q "MINIO_ENDPOINT"       ~/.bashrc || echo "export MINIO_ENDPOINT=http://127.0.0.1:19000"         >> ~/.bashrc
grep -q "SPTAG_IO_BACKEND"     ~/.bashrc || echo "export SPTAG_IO_BACKEND=s3"                           >> ~/.bashrc

source ~/.bashrc

# Make sure runtime linker can find AWS SDK shared libs
export LD_LIBRARY_PATH=/home/tang627/awssdk/lib:$LD_LIBRARY_PATH

# 2. Setup RAM Disk Metadata (The "Hot" Data - loaded once at startup)
# These files are small enough to fit in RAM and give microsecond-latency
# head index lookups. Only the 16GB SPTAGFullList.bin stays in S3/MinIO.
rm -rf /dev/shm/sptag_metadata
mkdir -p /dev/shm/sptag_metadata/HeadIndex

echo "Copying index metadata to RAM..."
cp ~/indexes_spann/SPTAGHeadVectorIDs.bin /dev/shm/sptag_metadata/
cp ~/indexes_spann/SPTAGHeadVectors.bin   /dev/shm/sptag_metadata/
cp ~/indexes_spann/DeletedIDs.bin         /dev/shm/sptag_metadata/
touch /dev/shm/sptag_metadata/SPTAGFullList.bin
cp ~/indexes_spann/HeadIndex/tree.bin        /dev/shm/sptag_metadata/HeadIndex/
cp ~/indexes_spann/HeadIndex/graph.bin       /dev/shm/sptag_metadata/HeadIndex/
cp ~/indexes_spann/HeadIndex/vectors.bin     /dev/shm/sptag_metadata/HeadIndex/

# 3. Write the corrected indexloader.ini
cat > /dev/shm/sptag_metadata/indexloader.ini << 'EOF'
[Index]
IndexAlgoType=SPANN
ValueType=Float

[Base]
ValueType=Float
DistCalcMethod=L2
IndexAlgoType=BKT
Dim=960
VectorPath=/dev/shm/sptag_metadata/HeadIndex/vectors.bin
VectorType=DEFAULT
VectorSize=1000000
QueryPath=/dev/shm/gist_query.bin
QueryType=DEFAULT
TruthPath=/dev/shm/gist_groundtruth.bin
TruthType=DEFAULT
IndexDirectory=/dev/shm/sptag_metadata
HeadVectorIDs=SPTAGHeadVectorIDs.bin
DeletedIDs=DeletedIDs.bin
HeadVectors=SPTAGHeadVectors.bin
HeadIndexFolder=HeadIndex
SSDIndex=SPTAGFullList.bin
SSDIndexFileNum=1
IOThreadNum=64
DataBlockSize=4194304
DataCapacity=2147483647

[SelectHead]
isExecute=false

[BuildHead]
isExecute=false
DistCalcMethod=L2
NeighborhoodSize=32
TPTNumber=32
TPTLeafSize=50
MaxCheck=16324
MaxCheckForRefineGraph=16324
RefineIterations=3
NumberOfThreads=2
BKTLambdaFactor=-1.0

[BuildSSDIndex]
isExecute=false
BuildSsdIndex=false
InternalResultNum=64
ReplicaCount=8
PostingPageLimit=12
NumberOfThreads=2
MaxCheck=4096
HashTableExponent=4
SearchPostingPageLimit=12
SearchInternalResultNum=64
MaxDistRatio=8.0
ResultNum=10
TmpDir=/tmp/
EOF

chmod 755 /dev/shm/sptag_metadata/indexloader.ini

# 4. Stage query and ground truth files
echo "Staging query and truth files..."
cp /hdd_root/tang627/GIST1M/gist_query.bin       /dev/shm/
cp /hdd_root/tang627/GIST1M/gist_groundtruth.bin /dev/shm/

# 5. Verify MinIO is reachable and file exists
echo "--- Environment Check ---"
echo "RAM disk contents:"
ls -lh /dev/shm/sptag_metadata/
echo ""
echo "HeadIndex contents:"
ls -lh /dev/shm/sptag_metadata/HeadIndex/
echo ""
echo "MinIO target (verified via curl):"
if ! curl -s --connect-timeout 2 "$MINIO_ENDPOINT/minio/health/live" > /dev/null; then
    echo "ERROR: Cannot reach MinIO at $MINIO_ENDPOINT"
    echo "  -> Check if 'docker compose' is running."
    echo "  -> Ensure 'network_mode: host' is used in docker-compose.yml."
    exit 1
fi
HTTP_STATUS=$(curl -I -s -o /dev/null -w "%{http_code}" "$MINIO_ENDPOINT/$SPTAG_S3_BUCKET/$SPTAG_S3_KEY")

if [ "$HTTP_STATUS" -eq 200 ]; then
    echo "SUCCESS: Found $SPTAG_S3_KEY in bucket '$SPTAG_S3_BUCKET'."
elif [ "$HTTP_STATUS" -eq 404 ]; then
    echo "ERROR: File NOT FOUND (HTTP 404)"
    echo "  -> Expected Path: $MINIO_ENDPOINT/$SPTAG_S3_BUCKET/$SPTAG_S3_KEY"
    echo "  -> Action: Check MinIO Console (Port 9001) and verify the folder structure."
    exit 1
elif [ "$HTTP_STATUS" -eq 403 ]; then
    echo "ERROR: Access Denied (HTTP 403)"
    echo "  -> Check MinIO bucket policies. Is 'warehouse' set to public or custom read?"
    exit 1
else
    echo "ERROR: MinIO returned unexpected status: $HTTP_STATUS"
    exit 1
fi

echo "AWS SDK libs:"
ls /home/tang627/awssdk/lib/libaws-cpp-sdk-{core,s3}.so
echo ""
echo "Environment ready."
echo "Run indexsearcher with:"
echo "  ./SPTAG/Release/indexsearcher \\"
echo "    --index /dev/shm/sptag_metadata \\"
echo "    --dimension 960 --vectortype Float \\"
echo "    --input /dev/shm/gist_query.bin --filetype DEFAULT \\"
echo "    --truth /dev/shm/gist_groundtruth.bin --KNN 10"
```
### Setup traffic control to mimic cloud native indexing
```
sudo tc qdisc add dev lo root netem delay 31ms 20ms distribution normal
```

# Experiment script
Runs experiments
```
#!/bin/bash

source ./Setup_Script
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
