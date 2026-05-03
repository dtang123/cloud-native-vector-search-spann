# Step 1: Clone the repository and build executables
```
git clone {THIS_REPO}
# cd into repo
git submodule update --init --recursive
mkdir -p build
cd build
cmake -DCMAKE_PREFIX_PATH=/path/to/awssdk ..
make indexbuilder
make indexsearcher
```
# Step 2: Index Build
### Command
I have provided buildconfig.ini file in root directory. Alter buildconfig.ini file to adjust parameters of how the index should be built such as centroid percentage.
-d is the dimensions of the dataset
-v is the datatype
-f is the file type your data is in. Use DEFAULT for a .bin file
-i is the input file path (for GIST1m the gist\_base file other datasets might have different files) 
-o is the output folder
-a indicates SPANN index
-t number of threads to build index with
Other build options are available. Refer to the Microsoft SPANN repo and their documentation or the code.
```
./Release/indexbuilder -d 960 -v Float -f DEFAULT -i ./GIST1M/gist_base.bin -o /output/folder -a SPANN -c ./buildconfig.ini -t 2
```
# Step 3: Set up Minio

I have provided the docker-compose.yml file to create the Minio container in root directory. This will replicate an S3 bucket, allowing us to request from it using aws\_sdk.
```
docker compose up -d
# Copy file to minio Container
~/mc cp /path/to/SPTAGFullList.bin minio-data/warehouse/spann
```
# Step 4: Add local file paths to Setup script
I have provided spann\_exp\_setup.sh in the root directory. It sets up environment by copying metadata to ramdisk and checking minio is properly set up. Need to fill out local paths to the index that was built in Step 2

# Step 5: Setup traffic control to mimic cloud native indexing
This will add latency to the requests to MinIO
```
sudo tc qdisc add dev lo root netem delay 31ms 20ms distribution normal
```
# Step 6: Experiment run script
Use the exp\_spann.sh file to run experiments. Can alter the number of threads, nprobe, and trials you want.
```
./exp_spann.sh
```

