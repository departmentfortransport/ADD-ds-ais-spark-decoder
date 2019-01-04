# Spark/Scala - AIS Decoder

A decoder to transform raw AIS messages to tabular parquet files. 

Currently supporting message types:

* **1**, **2** and **3**: Position Report Class A
* **5**: Static and Voyage Related Data
* **18**: Standard Class B CS Position Report
* **24**: Static Data Report

## Approach

The decoder builds on the code from [datasciencecampus/off-course](https://github.com/datasciencecampus/off-course). We also followed the specification detailed in [Eric S. Raymond's writeup](http://catb.org/gpsd/AIVDM.html) of AIVDM/AIVDO protocol and adapted [schwehr/libais](https://github.com/schwehr/libais) C++ implementation. 

## Code

Written in `scala` 2.11.8 and built using `sbt` 1.2.6. 

To compile a package from `./src`that can be run on  a Spark cluster use `sbt package`. The output `.jar` will be in `./target`

```
.
├── README.md			
├── build.sbt			# Build config
├── project				# Project config
├── src					# Source code
└── target				# Compiled Jars
```

## Running Spark job on GCP Dataproc

The following commands summarise the steps needed to run the job on GCP Dataproc. 

Note the example here is just one of the stages. The whole process is formalised in an Apache Airflow DAG, see `/airflow`. 

### Build `.jar` and upload to bucket

```sh
cd /aisdecode # Project root, same location as build.sbt

sbt package 

# Put the compiled jar in a bucket the dataproc cluster has access to
gsutil cp \
	target/scala-2.11/ais-decode_*.jar \
	gs://dft-dst-prt-ais-resources/
```

### Create a cluster 

Here we are using 20 16 core high memory machines machines:

```sh
gcloud beta dataproc clusters create ais-dataproc  \
   --project=dft-dst-prt-ais \
   --region=europe-west1 \
   --zone=europe-west1-b \
   --image-version 1.3 \
   --master-machine-type n1-highmem-4 \
   --master-boot-disk-size 500 \
   --num-workers 20 \
   --worker-machine-type n1-highmem-16 \
   --max-idle=10m
```

### Submit a job

```sh
gcloud beta dataproc jobs submit spark \
  --cluster ais-dataproc \
  --project dft-dst-prt-ais \
  --region=europe-west1 \
  --jars gs://dft-dst-prt-ais-resources/ais-decode_2.11-0.1.0.jar \
  --class uk.gov.dft.ais.decode.RawDecode \
  -- 'gs://dft-dst-prt-ais-raw-2016/*.dat' 'gs://dft-dst-prt-ais-decoded-2016/2016_Decoded_raw/'
```

Where the `--jars` is the compiled script stored in a bucket and `--class` is the object we want to call, where the main function will be called. 

### Monitor the job

The UIs are not exposed to the internet so you need to establish an SSH tunnel:

```sh
# Create SSH  to dataproc cluster master
gcloud compute ssh ais-dataproc-m \
    --project=dft-dst-prt-ais \
    --zone=europe-west1-b  -- \
    -D 1080 -N
```

Then launch a Chrome instance configured to use that tunnel:

```sh
# Launch browser with socks5 proxy
"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" \
  --proxy-server="socks5://localhost:1080" \
  --host-resolver-rules="MAP * 0.0.0.0 , EXCLUDE localhost" \
  --user-data-dir=/tmp/ais-dataproc-m
```

The UI's will then be available from that browser at: 

- Hadoop dashboard: http://ais-dataproc-m:8088/cluster

- HDFS namenode: http://ais-dataproc-m:9870/

### Delete cluster

```sh
gcloud dataproc clusters delete ais-dataproc --quiet
```



## Walkthrough of jobs

At present, to generate the files needed for the port clusters the following jobs need to be run. This will probably be formalised as an Airflow DAG in future. 

```sh
# 1 - Generate intermediate files
# Note: at this step google cloud storage files with spaces cause an error
# rename with underscore if so
gcloud beta dataproc jobs submit spark \
  --cluster ais-dataproc \
  --project dft-dst-prt-ais \
  --region=europe-west1 \
  --jars gs://dft-dst-prt-ais-resources/ais-decode_2.11-0.1.0.jar \
  --class uk.gov.dft.ais.decode.RawDecode \
  -- 'gs://dft-dst-prt-ais-raw-2016/*.dat' 'gs://dft-dst-prt-ais-decoded-2016/2016_Decoded_raw/'
  
# 2 - Then decode the message types you need, here I'll do 123 and 5
# note, we are using the files generated at step 1
gcloud beta dataproc jobs submit spark \
  --cluster ais-dataproc \
  --project dft-dst-prt-ais \
  --region=europe-west1 \
  --jars gs://dft-dst-prt-ais-resources/ais-decode_2.11-0.1.0.jar \
  --class uk.gov.dft.ais.decode.Decode123 \
  -- 'gs://dft-dst-prt-ais-decoded-2016/2016_Decoded_raw/' 'gs://dft-dst-prt-ais-decoded-2016/2016_Decoded_123/'
  
gcloud beta dataproc jobs submit spark \
  --cluster ais-dataproc \
  --project dft-dst-prt-ais \
  --region=europe-west1 \
  --jars gs://dft-dst-prt-ais-resources/ais-decode_2.11-0.1.0.jar \
  --class uk.gov.dft.ais.decode.Decode5 \
  -- 'gs://dft-dst-prt-ais-decoded-2016/2016_Decoded_raw/' 'gs://dft-dst-prt-ais-decoded-2016/2016_Decoded_5/'
  
# 3+ - We can then keep doing this for each message type we want to decode.
```



