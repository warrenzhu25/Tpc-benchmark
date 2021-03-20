# Tpcds-benchmark
Tool for generate tpc-ds data and run benchmark. 

# Motivation
This is a refactor of https://github.com/databricks/spark-sql-perf. The source project tightly couples generating data and running benchmark. But we want to decouple it as two seperate steps:

1. Generate data as standard outout or directly into HDFS
2. Run benchmark test by reading data source from local file or HDFS.
3. Make this could run on windows, so changed dsdgen into Java version. 

# How to build

Run `mvn clean package`

# How to run TPC-DS data generation

Data generation relies on java version of dsdgen. You can refer https://github.com/warrenzhu25/tpcds

```
TpcdsDataGen
Usage: dataDir scaleFactor partitions table overwrite
dataDir - (string) Directory to put tpcds dataset in parquet format. Required 
scaleFactor - (int) Volume of data to generate in GB. Required.
partitions - (int) parallelism on datagen and number of writers. Default same as scaleFactor
table - (string) Table to generate. Default all
overwrite - (bool) Overwrite if existed. Default false 
```

Sample command

```
spark-submit.cmd
--master yarn
--deploy-mode cluster 
--class com.microsoft.TpcdsDataGen 
--num-executors 1000 
--driver-cores 4
--driver-memory 30G
--executor-cores 2 
--executor-memory 10G 
--conf spark.executor.memoryOverhead=6G 
--files "hdfs://tpcds-1.3.jar"
tpcds-benchmark_2.12-1.1-SNAPSHOT.jar 
hdfs://project/spark/tpcds 100000 100000 store_sales true

```

# How to run TPC-DS benchmark

```
TpcdsBenchmark
Usage:
  -c, --cbo                 Enable cbo
  -d, --data-dir  <arg>     Directory contains tpcds dataset in parquet format
  -e, --exclude  <arg>      Exclude query separated by comma. Example: q1,q5
  -i, --iterations  <arg>   The number of iterations for each query
  -q, --queries  <arg>      Queries to run separated by comma, such as 'q4,q5'
  -r, --result-dir  <arg>   Directory for writing benchmark result
  -h, --help                Show help message
```

Sample command

```
spark-submit.cmd
--master yarn
--deploy-mode cluster
--class com.microsoft.TpcdsBenchmark 
--num-executors 1000 
--driver-cores 4
--driver-memory 30G
--executor-cores 2 
--executor-memory 10G 
--conf spark.executor.memoryOverhead=6G
tpcds-benchmark_2.12-1.1-SNAPSHOT.jar.jar 
-d hdfs://project/spark/tpcds
-r hdfs://tpcds/result
```



