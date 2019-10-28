# Tpcds-benchmark
Tool for generate tpc-ds data and run benchmark. 

# Motivation
This is a refactor of https://github.com/databricks/spark-sql-perf. The source project tightly couples generating data and running benchmark. But we want to decouple it as two seperate steps:

1. Generate data as standard outout or directly into HDFS
2. Run benchmark test by reading data source from local file or HDFS.

# How to build

Run `mvn clean package`


