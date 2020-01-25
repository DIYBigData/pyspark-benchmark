# PySpark Benchmark

This project is intended to provide a lightweight benchmarking utility for PySpark, part of [Apache Spark](https://spark.apache.org). The motivation behind it was to create benchmarks of common PySpark operations so that before and after comparisons can be made when changes in the Spark configuration or the underlying cluster hardware are made.

# Instructions
The benchmarking process has two basic steps: generate test data, then run the benchmark.
## Generate Test Data
Generating test data is done with the `generate-data.py` PySpark job. The file generated will be a partitioned CSV file. To run it, use this command:

```
spark-submit --master spark://spark-master:7077 --name 'generate-benchmark-test-data' \
	generate-data.py /path/to/test/data/file -r num_rows -p num_partitions
```
Where
* `--master spark://spark-master:7077` - The `spark-submit` option identifying where your Spark master is.
* `--name 'generate-benchmark-test-data'` - The `spark-submit`	option for naming the submitted job. this is optional.
* `/path/to/test/data/file` - The complete file path to where the test data should be generated to. This can be a HDFS file URL. The exact value depends on your Spark cluster set up.
* `-r num_rows` - The total number of rows to generate. Each row is about 75 bytes.
* `-p num_partitions` - The number of partitions the test data file should have. 

The schema of the data produced looks like:
```
root
 |-- value: string (nullable = true)
 |-- prefix2: string (nullable = true)
 |-- prefix4: string (nullable = true)
 |-- prefix8: string (nullable = true)
 |-- float_val: string (nullable = true)
 |-- integer_val: string (nullable = true)
```
And some sample rows looks like:
```
value,prefix2,prefix4,prefix8,float_val,integer_val
52fcc47f62a8486790270c7b05af11a4,52,52fc,52fcc47f,850015.3529053964,850015
0f6617e9fdd647888d69328e91aa127b,0f,0f66,0f6617e9,644648.2860366029,644648
```
It is recommended that once a benchmark test file is generated, you re-use it across multiple benchmark runs. There is no need to generate a new test data file for each benchmark run.

## Running the Benchmark Tests

### Shuffle Benchmark
The Shuffle Benchmark stresses common PySpark operations on data frames that would trigger a shuffle. The test operations include:
* Group By and Aggregate 
* Repartition
* Inner Join
* Broadcast Inner Join

Each operation is timed independently. 

To run the shuffle benchmark, use this command:
```
spark-submit --master spark://spark-master:7077 \
	benchmark-shuffle.py /path/to/test/data/file -r num_partitions -n 'benchmark-job-name'
```
Where:
* `--master spark://spark-master:7077` - The `spark-submit` option identifying where your Spark master is. 
* `/path/to/test/data/file` - The complete file path to where the test data to be used was generated to. This can be a HDFS file URL. The exact value depends on your Spark cluster set up and what filepath you used when generating the test data.
* `-r num_partitions` - This sets the number of partitions that the the test data should be repartitioned to during the Repartition benchmark test. 
* `-n 'benchmark-job-name'` - The name to use for this job. In this case, it is not a `spark-submit` option because the benchmarking job uses it too.
* `-o /results/file/path` - qualified file path to where the test timing results should be emitted. This optional, and if not present no results file will be written.

The results of the benchmarking will be printed to the job's `INFO` logger, and will appear near the end of the log stream. It will look something like this:
```
20/01/11 22:49:55 INFO __main__: 
20/01/11 22:49:55 INFO __main__: **********************************************************************
20/01/11 22:49:55 INFO __main__:     RESULTS    RESULTS    RESULTS    RESULTS    RESULTS    RESULTS
20/01/11 22:49:55 INFO __main__:     Test Run = run-shuffle-benchmark
20/01/11 22:49:55 INFO __main__: 
20/01/11 22:49:55 INFO __main__: Group By test time         = 123.87005627900362 seconds
20/01/11 22:49:55 INFO __main__: Repartition test time      = 247.30380326602608 seconds (200 partitions)
20/01/11 22:49:55 INFO __main__: Inner join test time       = 373.45172010397073 seconds 
20/01/11 22:49:55 INFO __main__: Broadcast inner join time  = 308.29632237099577 seconds 
20/01/11 22:49:55 INFO __main__: 
20/01/11 22:49:55 INFO __main__: **********************************************************************
```

### CPU Benchmark
The CPU Benchmark stresses PySpark operations that should be primarily CPU bound. This is not to say that there would be any disk or network I/O, just that the CPU speed and task efficiency should be the primary factors in the benchmark's performance. The test operations for this benchmark include:

* SHA-512 hashing of a string
* Estimating Pi with random samples and an user defined Python function
* Estimating Pi with random samples and native Spark functions only

Each operation is timed independently.

To run the CPU benchmark, use this command:

```
spark-submit --master spark://spark-master:7077 \
	benchmark-cpu.py /path/to/test/data/file -s pi_samples -p pi_tasks -n 'benchmark-job-name'
```
Where:
* `--master spark://spark-master:7077` - The `spark-submit` option identifying where your Spark master is. 
* `/path/to/test/data/file` - The complete file path to where the test data to be used was generated to. This can be a HDFS file URL. The exact value depends on your Spark cluster set up and what filepath you used when generating the test data.
* `-s pi_samples` - The number of random samples that will be taken to calculate Pi. Defaults to 5 billion.
* `-p pi_tasks` - The number of parallel tasks that will be used to take random samples to calculate pi.
* `-o /results/file/path` - qualified file path to where the test timing results should be emitted. This optional, and if not present no results file will be written.

The results of the benchmarking will be printed to the job's `INFO` logger, and will appear near the end of the log stream. It will look something like this:
```
20/01/25 04:33:58 INFO __main__: ****************************************************************************
20/01/25 04:33:58 INFO __main__:     RESULTS    RESULTS    RESULTS    RESULTS    RESULTS    RESULTS
20/01/25 04:33:58 INFO __main__:     Test Run = pyspark-benchmark-cpu
20/01/25 04:33:58 INFO __main__: 
20/01/25 04:33:58 INFO __main__: SHA-512 benchmark time                 = 1021.0458517669999 seconds for 2,000,000,000 hashes
20/01/25 04:33:58 INFO __main__: Calculate Pi benchmark                 = 636.8594892990002 seconds with pi = 3.14151414112, samples = 25,000,000,000
20/01/25 04:33:58 INFO __main__: Calculate Pi benchmark using dataframe = 9.309087140999964 seconds with pi = 3.14159186944, samples = 25,000,000,000
20/01/25 04:33:58 INFO __main__: 
20/01/25 04:33:58 INFO __main__: ****************************************************************************
```

# Further Development
Other benchmark tests will be added. Pull requests are welcome.
