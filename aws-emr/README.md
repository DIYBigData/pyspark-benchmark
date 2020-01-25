# Running PySpark Benchmark jobs in AWS EMR

These files support running the PySpark Benchmark jobs in AWS EMR. It is required that you have `awscli` installed and set up to use to a AWS account profile. To run the benchmark in AWS EMR, follow these steps:

1. Create an S3 bucket, or reuse one of your existing ones.
2. Copy the following files to your S3 bucket:
  * `generate-data.py`
  * `benchmark-shuffle.py`
  * `benchmark-cpu.py`
  * `emr_bootstrap.sh`
3. Generate the test data using this `awscli` command from the root fo this repository, replacing `your-s3-bucket` with the name of your S3 bucket and adjusting the parameters to the job as you need: 

    ```
    aws emr create-cluster --name "Pyspark Benchmark - Generate Data" \
    --release-label emr-5.29.0 \
    --applications Name=Spark \
    --log-uri s3://your-s3-bucket/logs/ \
    --ec2-attributes KeyName=your-key-pair \
    --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r5d.xlarge,BidPrice=OnDemandPrice \
      InstanceGroupType=CORE,InstanceCount=6,InstanceType=r5d.2xlarge,BidPrice=OnDemandPrice \
    --bootstrap-actions Path=s3://your-s3-bucket/emr_bootstrap.sh \
    --configurations  file://./aws-emr/emr-spark-cluster-config.json \
    --steps Type=Spark,Name="Pyspark Benchmark - Generate Data",\
    ActionOnFailure=CONTINUE,\
    Args=[--deploy-mode,cluster,--master,yarn,s3://your-s3-bucket/jobs/generate-data.py,s3://your-s3-bucket/data/,-r,2000000000,-p,1000] \
    --use-default-roles \
    --auto-terminate
    ```
    Note the usage of a configuration file located in the `aws-emr` directory of this repository.
4. Run the benchmark jobs with the following `awscli` commands:
    ```
    aws emr create-cluster --name "Pyspark Benchmark - Shuffle" \
    --release-label emr-5.29.0 \
    --applications Name=Spark \
    --log-uri s3://your-s3-bucket/logs/ \
    --ec2-attributes KeyName=your-key-pair \
    --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r5d.xlarge,BidPrice=OnDemandPrice \
      InstanceGroupType=CORE,InstanceCount=6,InstanceType=r5d.2xlarge,BidPrice=OnDemandPrice \
    --bootstrap-actions Path=s3://your-s3-bucket/emr_bootstrap.sh \
    --configurations  file://path/to/emr-spark-cluster-config.json \
    --steps Type=Spark,Name="Pyspark Benchmark - Shuffle",\
    ActionOnFailure=CONTINUE,\
    Args=[--deploy-mode,cluster,--master,yarn,s3://your-s3-bucket/jobs/benchmark-shuffle.py,s3://your-s3-bucket/data/,-r,250,-n,'pyspark-benchmark-shuffle',-o,s3://your-s3-bucket/results/pyspark-shuffle] \
    --use-default-roles \
    --auto-terminate
    ```
    ```
    aws emr create-cluster --name "Pyspark Benchmark - CPU" \
    --release-label emr-5.29.0 \
    --applications Name=Spark \
    --log-uri s3://your-s3-bucket/logs/ \
    --ec2-attributes KeyName=your-key-pair \
    --instance-groups \
      InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r5d.xlarge,BidPrice=OnDemandPrice \
      InstanceGroupType=CORE,InstanceCount=6,InstanceType=r5d.2xlarge,BidPrice=OnDemandPrice \
    --bootstrap-actions Path=s3://your-s3-bucket/emr_bootstrap.sh \
    --configurations  file://path/to/emr-spark-cluster-config.json \
    --steps Type=Spark,Name="Pyspark Benchmark - CPU",\
    ActionOnFailure=CONTINUE,\
    Args=[--deploy-mode,cluster,--master,yarn,s3://your-s3-bucket/jobs/benchmark-cpu.py,s3://your-s3-bucket/data/,-s,25000000000,-p,1000,-n,'pyspark-benchmark-cpu',-o,s3://your-s3-bucket/results/pyspark-cpu] \
    --use-default-roles \
    --auto-terminate
    ```
   
The results of the benchmarks will be written to the job's log output and to CSV files placed in `s3://your-s3-bucket/results/`.