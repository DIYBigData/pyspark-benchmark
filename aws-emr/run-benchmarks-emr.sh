#!/bin/bash

set -e

aws emr create-cluster --name "Pyspark Benchmark - Shuffle" \
--release-label emr-5.29.0 \
--applications Name=Spark \
--log-uri s3://your-s3-bucket/logs/ \
--ec2-attributes KeyName=your-key-pair \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r5d.xlarge,BidPrice=OnDemandPrice InstanceGroupType=CORE,InstanceCount=6,InstanceType=r5d.2xlarge,BidPrice=OnDemandPrice \
--bootstrap-actions Path=s3://your-s3-bucket/emr_bootstrap.sh \
--steps Type=Spark,Name="Pyspark Benchmark - Shuffle",\
ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,--master,yarn,s3://your-s3-bucket/jobs/benchmark-shuffle.py,s3://your-s3-bucket/data/,-r,250,-n,'pyspark-benchmark-shuffle',-o,s3://your-s3-bucket/results/pyspark-shuffle] \
--use-default-roles \
--auto-terminate


aws emr create-cluster --name "Pyspark Benchmark - CPU" \
--release-label emr-5.29.0 \
--applications Name=Spark \
--log-uri s3://your-s3-bucket/logs/ \
--ec2-attributes KeyName=your-key-pair \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r5d.xlarge,BidPrice=OnDemandPrice InstanceGroupType=CORE,InstanceCount=6,InstanceType=r5d.2xlarge,BidPrice=OnDemandPrice \
--bootstrap-actions Path=s3://your-s3-bucket/emr_bootstrap.sh \
--steps Type=Spark,Name="Pyspark Benchmark - CPU",\
ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,--master,yarn,s3://your-s3-bucket/jobs/benchmark-cpu.py,s3://your-s3-bucket/data/,-s,25000000000,-p,1000,-n,'pyspark-benchmark-cpu',-o,s3://your-s3-bucket/results/pyspark-cpu] \
--use-default-roles \
--auto-terminate
