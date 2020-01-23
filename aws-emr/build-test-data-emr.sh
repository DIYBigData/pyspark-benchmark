#!/bin/bash

set -e

aws emr create-cluster --name "Pyspark Benchmark - Generate Data" \
--release-label emr-5.29.0 \
--applications Name=Spark \
--log-uri s3://your-s3-bucket/logs/ \
--ec2-attributes KeyName=your-key-pair \
--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=r5d.xlarge,BidPrice=OnDemandPrice InstanceGroupType=CORE,InstanceCount=6,InstanceType=r5d.2xlarge,BidPrice=OnDemandPrice \
--bootstrap-actions Path=s3://your-s3-bucket/emr_bootstrap.sh \
--steps Type=Spark,Name="Pyspark Benchmark - Generate Data",\
ActionOnFailure=CONTINUE,\
Args=[--deploy-mode,cluster,--master,yarn,s3://your-s3-bucket/jobs/generate-data.py,s3://your-s3-bucket/data/,-r,2000000000,-p,1000] \
--use-default-roles \
--auto-terminate

