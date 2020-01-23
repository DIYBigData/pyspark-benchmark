#
#     This file is part of PySpark Benchmark.
# 
#     PySpark Benchmark is free software: you can redistribute it and/or modify
#     it under the terms of the GNU General Public License as published by
#     the Free Software Foundation, either version 3 of the License, or
#     (at your option) any later version.
# 
#     PySpark Benchmark is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU General Public License for more details.
# 
#     You should have received a copy of the GNU General Public License
#     along with PySpark Benchmark.  If not, see <https://www.gnu.org/licenses/>.
#
#     Originally written by Michael Kamprath <michael@kamprath.net>
#

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.window import Window as W

import argparse
import sys
from timeit import default_timer as timer


def parseArguments():
    arguments = argparse.ArgumentParser(
            description='Runs a benchmarking of PySpark. Assumes you have used '
                        'generate-data.py to createa test data set.'
        )
    arguments.add_argument(
            'inputfile',
            type=str,
            metavar='file_url',
            help='The dataset to use. Input file URL.'
        )
    arguments.add_argument(
            '-n', '--job-name',
            metavar='name',
            type=str,
            default='shuffle-benchmark',
            dest='appName',
            help='The name given this PySpark job'
        )
    arguments.add_argument(
            '-r', '--repartition-size',
            metavar='N',
            type=int,
            default=200,
            dest='repartitions',
            help='The number of partitions to use in repartition benchmark'
        )
    arguments.add_argument(
            '-o', '--results-output',
            metavar='results-file-path',
            type=str,
            default=None,
            dest='results_output_file',
            help='The file path to place the results output'
        )
    return arguments.parse_args()

def benchmarkGroupBy(df, jobLogger):
    jobLogger.info('****************************************************************')
    jobLogger.info('Starting bench mark test for Group By')
    start_time = timer()
    res = (
        df
        .groupBy('prefix2')
        .agg(
            F.count('*').alias('total_count'),
            F.countDistinct('prefix4').alias('prefix4_count'),
            F.countDistinct('prefix8').alias('prefix8_count'),
            F.sum('float_val').alias('float_val_sum'),
            F.sum('integer_val').alias('integer_val_sum'),
        )
    )

    # now trigger the computations by fetching a count at the RDD level
    count_value = res.rdd.count()
    end_time = timer()

    jobLogger.info(
        'The count value for the groupBy benchmark is = {0}'.format(count_value))
    jobLogger.info('')
    return end_time-start_time

def benchmarkRepartition(df, partitions, jobLogger):
    jobLogger.info('****************************************************************')
    jobLogger.info('Starting bench mark test for Repartition')
    start_time = timer()
    res = (
        df
        .repartition(partitions, 'prefix4')
    )

    # now trigger the computations by fetching a count at the RDD level
    count_value = res.rdd.count()
    end_time = timer()

    jobLogger.info(
        'The count value for the repartition benchmark is = {0}'.format(count_value))
    jobLogger.info(
        'The number of partitions after the repartition benchmark is = {0}'.format(
            res.rdd.getNumPartitions()
        ))
    jobLogger.info('')
    return end_time-start_time

def benchmarkInnerJoins(df, jobLogger):
    jobLogger.info('****************************************************************')
    jobLogger.info('Starting bench mark test for Inner Join')
    start_time = timer()
    join_df = (
        df
        .groupBy('prefix2')
        .agg(F.count('*').alias('total_count'))
    )
    res = (
        df
        .join(
            join_df,
            on='prefix2',
            how='inner'
        )
    )
    # now trigger the computations by fetching a count at the RDD level
    count_value = res.rdd.count()
    end_time = timer()
    
    jobLogger.info(
        'The count value for the inner join benchmark is = {0}'.format(count_value))
    jobLogger.info('')
    return end_time-start_time

def benchmarkBroadcastInnerJoins(df, jobLogger):
    jobLogger.info('****************************************************************')
    jobLogger.info('Starting bench mark test for Broadcast Inner Join')
    start_time = timer()
    join_df = (
        df
        .groupBy('prefix2')
        .agg(F.count('*').alias('total_count'))
    )
    res = (
        df
        .join(
            F.broadcast(join_df),
            on='prefix2',
            how='inner'
        )
    )
    # now trigger the computations by fetching a count at the RDD level
    count_value = res.rdd.count()
    end_time = timer()
    
    jobLogger.info(
        'The count value for the broadcast inner join benchmark is = {0}'.format(
            count_value))
    jobLogger.info('')
    return end_time-start_time
def main():
    args = parseArguments()

    spark = SparkSession.builder.appName(args.appName).getOrCreate()

    Logger= spark._jvm.org.apache.log4j.Logger
    joblogger = Logger.getLogger(__name__)
    joblogger.info('**********************************************************************')
    joblogger.info('')
    joblogger.info(
        'Benchmarking PySpark\'s shuffle capacity using input data at {0}'.format(
            args.inputfile))
    joblogger.info('')
    joblogger.info('**********************************************************************')
    
    callSite_short_orig = spark.sparkContext.getLocalProperty('callSite.short')
    callSite_long_orig = spark.sparkContext.getLocalProperty('callSite.long')

    data_schema = T.StructType([
        T.StructField("value", T.StringType()),
        T.StructField("prefix2", T.StringType()),
        T.StructField("prefix4", T.StringType()),
        T.StructField("prefix8", T.StringType()),
        T.StructField("float_val", T.DoubleType()),
        T.StructField("integer_val", T.LongType())
    ])

    df = spark.read.csv(args.inputfile, header=True, schema=data_schema)

    spark.sparkContext.setLocalProperty('callSite.short', 'groupBy-benchmark')
    spark.sparkContext.setLocalProperty(
        'callSite.long', 'Benchmark for performing groupBy-agg on a dataframe')
    groupBy_time = benchmarkGroupBy(df, joblogger)

    spark.sparkContext.setLocalProperty('callSite.short', 'repartition-benchmark')
    spark.sparkContext.setLocalProperty(
        'callSite.long', 'Benchmark for repartitioning a dataframe')
    repartition_time = benchmarkRepartition(df, args.repartitions, joblogger)

    spark.sparkContext.setLocalProperty('callSite.short', 'inner-join-benchmark')
    spark.sparkContext.setLocalProperty(
        'callSite.long', 'Benchmark for performing inner joins')
    innerJoin_time = benchmarkInnerJoins(df, joblogger)

    spark.sparkContext.setLocalProperty('callSite.short', 'broadcast-inner-join-benchmark')
    spark.sparkContext.setLocalProperty(
        'callSite.long', 'Benchmark for performing broadcast inner joins')
    broadcastInnerJoin_time = benchmarkBroadcastInnerJoins(df, joblogger)

    #restore properties
    spark.sparkContext.setLocalProperty('callSite.short', callSite_short_orig)
    spark.sparkContext.setLocalProperty('callSite.long', callSite_long_orig)

    joblogger.info('**********************************************************************')
    joblogger.info('    RESULTS    RESULTS    RESULTS    RESULTS    RESULTS    RESULTS')
    joblogger.info('    Test Run = {0}'.format(args.appName))
    joblogger.info('')
    joblogger.info('Group By test time         = {0} seconds'.format(groupBy_time))
    joblogger.info('Repartition test time      = {0} seconds ({1} partitions)'.format(
                        repartition_time, args.repartitions))
    joblogger.info('Inner join test time       = {0} seconds '.format(innerJoin_time))
    joblogger.info('Broadcast inner join time  = {0} seconds '.format(
                        broadcastInnerJoin_time))
    joblogger.info('')
    joblogger.info('**********************************************************************')

    if args.results_output_file is not None:
        joblogger.info('')
        joblogger.info('Writing results to {0}'.format(args.results_output_file))

        results_list = [
            ('group-by',groupBy_time),
            ('repartition',repartition_time),
            ('inner-join',innerJoin_time),
            ('broadcast-inner-join',broadcastInnerJoin_time),
        ]
    
        results_schema = T.StructType([
            T.StructField("test", T.StringType()),
            T.StructField("seconds", T.DoubleType())
        ])
        results_df = spark.createDataFrame(results_list, schema=results_schema).coalesce(1)
        results_df.write.csv(
            args.results_output_file,
            header=True,
            mode='overwrite'
        )

if __name__ == '__main__':
    main()
