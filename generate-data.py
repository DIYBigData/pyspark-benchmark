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
import uuid


def parseArguments():
    arguments = argparse.ArgumentParser(
            description='Generates sample data of a prescribed size for '
                        'Spark performance testing'
        )
    arguments.add_argument(
            'outfile',
            type=str,
            metavar='file_url',
            help='The output file URL'
        )
    arguments.add_argument(
            '-r', '--row-count',
            metavar='N',
            type=int,
            default=1000000,
            dest='rows',
            help='The desired size of the output file in rows'
        )
    arguments.add_argument(
            '-p', '--partitions',
            metavar='K',
            type=int,
            default=100,
            help='The number of partitions to create'
        )
    return arguments.parse_args()

def getUUID():
    return uuid.uuid4().hex

def main():
    args = parseArguments()

    spark = SparkSession.builder.getOrCreate()
    
    Logger= spark._jvm.org.apache.log4j.Logger
    joblogger = Logger.getLogger(__name__)
    joblogger.info('****************************************************************')
    joblogger.info('')
    joblogger.info(
        'Starting creation of test data file with {0} rows and {1} '
        'partitions at {2}'.format(
            args.rows,
            args.partitions,
            args.outfile
        )
    )
    joblogger.info('')
    joblogger.info('****************************************************************')
    
    udfGetUUID = F.udf(getUUID, T.StringType())
    
    df = (
        spark.range(0, args.rows, numPartitions=args.partitions)
        .withColumn('value', udfGetUUID())
        .withColumn('prefix2', F.substring(F.col('value'),1,2))
        .withColumn('prefix4', F.substring(F.col('value'),1,4))
        .withColumn('prefix8', F.substring(F.col('value'),1,8))
        .withColumn('float_val', F.rand(seed=8675309)*1000000)
        .withColumn('integer_val', F.col('float_val').cast(T.LongType()))
        .drop('id')
    )    
    
    df.write.csv(
        args.outfile,
        mode='overwrite',
        header=True
    )
    joblogger.info('Done writing to {0}'.format(args.outfile))


if __name__ == '__main__':
    main()
