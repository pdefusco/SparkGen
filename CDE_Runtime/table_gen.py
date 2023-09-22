#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import random
import configparser
import json
import sys
import os
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from datagen import *
from datetime import datetime

timestamp = datetime.now().timestamp()

## CDE PROPERTIES
config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

print("\nRunning as Username: ", username)

dbname = "SPARKGEN_{}".format(username)

print("\nUsing DB Name: ", dbname)

#---------------------------------------------------
#               CREATE SPARK SESSION WITH ICEBERG
#---------------------------------------------------

spark = SparkSession \
    .builder \
    .appName("ICEBERG LOAD") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.kubernetes.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

print("SPARK MEASURE METRICS TRACKING\n")

spark.sql("DROP TABLE IF EXISTS {}.TABLE_METRICS_TABLE".format(dbname))

spark.sql("CREATE TABLE IF NOT EXISTS {}.TABLE_METRICS_TABLE\
                (RUN_ID FLOAT,\
                ROW_COUNT_car_sales_gen BIGINT,\
                UNIQUE_VALS_car_sales_gen BIGINT,\
                PARTITIONS_NUM_car_sales_gen BIGINT,\
                x_gen BIGINT,\
                y_gen BIGINT,\
                z_gen BIGINT,\
                ROW_COUNT_car_sales_source_sample BIGINT,\
                UNIQUE_VALS_car_sales_source_sample BIGINT,\
                ROW_PERCENT_car_sales_source_sample BIGINT,\
                ROW_COUNT_car_sales_staging BIGINT,\
                UNIQUE_VALS_car_sales_staging BIGINT,\
                ROW_PERCENT_car_sales_staging BIGINT)".format(dbname))

spark.sql("DROP TABLE IF EXISTS {}.STAGE_METRICS_TABLE".format(dbname))

spark.sql("CREATE TABLE IF NOT EXISTS {}.STAGE_METRICS_TABLE\
                (RUN_ID FLOAT,\
                JOBID STRING,\
                JOBGROUP STRING,\
                STAGEID STRING,\
                NAME STRING,\
                SUBMISSIONTIME BIGINT,\
                COMPLETIONTIME BIGINT,\
                STAGEDURATION BIGINT,\
                NUMTASKS INT,\
                EXECUTORRUNTIME BIGINT,\
                EXECUTORCPUTIME BIGINT,\
                EXECUTORDESERIALIZETIME BIGINT,\
                EXECUTORDESERIALIZECPUTIME BIGINT,\
                RESULTSERIALIZATIONTIME BIGINT,\
                JVMGCTIME BIGINT,\
                RESULTSIZE BIGINT,\
                DISKBYTESSPILLED BIGINT,\
                MEMORYBYTESSPILLED BIGINT,\
                PEAKEXECUTIONMEMORY BIGINT,\
                RECORDSREAD BIGINT,\
                BYTESREAD BIGINT,\
                RECORDSWRITTEN BIGINT,\
                BYTESWRITTEN BIGINT,\
                SHUFFLEFETCHWAITTIME BIGINT,\
                SHUFFLETOTALBYTESREAD BIGINT,\
                SHUFFLETOTALBLOCKSFETCHED BIGINT,\
                SHUFFLELOCALBLOCKSFETCHED BIGINT,\
                SHUFFLEREMOTEBLOCKSFETCHED BIGINT,\
                SHUFFLELOCALBYTESREAD BIGINT,\
                SHUFFLEREMOTEBYTESREAD BIGINT,\
                SHUFFLEREMOTEBYTESREADTODISK BIGINT,\
                SHUFFLERECORDSREAD BIGINT,\
                SHUFFLEWRITETIME BIGINT,\
                SHUFFLEBYTESWRITTEN BIGINT,\
                SHUFFLERECORDSWRITTEN BIGINT)".format(dbname))

print("CURRENT TABLES IN {}".format(dbname))
spark.sql("SHOW TABLES IN {}".format(dbname)).show()
