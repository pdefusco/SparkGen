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
import sys
import random

print(sys.argv)

timestamp = float(sys.argv[1])

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

#---------------------------------------------------
#               ICEBERG METADATA QUERIES
#---------------------------------------------------

spark.sql("SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.history").show()

spark.sql(
    "WITH Ranked_Entries AS (\
        SELECT \
            latest_snapshot_id, \
            latest_schema_id, \
            timestamp, \
            ROW_NUMBER() OVER(PARTITION BY latest_schema_id ORDER BY timestamp DESC) as row_num\
        FROM \
            SPARKGEN_pdefusco.CAR_SALES_pdefusco.metadata_log_entries\
        WHERE \
            latest_schema_id IS NOT NULL\
    )\
    SELECT \
        latest_snapshot_id,\
        latest_schema_id,\
        timestamp AS latest_timestamp\
    FROM \
        Ranked_Entries\
    WHERE \
        row_num = 1\
    ORDER BY \
        latest_schema_id DESC;"\
).show()

spark.sql("SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.metadata_log_entries;").show()

spark.sql(
    "SELECT \
        committed_at,\
        snapshot_id,\
        summary['added-records'] AS added_records\
    FROM \
        SPARKGEN_pdefusco.CAR_SALES_pdefusco.snapshots;"\
    ).show()


spark.sql(
    "SELECT \
        operation,\
        COUNT(*) AS operation_count,\
        DATE(committed_at) AS date\
    FROM \
        SPARKGEN_pdefusco.CAR_SALES_pdefusco.snapshots\
    GROUP BY \
        operation, \
        DATE(committed_at)\
    ORDER BY \
        date;"\
    ).show()


spark.sql(
    "SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.snapshots;"
).show()

#Determining if a partition should be rewritten:
#If a partition has many small files, it may be a good candidate for compaction to improve performance as discussed in Chapter 4 on optimization.
#The following query can help you breakdown each partitions number of files and average file size to help identify partitions to rewrite.

spark.sql(
    "SELECT \
        partition,\
        COUNT(*) AS num_files,\
        AVG(file_size_in_bytes) AS avg_file_size\
    FROM \
        SPARKGEN_pdefusco.CAR_SALES_pdefusco.files\
    GROUP BY \
        partition\
    ORDER BY \
        num_files DESC, \
        avg_file_size ASC"\
).show()


#Identifying partitions that may need data repair:
#Some fields probably shouldn’t have null values in your data, using the files metadata table we can identify partitions
#or files that may have missing values in a much more lightweight operation than scanning the actual data.
#This query returns the partition and file name of any files with null data in their third column.

spark.sql(
    "SELECT \
        partition, file_path\
    FROM \
        SPARKGEN_pdefusco.CAR_SALES_pdefusco.files\
    WHERE \
        null_value_counts['3'] > 0\
    GROUP BY \
        partition"\
).show()

#Identifying the total file size of the snapshot: Using the files table you can sum all the file sizes to get a total size of the snapshot.

spark.sql("SELECT sum(file_size_in_bytes) from SPARKGEN_pdefusco.CAR_SALES_pdefusco.files;").show()

#Getting list of files from a previous snapshot: Using time travel you can get the list of files from a previous snapshot.

spark.sql(
    "SELECT file_path, file_size_in_bytes \
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.files \
    VERSION AS OF <snapshot_id>;"\
)

# Are the manifests sorted: examine the manifests upper and lower bounds to see if they are sorted well or should be rewrtitten for better clustering.

spark.sql(
"SELECT path, partition_summaries \
FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests;"
).show()

#Find manifests that are good targets for rewriting:With the following query we can find which manifests are below the average size
#of manifest files which can help use discover which manifests can possibly be compacted with rewrite_manifests.

spark.sql("
WITH avg_length AS (\
    SELECT AVG(length) as average_manifest_length\
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests\
)\
SELECT \
    path,\
    length\
FROM \
    SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests\
WHERE \
    length < (SELECT average_manifest_length FROM avg_length);\
").show()

#Find total number of files added per snapshot:
#You may be curious about the pace of file growth in your table, with this query,
#you can see how many files were added for each snapshot.

spark.sql(
"SELECT \
    added_snapshot_id,\
    SUM(added_data_files_count) AS total_added_data_files\
FROM \
    SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests\
GROUP BY \
    added_snapshot_id;"\
).show()

#Find snapshots where files were deleted: You may be wanting to monitor your deletion patterns for purposes of complying with requests to clean PII,
#Knowing which snapshots have deletes can help monitor which snapshots may need expiration to hard delete the data.

spark.sql(
"SELECT \
    added_snapshot_id\
FROM \
    SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests\
WHERE\
    deleted_data_files_count > 0;"\
).show()

#How many files are inside a partition: You may want to see how many files are in a partition,
#if a particular partition has a large number of files, it may be a candidate for compaction.

spark.sql(
    "SELECT partition, file_count FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.partitions"
).show()

#What is the total size in bytes of the partition: Along with looking at the number of files,
#you may want to look at the size of the partition. If one partition is particular large you may
#want to alter your partitioning scheme to better balance out distribution.

spark.sql("SELECT partition, SUM(file_size_in_bytes) AS partition_size FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.files GROUP BY partition").show()

#Find the number of partitions per partition scheme: With partition evolution, you may have different partitioning schemes overtime.
#If your curious how different partitioning schemes effected the number of partitions for the data written with it, this query should be helpful.

spark.sql("
    SELECT \
        spec_id,\
        COUNT(*) as partition_count\
    FROM \
        catalog.table.partitions\
    GROUP BY \
        spec_id;\
    ").show()
