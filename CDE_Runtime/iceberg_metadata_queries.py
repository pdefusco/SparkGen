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
from datetime import datetime
import sys
import random

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

try:
    spark.sql("SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.history").show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Finding the latest snapshot with a previous schema: Maybe you made a change to the schema, and
now want to go back to the previous schema. You’ll want to find the latest snapshot using that schema which can be determined with a
query that will rank the snapshots for each schema_id then return only the top ranked snapshot for each schema_id:""")

QUERY = "WITH Ranked_Entries AS (\
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
    latest_schema_id DESC;"

print(QUERY)

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("ALL METADATA LOG ENTRIES")
QUERY = "SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.metadata_log_entries;"
print(QUERY)

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Understanding Data Addition Patterns: Another use case is to understand the pattern of data additions to the table.
This could be useful in capacity planning or understanding data growth over time.
 Here is an SQL query that shows the total records added at each snapshot:""")

QUERY = "SELECT \
            committed_at,\
            snapshot_id,\
            summary['added-records'] AS added_records\
        FROM \
            SPARKGEN_pdefusco.CAR_SALES_pdefusco.snapshots;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Monitoring Operations Over Time: Another use case for the snapshots metadata table is
to monitor the types and frequency of operations performed on the table over time.
This could be useful in understanding the workload and usage patterns of the table.
Here is an SQL query that shows the count of each operation type over time:""")

QUERY = "SELECT \
            operation,\
            COUNT(*) AS operation_count,\
            DATE(committed_at) AS date\
        FROM \
            SPARKGEN_pdefusco.CAR_SALES_pdefusco.snapshots\
        GROUP BY \
            operation, \
            DATE(committed_at)\
        ORDER BY \
            date;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("QUERY ALL SNAPSHOTS")
QUERY = "SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.snapshots;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Determining if a partition should be rewritten:
If a partition has many small files, it may be a good candidate for compaction to improve performance as discussed in Chapter 4 on optimization.
The following query can help you breakdown each partitions number of files and average file size to help identify partitions to rewrite.""")

QUERY = "SELECT \
            partition,\
            COUNT(*) AS num_files,\
            AVG(file_size_in_bytes) AS avg_file_size\
        FROM \
            SPARKGEN_pdefusco.CAR_SALES_pdefusco.files\
        GROUP BY \
            partition\
        ORDER BY \
            num_files DESC, \
            avg_file_size ASC"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")


print("""Identifying partitions that may need data repair:
Some fields probably shouldn’t have null values in your data, using the files metadata table we can identify partitions
or files that may have missing values in a much more lightweight operation than scanning the actual data.
This query returns the partition and file name of any files with null data in their third column.""")

QUERY = "SELECT \
            partition, file_path\
        FROM \
            SPARKGEN_pdefusco.CAR_SALES_pdefusco.files\
        WHERE \
            null_value_counts['3'] > 0\
        GROUP BY \
            partition"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Identifying the total file size of the snapshot: Using the files table you can sum all the file sizes to get a total size of the snapshot.""")

QUERY = "SELECT sum(file_size_in_bytes) from SPARKGEN_pdefusco.CAR_SALES_pdefusco.files;"
try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Getting list of files from a previous snapshot: Using time travel you can get the list of files from a previous snapshot.""")

QUERY = "SELECT file_path, file_size_in_bytes \
        FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.files \
        VERSION AS OF <snapshot_id>;"

try:
    spark.sql(QUERY)
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Are the manifests sorted: examine the manifests upper and lower bounds to see if they are sorted well or should be rewrtitten for better clustering.""")

QUERY = "SELECT path, partition_summaries \
        FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests;"
try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Find manifests that are good targets for rewriting:With the following query we can find which manifests are below the average size
of manifest files which can help use discover which manifests can possibly be compacted with rewrite_manifests.""")

QUERY = """WITH avg_length AS (
    SELECT AVG(length) as average_manifest_length
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests
)
SELECT
    path,
    length
FROM
    SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests
WHERE
    length < (SELECT average_manifest_length FROM avg_length);"""

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""#Find total number of files added per snapshot:
#You may be curious about the pace of file growth in your table, with this query,
#you can see how many files were added for each snapshot.""")

QUERY = "SELECT \
        added_snapshot_id,\
        SUM(added_data_files_count) AS total_added_data_files\
    FROM \
        SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests\
    GROUP BY \
        added_snapshot_id;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""#Find snapshots where files were deleted: You may be wanting to monitor your deletion patterns for purposes of complying with requests to clean PII,
#Knowing which snapshots have deletes can help monitor which snapshots may need expiration to hard delete the data.""")

QUERY = "SELECT \
            added_snapshot_id\
        FROM \
            SPARKGEN_pdefusco.CAR_SALES_pdefusco.manifests\
        WHERE\
            deleted_data_files_count > 0;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""#How many files are inside a partition: You may want to see how many files are in a partition,
#if a particular partition has a large number of files, it may be a candidate for compaction.""")

try:
    spark.sql(
        "SELECT partition, file_count FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.partitions"
    ).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""What is the total size in bytes of the partition: Along with looking at the number of files,
you may want to look at the size of the partition. If one partition is particular large you may
want to alter your partitioning scheme to better balance out distribution.""")

QUERY = "SELECT partition, SUM(file_size_in_bytes) AS partition_size FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.files GROUP BY partition"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Find the number of partitions per partition scheme: With partition evolution, you may have different partitioning schemes overtime.
If your curious how different partitioning schemes effected the number of partitions for the data written with it, this query should be helpful.""")

QUERY = "SELECT \
        spec_id,\
        COUNT(*) as partition_count\
    FROM \
        SPARKGEN_pdefusco.CAR_SALES_pdefusco.partitions\
    GROUP BY \
        spec_id;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")


print("""Find the number of partitions per partition scheme: With partition evolution, you may have different partitioning schemes overtime.
If your curious how different partitioning schemes effected the number of partitions for the data written with it, this query should be helpful.""")

QUERY = "SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.partitions;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Find the Largest Data Files across all snapshots: This query will first make sure you have only distinct files since the same file can have multiple records.
 It then returns you the 5 largest files from that list of distinct files.""")


QUERY = "WITH distinct_files AS (\
    SELECT DISTINCT file_path, file_size_in_bytes \
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_data_files\
)\
SELECT file_path, file_size_in_bytes \
FROM distinct_files\
ORDER BY file_size_in_bytes DESC\
LIMIT 5;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("Find the total size of files across all snapshots: If you want to see a total picture of how many files, \
the size of those files and number of records you have across all snapshots you can run this query.")

QUERY = "WITH unique_files AS (\
    SELECT DISTINCT file_path, record_count, file_size_in_bytes\
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_data_files\
)\
SELECT COUNT(*) as num_unique_files, \
       SUM(record_count) as total_records,\
       SUM(file_size_in_bytes) as total_file_size\
FROM unique_files;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("""Assess partitions across all snapshots: With this query you can see the number of files,
number of records and total file size of each partition across all snapshots.
This information can be used to help understand your data storage status by partition.""")

QUERY = """WITH unique_files AS (\
    SELECT DISTINCT file_path, partition, record_count, file_size_in_bytes\
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_data_files\
)\
SELECT partition, \
       COUNT(*) as num_unique_files, \
       SUM(record_count) as total_records,\
       SUM(file_size_in_bytes) as total_file_size\
FROM unique_files\
GROUP BY partition;"""

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

QUERY = "SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_data_files;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("Monitoring the growth of manifests from snapshot to snapshot: In this query we return the total manifest \
size and data file counts for each snapshot to see the growth of files and manifest size from snapshot to snapshot.")

QUERY = "SELECT reference_snapshot_id, SUM(length) as manifests_length, SUM(added_data_files_count + existing_data_files_count)AS total_data_files \
FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_manifests \
GROUP BY reference_snapshot_id;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

print("Get total size of all valid manifests: With this query you can get the storage being used by all valid manifests.")


QUERY = "SELECT \
    SUM(length) AS total_length\
FROM (\
    SELECT DISTINCT path, length\
    FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_manifests"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")

QUERY = "SELECT * FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.all_manifests;"

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")


print("""Track the evolution of the table by partition across snapshots
You may want to see how partitions evolve acorss snapshots such as how many files are added, here is an example query of
how you can build that data view. You can use this as the base to assess the number of files added, the size of files added by partition, etc.""")

QUERY = """SELECT e.snapshot_id, f.partition, COUNT(*) AS files_added
FROM SPARKGEN_pdefusco.CAR_SALES_pdefusco.entries AS e
JOIN catalog.entries.files AS f
ON e.data_file.file_path = f.file_path
WHERE e.status = 1
GROUP BY e.snapshot_id, f.partition;"""

try:
    spark.sql(QUERY).show()
except Exception as e:
    print(f'caught {type(e)}: e')
    print("Query did not run Successfully")
