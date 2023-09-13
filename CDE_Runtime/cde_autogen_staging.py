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

now = datetime.now()
today = now.timestamp()

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

#-----------------------------------------------------------------------------------
# CREATE DATASETS WITH RANDOM DISTRIBUTIONS
#-----------------------------------------------------------------------------------

dg = DataGen(spark, username)

x = random.randint(1, 3)
y = random.randint(1, 4)
z = random.randint(2, 5)

def check_partitions(partitions):
  if partitions > 100:
    partitions = 100
  if partitions < 5:
    partitions = 5
  else:
    return partitions
  return partitions

ROW_COUNT_car_sales = random.randint(500000, 500000)
UNIQUE_VALS_car_sales = random.randint(500, ROW_COUNT_car_sales-1)
PARTITIONS_NUM_car_sales = round(ROW_COUNT_car_sales / UNIQUE_VALS_car_sales)
PARTITIONS_NUM_car_sales = check_partitions(PARTITIONS_NUM_car_sales)

print("SPARKGEN PIPELINE SPARK HYPERPARAMS")
print("\n")
print("x: {}".format(x))
print("y: {}".format(y))
print("z: {}".format(z))
print("\n")
print("ROW_COUNT_car_sales: {}".format(ROW_COUNT_car_sales))
print("UNIQUE_VALS_car_sales: {}".format(UNIQUE_VALS_car_sales))
print("PARTITIONS_NUM_car_sales: {}".format(PARTITIONS_NUM_car_sales))

car_sales_df = dg.car_sales_gen(x, y, z, PARTITIONS_NUM_car_sales, ROW_COUNT_car_sales, UNIQUE_VALS_car_sales, True)

#---------------------------------------------------
#               POPULATE TABLES
#---------------------------------------------------
print("CREATING ICBERG TABLES FROM SPARK DATAFRAMES \n")
print("\n")

car_sales_df.writeTo("{0}.CAR_SALES_STAGING_{1}".format(dbname, username)).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()

'''car_sales_df.write.mode("overwrite").partitionedBy("month").saveAsTable('{0}.CAR_SALES_{1}'.format(dbname, username), format="parquet")
car_installs_df.write.mode("overwrite").saveAsTable('{0}.CAR_INSTALLS_{1}'.format(dbname, username), format="parquet")
factory_data_df.write.mode("overwrite").saveAsTable('{0}.EXPERIMENTAL_MOTORS_{1}'.format(dbname, username), format="parquet")
customer_data_df.write.mode("overwrite").saveAsTable('{0}.CUSTOMER_DATA_{1}'.format(dbname, username), format="parquet")
geo_data_df.write.mode("overwrite").saveAsTable('{0}.GEO_DATA_XREF_{1}'.format(dbname, username), format="parquet")
'''
'''car_sales_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/car_sales")
car_installs_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/car_installs")
factory_data_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/factory_data")
customer_data_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/customer_data")
geo_data_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/geo_data")
'''

print("\tPOPULATE TABLE(S) COMPLETED")

print("JOB COMPLETED.\n\n")

table_data = [{
    "RUN_ID" : today,
    "ROW_COUNT_car_sales" : ROW_COUNT_car_sales,
    "UNIQUE_VALS_car_sales" : UNIQUE_VALS_car_sales,
    "PARTITIONS_NUM_car_sales" : PARTITIONS_NUM_car_sales,
    "x" : x,
    "y" : y,
    "z" : z
}]

table_metrics_df = spark.createDataFrame(table_data)

table_metrics_df.registerTempTable("TABLE_METRICS_TEMPTABLE")

print("INSERT INTO {}.TABLE_METRICS_TABLE SELECT * FROM TABLE_METRICS_TEMPTABLE".format(dbname))
spark.sql("INSERT INTO {}.TABLE_METRICS_TABLE SELECT * FROM TABLE_METRICS_TEMPTABLE".format(dbname))
