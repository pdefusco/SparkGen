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

import numpy as np
import pandas as pd
from os.path import exists
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datagen import *
from datetime import datetime

## CDE PROPERTIES
config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

# current date and time
now = datetime.now()

timestamp = datetime.timestamp(now)

print("\nRunning as Username: ", username)

dbname = "SPARKGEN_{}".format(username)

print("\nUsing DB Name: ", dbname)

#---------------------------------------------------
#               CREATE SPARK SESSION WITH ICEBERG
#---------------------------------------------------

spark = SparkSession.builder.\
        appName('INGEST')\
        .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
        .getOrCreate()

#-----------------------------------------------------------------------------------
# CREATE DATASETS WITH RANDOM DISTRIBUTIONS
#-----------------------------------------------------------------------------------

dg = DataGen(spark, username)

"""x = int(os.environ["x"])
y = int(os.environ["y"])
z = int(os.environ["z"])

ROW_COUNT_car_installs = int(os.environ["ROW_COUNT_car_installs"])
UNIQUE_VALS_car_installs = int(os.environ["UNIQUE_VALS_car_installs"])
PARTITIONS_NUM_car_installs = int(os.environ["PARTITIONS_NUM_car_installs"])

ROW_COUNT_car_sales = int(os.environ["ROW_COUNT_car_sales"])
UNIQUE_VALS_car_sales = int(os.environ["UNIQUE_VALS_car_sales"])
PARTITIONS_NUM_car_sales = int(os.environ["PARTITIONS_NUM_car_sales"])

ROW_COUNT_customer_data = int(os.environ["ROW_COUNT_customer_data"])
UNIQUE_VALS_customer_data = int(os.environ["UNIQUE_VALS_customer_data"])
PARTITIONS_NUM_customer_data = int(os.environ["PARTITIONS_NUM_customer_data"])

ROW_COUNT_factory_data = int(os.environ["ROW_COUNT_factory_data"])
UNIQUE_VALS_factory_data = int(os.environ["UNIQUE_VALS_factory_data"])
PARTITIONS_NUM_factory_data = int(os.environ["PARTITIONS_NUM_factory_data"])

ROW_COUNT_geo_data = int(os.environ["ROW_COUNT_geo_data"])
UNIQUE_VALS_geo_data = int(os.environ["UNIQUE_VALS_geo_data"])
PARTITIONS_NUM_geo_data = int(os.environ["PARTITIONS_NUM_geo_data"])

print("\nValue for x: ")
print(x)
print("\nValue for y: ")
print(y)
print("\nValue for z: ")
print(z)"""

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

ROW_COUNT_car_installs = random.randint(100000, 100000)
UNIQUE_VALS_car_installs = random.randint(500, ROW_COUNT_car_installs-1)
PARTITIONS_NUM_car_installs = round(ROW_COUNT_car_installs / UNIQUE_VALS_car_installs)
PARTITIONS_NUM_car_installs = check_partitions(PARTITIONS_NUM_car_installs)

ROW_COUNT_car_sales = random.randint(500000, 500000)
UNIQUE_VALS_car_sales = random.randint(500, ROW_COUNT_car_sales-1)
PARTITIONS_NUM_car_sales = round(ROW_COUNT_car_sales / UNIQUE_VALS_car_sales)
PARTITIONS_NUM_car_sales = check_partitions(PARTITIONS_NUM_car_sales)

ROW_COUNT_customer_data = random.randint(500000, 500000)
UNIQUE_VALS_customer_data = random.randint(500, ROW_COUNT_customer_data-1)
PARTITIONS_NUM_customer_data = round(ROW_COUNT_customer_data / UNIQUE_VALS_customer_data)
PARTITIONS_NUM_customer_data = check_partitions(PARTITIONS_NUM_customer_data)

ROW_COUNT_factory_data = random.randint(100000, 100000)
UNIQUE_VALS_factory_data = random.randint(500, ROW_COUNT_factory_data-1)
PARTITIONS_NUM_factory_data = round(ROW_COUNT_factory_data / UNIQUE_VALS_factory_data)
PARTITIONS_NUM_factory_data = check_partitions(PARTITIONS_NUM_factory_data)

ROW_COUNT_geo_data = random.randint(10000, 100000)
UNIQUE_VALS_geo_data = random.randint(500, ROW_COUNT_geo_data-1)
PARTITIONS_NUM_geo_data = round(ROW_COUNT_geo_data / UNIQUE_VALS_geo_data)
PARTITIONS_NUM_geo_data = check_partitions(PARTITIONS_NUM_geo_data)

print("SPARKGEN PIPELINE SPARK HYPERPARAMS")

print("x: {}", x)
print("y: {}", y)
print("z: {}", z)

print("ROW_COUNT_car_installs: {}", ROW_COUNT_car_installs)
print("UNIQUE_VALS_car_installs: {}", UNIQUE_VALS_car_installs)
print("PARTITIONS_NUM_car_installs: {}", PARTITIONS_NUM_car_installs)

print("ROW_COUNT_car_sales: {}", ROW_COUNT_car_sales)
print("UNIQUE_VALS_car_sales: {}", UNIQUE_VALS_car_sales)
print("PARTITIONS_NUM_car_sales: {}", PARTITIONS_NUM_car_sales)

print("ROW_COUNT_customer_data: {}", ROW_COUNT_customer_data)
print("UNIQUE_VALS_customer_data: {}", UNIQUE_VALS_customer_data)
print("PARTITIONS_NUM_customer_data: {}", PARTITIONS_NUM_customer_data)

print("ROW_COUNT_factory_data: {}", ROW_COUNT_factory_data)
print("UNIQUE_VALS_factory_data: {}", UNIQUE_VALS_factory_data)
print("PARTITIONS_NUM_factory_data: {}", PARTITIONS_NUM_factory_data)

print("ROW_COUNT_geo_data: {}", ROW_COUNT_geo_data)
print("UNIQUE_VALS_geo_data: {}", UNIQUE_VALS_geo_data)
print("PARTITIONS_NUM_geo_data: {}", PARTITIONS_NUM_geo_data)

car_installs_df  = dg.car_installs_gen(x,y, PARTITIONS_NUM_car_installs, ROW_COUNT_car_installs, UNIQUE_VALS_car_installs, True)
car_sales_df     = dg.car_sales_gen(x, y, z, PARTITIONS_NUM_car_sales, ROW_COUNT_car_sales, UNIQUE_VALS_car_sales, True)
customer_data_df = dg.customer_gen(x, y, z, PARTITIONS_NUM_customer_data, ROW_COUNT_customer_data, UNIQUE_VALS_customer_data, True)
factory_data_df  = dg.factory_gen(x, y, z, PARTITIONS_NUM_factory_data, ROW_COUNT_factory_data, UNIQUE_VALS_factory_data, True)
geo_data_df      = dg.geo_gen(x, y, z, PARTITIONS_NUM_geo_data, ROW_COUNT_geo_data, UNIQUE_VALS_geo_data, True)

#---------------------------------------------------
#       SQL CLEANUP: DATABASES, TABLES, VIEWS
#---------------------------------------------------

# Show catalog and database
print("REMOVE PRIOR RUN DATABASE")
spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(dbname))
print("SHOW DATABASES")
spark.sql("SHOW DATABASES").show()

##---------------------------------------------------
##                 CREATE DATABASES
##---------------------------------------------------

spark.sql("CREATE DATABASE {}".format(dbname))
spark.sql("USE {}".format(dbname))

# Show catalog and database
print("SHOW DATABASES")
spark.sql("SHOW DATABASES").show()

#---------------------------------------------------
#               POPULATE TABLES
#---------------------------------------------------

car_sales_df.write.mode("overwrite").partitionBy("month").saveAsTable('{0}.CAR_SALES_{1}'.format(dbname, username), format="parquet")
car_installs_df.write.mode("overwrite").saveAsTable('{0}.CAR_INSTALLS_{1}'.format(dbname, username), format="parquet")
factory_data_df.write.mode("overwrite").saveAsTable('{0}.EXPERIMENTAL_MOTORS_{1}'.format(dbname, username), format="parquet")
customer_data_df.write.mode("overwrite").saveAsTable('{0}.CUSTOMER_DATA_{1}'.format(dbname, username), format="parquet")
geo_data_df.write.mode("overwrite").saveAsTable('{0}.GEO_DATA_XREF_{1}'.format(dbname, username), format="parquet")

car_sales_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/car_sales")
car_installs_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/car_installs")
factory_data_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/factory_data")
customer_data_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/customer_data")
geo_data_df.write.mode("overwrite").option("header",True).csv("s3a://go01-demo/datalake/pdefusco/datagen/geo_data")

print("\tPOPULATE TABLE(S) COMPLETED")

print("JOB COMPLETED.\n\n")
