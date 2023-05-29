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

!pip3 install -r requirements.txt

import numpy as np
import pandas as pd
import os
from os.path import exists
import json
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import configparser
from cde_resource_files.datagen import *
from datetime import datetime
import random

## CDE PROPERTIES
#config = configparser.ConfigParser()
#config.read('/app/mount/parameters.conf')
#data_lake_name=config.get("general","data_lake_name")
#s3BucketName=config.get("general","s3BucketName")
#username=config.get("general","username")

# current date and time
now = datetime.now()

timestamp = datetime.timestamp(now)

## CML PROPERTIES
data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/sparkgen"
username = "pdefusco_052923"

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

x = int(os.environ["x"])
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
print(z)

car_installs_df  = dg.car_installs_gen(z, PARTITIONS_NUM_car_installs, ROW_COUNT_car_installs, UNIQUE_VALS_car_installs, True)
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

print("\tPOPULATE TABLE(S) COMPLETED")

print("JOB COMPLETED.\n\n")
