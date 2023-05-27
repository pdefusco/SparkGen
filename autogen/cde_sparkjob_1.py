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

import numpy as np
import pandas as pd
import os
from os.path import exists
import json
import sys
import xmltodict as xd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
import configparser
from datagen import *
from datetime import datetime

config = configparser.ConfigParser()
config.read('/app/mount/parameters.conf')
data_lake_name=config.get("general","data_lake_name")
s3BucketName=config.get("general","s3BucketName")
username=config.get("general","username")

print("\nRunning as Username: ", username)

dbname = "SPARKGEN_{}".format(username)

print("\nUsing DB Name: ", dname)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------
spark = SparkSession.builder.appName('INGEST').config("spark.yarn.access.hadoopFileSystems", data_lake_name).getOrCreate()

#-----------------------------------------------------------------------------------
# CREATE DATASETS WITH RANDOM DISTRIBUTIONS
#-----------------------------------------------------------------------------------

dg = DataGen(spark, username)

x = 1
y = 2
z = 3

car_installs_df  = dg.car_installs_gen(z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True)
car_sales_df     = dg.car_sales_gen(x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True)
customer_data_df = dg.customer_gen(x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True)
factory_data_df  = dg.factory_gen(x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True)
geo_data_df      = dg.geo_gen(x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True)

#---------------------------------------------------
#       SQL CLEANUP: DATABASES, TABLES, VIEWS
#---------------------------------------------------
spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(dbname))

##---------------------------------------------------
##                 CREATE DATABASES
##---------------------------------------------------
spark.sql("CREATE DATABASE {}".format(dbname))

#---------------------------------------------------
#               POPULATE TABLES
#---------------------------------------------------


df.write.mode("overwrite").saveAsTable('{0}_CAR_INSTALLS_{1}'.format(dbname, username), format="parquet") #partitionBy("month")
df.write.mode("overwrite").saveAsTable('{0}_CAR_SALES_{1}'.format(dbname, username), format="parquet")
df.write.mode("overwrite").saveAsTable('{0}_CUSTOMER_DATA_{1}'.format(dbname, username), format="parquet")
df.write.mode("overwrite").saveAsTable('{0}_EXPERIMENTAL_MOTORS_{1}'.format(dbname, username), format="parquet")
df.write.mode("overwrite").saveAsTable('{0}_GEO_DATA_XREF_{1}'.format(dbname, username), format="parquet")

print("\tPOPULATE TABLE(S) COMPLETED")

print("JOB COMPLETED.\n\n")
