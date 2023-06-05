#****************************************************************************
# (C) Cloudera, Inc. 2020-2022
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

#!pip3 install -r requirements.txt

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from datetime import datetime
import os
import random

## CML PROPERTIES
data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/sparkgen"
username = "pdefusco_052923"

print("Running as Username: ", username)

dbname = "SPARKGEN_{}".format(username)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

spark = SparkSession.builder.\
        appName('ENRICH')\
        .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
        .getOrCreate()

_DEBUG_ = True

### CML SPARK SESSION

print("ALL SPARK CONFIGS: ")
print(spark.sparkContext.getConf().getAll())

#---------------------------------------------------
#                READ SOURCE TABLES
#---------------------------------------------------
print("JOB STARTED...")
car_sales_df     = spark.sql("SELECT * FROM {0}.CAR_SALES_{1}".format(dbname, username)) #could also checkpoint here but need to set checkpoint dir
customer_data_df = spark.sql("SELECT * FROM {0}.CUSTOMER_DATA_{1}".format(dbname, username))
car_installs_df  = spark.sql("SELECT * FROM {0}.CAR_INSTALLS_{1}".format(dbname, username))
factory_data_df  = spark.sql("SELECT * FROM {0}.EXPERIMENTAL_MOTORS_{1}".format(dbname, username))
geo_data_df      = spark.sql("SELECT postalcode as zip, latitude, longitude FROM {0}.GEO_DATA_XREF_{1}".format(dbname, username))
print("\tREAD TABLE(S) COMPLETED")

#---------------------------------------------------
#                  APPLY FILTERS
# - Remove under aged drivers (less than 16 yrs old)
#---------------------------------------------------
#before = customer_data_df.count()

#print(customer_data_df.dtypes)
#print(customer_data_df.schema)

#customer_data = customer_data.filter(col('birthdate') <= F.add_months(F.current_date(),-192))
#after = customer_data.count()
#print(f"\tFILTER DATA (CUSTOMER_DATA): Before({before}), After ({after}), Difference ({after - before}) rows")

#---------------------------------------------------
#             JOIN DATA INTO ONE TABLE
#---------------------------------------------------
# SQL way to do things

salesandcustomers_sql = "SELECT customers.*, sales.saleprice, sales.model, sales.VIN\
                            FROM {0}.CAR_SALES_{1} sales JOIN {0}.CUSTOMER_DATA_{1} customers\
                             ON sales.customer_id = customers.customer_id\
                             WHERE customers.salary > 30000".format(dbname, username)

sales_x_customers_df = spark.sql(salesandcustomers_sql)
if (_DEBUG_):
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA")

print("sales_x_customers_df NUM PARTITIONS")
#print(sales_x_customers_df.rdd.getNumPartitions())

# Add geolocations based on ZIP
sales_x_customers_x_geo_df = sales_x_customers_df.join(geo_data_df, "zip")
if (_DEBUG_):
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF")

print("sales_x_customers_x_geo_df NUM PARTITIONS")
#print(sales_x_customers_x_geo_df.rdd.getNumPartitions())

print("CHANGING SHUFFLE PARTITIONS TO 24")
spark.conf.set("spark.sql.shuffle.partitions",24)

# Add installation information (What part went into what car?)
sales_x_customers_x_geo_x_carinstalls_df = sales_x_customers_x_geo_df.join(car_installs_df, ["VIN","model"])
if (_DEBUG_):
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model)")

print("sales_x_customers_x_geo_x_carinstalls_df NUM PARTITIONS")
#print(sales_x_customers_x_geo_x_carinstalls_df.rdd.getNumPartitions())

# Add factory information (For each part, in what factory was it made, from what machine, and at what time)
sales_x_customers_x_geo_x_carinstalls_x_factory_df = sales_x_customers_x_geo_x_carinstalls_df.join(factory_data_df, ["serial_no"])
if (_DEBUG_):
    print("\tJOIN QUERY: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model) x EXPERIMENTAL_MOTORS (serial_no)")

print("sales_x_customers_x_geo_x_carinstalls_x_factory_df NUM PARTITIONS")
#print(sales_x_customers_x_geo_x_carinstalls_x_factory_df.rdd.getNumPartitions())

# Triggering the Action
sales_x_customers_x_geo_x_carinstalls_x_factory_df.count()

spark.stop()
print("JOB COMPLETED!\n\n")
