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

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
import configparser
from sparkmeasure import StageMetrics
from datetime import datetime
import os
import random
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
#               CREATE SPARK SESSION
#---------------------------------------------------

### CDE JAR OPTIONS
# Adding the following in your spark session in CDE won't work. You must add the two configs directly as CDE Job configurations
#.config("spark.jars","/app/mount/{}/spark-measure_2.13-0.23.jar".format(CDE_RESOURCE_NAME))\
#.config("spark.driver.extraClassPath","/app/mount/{}/spark-measure_2.13-0.23.jar".format(CDE_RESOURCE_NAME))\

spark = SparkSession \
    .builder \
    .appName("ICEBERG LOAD") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")\
    .config("spark.sql.catalog.spark_catalog.type", "hive")\
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")\
    .config("spark.kubernetes.access.hadoopFileSystems", data_lake_name)\
    .getOrCreate()

# Show catalog and database
print("SHOW CURRENT NAMESPACE")
spark.sql("SHOW CURRENT NAMESPACE").show()
spark.sql("USE {}".format(dbname))

# Show catalog and database
print("SHOW NEW NAMESPACE IN USE\n")
spark.sql("SHOW CURRENT NAMESPACE").show()

### CML SPARK SESSION

print("ALL SPARK CONFIGS: ")
print(spark.sparkContext.getConf().getAll())

_DEBUG_ = False

stagemetrics = StageMetrics(spark)

#---------------------------------------------------
#                READ SOURCE TABLES
#---------------------------------------------------
print("JOB STARTED...")
car_sales_df     = spark.sql("SELECT * FROM {0}.CAR_SALES_{1}".format(dbname, username)) #could also checkpoint here but need to set checkpoint dir

'''customer_data_df = spark.sql("SELECT * FROM {0}.CUSTOMER_DATA_{1}".format(dbname, username))
car_installs_df  = spark.sql("SELECT * FROM {0}.CAR_INSTALLS_{1}".format(dbname, username))
factory_data_df  = spark.sql("SELECT * FROM {0}.EXPERIMENTAL_MOTORS_{1}".format(dbname, username))
geo_data_df      = spark.sql("SELECT postalcode as zip, latitude, longitude FROM {0}.GEO_DATA_XREF_{1}".format(dbname, username))'''

print("\tREAD TABLE(S) COMPLETED")

#---------------------------------------------------
#             JOIN DATA INTO ONE TABLE
#---------------------------------------------------
# SQL way to do things
stagemetrics.begin()

#---------------------------------------------------
#               ICEBERG MERGE INTO
#---------------------------------------------------

# PRE-INSERT COUNT
print("\n")
print("PRE-MERGE COUNT")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{0}.CAR_SALES_{1}".format(dbname, username)).show()

ICEBERG_MERGE_INTO = "MERGE INTO spark_catalog.{0}.CAR_SALES_{1} t\
                      USING (SELECT * FROM spark_catalog.{0}.CAR_SALES_STAGING_{1}) s\
                      ON t.id = s.id\
                      WHEN MATCHED THEN UPDATE SET t.saleprice = s.saleprice\
                      WHEN NOT MATCHED THEN INSERT *".format(dbname, username)

print("\n")
print("EXECUTING ICEBERG MERGE INTO QUERY")
print("\n")
print(ICEBERG_MERGE_INTO)
spark.sql(ICEBERG_MERGE_INTO)

# PRE-INSERT COUNT
print("\n")
print("POST-MERGE COUNT")
print("\n")
spark.sql("SELECT COUNT(*) FROM spark_catalog.{0}.CAR_SALES_{1}".format(dbname, username)).show()

stagemetrics.end()
stagemetrics.print_report()

#Saving metrics to df
stage_metrics_df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")
stage_metrics_df = stage_metrics_df.withColumn("DAY_OF_RUN", lit(today))

stage_metrics_df.registerTempTable("STAGE_METRICS_TEMPTABLE")

print("COLS\n")
print(stage_metrics_df.dtypes)

print("INSERT INTO {}.STAGE_METRICS_TABLE SELECT * FROM STAGE_METRICS_TEMPTABLE".format(dbname))
spark.sql("INSERT INTO {}.STAGE_METRICS_TABLE SELECT * FROM STAGE_METRICS_TEMPTABLE".format(dbname))

print("JOB COMPLETED!\n\n")
