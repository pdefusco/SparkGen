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
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
import configparser
from sparkmeasure import StageMetrics
from datetime import datetime

# current date and time
now = datetime.now()

timestamp = datetime.timestamp(now)

## CDE PROPERTIES
#config = configparser.ConfigParser()
#config.read('/app/mount/parameters.conf')
#data_lake_name=config.get("general","data_lake_name")
#s3BucketName=config.get("general","s3BucketName")
#username=config.get("general","username")

## CML PROPERTIES
data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/sparkgen"
username = "pdefusco_052923"

print("Running as Username: ", username)

dbname = "SPARKGEN_{}".format(username)
sparkmetrics_dbname = "SPARKGEN_METRICS_{}".format(username)

print("\nUsing DB Name: ", dbname)

#---------------------------------------------------
#               CREATE SPARK SESSION
#---------------------------------------------------

### CDE JAR OPTIONS
# Adding the following in your spark session in CDE won't work. You must add the two configs directly as CDE Job configurations
#.config("spark.jars","/app/mount/{}/spark-measure_2.13-0.23.jar".format(CDE_RESOURCE_NAME))\
#.config("spark.driver.extraClassPath","/app/mount/{}/spark-measure_2.13-0.23.jar".format(CDE_RESOURCE_NAME))\

CDE_RESOURCE_NAME = "SPARKGEN_FILES"

spark = SparkSession.builder.\
        appName('ENRICH')\
        .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
        .config("spark.jars.packages","ch.cern.sparkmeasure:spark-measure_2.12:0.23")\
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
#               SPARKMEASURE STAGEMETRICS
#---------------------------------------------------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(sparkmetrics_dbname))

spark.sql("DROP TABLE IF EXISTS {}.STAGE_METRICS_TABLE".format(sparkmetrics_dbname))

spark.sql("CREATE TABLE IF NOT EXISTS {}.STAGE_METRICS_TABLE\
                (JOBID BIGINT,\
                JOBGROUP STRING,\
                STAGEID INT,\
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
                SHUFFLERECORDSWRITTEN BIGINT,\
                INSERT_TIME FLOAT\
                )".format(sparkmetrics_dbname))

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
stagemetrics.begin()
salesandcustomers_sql = "SELECT customers.*, sales.saleprice, sales.model, sales.VIN\
                            FROM {0}.CAR_SALES_{1} sales JOIN {0}.CUSTOMER_DATA_{1} customers\
                             ON sales.customer_id = customers.customer_id\
                             WHERE customers.salary > 30000".format(dbname, username)

sales_x_customers_df = spark.sql(salesandcustomers_sql)
if (_DEBUG_):
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA")

# Add geolocations based on ZIP
sales_x_customers_x_geo_df = sales_x_customers_df.join(geo_data_df, "zip")
if (_DEBUG_):
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF")

# Add installation information (What part went into what car?)
sales_x_customers_x_geo_x_carinstalls_df = sales_x_customers_x_geo_df.join(car_installs_df, ["VIN","model"])
if (_DEBUG_):
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model)")

# Add factory information (For each part, in what factory was it made, from what machine, and at what time)
sales_x_customers_x_geo_x_carinstalls_x_factory_df = sales_x_customers_x_geo_x_carinstalls_df.join(factory_data_df, ["serial_no"])
if (_DEBUG_):
    print("\tJOIN QUERY: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model) x EXPERIMENTAL_MOTORS (serial_no)")

# Triggering the Action
sales_x_customers_x_geo_x_carinstalls_x_factory_df.count()

#Saving metrics to df
metrics_df = stagemetrics.create_stagemetrics_DF("PerfStageMetrics")

stagemetrics.end()
stagemetrics.print_report()

metrics_df = metrics_df.withColumn("INSERT_TIME", lit(timestamp))
metrics_df.registerTempTable("STAGE_METRICS_TEMPTABLE")
spark.sql("INSERT INTO {}.STAGE_METRICS_TABLE SELECT * FROM STAGE_METRICS_TEMPTABLE".format(sparkmetrics_dbname))


cumulative_metrics_df = spark.sql("SELECT * FROM {}.STAGE_METRICS_TABLE".format(sparkmetrics_dbname))
display(cumulative_metrics_df.toPandas())


spark.stop()
print("JOB COMPLETED!\n\n")
