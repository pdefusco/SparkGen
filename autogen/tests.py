!pip3 install -r requirements.txt

from pyspark.sql import SparkSession
#from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.functions import lit
from datetime import datetime
import os
import random

# current date and time
now = datetime.now()

timestamp = datetime.timestamp(now)

## CML PROPERTIES
data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/datalake/pdefusco/cde119_workshop"
username = "pdefusco_062223"

print("Running as Username: ", username)
_DEBUG_=False
spark = SparkSession.builder.appName('INGEST').config("spark.yarn.access.hadoopFileSystems", data_lake_name).getOrCreate()


car_installs_df  = spark.sql("SELECT * FROM CDE_WORKSHOP.CAR_INSTALLS_{}".format(username))
car_sales_df  = spark.sql("SELECT * FROM CDE_WORKSHOP.CAR_SALES_{}".format(username))
factory_data_df  = spark.sql("SELECT * FROM CDE_WORKSHOP.EXPERIMENTAL_MOTORS_{}".format(username))
customer_data_df  = spark.sql("SELECT * FROM CDE_WORKSHOP.CUSTOMER_DATA_{}".format(username))
geo_data_df = spark.sql("SELECT * FROM CDE_WORKSHOP.GEO_DATA_XREF_{}".format(username))

print("\tPOPULATE TABLE(S) COMPLETED")


car_sales_df = car_sales_df.withColumn("car_model", F.when(F.col("model") == "Model A","A") \
      .when(F.col("model") == "Model B","B") \
      .when(F.col("model") == "Model C","C") \
      .when(F.col("model") == "Model D","D") \
      .when(F.col("model") == "Model E","E")).drop(F.col("model")).withColumnRenamed("car_model","model")
    


#---------------------------------------------------
#             JOIN DATA INTO ONE TABLE
#---------------------------------------------------
# SQL way to do things
salesandcustomers_sql = "SELECT customers.*, sales.saleprice, sales.model, sales.VIN \
                            FROM CDE_WORKSHOP.CAR_SALES_{0} sales JOIN CDE_WORKSHOP.CUSTOMER_DATA_{0} customers \
                             ON sales.customer_id = customers.customer_id".format(username)

tempTable = spark.sql(salesandcustomers_sql)
if (_DEBUG_):
    print("\tTABLE: CAR_SALES")
    car_sales.show(n=5)
    print("\tTABLE: CUSTOMER_DATA")
    customer_data.show(n=5)
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA")
    tempTable.show(n=5)

# Add geolocations based on ZIP
tempTable = tempTable.withColumn("postalcode", F.col("zip")).join(geo_data_df.drop("id"), "postalcode")
if (_DEBUG_):
    print("\tTABLE: GEO_DATA_XREF")
    geo_data_df.show(n=5)
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip)")
    tempTable.show(n=5)

# Add installation information (What part went into what car?)
tempTable = tempTable.join(car_installs_df.drop("id", "vin"), ["model"], "left")
if (_DEBUG_):
    print("\tTABLE: CAR_INSTALLS")
    car_installs_df.show(n=5)
    print("\tJOIN: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model)")
    tempTable.show(n=5)

# Add factory information (For each part, in what factory was it made, from what machine, and at what time)
tempTable = tempTable.join(factory_data_df.drop("id"), ["serial_no"])
if (_DEBUG_):
    print("\tTABLE: EXPERIMENTAL_MOTORS")
    factory_data_df.show(n=5)
    print("\tJOIN QUERY: CAR_SALES x CUSTOMER_DATA x GEO_DATA_XREF (zip) x CAR_INSTALLS (vin, model) x EXPERIMENTAL_MOTORS (serial_no)")
    tempTable.show(n=5)

#---------------------------------------------------
#             CREATE NEW HIVE TABLE
#---------------------------------------------------
tempTable.write.mode("overwrite").saveAsTable('CDE_WORKSHOP.experimental_motors_enriched_{}'.format(username), format="parquet")
print("\tNEW ENRICHED TABLE CREATED: CDE_WORKSHOP.experimental_motors_enriched_{}".format(username))
tempTable.show(n=5)
print("\n")
tempTable.dtypes