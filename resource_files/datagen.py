import numpy as np
import pandas as pd
import os
from datetime import datetime
from pyspark.sql.types import LongType, IntegerType, StringType
import dbldatagen as dg

class DataGen:

    '''Class to Generate Data'''

    def __init__(self, spark, username):
        self.spark = spark
        self.username = username
        ## TODO: look into adding custom db functionality


    def iot_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        #shuffle_partitions_requested = 8

        #spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        country_codes = [
            "CN", "US", "FR", "CA", "IN", "JM", "IE", "PK", "GB", "IL", "AU",
            "SG", "ES", "GE", "MX", "ET", "SA", "LB", "NL", "IT"
        ]
        country_weights = [
            1300, 365, 67, 38, 1300, 3, 7, 212, 67, 9, 25, 6, 47, 83,
            126, 109, 58, 8, 17, 20
        ]

        manufacturers = [
            "Delta corp", "Xyzzy Inc.", "Lakehouse Ltd", "Acme Corp", "Embanks Devices",
        ]

        lines = ["delta", "xyzzy", "lakehouse", "gadget", "droid"]

        testDataSpec = (
            dg.DataGenerator(spark, name="device_data_set", rows=row_count,partitions=partitions_num).withIdOutput()
            # we'll use hash of the base field to generate the ids to
            # avoid a simple incrementing sequence
            .withColumn("internal_device_id", "long", minValue=0x1000000000000,
                        uniqueValues=unique_vals, omit=True, baseColumnType="hash",
            )
            # note for format strings, we must use "%lx" not "%x" as the
            # underlying value is a long
            .withColumn(
                "device_id", "string", format="0x%013x", baseColumn="internal_device_id"
            )
            # the device / user attributes will be the same for the same device id
            # so lets use the internal device id as the base column for these attribute
            .withColumn("country", "string", values=country_codes, #weights=country_weights,
                        baseColumn="internal_device_id")
            .withColumn("manufacturer", "string", values=manufacturers,
                        baseColumn="internal_device_id", )
            # use omit = True if you don't want a column to appear in the final output
            # but just want to use it as part of generation of another column
            .withColumn("line", "string", values=lines, baseColumn="manufacturer",
                        baseColumnType="hash", omit=True )
            .withColumn("model_ser", "integer", minValue=1, maxValue=11, baseColumn="device_id",
                        baseColumnType="hash", omit=True, )
            .withColumn("model_line", "string", expr="concat(line, '#', model_ser)",
                        baseColumn=["line", "model_ser"] )
            .withColumn("event_type", "string",
                        values=["activation", "deactivation", "plan change", "telecoms activity",
                                "internet activity", "device error", ],
                        random=True)
            .withColumn("event_ts", "timestamp", begin="2020-01-01 01:00:00",
                        end="2020-12-31 23:59:00",
                        interval="1 minute", random=True )
        )

        dfTestData = testDataSpec.build()

        if display_option == True:
            display(dfTestData)

        return dfTestData


    def car_installs_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        model_codes = ["A","B","D","E"]

        testDataSpec = (
            dg.DataGenerator(spark, name="car_installs", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("model", "string", values=model_codes, random=True)
            .withColumn("VIN", "string", template=r'\\N8UCGTTVDK5J', random=True)
            .withColumn("serial_no", "string", template=r'\\N42CLDR0156661577860220', random=True)
        )

        dfTestData = testDataSpec.build()

        return dfTestData


    def car_sales_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        model_codes = ["Model A","Model B","Model D","Model E"]

        testDataSpec = (
            dg.DataGenerator(spark, name="car_sales", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("customer_id", "integer", minValue=10000, maxValue=1000000, random=True)
            .withColumn("model", "string", values=model_codes, random=True)
            .withColumn("saleprice", "decimal(10,2)", minValue=5000, maxValue=100000, random=True)
            .withColumn("VIN", "string", template=r'\\N8UCGTTVDK5J', random=True)
            .withColumn("month", "integer", minValue=1, maxValue=12, random=True)
            .withColumn("year", "integer", minValue=1999, maxValue=2023, random=True)
            .withColumn("day", "integer", minValue=1, maxValue=28, random=True)
        )

        dfTestData = testDataSpec.build()

        return dfTestData

    def customer_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        model_codes = ["Model A","Model B","Model D","Model E"]
        gender_codes = ["M","F"]

        testDataSpec = (
            dg.DataGenerator(spark, name="customer_data", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("customer_id", "integer", minValue=10000, maxValue=1000000, random=True)
            .withColumn('username', 'string', template=r'\\w', random=True)
            .withColumn('name', 'string', template=r'\\w', random=True)
            .withColumn('gender', 'string', values=gender_codes, random=True)
            .withColumn("email", 'string', template=r"\\w.\\w@\\w.com", random=True)
            .withColumn("birthdate", "timestamp", begin="1950-01-01 01:00:00",
                    end="2003-12-31 23:59:00", interval="1 minute", random=True )
            .withColumn("salary", "decimal(10,2)", minValue=50000, maxValue=1000000, random=True)
            .withColumn("zip", "integer", minValue=10000, maxValue=99999, random=True)
        )

        dfTestData = testDataSpec.build()

        return dfTestData


    def factory_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        testDataSpec = (
            dg.DataGenerator(spark, name="factory_data", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("factory_no", "int", minValue=10000, maxValue=1000000, random=True)
            .withColumn("machine_no", "int", minValue=120, maxValue=99999, random=True)
            .withColumn("serial_no", "string", template=r'\\N42CLDR0156661577860220', random=True)
            .withColumn("part_no", "string", template=r'\\a42CLDR', random=True)
            .withColumn("timestamp", "timestamp", begin="2000-01-01 01:00:00",
                    end="2003-12-31 23:59:00", interval="1 minute", random=True )
            .withColumn("status", "string", values=["beta_engine"])

        )

        dfTestData = testDataSpec.build()

        return dfTestData

    def geo_gen(partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        state_names = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida"]

        testDataSpec = (
            dg.DataGenerator(spark, name="geo_data", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("country_code", "string", values=["US"])
            .withColumn("state", "string", values=state_names, random=True)
            .withColumn("postalcode", "integer", minValue=10000, maxValue=99999, random=True)
            .withColumn("latitude", "decimal(10,2)", minValue=-90, maxValue=90, random=True)
            .withColumn("longitude", "decimal(10,2)", minValue=-180, maxValue=180, random=True)
        )

        dfTestData = testDataSpec.build()

        return dfTestData


    def save_table(df, table_name_prefix, username):

        now = datetime.now()
        timestamp = datetime.timestamp(now)
        print("TIMESTAMP: ", timestamp)
        df.write.mode("overwrite").saveAsTable('{0}_{1}_{2}'.format(table_name_prefix, username, timestamp), format="parquet") #partitionBy()
