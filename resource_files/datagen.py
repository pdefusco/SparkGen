import numpy as np
import pandas as pd
import os
from datetime import datetime
from pyspark.sql.types import LongType, IntegerType, StringType
import dbldatagen as dg
import dbldatagen.distributions as dist

class DataGen:

    '''Class to Generate Data'''

    def __init__(self, spark, username):
        self.spark = spark
        self.username = username
        ## TODO: look into adding custom db functionality


    def iot_gen(self, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

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
            dg.DataGenerator(self.spark, name="device_data_set", rows=row_count,partitions=partitions_num).withIdOutput()
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


    def car_installs_gen(self, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        model_codes = ["A","B","D","E"]

        testDataSpec = (
            dg.DataGenerator(self.spark, name="car_installs", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("model", "string", values=model_codes, random=True, distribution="normal")#, distribution="normal"
            .withColumn("VIN", "string", template=r'\\N8UCGTTVDK5J', random=True)
            .withColumn("serial_no", "string", template=r'\\N42CLDR0156661577860220', random=True)
        )

        df = testDataSpec.build()

        return df


    def car_sales_gen(self, x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        model_codes = ["Model A","Model B","Model D","Model E"]

        testDataSpec = (
            dg.DataGenerator(self.spark, name="car_sales", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("customer_id", "integer", minValue=10000, maxValue=1000000, random=True, distribution="normal")
            .withColumn("model", "string", values=model_codes, random=True, distribution=dist.Gamma(x, y))
            .withColumn("saleprice", "decimal(10,2)", minValue=5000, maxValue=100000, random=True, distribution=dist.Exponential(z))
            .withColumn("VIN", "string", template=r'\\N8UCGTTVDK5J', random=True)
            .withColumn("month", "integer", minValue=1, maxValue=12, random=True, distribution=dist.Exponential(z))
            .withColumn("year", "integer", minValue=1999, maxValue=2023, random=True, distribution="normal")
            .withColumn("day", "integer", minValue=1, maxValue=28, random=True, distribution=dist.Gamma(x, y))
        )

        df = testDataSpec.build()

        return df


    def customer_gen(self, x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        model_codes = ["Model A","Model B","Model D","Model E"]
        gender_codes = ["M","F"]

        testDataSpec = (
            dg.DataGenerator(self.spark, name="customer_data", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("customer_id", "integer", minValue=10000, maxValue=1000000, random=True)
            .withColumn('username', 'string', template=r'\\w', random=True, distribution=dist.Gamma(x, y))
            .withColumn('name', 'string', template=r'\\w', random=True, distribution=dist.Gamma(x, y))
            .withColumn('gender', 'string', values=gender_codes, random=True)
            .withColumn("email", 'string', template=r"\\w.\\w@\\w.com", random=True, distribution=dist.Gamma(x, y))
            .withColumn("birthdate", "timestamp", begin="1950-01-01 01:00:00",
                    end="2003-12-31 23:59:00", interval="1 minute", random=True, distribution="normal")
            .withColumn("salary", "decimal(10,2)", minValue=50000, maxValue=1000000, random=True, distribution="normal")
            .withColumn("zip", "integer", minValue=10000, maxValue=99999, random=True, distribution="normal")
        )

        df = testDataSpec.build()

        return df


    def factory_gen(self, x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        testDataSpec = (
            dg.DataGenerator(self.spark, name="factory_data", rows=row_count,partitions=partitions_num).withIdOutput()
            .withColumn("factory_no", "int", minValue=10000, maxValue=1000000, random=True, distribution=dist.Gamma(x, y))
            .withColumn("machine_no", "int", minValue=120, maxValue=99999, random=True, distribution=dist.Gamma(x, y))
            .withColumn("serial_no", "string", template=r'\\N42CLDR0156661577860220', random=True)
            .withColumn("part_no", "string", template=r'\\a42CLDR', random=True)
            .withColumn("timestamp", "timestamp", begin="2000-01-01 01:00:00",
                    end="2003-12-31 23:59:00", interval="1 minute", random=True, distribution="normal")
            .withColumn("status", "string", values=["beta_engine"])

        )

        df = testDataSpec.build()

        return df


    def geo_gen(self, x, y, z, partitions_num=10, row_count = 100000, unique_vals=100000, display_option=True):

        state_names = ["Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado", "Connecticut", "Delaware", "Florida"]

        testDataSpec = (
            dg.DataGenerator(self.spark, name="geo_data", rows=row_count, partitions=partitions_num).withIdOutput()
            .withColumn("country_code", "string", values=["US"])
            .withColumn("state", "string", values=state_names, random=True, distribution=dist.Gamma(x, y))
            .withColumn("postalcode", "integer", minValue=10000, maxValue=99999, random=True, distribution="normal")
            .withColumn("latitude", "decimal(10,2)", minValue=-90, maxValue=90, random=True, distribution=dist.Exponential(z))
            .withColumn("longitude", "decimal(10,2)", minValue=-180, maxValue=180, random=True)
        )

        df = testDataSpec.build()

        return df
