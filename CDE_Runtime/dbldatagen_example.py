import sys
print("Python version:")
print(sys.version)
print('\n')
help('modules')

import dbldatagen as dg
from pyspark.sql.types import IntegerType, FloatType, StringType
from pyspark.sql import SparkSession

data_lake_name = "s3a://go01-demo/"
spark = SparkSession.builder.\
        appName('INGEST')\
        .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
        .getOrCreate()

column_count = 10
data_rows = 1000 * 1000

df_spec = (dg.DataGenerator(spark, name="test_data_set1", rows=data_rows,
                                                  partitions=4)
           .withIdOutput()
           .withColumn("r", FloatType(),
                            expr="floor(rand() * 350) * (86400 + 3600)",
                            numColumns=column_count)
           .withColumn("code1", IntegerType(), minValue=100, maxValue=200)
           .withColumn("code2", IntegerType(), minValue=0, maxValue=10)
           .withColumn("code3", StringType(), values=['a', 'b', 'c'])
           .withColumn("code4", StringType(), values=['a', 'b', 'c'],
                          random=True)
           .withColumn("code5", StringType(), values=['a', 'b', 'c'],
                          random=True, weights=[9, 1, 1])

           )

df = df_spec.build()
num_rows=df.count()
