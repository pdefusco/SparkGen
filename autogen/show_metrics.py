
from pyspark.sql import SparkSession

data_lake_name = "s3a://go01-demo/"
s3BucketName = "s3a://go01-demo/sparkgen"
username = "pdefusco_052923"

print("Running as Username: ", username)

dbname = "SPARKGEN_{}".format(username)
sparkmetrics_dbname = "SPARKGEN_METRICS_{}".format(username)

print("\nUsing DB Name: ", dbname)

spark = SparkSession.builder.\
        appName('ENRICH')\
        .config("spark.yarn.access.hadoopFileSystems", data_lake_name)\
        .getOrCreate()

cumulative_metrics_df = spark.sql("SELECT * FROM {}.STAGE_METRICS_TABLE".format(sparkmetrics_dbname))
display(cumulative_metrics_df.toPandas())
