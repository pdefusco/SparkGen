###########################################################  full_analog_cct_0  ###########################################################
# Outline of the data processing logic:
#
#   Summary:
#   - loads analog_cct_0 data 
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_scs_analog_cct_0/year=*/month=*/day=*
#       - filter: where analog_type = 'MW' and(created>=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table mart.analog_cct_0
#
#   Steps: 
#   - drop temp view in Spark session if it exists: 'raw_scs_analog_cct_0' 
#   - create SparkSQL database 'raw' on S3
#   - drop SparkSQL table'raw.analog_cct_0' on S3
#   - create dataframe ${inputDf_raw_analog_cct} from S3 landing bucket
#   - filter dataframe ${inputDf_raw_analog_cct} by date range from job configuration
#   - create temp view 'raw_scs_analog_cct_0' in Spark session
#   - load dataframe ${inputDf_raw_analog_cct} to temp view 'raw_scs_analog_cct_0' 
#   - create SparkSQL table 'raw.analog_cct_0' on S3
#   - load temp view 'raw_scs_analog_cct_0' to 'raw.analog_cct_0' table
#   - create SparkSQL database 'mart' on S3
#   - create SparkSQL table 'mart.analog_cct_0'
#   - load selected data from 'raw.analog_cct_0' to 'mart.analog_cct_0'
# 
#   Details:
#   - create dataframe from data in S3 landing bucket
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_scs_analog_cct_0/year=*/month=*/day=*
#       - to: dataframe ${inputDf_raw_analog_cct}
# 
#   - load dataframe to 'raw_scs_analog_cct_0' 
#       - from: data frame ${inputDf_raw_analog_cct}
#       - filter: date range from job configuration
#       - to: temp view 'raw_scs_analog_cct_0'
# 
#   - select data from 'temp view' into SparkSQL TABLE 'raw.analog_cct_0'
#       - from: temp view 'raw_scs_analog_cct_0'
#       - to: table 'raw.analog_cct_0'
# 
#   - select data from 'raw.analog_cct_0' into dataframe ${insertDf_raw_analog_cct}
#       - from: table 'raw.analog_cct_0'
#       - filter: where analog_type = 'MW' and(created>=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
#       - to: dataframe ${insertDf_raw_branch}
# 
#   - select dataframe ${insertDf_raw_analog_cct} into table 'mart.analog_cct_0'
#       - from: dataframe ${insertDf_raw_analog_cct}    
#       - to: table mart.analog_cct_0

from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_date, date_sub, dayofmonth, month,
                                   to_date, to_timestamp, year)
import util


##
# combines these two files:
#   * raw/raw_analog_0.py
#   * mart/mart_analog_0.py

# set top level variables
raw_database = "raw"
mart_database = "mart"
table_analog_cct = "analog_cct_0"

# initiate spark session
spark = SparkSession \
    .builder \
    .appName(table_analog_cct) \
    .enableHiveSupport() \
    .getOrCreate()

# Setup logger
log4jLogger = spark._jvm.org.apache.log4j
logger = log4jLogger.LogManager.getLogger(__name__)
logger.info("Job is running on "+ util.get_version_number())
#logger.info('Log this text')

# pull environment from spark session
env = spark.conf.get("spark.driver.env")
bucket = "s3a://tp-datalake-%s" % (env)
project = "datalake"

# hive configuration
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("hive.exec.dynamic.partition", "true")
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.hive.mapred.supports.subdirectories", "true")
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")


######################### RAW ##################################
# drop temp view
spark.catalog.dropTempView("%s_scs_%s" % (raw_database, table_analog_cct))

# create 'raw' database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (raw_database, bucket, env, project, raw_database))

# drop 'analog_cct_0' table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (raw_database, table_analog_cct))

basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s" % (
    env, raw_database, table_analog_cct)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s/year=*/month=*/day=*" % (
    env, raw_database, table_analog_cct)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s" % (
        'dev', raw_database, table_analog_cct)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s/year=*/month=*/day=*" % (
        'dev', raw_database, table_analog_cct)

inputDf_raw_analog_cct = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)
    
# date range for job
firstDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.firstDay", '2_DAYS_AGO'))      # firstDay="2021-11-05"
lastDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.lastDay", 'TODAY'))             # lastDay="2021-11-07"
logger.info(f'Data date range ("firstDay" -> "lastDay"): "{firstDay}" -> "{lastDay}"')

inputDf_raw_analog_cct \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_scs_%s" % (table_analog_cct))


# read last 3 day's of partitions from landing parquet files
# inputDf_raw_analog_cct.filter("""
#     (year = year(to_date("2021-11-07", "yyyy-MM-dd")) and month = month(to_date("2021-11-07", "yyyy-MM-dd")) and day = dayofmonth(to_date("2021-11-07", "yyyy-MM-dd"))) or 
#     (year = year(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1)) and month = month(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1)) and day = dayofmonth(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1))) or
#     (year = year(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2)) and month = month(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2)) and day = dayofmonth(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2)))
# """).createOrReplaceTempView("raw_scs_%s" % (table_analog_cct))

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    scada_key string,
    timestamp bigint,
    dis_analog float,
    good_analog smallint,
    garbage_analog smallint,
    replaced_analog smallint,
    suspect_analog smallint,
    sctime_analog timestamp,
    substn string,
    device_type string,
    device_name string,
    meas_type string,
    analog_type string,
    quality string
)
PARTITIONED BY (created date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (raw_database, table_analog_cct, bucket, env, project, raw_database, table_analog_cct))

# insert into hive table
spark.sql("""
insert 
into %s.%s 
partition(created)
from raw_scs_%s
select 
    scada_key,
    timestamp,
    dis_analog,
    good_analog,
    garbage_analog,
    replaced_analog,
    suspect_analog,
    to_timestamp(sctime_analog),
    substn,
    device_type,
    device_name,
    meas_type,
    analog_type,
    quality,
    to_date(to_timestamp(sctime_analog)) as created
""" % (raw_database, table_analog_cct, table_analog_cct))


######################### MART ##################################
# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (mart_database, bucket, env, project, mart_database))

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    scada_key string,
    timestamp bigint,
    dis_analog float,
    good_analog smallint,
    garbage_analog smallint,
    replaced_analog smallint,
    suspect_analog smallint,
    sctime_analog timestamp,
    substn string,
    device_type string,
    device_name string,
    meas_type string,
    analog_type string,
    quality string
)
PARTITIONED BY (created date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (mart_database, table_analog_cct, bucket, env, project, mart_database, table_analog_cct))

insertDf_raw_analog_cct = spark.sql("""
select 
    scada_key,
    timestamp,
    dis_analog,
    good_analog,
    garbage_analog,
    replaced_analog,
    suspect_analog,
    sctime_analog,
    substn,
    device_type,
    device_name,
    meas_type,
    analog_type,
    quality,
    created
from %s.%s
where 
    analog_type = 'MW' and
    (created>=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (raw_database, table_analog_cct, firstDay))

insertDf_raw_analog_cct.write.insertInto("%s.%s" % (mart_database, table_analog_cct), overwrite=True)

spark.stop()
