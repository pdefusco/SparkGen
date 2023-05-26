
###########################################################  full_contingencyviol_0  ###########################################################
# Outline of the data processing logic:
#
#   Summary:
#   - loads contingencyviol_0 data 
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_scs_contingencyviol_0/year=*/month=*/day=* 
#       - filter:  (created>=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table mart.contingencyviol_0
#     

from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_date, date_sub, dayofmonth,
                                   from_unixtime, month, to_date, year)
import util

##
# combines these two files:
#   * raw/raw_contingencyviol_0.py
#   * mart/mart_contingencyviol_0.py


# set top level variables
database_raw = "raw"
database_mart = "mart"
table_contingencyviol = "contingencyviol_0"

# initiate spark session
spark = SparkSession \
    .builder \
    .appName(table_contingencyviol) \
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
spark.conf.set(
    "spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")


########### RAW ##############


# create temp table
spark.catalog.dropTempView("%s_scs_%s" % (database_raw, table_contingencyviol))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_contingencyviol))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s" % (
    env, database_raw, table_contingencyviol)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s/year=*/month=*/day=*" % (
    env, database_raw, table_contingencyviol)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s" % (
        "dev", database_raw, table_contingencyviol)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_scs_%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_contingencyviol)

inputDf_raw_contingencyviol = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

# date range for job
firstDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.firstDay", '2_DAYS_AGO'))      # firstDay="2021-11-05"
lastDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.lastDay", 'TODAY'))             # lastDay="2021-11-07"
logger.info(f'Data date range ("firstDay" -> "lastDay"): "{firstDay}" -> "{lastDay}"')


inputDf_raw_contingencyviol \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_scs_%s" % (table_contingencyviol))

## LOG num records ##
# setup a basic logger
# import logging
# log4jLogger = spark._jvm.org.apache.log4j 
# logger = log4jLogger.LogManager.getLogger(__name__) 

# countRows = spark.sql(f"select 1 from raw_scs_%s" % (table_contingencyviol)).count()
# logger.info(f"Count records in filtered dataframe 'inputDf_raw_contingencyviol': {countRows}")

# logger.info(f"Count records in filtered dataframe 'inputDf_raw_contingencyviol': {inputDf_raw_contingencyviol.count()}")
## END: LOG num records ##



# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    scada_key string,
    timestamp bigint,
    fullpf_ctg int,
    perclim2_ctvl float,
    monelm_ctvl string,
    perclim1_ctvl float,
    unsolv_cndme string,
    limit2_ctvl float,
    active_ctg int,
    wrsval_ctvl float,
    violtyp_ctvl string,
    prectg_ctvl float,
    limit3_ctvl float,
    preras_ctvl int,
    withrap_ctg int,
    prerap_ctvl int,
    withras_ctg int,
    id_ctg string,
    timin_ctvl timestamp,
    unmngalm_ctg int,
    wrspct_ctvl float,
    harmful_ctg int,
    unsolv_ctg int,
    curval_ctvl float,
    partsolv_ctg int,
    eqplim_ctvl int,
    limit1_ctvl float,
    tmanage_ctvl timestamp,
    potharm_ctg int,
    perclim3_ctvl float,
    postcp_ctvl float,
    local_ctg int,
    timwrst_ctvl timestamp,
    widespr_ctg int
)
PARTITIONED BY (created date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_contingencyviol, bucket, env, project, database_raw, table_contingencyviol))

# insert into hive table
spark.sql("""
insert
into %s.%s
partition(created)
from raw_scs_%s
select
    scada_key,
    timestamp,
    fullpf_ctg,
    perclim2_ctvl,
    monelm_ctvl,
    perclim1_ctvl,
    unsolv_cndme,
    limit2_ctvl,
    active_ctg,
    wrsval_ctvl,
    violtyp_ctvl,
    prectg_ctvl,
    limit3_ctvl,
    preras_ctvl,
    withrap_ctg,
    prerap_ctvl,
    withras_ctg,
    id_ctg,
    timin_ctvl,
    unmngalm_ctg,
    wrspct_ctvl,
    harmful_ctg,
    unsolv_ctg,
    curval_ctvl,
    partsolv_ctg,
    eqplim_ctvl,
    limit1_ctvl,
    tmanage_ctvl,
    potharm_ctg,
    perclim3_ctvl,
    postcp_ctvl,
    local_ctg,
    timwrst_ctvl,
    widespr_ctg,
    to_date(from_unixtime(cast(round(timestamp/1000) as int))) as created
""" % (database_raw, table_contingencyviol, table_contingencyviol))


########### MART #############

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    scada_key string,
    timestamp bigint,
    fullpf_ctg int,
    perclim2_ctvl float,
    monelm_ctvl string,
    perclim1_ctvl float,
    unsolv_cndme string,
    limit2_ctvl float,
    active_ctg int,
    wrsval_ctvl float,
    violtyp_ctvl string,
    prectg_ctvl float,
    limit3_ctvl float,
    preras_ctvl int,
    withrap_ctg int,
    prerap_ctvl int,
    withras_ctg int,
    id_ctg string,
    timin_ctvl timestamp,
    unmngalm_ctg int,
    wrspct_ctvl float,
    harmful_ctg int,
    unsolv_ctg int,
    curval_ctvl float,
    partsolv_ctg int,
    eqplim_ctvl int,
    limit1_ctvl float,
    tmanage_ctvl timestamp,
    potharm_ctg int,
    perclim3_ctvl float,
    postcp_ctvl float,
    local_ctg int,
    timwrst_ctvl timestamp,
    widespr_ctg int
)
PARTITIONED BY (created date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_contingencyviol, bucket, env, project, database_mart, table_contingencyviol))
insertDf_raw_contingencyviol = spark.sql("""
select 
    a.scada_key,
    a.timestamp,
    a.fullpf_ctg,
    a.perclim2_ctvl,
    a.monelm_ctvl,
    a.perclim1_ctvl,
    a.unsolv_cndme,
    a.limit2_ctvl,
    a.active_ctg,
    a.wrsval_ctvl,
    a.violtyp_ctvl,
    a.prectg_ctvl,
    a.limit3_ctvl,
    a.preras_ctvl,
    a.withrap_ctg,
    a.prerap_ctvl,
    a.withras_ctg,
    a.id_ctg,
    a.timin_ctvl,
    a.unmngalm_ctg,
    a.wrspct_ctvl,
    a.harmful_ctg,
    a.unsolv_ctg,
    curval_ctvl,
    a.partsolv_ctg,
    a.eqplim_ctvl,
    a.limit1_ctvl,
    a.tmanage_ctvl,
    a.potharm_ctg,
    a.perclim3_ctvl,
    a.postcp_ctvl,
    a.local_ctg,
    a.timwrst_ctvl,
    a.widespr_ctg,
    a.created
from %s.%s as a
where 
    (created>=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (database_raw, table_contingencyviol, firstDay))
insertDf_raw_contingencyviol.write.insertInto("%s.%s" % (database_mart, table_contingencyviol), overwrite=True)

spark.stop()
