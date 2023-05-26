###########################################################  full_case_dispatchhistory  ###########################################################
# Outline of the data processing logic:
#
#   Summary:
#   - loads case_0 data 
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_case_0/year=*/month=*/day=* 
#       - filter: 
#        (
#           (modified>=to_date(firstDay, "yyyy-MM-dd")) and
#           (modified<=to_date(firstDay, "yyyy-MM-dd"))
#        ) or
#        (modified=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table optimise.case_0
#    
#   - loads dispatchhistory_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_dispatchhistory_0/year=*/month=*/day=*
#       - filter:     
#         cast(substring(instructionsource, 5) as float) > 0 and
#         (
#           (modified>=to_date(firstDay, "yyyy-MM-dd")) and
#           (modified<=to_date(firstDay, "yyyy-MM-dd"))
#         ) or
#        (modified=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table optimise.dispatchhistory_0
#
#   - loads case_joined_0 data
#       - from: table optimise.case_0, optimise.dispatchhistory_0
#       - filter: see source code line 378 -392
#       - to: table optimise.case_joined_0
# 

from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_date, date_sub, dayofmonth, month,
                                   to_date, year)
import util

### Combines:
# * raw_case
# * optimise_case
# * raw_dispatchhistory
# * optimise_dispatchhistory
# * optimise_case_joined


# set top level variables
database_raw = "raw"
database_optimise = "optimise"
table_case = "case_0"
table_dispatchhistory = "dispatchhistory_0"
table_case_joined = "case_joined_0"

# initiate spark session
spark = SparkSession \
    .builder \
    .appName("case_dispatchhistory_joined") \
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

# date range for job
firstDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.firstDay", '2_DAYS_AGO'))      # firstDay="2021-11-05"
lastDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.lastDay", 'TODAY'))             # lastDay="2021-11-07"
logger.info(f'Data date range ("firstDay" -> "lastDay"): "{firstDay}" -> "{lastDay}"')


#######################################  raw_case #########################################################

# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_case))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_case))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_case)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_case)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_case)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_case)

inputDf_raw_case = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_case \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_case))



## LOG num records ##
# setup a basic logger
# import logging
# log4jLogger = spark._jvm.org.apache.log4j 
# logger = log4jLogger.LogManager.getLogger(__name__) 

# countRows = spark.sql(f"select 1 from raw_mkt_%s" % (table_case)).count()
# logger.info(f"Count records in filtered dataframe 'inputDf_raw_case': {countRows}")

# logger.info(f"Count records in filtered dataframe 'inputDf_raw_case': {inputDf_raw_case.count()}")
## END: LOG num records ##



# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    casestate int,
    casename string,
    description string,
    mktday bigint,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_case, bucket, env, project, database_raw, table_case))

# insert into hive table
spark.sql("""
insert 
into %s.%s 
partition(modified)
from raw_mkt_%s
select 
    operation,
    timestamp,
    primarykey,
    caseid,
    casestate,
    casename,
    description,
    mktday,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_case, table_case))

#######################################  optimise_case #########################################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    casestate int,
    casename string,
    description string,
    mktday bigint,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_case, bucket, env, project, database_optimise, table_case))
insertDf_raw_case = spark.sql("""
select 
    operation,
    timestamp,
    primarykey,
    caseid,
    casestate,
    casename,
    description,
    mktday,
    createddate,
    modifieddate,
    modified
from raw.%s 
where
    (
        (modified>=to_date("%s", "yyyy-MM-dd")) and
        (modified<=to_date("%s", "yyyy-MM-dd"))
    ) or
    (modified=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (table_case, firstDay, lastDay, firstDay))
insertDf_raw_case.write.insertInto("%s.%s" % (database_optimise, table_case), overwrite=True)


#######################################  raw_dispatchhistory #########################################################

# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_dispatchhistory))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_dispatchhistory))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_dispatchhistory)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_dispatchhistory)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_dispatchhistory)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_dispatchhistory)

inputDf_raw_dispatchhistory = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_dispatchhistory \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_dispatchhistory))


# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    dispatchhistoryid string,
    dispatchid string,
    dispatchtype string,
    dispatchtime bigint,
    instructionsource string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_dispatchhistory, bucket, env, project, database_raw, table_dispatchhistory))

# insert into hive table
spark.sql("""
insert 
into %s.%s 
partition(modified)
from raw_mkt_%s
select 
    operation,
    timestamp,
    primarykey,
    dispatchhistoryid,
    dispatchid,
    dispatchtype,
    dispatchtime,
    instructionsource,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_dispatchhistory, table_dispatchhistory))

#######################################  optimise_dispatchhistory #########################################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    dispatchhistoryid string,
    dispatchid string,
    dispatchtype string,
    dispatchtime bigint,
    instructionsource string,
    caseid string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_dispatchhistory, bucket, env, project, database_optimise, table_dispatchhistory))
insertDf_raw_dispatchhistory = spark.sql("""
select 
    operation,
    timestamp,
    primarykey,
    dispatchhistoryid,
    dispatchid,
    dispatchtype,
    dispatchtime,
    instructionsource,
    substring(instructionsource, 5),
    createddate,
    modifieddate,
    modified
from raw.%s  
where
    cast(substring(instructionsource, 5) as float) > 0 and
    (
        (modified>=to_date("%s", "yyyy-MM-dd")) and
        (modified<=to_date("%s", "yyyy-MM-dd"))
    ) or
    (modified=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (table_dispatchhistory, firstDay, lastDay, firstDay))
insertDf_raw_dispatchhistory.write.insertInto("%s.%s" % (database_optimise, table_dispatchhistory), overwrite=True)

#######################################  optimise_case_joined #########################################################


# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_optimise, table_case_joined))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    primarykey string,
    caseid string,
    casestate int,
    casename string,
    description string,
    mktday bigint,
    createddate bigint,
    modifieddate bigint,
    modified date,
    instructionsource string
)
PARTITIONED BY (created date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_case_joined, bucket, env, project, database_optimise, table_case_joined))

# give Spark broadcast hint for join - assumption is 'dispatchhistory_0' is smaller table than optimise.case_0
spark.sql("""
insert 
into %s.%s 
partition(created)
select  /*+ BROADCAST(b) */
    a.primarykey,
    a.caseid,
    a.casestate,
    a.casename,
    a.description,
    a.mktday,
    a.createddate,
    a.modifieddate,
    a.modified,
    b.instructionsource,
    to_date(from_unixtime(cast(round(a.createddate/1000) as bigint))) as created
from 
    (
        select *
        from optimise.case_0
        where
        operation != 'DELETE' and
        primarykey || modifieddate in (select primarykey || modifieddate from (select primarykey, createddate, max(modifieddate) as modifieddate from optimise.case_0 group by primarykey, createddate) as a)
    ) as a
    left join (
            select distinct caseid, instructionsource
            from optimise.dispatchhistory_0
            where
            operation != 'DELETE' and
            primarykey || modifieddate in (select primarykey || modifieddate from (select primarykey, createddate, max(modifieddate) as modifieddate from optimise.dispatchhistory_0 group by primarykey, createddate) as a)
        ) as b
        on a.caseid = b.caseid
""" % (database_optimise, table_case_joined))


spark.stop()
