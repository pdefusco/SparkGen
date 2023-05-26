##############################full_planbranch_nrsl_planbranch_nrss_planconstraint_nrsl_planconstraint_nrss_planconstraint_rtd##########################################
# Outline of the data processing logic:
#
#   Summary:
#   - loads planbranch_nrsl_0 data 
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_planbranch_nrsl_0/year=*/month=*/day=* 
#       - filter: 
#            from raw.planbranch_nrss_0 as a
#            left join optimise_case_joined_0 as b
#                on a.caseid = b.caseid
#            where 
#                a.modified >= date_sub(to_date(firstDay, "yyyy-MM-dd"), 1)
#       - to: table mart.planbranch_nrsl_0
#    
#   - loads planbranch_nrss_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_planbranch_nrss_0/year=*/month=*/day=*
#       - filter: 
#            from raw.planbranch_nrss_0 as a
#            left join optimise_case_joined_0 as b
#                on a.caseid = b.caseid
#            where 
#                a.modified >= date_sub(to_date(firstDay, "yyyy-MM-dd"), 1)
#       - to: table mart.planbranch_nrss_0
#
#   - loads planconstraint_nrsl_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_planconstraint_nrsl_0/year=*/month=*/day=*
#       - filter: 
#            from raw.planconstraint_nrsl_0 as a
#            left join optimise_case_joined_0 as b
#                on a.caseid = b.caseid
#            where 
#                a.modified >= date_sub(to_date(firstDay, "yyyy-MM-dd"), 1)
#       - to: mart.planconstraint_nrsl_0 
#
#   - loads planconstraint_nrss_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_planconstraint_nrss_0/year=*/month=*/day=*
#       - filter: 
#            from raw.planconstraint_nrss_0 as a
#            left join optimise_case_joined_0 as b
#                on a.caseid = b.caseid
#            where 
#                a.modified >= date_sub(to_date("%s", "yyyy-MM-dd"), 1)
#       - to: mart.planconstraint_nrss_0 
#
#   - loads planconstraint_rtd_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_planconstraint_rtd_0/year=*/month=*/day=*
#       - filter: 
#            from raw.planconstraint_rtd_0 as a
#            left join optimise_case_joined_0 as b
#                on a.caseid = b.caseid
#            where 
#                a.modified >= date_sub(to_date(firstDay, "yyyy-MM-dd"), 1)
#       - to: mart.planconstraint_rtd_0 

from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_date, date_sub, dayofmonth, month,
                                   to_date, year)

import util

## Combines:
# * raw_planbranch_nrsl_0
# * mart_planbranch_nrsl_0
# * raw_planbranch_nrss_0
# * mart_planbranch_nrss_0
# * raw_planconstraint_nrsl_0
# * mart_planconstraint_nrsl_0
# * raw_planconstraint_nrss_0
# * mart_planconstraint_nrss_0
# * raw_planconstraint_rtd_0
# * mart_planconstraint_rtd_0

# set top level variables
database_raw = "raw"
database_mart = "mart"
database_optimise="optimise"
table_planbranch_nrsl = "planbranch_nrsl_0"
table_planbranch_nrss = "planbranch_nrss_0"
table_planconstraint_nrsl = "planconstraint_nrsl_0"
table_planconstraint_nrss = "planconstraint_nrss_0"
table_planconstraint_rtd = "planconstraint_rtd_0"

####################################### Setup Spark Session       #######################################
# initiate spark session
spark = SparkSession \
    .builder \
    .appName("full_planbranch_nrsl_planbranch_nrss_planconstraint_nrsl_planconstraint_nrss_planconstraint_rtd") \
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

#### LOAD OPTIMISE_CASE_JOINED ###

# basePath = "'%s/%s/%s/%s/%s'" % (bucket, env, project, "optimise", "case_joined_0")
basePath = f"{bucket}/{env}/{project}/optimise/case_joined_0"
inputPath = f"{basePath}/created=*"

inputDf_optimise_case_joined = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

## TODO: is 3 days correct? looks like joins are using the full 'optimise.case_joined_0' table
# date range for job
firstDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.firstDay", '2_DAYS_AGO'))      # firstDay="2021-11-05"
lastDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.lastDay", 'TODAY'))             # lastDay="2021-11-07"
logger.info(f'Data date range ("firstDay" -> "lastDay"): "{firstDay}" -> "{lastDay}"')


# inputDf_optimise_case_joined.filter("""
#     (created = to_date("2021-11-07", "yyyy-MM-dd")) or 
#     (created = date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1)) or 
#     (created = date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2))
# """) \
#     .cache() \
#     .createOrReplaceTempView("optimise_case_joined_0")

inputDf_optimise_case_joined.filter(f"""
    (created >= date_sub(to_date("{firstDay}", "yyyy-MM-dd"), 2)) and 
    (created <= to_date("{lastDay}", "yyyy-MM-dd"))
""") \
    .cache() \
    .createOrReplaceTempView("optimise_case_joined_0")


## LOG num records ##
# setup a basic logger
# import logging
# log4jLogger = spark._jvm.org.apache.log4j 
# logger = log4jLogger.LogManager.getLogger(__name__) 

# countRows = spark.sql(f"select 1 from optimise_case_joined_0").count()
# logger.info(f"Count records in filtered dataframe 'inputDf_optimise_case_joined': {countRows}")

# logger.info(f"Count records in filtered dataframe 'inputDf_optimise_case_joined': {inputDf_optimise_case_joined.count()}")
## END: LOG num records ##



####################################### raw_planbranch_nrsl_0       #######################################
# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_planbranch_nrsl))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_planbranch_nrsl))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_planbranch_nrsl)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_planbranch_nrsl)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_planbranch_nrsl)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_planbranch_nrsl)

inputDf_raw_table_planbranch_nrsl = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_table_planbranch_nrsl \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_planbranch_nrsl))

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    branchname string,
    fromid_bus bigint,
    toid_bus bigint,
    fromstid string,
    tostid string,
    fromkvid int,
    tokvid int,
    fixedloss string,
    frommw float,
    tomw float,
    maxmw float,
    marginalprice float,
    branchlosses float,
    nonphysicalloss float,
    disconnected int,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_planbranch_nrsl, bucket, env, project, database_raw, table_planbranch_nrsl))

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
    mkttime,
    branchname,
    fromid_bus,
    toid_bus,
    fromstid,
    tostid,
    fromkvid,
    tokvid,
    fixedloss,
    frommw,
    tomw,
    maxmw,
    marginalprice,
    branchlosses,
    nonphysicalloss,
    disconnected,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_planbranch_nrsl, table_planbranch_nrsl))


####################################### mart_planbranch_nrsl_0      #######################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    branchname string,
    fromid_bus bigint,
    toid_bus bigint,
    fromstid string,
    tostid string,
    fromkvid int,
    tokvid int,
    fixedloss string,
    frommw float,
    tomw float,
    maxmw float,
    marginalprice float,
    branchlosses float,
    nonphysicalloss float,
    disconnected int,
    createddate bigint,
    modifieddate bigint,
    casestate int
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_planbranch_nrsl, bucket, env, project, database_mart, table_planbranch_nrsl))
insertDf_raw_planbranch_nrsl = spark.sql("""
select /*+ BROADCAST(b) */
    a.operation,
    a.timestamp,
    a.primarykey,
    a.caseid,
    a.mkttime,
    a.branchname,
    a.fromid_bus,
    a.toid_bus,
    a.fromstid,
    a.tostid,
    a.fromkvid,
    a.tokvid,
    a.fixedloss,
    a.frommw,
    a.tomw,
    a.maxmw,
    a.marginalprice,
    a.branchlosses,
    a.nonphysicalloss,
    a.disconnected,
    a.createddate,
    a.modifieddate,
    b.casestate,
    a.modified
from raw.%s as a
    left join optimise_case_joined_0 as b
        on a.caseid = b.caseid
where 
    a.modified >= date_sub(to_date("%s", "yyyy-MM-dd"), 1)
""" % (table_planbranch_nrsl, firstDay))
insertDf_raw_planbranch_nrsl.write.insertInto("%s.%s" % (database_mart, table_planbranch_nrsl), overwrite=True)


####################################### raw_planbranch_nrss_0       #######################################
# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_planbranch_nrss))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_planbranch_nrss))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_planbranch_nrss)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_planbranch_nrss)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_planbranch_nrss)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_planbranch_nrss)

inputDf_raw_planbranch_nrss = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_planbranch_nrss \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_planbranch_nrss))

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    branchname string,
    fromid_bus bigint,
    toid_bus bigint,
    fromstid string,
    tostid string,
    fromkvid int,
    tokvid int,
    fixedloss string,
    frommw float,
    tomw float,
    maxmw float,
    marginalprice float,
    branchlosses float,
    nonphysicalloss float,
    disconnected int,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_planbranch_nrss, bucket, env, project, database_raw, table_planbranch_nrss))

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
    mkttime,
    branchname,
    fromid_bus,
    toid_bus,
    fromstid,
    tostid,
    fromkvid,
    tokvid,
    fixedloss,
    frommw,
    tomw,
    maxmw,
    marginalprice,
    branchlosses,
    nonphysicalloss,
    disconnected,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_planbranch_nrss, table_planbranch_nrss))

####################################### mart_planbranch_nrss_0      #######################################
# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    branchname string,
    fromid_bus bigint,
    toid_bus bigint,
    fromstid string,
    tostid string,
    fromkvid int,
    tokvid int,
    fixedloss string,
    frommw float,
    tomw float,
    maxmw float,
    marginalprice float,
    branchlosses float,
    nonphysicalloss float,
    disconnected int,
    createddate bigint,
    modifieddate bigint,
    casestate int
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_planbranch_nrss, bucket, env, project, database_mart, table_planbranch_nrss))
insertDf_raw_planbranch_nrss = spark.sql("""
select /*+ BROADCAST(b) */
    a.operation,
    a.timestamp,
    a.primarykey,
    a.caseid,
    a.mkttime,
    a.branchname,
    a.fromid_bus,
    a.toid_bus,
    a.fromstid,
    a.tostid,
    a.fromkvid,
    a.tokvid,
    a.fixedloss,
    a.frommw,
    a.tomw,
    a.maxmw,
    a.marginalprice,
    a.branchlosses,
    a.nonphysicalloss,
    a.disconnected,
    a.createddate,
    a.modifieddate,
    b.casestate,
    a.modified
from raw.%s as a
    left join optimise_case_joined_0 as b
        on a.caseid = b.caseid
where 
    a.modified >= date_sub(to_date("%s", "yyyy-MM-dd"), 1)
""" % (table_planbranch_nrss, firstDay))
insertDf_raw_planbranch_nrss.write.insertInto("%s.%s" % (database_mart, table_planbranch_nrss), overwrite=True)

####################################### raw_planconstraint_nrsl_0       #######################################
# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_planconstraint_nrsl))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_planconstraint_nrsl))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_planconstraint_nrsl)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_planconstraint_nrsl)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_planconstraint_nrsl)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_planconstraint_nrsl)

inputDf_raw_planconstraint_nrsl = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_planconstraint_nrsl \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_planconstraint_nrsl))

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    constraintname string,
    constrainttype string,
    contingencyname string,
    branchname string,
    watchlist int,
    sftupdatedwl int,
    lowerlimit float,
    upperlimit float,
    lowermarginalprice float,
    uppermarginalprice float,
    conditioncheck int,
    constraintvalue float,
    offloadtime int,
    scndlowerused float,
    scndlowerlimit float,
    scndupperused float,
    scndupperlimit float,
    constraintdeficit float,
    sensitivity float,
    ctgbranchname string,
    mesensitivity float,
    offloadconstraint int,
    scheduletype string,
    casetype string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_planconstraint_nrsl, bucket, env, project, database_raw, table_planconstraint_nrsl))

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
    mkttime,
    constraintname,
    constrainttype,
    contingencyname,
    branchname,
    watchlist,
    sftupdatedwl,
    lowerlimit,
    upperlimit,
    lowermarginalprice,
    uppermarginalprice,
    conditioncheck,
    constraintvalue,
    offloadtime,
    scndlowerused,
    scndlowerlimit,
    scndupperused,
    scndupperlimit,
    constraintdeficit,
    sensitivity,
    ctgbranchname,
    mesensitivity,
    offloadconstraint,
    scheduletype,
    casetype,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_planconstraint_nrsl, table_planconstraint_nrsl))

####################################### mart_planconstraint_nrsl_0      #######################################
# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    constraintname string,
    constrainttype string,
    contingencyname string,
    branchname string,
    watchlist int,
    sftupdatedwl int,
    lowerlimit float,
    upperlimit float,
    lowermarginalprice float,
    uppermarginalprice float,
    conditioncheck int,
    constraintvalue float,
    offloadtime int,
    scndlowerused float,
    scndlowerlimit float,
    scndupperused float,
    scndupperlimit float,
    constraintdeficit float,
    sensitivity float,
    ctgbranchname string,
    mesensitivity float,
    offloadconstraint int,
    scheduletype string,
    casetype string,
    createddate bigint,
    modifieddate bigint,
    casestate string,
    instructionsource string
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_planconstraint_nrsl, bucket, env, project, database_mart, table_planconstraint_nrsl))
insertDf_raw_planconstraint_nrsl = spark.sql("""
select /*+ BROADCAST(b) */
    a.operation,
    a.timestamp,
    a.primarykey,
    a.caseid,
    a.mkttime,
    a.constraintname,
    a.constrainttype,
    a.contingencyname,
    a.branchname,
    a.watchlist,
    a.sftupdatedwl,
    a.lowerlimit,
    a.upperlimit,
    a.lowermarginalprice,
    a.uppermarginalprice,
    a.conditioncheck,
    a.constraintvalue,
    a.offloadtime,
    a.scndlowerused,
    a.scndlowerlimit,
    a.scndupperused,
    a.scndupperlimit,
    a.constraintdeficit,
    a.sensitivity,
    a.ctgbranchname,
    a.mesensitivity,
    a.offloadconstraint,
    a.scheduletype,
    a.casetype,
    a.createddate,
    a.modifieddate,
    b.casestate,
    b.instructionsource,
    a.modified
from raw.%s as a
    left join optimise_case_joined_0 as b
        on a.caseid = b.caseid
where 
    a.modified >= date_sub(to_date("%s", "yyyy-MM-dd"), 1)
""" % (table_planconstraint_nrsl, firstDay))
insertDf_raw_planconstraint_nrsl.write.insertInto("%s.%s" % (database_mart, table_planconstraint_nrsl), overwrite=True)


####################################### raw_planconstraint_nrss_0       #######################################
# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_planconstraint_nrss))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_planconstraint_nrss))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_planconstraint_nrss)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_planconstraint_nrss)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_planconstraint_nrss)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_planconstraint_nrss)

inputDf_raw_planconstraint_nrss = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_planconstraint_nrss \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_planconstraint_nrss))


# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    constraintname string,
    constrainttype string,
    contingencyname string,
    branchname string,
    watchlist int,
    sftupdatedwl int,
    lowerlimit float,
    upperlimit float,
    lowermarginalprice float,
    uppermarginalprice float,
    conditioncheck int,
    constraintvalue float,
    offloadtime int,
    scndlowerused float,
    scndlowerlimit float,
    scndupperused float,
    scndupperlimit float,
    constraintdeficit float,
    sensitivity float,
    ctgbranchname string,
    mesensitivity float,
    offloadconstraint int,
    scheduletype string,
    casetype string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_planconstraint_nrss, bucket, env, project, database_raw, table_planconstraint_nrss))

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
    mkttime,
    constraintname,
    constrainttype,
    contingencyname,
    branchname,
    watchlist,
    sftupdatedwl,
    lowerlimit,
    upperlimit,
    lowermarginalprice,
    uppermarginalprice,
    conditioncheck,
    constraintvalue,
    offloadtime,
    scndlowerused,
    scndlowerlimit,
    scndupperused,
    scndupperlimit,
    constraintdeficit,
    sensitivity,
    ctgbranchname,
    mesensitivity,
    offloadconstraint,
    scheduletype,
    casetype,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_planconstraint_nrss, table_planconstraint_nrss))

####################################### mart_planconstraint_nrss_0      #######################################
# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    constraintname string,
    constrainttype string,
    contingencyname string,
    branchname string,
    watchlist int,
    sftupdatedwl int,
    lowerlimit float,
    upperlimit float,
    lowermarginalprice float,
    uppermarginalprice float,
    conditioncheck int,
    constraintvalue float,
    offloadtime int,
    scndlowerused float,
    scndlowerlimit float,
    scndupperused float,
    scndupperlimit float,
    constraintdeficit float,
    sensitivity float,
    ctgbranchname string,
    mesensitivity float,
    offloadconstraint int,
    scheduletype string,
    casetype string,
    createddate bigint,
    modifieddate bigint,
    casestate string,
    instructionsource string
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_planconstraint_nrss, bucket, env, project, database_mart, table_planconstraint_nrss))
insertDf_raw_planconstraint_nrss = spark.sql("""
select /*+ BROADCAST(b) */
    a.operation,
    a.timestamp,
    a.primarykey,
    a.caseid,
    a.mkttime,
    a.constraintname,
    a.constrainttype,
    a.contingencyname,
    a.branchname,
    a.watchlist,
    a.sftupdatedwl,
    a.lowerlimit,
    a.upperlimit,
    a.lowermarginalprice,
    a.uppermarginalprice,
    a.conditioncheck,
    a.constraintvalue,
    a.offloadtime,
    a.scndlowerused,
    a.scndlowerlimit,
    a.scndupperused,
    a.scndupperlimit,
    a.constraintdeficit,
    a.sensitivity,
    a.ctgbranchname,
    a.mesensitivity,
    a.offloadconstraint,
    a.scheduletype,
    a.casetype,
    a.createddate,
    a.modifieddate,
    b.casestate,
    b.instructionsource,
    a.modified
from raw.%s as a
    left join optimise_case_joined_0 as b
        on a.caseid = b.caseid
where 
    a.modified >= date_sub(to_date("%s", "yyyy-MM-dd"), 1)
""" % (table_planconstraint_nrss, firstDay))
insertDf_raw_planconstraint_nrss.write.insertInto("%s.%s" % (database_mart, table_planconstraint_nrss), overwrite=True)

####################################### raw_planconstraint_rtd_0        #######################################
# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_planconstraint_rtd))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_planconstraint_rtd))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_planconstraint_rtd)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_planconstraint_rtd)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_planconstraint_rtd)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_planconstraint_rtd)

inputDf_raw_planconstraint_rtd = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_planconstraint_rtd \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_planconstraint_rtd))

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    constraintname string,
    constrainttype string,
    contingencyname string,
    branchname string,
    watchlist int,
    sftupdatedwl int,
    lowerlimit float,
    upperlimit float,
    lowermarginalprice float,
    uppermarginalprice float,
    conditioncheck int,
    constraintvalue float,
    offloadtime int,
    scndlowerused float,
    scndlowerlimit float,
    scndupperused float,
    scndupperlimit float,
    constraintdeficit float,
    sensitivity float,
    ctgbranchname string,
    mesensitivity float,
    offloadconstraint int,
    scheduletype string,
    casetype string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_planconstraint_rtd, bucket, env, project, database_raw, table_planconstraint_rtd))

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
    mkttime,
    constraintname,
    constrainttype,
    contingencyname,
    branchname,
    watchlist,
    sftupdatedwl,
    lowerlimit,
    upperlimit,
    lowermarginalprice,
    uppermarginalprice,
    conditioncheck,
    constraintvalue,
    offloadtime,
    scndlowerused,
    scndlowerlimit,
    scndupperused,
    scndupperlimit,
    constraintdeficit,
    sensitivity,
    ctgbranchname,
    mesensitivity,
    offloadconstraint,
    scheduletype,
    casetype,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_planconstraint_rtd, table_planconstraint_rtd))

####################################### mart_planconstraint_rtd_0       #######################################
# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    caseid string,
    mkttime bigint,
    constraintname string,
    constrainttype string,
    contingencyname string,
    branchname string,
    watchlist int,
    sftupdatedwl int,
    lowerlimit float,
    upperlimit float,
    lowermarginalprice float,
    uppermarginalprice float,
    conditioncheck int,
    constraintvalue float,
    offloadtime int,
    scndlowerused float,
    scndlowerlimit float,
    scndupperused float,
    scndupperlimit float,
    constraintdeficit float,
    sensitivity float,
    ctgbranchname string,
    mesensitivity float,
    offloadconstraint int,
    scheduletype string,
    casetype string,
    createddate bigint,
    modifieddate bigint,
    casestate string,
    instructionsource string
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_planconstraint_rtd, bucket, env, project, database_mart, table_planconstraint_rtd))
insertDf_raw_planconstraint_rtd = spark.sql("""
select /*+ BROADCAST(b) */
    a.operation,
    a.timestamp,
    a.primarykey,
    a.caseid,
    a.mkttime,
    a.constraintname,
    a.constrainttype,
    a.contingencyname,
    a.branchname,
    a.watchlist,
    a.sftupdatedwl,
    a.lowerlimit,
    a.upperlimit,
    a.lowermarginalprice,
    a.uppermarginalprice,
    a.conditioncheck,
    a.constraintvalue,
    a.offloadtime,
    a.scndlowerused,
    a.scndlowerlimit,
    a.scndupperused,
    a.scndupperlimit,
    a.constraintdeficit,
    a.sensitivity,
    a.ctgbranchname,
    a.mesensitivity,
    a.offloadconstraint,
    a.scheduletype,
    a.casetype,
    a.createddate,
    a.modifieddate,
    b.casestate,
    b.instructionsource,
    a.modified
from raw.%s as a
    left join optimise_case_joined_0 as b
        on a.caseid = b.caseid
where 
    a.modified >= date_sub(to_date("%s", "yyyy-MM-dd"), 1)
""" % (table_planconstraint_rtd, firstDay))
insertDf_raw_planconstraint_rtd.write.insertInto("%s.%s" % (database_mart, table_planconstraint_rtd), overwrite=True)


####################################### Teardown Spark Session       #######################################
spark.stop()