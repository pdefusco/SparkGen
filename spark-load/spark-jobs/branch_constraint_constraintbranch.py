###########################################################  full_branch_constraint_constraintbranch  ###########################################################
# Outline of the data processing logic:
#
#   Summary:
#   - loads branch_0 data 
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_branch_0/year=*/month=*/day=* 
#       - filter: where (modified>=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table optimise.branch_0
#    
#   - loads constraint_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_constraint_0/year=*/month=*/day=*
#       - filter: where (modified>=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table optimise.constraint_0
#
#   - loads constraintbranch_0 data
#       - from: s3a://tp-cdp-datalate-landing-dev/raw/raw_mkt_mkt_constraintbranch_0/year=*/month=*/day=*
#       - filter: where
#         (
#            (modified>=to_date(firstDay, "yyyy-MM-dd")) and
#            (modified<=to_date(firstDay, "yyyy-MM-dd"))
#         ) or
#            (modified=date_sub(to_date(firstDay, "yyyy-MM-dd"), 1))
#       - to: table optimise.constraintbranch
#       - from table optimise.constraintbranch_0, optimise.branch_0 and optimise.constraint_0
#       - filter: see source code line 665 - 691
#       - to: mart.constraintbranch
# 

from pyspark.sql import SparkSession
from pyspark.sql.functions import (current_date, date_sub, dayofmonth, month,
                                   to_date, year)
import util                                   

##
# combines these files:
# * raw_branch_0.py
# * optimise_branch_0.py
# * raw_constraint_0.py
# * optimise_constraint_0.py
# * raw_constraintbranch_0.py
# * optimise_constraintbranch_0.py
# * mart_constraintbranch_0.py


# set top level variables
database_raw = "raw"
database_optimise = "optimise"
database_mart = "mart"
table_branch = "branch_0"
table_constraint = "constraint_0"
table_constraintbranch = "constraintbranch_0"


# initiate spark session
spark = SparkSession \
    .builder \
    .appName("full_branch_constraint_constraintbranch") \
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


###########################################################  raw_branch_0  ###########################################################
# drop temp view
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_branch))

# create raw database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_branch))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_branch)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_branch)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_branch)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_branch)

inputDf_raw_branch = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

# date range for job
firstDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.firstDay", '2_DAYS_AGO'))      # firstDay="2021-11-05"
lastDay=util.interpret_input_date_arg(spark.conf.get("spark.driver.lastDay", 'TODAY'))             # lastDay="2021-11-07"
logger.info(f'Data date range ("firstDay" -> "lastDay"): "{firstDay}" -> "{lastDay}"')

inputDf_raw_branch \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_branch))

## LOG num records ##
# setup a basic logger
# import logging
# log4jLogger = spark._jvm.org.apache.log4j 
# logger = log4jLogger.LogManager.getLogger(__name__) 

# countRows = spark.sql(f"select 1 from raw_mkt_%s" % (table_branch)).count()
# logger.info(f"Count records in filtered dataframe 'inputDf_raw_branch': {countRows}")
# logger.info(f"Count records in filtered dataframe 'inputDf_raw_branch': {inputDf_raw_branch.count()}")
## END: LOG num records ##

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    branchid string,
    branchname string,
    branchtype string,
    currentnearopen int,
    currentneardead int,
    currentfaropen int,
    currentfardead int,
    currentnearmw float,
    currentfarmw float,
    currentnearflow float,
    currentfarflow float,
    currentlimit float,
    threewindingtype int,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_branch, bucket, env, project, database_raw, table_branch))

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
    branchid,
    branchname,
    branchtype,
    currentnearopen,
    currentneardead,
    currentfaropen,
    currentfardead,
    currentnearmw,
    currentfarmw,
    currentnearflow,
    currentfarflow,
    currentlimit,
    threewindingtype,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_branch, table_branch))


###########################################################  optimise_branch_0  ###########################################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    branchid string,
    branchname string,
    branchtype string,
    currentnearopen int,
    currentneardead int,
    currentfaropen int,
    currentfardead int,
    currentnearmw float,
    currentfarmw float,
    currentnearflow float,
    currentfarflow float,
    currentlimit float,
    threewindingtype int,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_branch, bucket, env, project, database_optimise, table_branch))

insertDf_raw_branch = spark.sql("""
select 
    operation,
    timestamp,
    primarykey,
    branchid,
    branchname,
    branchtype,
    currentnearopen,
    currentneardead,
    currentfaropen,
    currentfardead,
    currentnearmw,
    currentfarmw,
    currentnearflow,
    currentfarflow,
    currentlimit,
    threewindingtype,
    createddate,
    modifieddate,
    modified
from %s.%s 
where
    (modified>=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (database_raw, table_branch, firstDay))

insertDf_raw_branch.write.insertInto("%s.%s" % (database_optimise, table_branch), overwrite=True)


###########################################################  raw_constraint_0           ###########################################################

# create temp table
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_constraint))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_constraint))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_constraint)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_constraint)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_constraint)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_constraint)

inputDf_raw_constraint = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)

inputDf_raw_constraint \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_constraint))

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    constraintid string,
    name string,
    hvdc int,
    islandid int,
    active int,
    description string,
    bonafide int,
    permanent string,
    customer_description string,
    mixedconstraint int,
    conditioncheck_branchid string,
    binaryconstraint_branchid string,
    upperlimit float,
    lowerlimit float,
    temperature float,
    dispatchid bigint,
    constraintclass string,
    constrainttype string,
    discretion int,
    limittype string,
    source string,
    locationbranch string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_constraint, bucket, env, project, database_raw, table_constraint))

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
    constraintid,
    name,
    hvdc,
    islandid,
    active,
    description,
    bonafide,
    permanent,
    customer_description,
    mixedconstraint,
    conditioncheck_branchid,
    binaryconstraint_branchid,
    upperlimit,
    lowerlimit,
    temperature,
    dispatchid,
    constraintclass,
    constrainttype,
    discretion,
    limittype,
    source,
    locationbranch,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_constraint, table_constraint))


###########################################################  optimise_constraint_0      ###########################################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    constraintid string,
    name string,
    hvdc int,
    islandid int,
    active int,
    description string,
    bonafide int,
    permanent string,
    customer_description string,
    mixedconstraint int,
    conditioncheck_branchid string,
    binaryconstraint_branchid string,
    upperlimit float,
    lowerlimit float,
    temperature float,
    dispatchid bigint,
    constraintclass string,
    constrainttype string,
    discretion int,
    limittype string,
    source string,
    locationbranch string,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_constraint, bucket, env, project, database_optimise, table_constraint))

insertDf_raw_constraint = spark.sql("""
select 
    operation,
    timestamp,
    primarykey,
    constraintid,
    name,
    hvdc,
    islandid,
    active,
    description,
    bonafide,
    permanent,
    customer_description,
    mixedconstraint,
    conditioncheck_branchid,
    binaryconstraint_branchid,
    upperlimit,
    lowerlimit,
    temperature,
    dispatchid,
    constraintclass,
    constrainttype,
    discretion,
    limittype,
    source,
    locationbranch,
    createddate,
    modifieddate,
    modified
from %s.%s 
where
    (modified>=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (database_raw, table_constraint, firstDay))
insertDf_raw_constraint.write.insertInto("%s.%s" % (database_optimise, table_constraint), overwrite=True)

###########################################################  raw_constraintbranch_0     ###########################################################

# drop temp view
spark.catalog.dropTempView("%s_mkt_%s" % (database_raw, table_constraintbranch))

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_raw, bucket, env, project, database_raw))

# drop table
spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_raw, table_constraintbranch))

# read last 3 day's of partitions from landing parquet files
basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
    env, database_raw, table_constraintbranch)
inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
    env, database_raw, table_constraintbranch)

# temporary: setup test environment but sharing the same kinesis source as dev
share_dev_source = spark.conf.get("spark.driver.share.dev.source", "false")
if share_dev_source and share_dev_source == "true":
    basePath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s" % (
        "dev", database_raw, table_constraintbranch)
    inputPath = "s3a://tp-cdp-datalake-landing-%s/%s/raw_mkt_mkt%s/year=*/month=*/day=*" % (
        "dev", database_raw, table_constraintbranch)

inputDf_raw_constraintbranch = spark.read.options(
    mergeSchema="true", basePath=basePath, recursiveFileLookup=True, pathGlobFilter="*.parquet").parquet(inputPath)
# inputDf_raw_constraintbranch.filter("""
#     (year = year(to_date("2021-11-07", "yyyy-MM-dd")) and month = month(to_date("2021-11-07", "yyyy-MM-dd")) and day = dayofmonth(to_date("2021-11-07", "yyyy-MM-dd"))) or 
#     (year = year(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1)) and month = month(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1)) and day = dayofmonth(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 1))) or
#     (year = year(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2)) and month = month(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2)) and day = dayofmonth(date_sub(to_date("2021-11-07", "yyyy-MM-dd"), 2)))
# """).createOrReplaceTempView("raw_mkt_%s" % (table_constraintbranch))

#### START : Temporary to load historic data
inputDf_raw_constraintbranch \
    .filter(util.create_spark_date_filter(firstDay, lastDay)) \
    .createOrReplaceTempView("raw_mkt_%s" % (table_constraintbranch))
#### END : Temporary to load historic data

# create hive table with location
spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    constraintid string,
    branchid string,
    factor float,
    fixedlossfactor float,
    variablelossfactor float,
    sortorder int,
    pviskey bigint,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_raw, table_constraintbranch, bucket, env, project, database_raw, table_constraintbranch))

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
    constraintid,
    branchid,
    factor,
    fixedlossfactor,
    variablelossfactor,
    sortorder,
    pviskey,
    createddate,
    modifieddate,
    to_date(from_unixtime(cast(round(modifieddate/1000) as bigint))) as modified
""" % (database_raw, table_constraintbranch, table_constraintbranch))

###########################################################  optimise_constraintbranch_0  #########################################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_optimise, bucket, env, project, database_optimise))

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    operation string,
    timestamp bigint,
    primarykey string,
    constraintid string,
    branchid string,
    factor float,
    fixedlossfactor float,
    variablelossfactor float,
    sortorder int,
    pviskey bigint,
    createddate bigint,
    modifieddate bigint
)
PARTITIONED BY (modified date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_optimise, table_constraintbranch, bucket, env, project, database_optimise, table_constraintbranch))

insertDf_raw_constraintbranch = spark.sql("""
select 
    operation,
    timestamp,
    primarykey,
    constraintid,
    branchid,
    factor,
    fixedlossfactor,
    variablelossfactor,
    sortorder,
    pviskey,
    createddate,
    modifieddate,
    modified
from %s.%s 
where
    (
        (modified>=to_date("%s", "yyyy-MM-dd")) and
        (modified<=to_date("%s", "yyyy-MM-dd"))
    ) or
    (modified=date_sub(to_date("%s", "yyyy-MM-dd"), 1))
""" % (database_raw, table_constraintbranch, firstDay, lastDay, firstDay))
insertDf_raw_constraintbranch.write.insertInto("%s.%s" % (database_optimise, table_constraintbranch), overwrite=True)


###########################################################  mart_constraintbranch_0    ###########################################################

# create database with location
spark.sql(
    "CREATE DATABASE IF NOT EXISTS %s LOCATION '%s/%s/%s/%s'" % (database_mart, bucket, env, project, database_mart))

spark.sql("DROP TABLE IF EXISTS %s.%s" % (database_mart, table_constraintbranch))

spark.sql("""
CREATE EXTERNAL TABLE IF NOT EXISTS %s.%s (
    primarykey string,
    constraintid string,
    branchid string,
    factor float,
    fixedlossfactor float,
    variablelossfactor float,
    sortorder int,
    pviskey bigint,
    createddate bigint,
    modifieddate bigint,
    modified date,
    branchname string,
    branchtype string,
    currentnearopen int,
    currentneardead int,
    currentfaropen int,
    currentfardead int,
    currentnearmw float,
    currentfarmw float,
    currentnearflow float,
    currentfarflow float,
    currentlimit float,
    threewindingtype int,
    name string,
    hvdc int,
    islandid int,
    active int,
    description string,
    bonafide int,
    permanent string,
    customer_description string,
    mixedconstraint int,
    conditioncheck_branchid string,
    binaryconstraint_branchid string,
    upperlimit float,
    lowerlimit float,
    temperature float,
    dispatchid bigint,
    constraintclass string,
    constrainttype string,
    discretion int,
    limittype string,
    source string,
    locationbranch string
)
PARTITIONED BY (created date)
STORED AS PARQUET LOCATION '%s/%s/%s/%s/%s'
""" % (database_mart, table_constraintbranch, bucket, env, project, database_mart, table_constraintbranch))

insertDf_optimise_constraintbranch_branch_constraint = spark.sql("""
select 
    a.primarykey,
    a.constraintid,
    a.branchid,
    a.factor,
    a.fixedlossfactor,
    a.variablelossfactor,
    a.sortorder,
    a.pviskey,
    a.createddate,
    a.modifieddate,
    a.modified,
    b.branchname,
    b.branchtype,
    b.currentnearopen,
    b.currentneardead,
    b.currentfaropen,
    b.currentfardead,
    b.currentnearmw,
    b.currentfarmw,
    b.currentnearflow,
    b.currentfarflow,
    b.currentlimit,
    b.threewindingtype,
    c.name,
    c.hvdc,
    c.islandid,
    c.active,
    c.description,
    c.bonafide,
    c.permanent,
    c.customer_description,
    c.mixedconstraint,
    c.conditioncheck_branchid,
    c.binaryconstraint_branchid,
    c.upperlimit,
    c.lowerlimit,
    c.temperature,
    c.dispatchid,
    c.constraintclass,
    c.constrainttype,
    c.discretion,
    c.limittype,
    c.source,
    c.locationbranch,
    to_date(from_unixtime(cast(round(a.createddate/1000) as bigint))) as created
from 
    (
        select *
        from optimise.constraintbranch_0
        where
        operation != 'DELETE' and
        primarykey || modifieddate in 
            (select primarykey || modifieddate from (select primarykey, createddate, max(modifieddate) as modifieddate from optimise.constraintbranch_0 group by primarykey, createddate)
        as a)
    ) as a
    left join (
            select * 
            from optimise.branch_0
            where
            operation != 'DELETE' and
            primarykey || modifieddate in 
                (select primarykey || modifieddate from (select primarykey, createddate, max(modifieddate) as modifieddate from optimise.branch_0 group by primarykey, createddate) as a)
        ) as b
        on a.branchid = b.branchid
    left join (
            select *
            from optimise.constraint_0
            where
            operation != 'DELETE' and
            primarykey || modifieddate in 
                (select primarykey || modifieddate from (select primarykey, createddate, max(modifieddate) as modifieddate from optimise.constraint_0 group by primarykey, createddate) as a)
        ) as c
        on a.constraintid = c.constraintid
""")
insertDf_optimise_constraintbranch_branch_constraint.write.insertInto("%s.%s" % (database_mart, table_constraintbranch), overwrite=True)


spark.stop()
