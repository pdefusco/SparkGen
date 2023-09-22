from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()

    from sparkmeasure import TaskMetrics
    taskmetrics = TaskMetrics(spark)
    taskmetrics.runandmeasure(globals(), 'spark.sql("select count(*) from range(1000) cross join range(1000) cross join range(1000)").show()')
    spark.stop()
