from pyspark.sql import SparkSession

def get_spark_session(appName = "BCG-Accident-Analysis"):
    """
    Initialize and return spark instance
    :return: spark instance
    """
    spark = SparkSession \
        .builder \
        .appName(appName) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def stop_spark(spark):
    """
    Terminatee the spark instance
    :return: Null
    """
    spark.stop()