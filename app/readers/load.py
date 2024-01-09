from pyspark.sql import DataFrame, SparkSession

def load_csv(spark: SparkSession, file_path: str)-> DataFrame:
    """
    Read csv files
    :param spark: sparkSession instance
    :param file_path: csv file path
    :return: dataframe
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)
