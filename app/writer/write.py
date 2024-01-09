from pyspark.sql import DataFrame, SparkSession

def write_to_parquet(dataframe: DataFrame, result_path:str)-> None:
    """
    Save result in Parquet
    :param dataframe: dataframe
    :param result_path: resultant file path
    :return: None
    """
    dataframe.write.mode("overwrite") \
        .option("header", "true")\
        .parquet(result_path)
