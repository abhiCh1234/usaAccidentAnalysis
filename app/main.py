from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from dependencies import utils
from analysis import accidents_analysis as acc


if __name__ == '__main__':
    
    # Get Config from variable file
    config_path = "config.yaml"
    yaml_config = utils.read_from_yaml(config_path)
    
    # Unizip/Update Base Data
    utils.extract_data_from_zip(yaml_config)
    
    # Initialize SparkSession and create dataframes
    analysis_instance = acc.AccidentDataAnalysis(yaml_config)
    
    # Fetch/Save results from analysis
    analysis_instance.getResult()
    
    # Terminate SparkSession
    analysis_instance.terminate_spark_session()
