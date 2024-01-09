import yaml
from zipfile import ZipFile 


def read_from_yaml(config_path):
    """
    Read YAML file
    :param config_path: path to config.yaml
    :return: dictionary with config details
    """
    with open(config_path, "r") as file:
        return yaml.safe_load(file)
    
def extract_data_from_zip(yaml_config)-> None:
    """
    Extract all csv file from Data.zip
    :param file_path: file path to config.yaml
    :return: None
    """
    with ZipFile(yaml_config.get("DATA_ZIP_PATH"), 'r') as f:
        f.extractall('..')
    