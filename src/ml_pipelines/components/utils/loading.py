import os
import pathlib
import dotenv
import yaml
import pprint
from typing import Dict, Any


def load_and_set_env_vars(env: str) -> Dict[str, Any]:
    """
    Utility function to use in Databricks notebooks to load .env files and set them via os
    Return a dict of set environment variables

    Parameters
    ----------
    env : str
        Name of deployment environment. One of

    Returns
    -------
    Dictionary of set environment variables
    """
    env_vars_path = os.path.join('envs', env, f'.{env}.env')
    dotenv.load_dotenv(env_vars_path)


    os_dict = dict(os.environ)
    pprint.pprint(os_dict)

    return os_dict


def load_config(pipeline_name) -> Dict[str, Any]:
    """
    Utility function to use in Databricks notebooks to load the config yaml file for a given pipeline
    Return dict of specified config params

    Parameters
    ----------
    pipeline_name :  str
        Name of pipeline

    Returns
    -------
    Dictionary of config params
    """
    config_path = os.path.join('cfg', f'{pipeline_name}.yaml')
    pipeline_config = yaml.safe_load(pathlib.Path(config_path).read_text())
    pprint.pprint(pipeline_config)

    return pipeline_config