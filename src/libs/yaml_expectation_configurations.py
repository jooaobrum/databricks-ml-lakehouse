import yaml
from great_expectations.core import ExpectationConfiguration


def get_yaml_expectation_configurations(yaml_file_location: str):
    with open(yaml_file_location, 'r') as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)

    list_of_expectations = yaml_dict['expectations']

    try:
        for line_item in list_of_expectations:
            ExpectationConfiguration(**line_item)
    except ValueError as e:
        print(f"Error validating expectation suite: {str(e)}")
        raise e

    return list_of_expectations