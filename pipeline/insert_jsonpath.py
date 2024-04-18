import os
import sys
import json

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from utilities.connect_to_s3 import insert_into_s3

def iter_jsonpath_files() -> None:
    """Iterate jsonpath files to insert into S3.
    Returns:
        None
    """
    directory_path = 'C:/Users/crist/Ubuntu20/data_lakehouse_with_redshift/json_path/'
    jsonpath_files_names = ['business_jsonpath', 'review_jsonpath',
                             'tip_jsonpath', 'user_jsonpath']
    for file_name in jsonpath_files_names:
        file_path = f'{directory_path}{file_name}.json'
        with open(file_path, 'r', encoding='utf-8') as open_file:
            jsonpath = json.load(open_file)
            json_data = json.dumps(jsonpath)
            insert_into_s3(json_data, file_name, 'yelp_jsonpath_files')
    return None

def main():
    iter_jsonpath_files()

if __name__ == "__main__":
    main()
