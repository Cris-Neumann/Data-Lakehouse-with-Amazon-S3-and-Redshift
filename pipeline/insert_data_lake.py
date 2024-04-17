import os
import sys
import json

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from utilities.connect_to_s3 import insert_into_s3

def json_to_dict(json_path:str) -> json:
    """Transform yelp json to matrix json
        and extract 5000 records.
    Args:
        json_path (str): json path.
    Returns:
        data_extract (json): file with yelp data.
    """
    yelp_data = []
    with open(json_path, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                yelp_data.append(json.loads(line))
            except json.decoder.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")
    data_extract = yelp_data[:5000]
    data_extract = '\n'.join([json.dumps(obj) for obj in data_extract])
    return data_extract

def iter_yelp_files() -> None:
    """Iterate yelp files to insert into S3.
    Returns:
        None
    """
    directory_path = r'D:\GIT\yelp_dataset'
    yelp_files_names = ['business', 'checkin', 'review', 'tip', 'user']
    for file_name in yelp_files_names:
        file_path = f'{directory_path}\\yelp_academic_dataset_{file_name}.json'
        yelp_data = json_to_dict(file_path)
        insert_into_s3(yelp_data, file_name, 'raw_yelp_files')
    return None

def main():
    iter_yelp_files()

if __name__ == "__main__":
    main()
