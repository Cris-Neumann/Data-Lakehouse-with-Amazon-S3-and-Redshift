import os
import sys
import json

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from utilities.connect_to_s3 import insert_into_s3
from utilities.connect_to_mongodb import donwload_from_MongoDB

def iter_yelp_files() -> None:
    """Iterate yelp files to insert into S3.
    Returns:
        None
    """
    yelp_files_names = ['business', 'checkin', 'review', 'tip', 'user']
    for file_name in yelp_files_names:
        raw_data = donwload_from_MongoDB('yelp_db', f'raw_{file_name}')
        data_extract = raw_data[:5000]
        yelp_data = '\n'.join([json.dumps(obj) for obj in data_extract])
        insert_into_s3(yelp_data, file_name, 'raw_yelp_files')
    return None

def main():
    iter_yelp_files()

if __name__ == "__main__":
    main()
