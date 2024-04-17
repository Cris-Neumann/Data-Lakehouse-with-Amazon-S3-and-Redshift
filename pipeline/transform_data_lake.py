import os
import sys
import json
import pandas as pd

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
os.chdir(path)
sys.path.append(path)
from utilities.connect_to_s3 import donwload_from_s3, insert_into_s3
from utilities.aux_file import yelp_files_names, keys_to_remove, attributes_cat

def transform_json(s3_file:json, keys_to_remove:dict, file_name:str) -> json:
    """Modify a json by removing certain given fields.
    Args:
        s3_file (json): J file to modify.
        keys_to_remove (dict): Fields to delete.
        file_name (str): Name of the file that serves
        as the dictionary key.
    Returns:
        clean_file (json): Modified json.
    """
    output_file = []
    for line in s3_file.splitlines():
        json_data = json.loads(line)
        for key in keys_to_remove[file_name]:
            json_data.pop(key, None)
        output_file.append(json_data)
    clean_file = '\n'.join([json.dumps(obj) for obj in output_file])
    return clean_file

def process_record(record:dict, attributes_cat:list) -> pd.DataFrame:
    """ Flatten dictionaries from certain given fields.
    Args:
        record (dict): Dictionary of one row.
        attributes_cat (list): List with attributes.
    Returns:
        new_df (DataFrame): DataFrame with flat records.
    """
    flat_record = {
        "business_id": record.get('business_id'),
        "name": record.get("name") ,
        "address": record.get("address"),
        "city": record.get("city"),
        "state": record.get("state"),
        "postal_code": record.get("postal_code"),
        "latitude": record.get("latitude"),
        "longitude": record.get("longitude"),
        "stars": record.get("stars"),
        "review_count": record.get("review_count"),
        "is_open": record.get("is_open"),
        **{category: (record.get("attributes", {}).get(category) if isinstance(record.\
            get("attributes", {}), dict) else None) for category in attributes_cat}}
    new_df = pd.DataFrame.from_dict([flat_record])
    return new_df

def iter_yelp_files(files_names:list, keys_to_delete:dict, attributes_cat:list) -> None:
    """Iterate yelp files to donwload from S3,
        modify files, and insert into S3.
    Args:
        files_names (list): Name of the files to modify.
        keys_to_delete (dict): Fields to delete.
        attributes_cat (list): List with fields to flatten
    Returns:
        None
    """
    for file_name in files_names:
        if (file_name == 'tip') | (file_name == 'review'):
            json_to_change = donwload_from_s3(file_name, 'raw_yelp_files')
            insert_into_s3(json_to_change, file_name, 'staging_yelp_files')
        else:  
            json_to_change = donwload_from_s3(file_name, 'raw_yelp_files')
            clean_json = transform_json(json_to_change, keys_to_delete, file_name)
            if (file_name == 'business'):
                json_file = []
                for line in clean_json.splitlines():
                    json_file.append(json.loads(line))
                df = pd.DataFrame(columns=[ 'business_id', 'name', 'address',
                'city', 'state', 'postal_code', 'latitude', 'longitude', 'stars',
                'review_count', 'is_open'] + attributes_cat)
                for record in json_file:
                    df = pd.concat([df, process_record(record, attributes_cat)])
                clean_json = df.to_json(orient='records', lines=True)                
            insert_into_s3(clean_json, file_name, 'staging_yelp_files')
    return None

def main():
    iter_yelp_files(yelp_files_names, keys_to_remove, attributes_cat)

if __name__ == "__main__":
    main()
