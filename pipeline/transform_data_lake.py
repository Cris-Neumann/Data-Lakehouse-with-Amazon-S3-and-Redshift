import json
import boto3
import pandas as pd

yelp_files_names = ['business', 'review', 'tip', 'user']    
keys_to_remove = {'business':['categories', 'hours'],
    'user':['useful', 'funny', 'cool', 'elite', 'friends',
    'fans', 'compliment_hot', 'compliment_more',
    'compliment_profile', 'compliment_cute',
    'compliment_list', 'compliment_note',
    'compliment_plain', 'compliment_cool',
    'compliment_funny', 'compliment_writer',
    'compliment_photos']}
    
def donwload_from_s3(s3_file_name:str) -> json:
    """Download data from Amazon S3.
    Args:
        s3_file_name (str): Name of the file in S3.
    Returns:
        json_content (json): json with yelp data.
    """
    s3_client = boto3.client('s3', 
        aws_access_key_id='YOUR_ACCESS_KEY', 
        aws_secret_access_key='YOUR_SECRET_ACCESS_KEY',
        region_name='sa-east-1')
    json_file = s3_client.get_object(
        Bucket='streaming-bucket-1',
        Key=f'raw_yelp_files/{s3_file_name}.json')
    json_content = json_file['Body'].read().decode('utf-8')
    return json_content

def insert_into_s3(yelp_data:json, s3_file_name:str) -> None:
    """Insert yelp data into Amazon S3.
    Args:
        yelp_data (json): Data to insert into S3.
        s3_file_name (str): Name of the file to insert into S3.
    Returns:
        None
    """
    s3_client = boto3.client('s3', 
        aws_access_key_id='YOUR_ACCESS_KEY',
        aws_secret_access_key='YOUR_SECRET_ACCESS_KEY', 
        region_name='sa-east-1')
    s3_client.put_object(
        Body=yelp_data,
        Bucket='streaming-bucket-1',
        Key=f'staging_yelp_files/{s3_file_name}.json')
    return None

def transform_json(s3_file, keys_to_remove, file_name):
    output_file = []
    for line in s3_file.splitlines():
        json_data = json.loads(line)
        for key in keys_to_remove[file_name]:
            json_data.pop(key, None)
        output_file.append(json_data)
    clean_file = '\n'.join([json.dumps(obj) for obj in output_file])
    return clean_file

def process_record(record:dict, attributes_cat:list) -> pd.DataFrame:
    """ Flatten dictionaries with one business data
    Args:
        record (dict): Dictionary with one business data.
        attributes_cat (list): List with attributes.
    Returns:
        new_df (DataFrame): DataFrame with flat record.
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

def iter_yelp_files(files_names, keys_to_delete) -> None:
    """Iterate yelp files to donwload from S3,
    modify files, and insert into S3.
        None
    """
    for file_name in files_names:
        if (file_name == 'tip') | (file_name == 'review'):
            json_to_change = donwload_from_s3(file_name)
            insert_into_s3(json_to_change, file_name)
        else:  
            json_to_change = donwload_from_s3(file_name)
            clean_json = transform_json(json_to_change, keys_to_delete, file_name)
            if (file_name == 'business'):
                json_file = []
                for line in clean_json.splitlines():
                    json_file.append(json.loads(line))
                attributes_cat = ['ByAppointmentOnly', 'RestaurantsTakeOut',
                                  'Alcohol', 'WiFi', 'BusinessAcceptsBitcoin']
                df = pd.DataFrame(columns=[ 'business_id', 'name', 'address',
                'city', 'state', 'postal_code', 'latitude', 'longitude', 'stars',
                'review_count', 'is_open'] + attributes_cat)
                for record in json_file:
                    df = pd.concat([df, process_record(record, attributes_cat)])
                clean_json = df.to_json(orient='records', lines=True)                
            insert_into_s3(clean_json, file_name)
    return None

def main():
    iter_yelp_files(yelp_files_names, keys_to_remove)

if __name__ == "__main__":
    main()
