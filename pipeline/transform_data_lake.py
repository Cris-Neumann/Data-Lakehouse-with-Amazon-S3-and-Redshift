import json
import boto3

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
        region_name='YOUR_REGION')
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
        region_name='YOUR_REGION')
    s3_client.put_object(
        Body=yelp_data,
        Bucket='streaming-bucket-1',
        Key=f'staging_yelp_files/{s3_file_name}.json')
    return None

def transform_json(s3_file_name):
    output_file = []
    for line in s3_file_name.splitlines():
        json_data = json.loads(line)
        keys_to_remove = ['business_id']
        for key in keys_to_remove:
            json_data.pop(key, None)
        output_file.append(json_data)
    clean_file = '\n'.join([json.dumps(obj) for obj in output_file])
    return clean_file

def iter_yelp_files() -> None:
    """Iterate yelp files to donwload from S3,
    modify files, and insert into S3.
    Returns:
        None
    """
    yelp_files_names = ['business', 'review', 'tip', 'user']
    ### falta un lostadod llaves a eliminar de cada dict/json
    
    for file_name in yelp_files_names:
        if file_name == 'tip':
            json_to_change = donwload_from_s3(file_name)
            insert_into_s3(json_to_change, file_name)
        else:  
            json_to_change = donwload_from_s3(file_name)
            clean_json = transform_json(json_to_change)
            insert_into_s3(clean_json, file_name)
    return None

def main():
    iter_yelp_files()

if __name__ == "__main__":
    main()
