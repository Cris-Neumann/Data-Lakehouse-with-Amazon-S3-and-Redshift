import json
import boto3

def insert_into_s3(jsonpath_data:json, s3_file_name:str) -> None:
    """Insert jsonpath data into Amazon S3.
    Args:
        jsonpath_data (json): Data to insert into S3.
        s3_file_name (str): Name of the file to insert into S3.
    Returns:
        None
    """
    s3_client = boto3.client('s3', 
        aws_access_key_id='YOUR_ACCESS_KEY', 
        aws_secret_access_key='YOUR_SECRET_ACCESS_KEY', 
        region_name='YOUR_REGION')
    s3_client.put_object(
        Body=json.dumps(jsonpath_data),
        Bucket='streaming-bucket-1',
        Key=f'yelp_jsonpath_files/{s3_file_name}.json')
    return None

def iter_jsonpath_files() -> None:
    """Iterate jsonpath files to insert into S3.
    Returns:
        None
    """
    directory_path = 'YOUR_JSONPATH_PATH'
    jsonpath_files_names = ['business_jsonpath', 'review_jsonpath', 'tip_jsonpath', 'user_jsonpath']
    for file_name in jsonpath_files_names:
        file_path = f'{directory_path}{file_name}.json'
        with open(file_path, 'r', encoding='utf-8') as open_file:
            jsonpath = json.load(open_file)
            insert_into_s3(jsonpath, file_name)
    return None

def main():
    iter_jsonpath_files()

if __name__ == "__main__":
    main()
