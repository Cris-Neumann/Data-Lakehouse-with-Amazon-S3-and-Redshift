import json
import boto3
import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

def donwload_from_s3(file_name:str, dir_name:str) -> json:
    """Download data from Amazon S3.
    Args:
        file_name (str): Name of the file in S3.
        dir_name (str): Directory of the data to extract.
    Returns:
        json_content (json): json with transform data.
    """
    s3_client = boto3.client('s3', 
        aws_access_key_id=config['S3']['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=config['S3']['AWS_SECRET_ACCESS_KEY'],
        region_name=config['S3']['REGION_NAME'])
    json_file = s3_client.get_object(
        Bucket=config['S3']['BUCKET_NAME'],
        Key=f'{dir_name}/{file_name}.json')
    json_content = json_file['Body'].read().decode('utf-8')
    return json_content

def insert_into_s3(json_data:json, file_name:str, dir_name:str) -> None:
    """Insert data into Amazon S3.
    Args:
        json_data (json): Data to insert into S3.
        file_name (str): Name of the file to insert into S3.
        dir_name (str): S3 directory to insert.
    Returns:
        None
    """
    s3_client = boto3.client('s3', 
        aws_access_key_id=config['S3']['AWS_ACCESS_KEY_ID'], 
        aws_secret_access_key=config['S3']['AWS_SECRET_ACCESS_KEY'], 
        region_name=config['S3']['REGION_NAME'])
    s3_client.put_object(
        Body=json_data,
        Bucket=config['S3']['BUCKET_NAME'],
        Key=f'{dir_name}/{file_name}.json')
    return None
