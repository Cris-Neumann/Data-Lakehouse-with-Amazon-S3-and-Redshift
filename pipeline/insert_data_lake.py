import json
import boto3

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
        Body=json.dumps(yelp_data),
        Bucket='streaming-bucket-1',
        Key=f'yelp_files/{s3_file_name}.json')
    return None

def iter_yelp_files() -> None:
    """Iterate yelp files to insert into S3.
    Returns:
        None
    """
    directory_path = r'YOUR_RAW_DIRECTORY_PATH'
    yelp_files_names = ['business', 'checkin', 'review', 'tip', 'user']
    for file_name in yelp_files_names:
        file_path = f'{directory_path}\\yelp_academic_dataset_{file_name}.json'
        yelp_data = json_to_dict(file_path)
        insert_into_s3(yelp_data, file_name)
    return None

def main():
    iter_yelp_files()

if __name__ == "__main__":
    main()
