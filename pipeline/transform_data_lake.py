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

def main():
    #### hacer un bucle de esto:
    s3_file_name = 'tip'
    json_to_change = donwload_from_s3(s3_file_name)
    final_file = transform_json(json_to_change)
    print(final_file)

if __name__ == "__main__":
    main()
