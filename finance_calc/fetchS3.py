import json
import boto3
import os

s3_client = boto3.client("s3", aws_access_key_id='AKIASBOYOHPAYA24EQOX',
                         aws_secret_access_key='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')

class get_combined_files:
    def __init__(self):
        self.S3_BUCKET = 'meritoapp'
        self.bucketPath = 'beta'
        if os.environ.get('ENVIRONMENT') != 'beta':
            self.S3_BUCKET = 'uploadfiles-jigsaw'
            self.bucketPath = 'dev'
            
        print(f'S3BUCKET: {self.S3_BUCKET}')

    def get_json_payload(self, key):
        # Download the payload from S3
        response = s3_client.get_object(Bucket=self.S3_BUCKET, Key=key)
        event = response['Body'].read()
        return json.loads(event)