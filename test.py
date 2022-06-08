import requests
import boto3
import io
from requests_oauthlib import OAuth1

TRACKER_API_URL_SEARCH_TASKS = 'https://api.tracker.yandex.net/v2/issues/_search'
TRACKER_HEADERS = os.environ['TRACKER_HEADERS']
TRACKER_QUERY_TEXT = os.environ['TRACKER_QUERY_TEXT']

YC_S3_ACCESS_KEY_ID = os.environ['YC_S3_ACCESS_KEY_ID']
YC_S3_SECRET_ACCESS_KEY = os.environ['YC_S3_SECRET_ACCESS_KEY']
YC_S3_ENDPOINT_URL = os.environ['YC_S3_ENDPOINT_URL']
YC_S3_BUCKET_NAME = os.environ['YC_S3_BUCKET_NAME']
YC_S3_FILENAME = os.environ['YC_S3_FILENAME']

#Query to filter Tracker issues
query={'query': TRACKER_QUERY_TEXT}

#Make Tracker API call
response = requests.post(TRACKER_API_URL_SEARCH_TASKS, headers=TRACKER_HEADERS, json=query)

#Upload data to S3 bucket
#Creating Session With Boto3.
s3_client = boto3.client('s3',
    aws_access_key_id=YC_S3_ACCESS_KEY_ID,
    aws_secret_access_key=YC_S3_SECRET_ACCESS_KEY,
    endpoint_url=YC_S3_ENDPOINT_URL)
#Upload HTTPResponse data to S3. Should be BytesIO
s3_client.upload_fileobj(io.BytesIO(response.content), YC_S3_BUCKET_NAME, YC_S3_FILENAME)

