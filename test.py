import requests
import boto3
import io
from requests_oauthlib import OAuth1

TRACKER_API_URL_SEARCH_TASKS = 'https://api.tracker.yandex.net/v2/issues/_search'
HEADERS = {'X-Org-ID' : '37789', 'Authorization' : 'OAuth AQAEA7qkELYcAAeIVgu9pWPIxUVMnQFMVQv5lP0'}
TRACKER_QUERY_TEXT = 'updated: >now()-16h'

YC_ACCESS_KEY_ID = 'YCAJEnE49YNOWeDT1je5FxuoW'
YC_SECRET_ACCESS_KEY = 'YCOw85vlK7UkYpaGcM8_rI9woGw626VcXgrLmTpc'
YC_ENDPOINT_URL='https://storage.yandexcloud.net'
S3_BUCKET_NAME = 'tracker-import'
S3_FILENAME = 'file_name.txt'

#Query to filter Tracker issues
query={'query': TRACKER_QUERY_TEXT}

#Make Tracker API call
response = requests.post(TRACKER_API_URL_SEARCH_TASKS, headers=HEADERS, json=query)

#Upload data to S3 bucket
#Creating Session With Boto3.
s3_client = boto3.client('s3',
    aws_access_key_id=YC_ACCESS_KEY_ID,
    aws_secret_access_key=YC_SECRET_ACCESS_KEY,
    endpoint_url=YC_ENDPOINT_URL)
#Upload HTTPResponse data to S3. Should be BytesIO
s3_client.upload_fileobj(io.BytesIO(response.content), S3_BUCKET_NAME, S3_FILENAME)

