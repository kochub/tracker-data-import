import requests
import json
from requests_oauthlib import OAuth1

url = 'https://api.tracker.yandex.net/v2/issues/_search'
headers = {'X-Org-ID' : '37789', 'Authorization' : 'OAuth AQAEA7qkELYcAAeIVgu9pWPIxUVMnQFMVQv5lP0', 'Content-Type': 'text/plain'}

query={'query': 'updated: >now()-1d'}

response = requests.post(url, headers=headers, json=query)

print(response.json())

