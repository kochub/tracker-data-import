from cgitb import text
from wsgiref import headers
import requests
import boto3
import io
import os
import pandas as pd

TRACKER_API_URL_BASE = 'https://api.tracker.yandex.net/v2/issues/_search'
TRACKER_API_URL_PARAMS = '?scrollType=unsorted&perScroll=1&scrollTTLMillis=10000'
#TRACKER_HEADERS = os.environ['TRACKER_HEADERS']
TRACKER_HEADERS = {'X-Org-ID' : os.environ['TRACKER_ORG_ID'], 'Authorization' : 'OAuth '+ os.environ['TRACKER_OAUTH_TOKEN']}
TRACKER_QUERY_TEXT = 'updated: >now()-30d'

YC_S3_ACCESS_KEY_ID = os.environ['YC_S3_ACCESS_KEY_ID']
YC_S3_SECRET_ACCESS_KEY = os.environ['YC_S3_SECRET_ACCESS_KEY']
YC_S3_ENDPOINT_URL = os.environ['YC_S3_ENDPOINT_URL']
YC_S3_BUCKET_NAME = os.environ['YC_S3_BUCKET_NAME']
YC_S3_FILENAME = os.environ['YC_S3_FILENAME']
#Number of YandexTracker issues retrieved per request
RESULTS_PER_PAGE = 20
#ClickHouse params
CH_PASSWORD = os.environ['CH_PASSWORD']
CH_URL = 'https://{host}:8443/?database={db}'.format(
    host=os.environ['CH_HOST'],
    db=os.environ['CH_DB'])
AUTH = {
    'X-ClickHouse-User': os.environ['CH_USER'],
    'X-ClickHouse-Key': CH_PASSWORD,
}
CERT = './YandexRootCA.pem'
TABLE = os.environ['CH_TABLE']

#Columns to load into database
columns = ['organization_id', 'self', 'id', 'key', 'version', 'pendingReplyFrom',
    'statusStartTime', 'Effort', 'boards', 'type',
    'previousStatusLastAssignee', 'createdAt', 'Confidence',
    'commentWithExternalMessageCount', 'deadline', 'updatedAt',
    'lastCommentUpdatedAt', 'storyPoints', 'summary', 'Impact', 'Reach',
    'originalEstimation', 'updatedBy', 'spent', 'start', 'priority',
    'estimation', 'Score', 'followers', 'createdBy',
    'commentWithoutExternalMessageCount', 'votes', 'assignee', 'queue',
    'status', 'previousStatus', 'favorite', 'tags', 'components', 'end',
    'parent', 'resolvedAt', 'resolvedBy', 'resolution', 'epic', 'sprint',
    'project', 'sla', 'otvetstvennyj', 'gorod',
    'otvetstvennyjZaKomandirovki', 'podrazdelenie', 'description', 'cel',
    'kadrovik', 'sotrudnik', 'buhgalter', 'unique', 'checklistDone',
    'checklistTotal', 'checklistItems', 'stoimost', 'zatratyrub',
    'programma', 'votedBy', 'aliases', 'previousQueue', 'emailCreatedBy',
    'emailTo', 'emailFrom', 'dopolnitelnoeSoglasovanie', 'soglasuusij',
    'cenaBileta', 'rekruter', 'uhodasijSotrudnik', 'professia',
    'dataObnovlenia', 'otsutstvie']

def get_tracker_data(query_url_base=TRACKER_API_URL_BASE, headers=TRACKER_HEADERS, query_text=TRACKER_QUERY_TEXT, perScroll=2):
    """
    Load data from Yandex Tracker using scroll method, see doc:
    https://cloud.yandex.ru/docs/tracker/concepts/issues/search-issues#scroll
    
    Arguments:
        query_url_base (str): Yandex Tracker API URL base
        headers (str): Yandex Tracker API Haders
        query_text (str): Yandex Tracker query for search issues
    Returns:
        json object with records
    """
    query_url = query_url_base+TRACKER_API_URL_PARAMS
    #Query to filter Tracker issues
    query_body={'query': query_text}
    #Make Tracker API call
    response = requests.post(query_url, headers=headers, json=query_body)
    data = response.json()
    
    #loop wile number of collected data less than tootal records in query result
    while len(data) < int(response.headers['X-Total-Count']):
        scrollId=response.headers['X-Scroll-Id']
        scrollToken=response.headers['X-Scroll-Token']
        query_url=query_url_base+'?scrollId='+scrollId+'&scrollToken='+scrollToken
        response = requests.post(query_url, headers=headers, json=query_body)
        data.extend(response.json())
    
    print('Tracker data loaded, total records: ', len(data))
    return data

def shape_data(json_data):
    """
    Convert json data to Pandas dataframe and add 'org_id' column
    
    Arguments:
        json_data (json): input JSON data
    Returns:
        Pandas dataframe object with records
    """
    df = pd.json_normalize(json_data, max_level=0)
    df.insert(0, 'organization_id', os.environ['TRACKER_ORG_ID'])
    return df

def init_database(drop_table=False):
    if (drop_table):
        query = '''drop table if exists ''' + TABLE + ''' on cluster '{cluster}';'''
        run_clickhouse_query(query)

    query = '''
        CREATE TABLE IF NOT EXISTS ''' + TABLE + ''' on cluster '{cluster}'
        (
            organization_id String,
            self         String,
            id String,
            key String,
            version String,
            pendingReplyFrom String,
            statusStartTime String,
            Effort String,
            boards String,
            type String,
            previousStatusLastAssignee String,
            createdAt String,
            Confidence String,
            commentWithExternalMessageCount String,
            deadline String,
            updatedAt String,
            lastCommentUpdatedAt String,
            storyPoints String,
            summary String,
            Impact String,
            Reach String,
            originalEstimation String,
            updatedBy String,
            spent String,
            start String,
            priority String,
            estimation String,
            Score String,
            followers String,
            createdBy String,
            commentWithoutExternalMessageCount String,
            votes String,
            assignee String,
            queue String,
            status String,
            previousStatus String,
            favorite String,
            tags String,
            components String,
            end String,
            parent String,
            resolvedAt String,
            resolvedBy String,
            resolution String,
            epic String,
            sprint String,
            project String,
            sla String,
            otvetstvennyj String,
            gorod String,
            otvetstvennyjZaKomandirovki String,
            podrazdelenie String,
            description String,
            cel String,
            kadrovik String,
            sotrudnik String,
            buhgalter String,
            unique String,
            checklistDone String,
            checklistTotal String,
            checklistItems String,
            stoimost String,
            zatratyrub String,
            programma String,
            votedBy String,
            aliases String,
            previousQueue String,
            emailCreatedBy String,
            emailTo String,
            emailFrom String,
            dopolnitelnoeSoglasovanie String,
            soglasuusij String,
            cenaBileta String,
            rekruter String,
            uhodasijSotrudnik String,
            professia String,
            dataObnovlenia String,
            otsutstvie String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/''' + TABLE + '''', '{replica}') 
        PARTITION BY id 
        ORDER BY (id) 
        '''
    run_clickhouse_query(query)

def run_clickhouse_query(query, connection_timeout=1500):
    url = 'https://{host}:8443/?database={db}'.format(
        host=os.environ['CH_HOST'],
        db=os.environ['CH_DB'])
    response = requests.post(url, data=query, headers=AUTH, verify=CERT, timeout=connection_timeout)
    if response.status_code == 200:
        return response.text
    else:
        raise ValueError(response.text)

def upload_data_to_db(df):
    init_database(drop_table=False)
    content = df.replace("\n", "\\\n", regex=True).iloc[20:].to_csv(index=False, sep='\t')
    #print(df)
    content = content.encode('utf-8')
    query_dict = {
        'query': 'INSERT INTO ' + TABLE + ' FORMAT TabSeparatedWithNames '
    }
    r = requests.post(CH_URL, data=content, params=query_dict, headers=AUTH, verify=CERT)
    result = r.text
    if r.status_code == 200:
        return result
    else:
        print(r.text)
        raise ValueError(r.text)

tracker_json_data = get_tracker_data(TRACKER_API_URL_BASE)
tracker_df_data = shape_data(tracker_json_data)
upload_data_to_db(tracker_df_data)

#test_clickhouse_query('SELECT version()')

#query='SELECT version()'
#run_clickhouse_query(query)

#df.description.iloc[15:25].str.replace('\n','\\\n')
#df.replace('\n','\\\n',regex=True).iloc[15:25,49:50]