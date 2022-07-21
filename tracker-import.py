from typing import List
import requests
import os
import pandas as pd

TRACKER_API_URL_BASE = 'https://api.tracker.yandex.net/v2/issues/_search'
TRACKER_API_URL_PARAMS = '?scrollType=unsorted&perScroll=1&scrollTTLMillis=10000'
#TRACKER_HEADERS = os.environ['TRACKER_HEADERS']
TRACKER_HEADERS = {'X-Org-ID' : os.environ['TRACKER_ORG_ID'], 'Authorization' : 'OAuth '+ os.environ['TRACKER_OAUTH_TOKEN']}
TRACKER_QUERY_TEXT = 'updated: >now()-5d'

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
columns = ['organization_id',
            'self',
            'id',
            'key',
            'version',
            'storyPoints',
            'summary',
            'statusStartTime',
            'boards_names',
            'createdAt',
            'commentWithoutExternalMessageCount',
            'votes',
            'commentWithExternalMessageCount',
            'deadline',
            'updatedAt',
            'favorite',
            'updatedBy_display',
            'type_display',
            'priority_display',
            'createdBy_display',
            'assignee_display',
            'queue_key',
            'queue_display',
            'status_display',
            'previousStatus_display',
            'parent_key',
            'parent_display',
            'components_display',
            'sprint',
            'epic_display',
            'previousStatusLastAssignee_display',
            'originalEstimation',
            'spent',
            'tags',
            'estimation',
            'checklistDone',
            'checklistTotal',
            'emailCreatedBy',
            'sla',
            'emailTo',
            'emailFrom',
            'lastCommentUpdatedAt',
            'followers',
            'pendingReplyFrom',
            'end',
            'start',
            'project_display',
            'votedBy_display',
            'aliases',
            'previousQueue_display',
            'access',
            'resolvedAt',
            'resolvedBy_display',
            'resolution_display',
            'lastQueue_display']

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
    raw_df = pd.json_normalize(json_data, sep='_', max_level=2)
    raw_df.insert(0, 'organization_id', os.environ['TRACKER_ORG_ID'])
    #for col_name in raw_df.columns:
    #    print(col_name)
    
    #function to transform boards column from list of dictionaries 
    #to simple comma separated string
    def format_boards_column(item):
        if (type(item)==list):
            s=''
            for i in range(len(item)):
                s += item[i]['name']
                if i < len(item)-1:
                    s += ', '
            return s
    #function to transform components column from list of dictionaries 
    #to simple comma separated string
    def format_components_column(item):
        if (type(item)==list):
            s=''
            for i in range(len(item)):
                s += item[i]['display']
                if i < len(item)-1:
                    s += ', '
            return s

    raw_df['boards_names'] = raw_df['boards'].apply(format_boards_column)
    raw_df['components_display'] = raw_df['components'].apply(format_components_column)

    #filter out unnecessary colums
    shaped_df = pd.DataFrame(columns=columns)
    for col in columns:
        try:
            shaped_df[col] = raw_df[col]
        except KeyError as cerr:
            shaped_df[col] = ""

    #reformat dateTime columns
    #List of columns with dateTime data format
    date_time_columns: List[str] = [
        'statusStartTime',
        'createdAt',
        'updatedAt',
        'lastCommentUpdatedAt',
        'start',
        'end',
        'resolvedAt'
    ]
    #Round dateTime columns up to seconds
    for col in date_time_columns:
        shaped_df[col] = pd.to_datetime(shaped_df[col]).dt.tz_localize(None).fillna('1970-01-01 00:00:00.000') #.round('S')
        #apply Lambda fuction to each datetime column to unify datetime sting representation
        shaped_df[col] = shaped_df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])


    #reformat decimal columns
    #List of columns with decimal data format
    decimal_columns: List[str] = [
        'storyPoints',
        'commentWithExternalMessageCount',
        'commentWithoutExternalMessageCount',
        'votes',
        'checklistDone',
        'checklistTotal'
    ]
    #Round dateTime columns up to 2 digits after comma
    for col in decimal_columns:
        shaped_df[col] = pd.to_numeric(shaped_df[col]).round(10)

    return shaped_df

def init_database(drop_table=False):
    """
    Initialise Clickhouse DB: Dropping & CReating table with columns
    
    Arguments:
        drop_table (Boleean): flag to indcate tha Dropping table is needed
    Returns:
        Nothing
    """
    if (drop_table):
        query = '''drop table if exists ''' + TABLE + ''' on cluster '{cluster}';'''
        run_clickhouse_query(query)

    query = '''
        CREATE TABLE IF NOT EXISTS ''' + TABLE + '''
        (
            organization_id                     String,
            self                                String,
            id                                  String,
            key                                 String,
            version                             String,
            storyPoints                         Decimal(15,2),
            summary                             String,
            statusStartTime                         DateTime64(3, 'Europe/Moscow'),
            boards_names                        String,
            createdAt                               DateTime64(3, 'Europe/Moscow'),
            commentWithoutExternalMessageCount  Decimal(15,2),
            votes                               Decimal(15,2),
            commentWithExternalMessageCount     Decimal(15,2),
            deadline                            String,
            updatedAt                               DateTime64(3, 'Europe/Moscow'),
            favorite                            String,
            updatedBy_display                   String,
            type_display                        String,
            priority_display                    String,
            createdBy_display                   String,
            assignee_display                    String,
            queue_key                           String,
            queue_display                       String,
            status_display                      String,
            previousStatus_display              String,
            parent_key                          String,
            parent_display                      String,
            components_display                  String,
            sprint                              String,
            epic_display                        String,
            previousStatusLastAssignee_display  String,
            originalEstimation                  String,
            spent                               String,
            tags                                String,
            estimation                          String,
            checklistDone                       Decimal(15,2),
            checklistTotal                      Decimal(15,2),
            emailCreatedBy                      String,
            sla                                 String,
            emailTo                             String,
            emailFrom                           String,
            lastCommentUpdatedAt                    DateTime64(3, 'Europe/Moscow'),
            followers                           String,
            pendingReplyFrom                    String,
            end                                     DateTime64(3, 'Europe/Moscow'),
            start                                   DateTime64(3, 'Europe/Moscow'),
            project_display                     String,
            votedBy_display                     String,
            aliases                             String,
            previousQueue_display               String,
            access                              String,
            resolvedAt                              DateTime64(3, 'Europe/Moscow'),
            resolvedBy_display                  String,
            resolution_display                  String,
            lastQueue_display                   String
        )
        ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/''' + TABLE + '''', '{replica}') 
        --PARTITION BY id 
        ORDER BY (id) 
        '''
    run_clickhouse_query(query)

def run_clickhouse_query(query, connection_timeout=1500):
    """
    Exec clickhouse query
    
    Arguments:
        query (string): Query string
    Returns:
        response from database
    """
    url = 'https://{host}:8443/?database={db}'.format(
        host=os.environ['CH_HOST'],
        db=os.environ['CH_DB'])
    #Run Clickhouse query, places in the body of POST request (Query string could be long and not fit in url string)
    response = requests.post(url, data=query, headers=AUTH, verify=CERT, timeout=connection_timeout)
    if response.status_code == 200:
        return response.text
    else:
        raise ValueError(response.text)

def upload_clickhouse_data(data, connection_timeout=1500):
    """
    Exec clickhouse query
    
    Arguments:
        data (string): Data to be uploaded in CSV format
    Returns:
        response from database
    """
    url = 'https://{host}:8443/?database={db}'.format(
        host=os.environ['CH_HOST'],
        db=os.environ['CH_DB'])
    query_dict = {
        'query': 'INSERT INTO ' + TABLE + ' FORMAT TabSeparatedWithNames'
    }
    response = requests.post(CH_URL, data=data, params=query_dict, headers=AUTH, verify=CERT)
    result = response.text
    if response.status_code == 200:
        return result
    else:
        print(response.text)
        raise ValueError(response.text)

def upload_data_to_db(df):
    """
    Exec clickhouse query
    
    Arguments:
        df (Dataframe): dataframe with tracker data
    Returns:
        Nothing
    """
    init_database(drop_table=False)
    #Prepare data to upload: escaping \n to allow fields with new lines be represented correctly in CSV format 
    content = df.replace("\n", "\\\n", regex=True).to_csv(index=False, sep='\t') #.iloc[:]
    content = content.encode('utf-8')    
    upload_clickhouse_data(content)

tracker_json_data = get_tracker_data(TRACKER_API_URL_BASE)
tracker_df_data = shape_data(tracker_json_data)
upload_data_to_db(tracker_df_data)

#test_clickhouse_query('SELECT version()')

#query='SELECT version()'
#run_clickhouse_query(query)

#df.description.iloc[15:25].str.replace('\n','\\\n')
#df.replace('\n','\\\n',regex=True).iloc[15:25,49:50]

#tracker_df_data[['key', 'statusStartTime', 'lastCommentUpdatedAt', 'start', 'end', 'createdAt', 'updatedAt']].iloc[1].to_csv(index=False, sep='\t', date_format='%r')