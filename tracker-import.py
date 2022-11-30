from typing import List
import requests
import os
import pandas as pd
from datetime import datetime, timedelta

TRACKER_API_URL_BASE_FOR_ISSUE_LIST = 'https://api.tracker.yandex.net/v2/issues/_search'
TRACKER_API_URL_PARAMS_FOR_ISSUE_LIST = '?scrollType=unsorted&perScroll=100&scrollTTLMillis=10000'
TRACKER_API_URL_BASE_FOR_ISSUE_CHANGELOG = 'https://api.tracker.yandex.net/v2/issues/'
TRACKER_API_URL_PARAMS_FOR_ISSUE_CHANGELOG = '/changelog?perPage=50&type=IssueWorkflow'

#TRACKER_HEADERS = os.environ['TRACKER_HEADERS']
TRACKER_HEADERS = {'X-Org-ID' : os.environ['TRACKER_ORG_ID'], 'Authorization' : 'OAuth '+ os.environ['TRACKER_OAUTH_TOKEN']}
#TRACKER_QUERY_TEXT = 'updated: >now()-730d'
try:
    TRACKER_INITIAL_HISTORY_DEPTH = os.environ['TRACKER_INITIAL_HISTORY_DEPTH']
except:
    TRACKER_INITIAL_HISTORY_DEPTH=''
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
#CERT = '/etc/ssl/certs/ca-certificates.crt'
CH_ISSUES_TABLE = os.environ['CH_ISSUES_TABLE']
CH_CHANGELOG_TABLE = os.environ['CH_CHANGELOG_TABLE']

#Columns to load into database
issues_columns = ['organization_id',
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
            'sprint_display',
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

issue_changelog_columns = ['organization_id',
            'id',
            'issue_key',
            'updatedAt',
            'updatedBy_display',
            'type',
            'field_display',
            'from_display',
            'to_display',
            'worklog']

def get_issues_query_text():
    """
    Get latest lecord which has been loaded to tracker_isuues table:    
    Arguments:
        query_url_base (str): Yandex Tracker API URL base
        headers (str): Yandex Tracker API Haders
        query_text (str): Yandex Tracker query for search issues
    Returns:
        json object with records
    """
    # Get latest updated issue form database
    get_max_uploaded_at_query='select MAX(updatedAt) from tracker_issues'
    response=run_clickhouse_query(get_max_uploaded_at_query)
    latest_record_time = datetime.strptime(response[:-5], "%Y-%m-%d %H:%M:%S")

    if latest_record_time > datetime.strptime('1970-01-01 03:00:00', "%Y-%m-%d %H:%M:%S"):
        # subtract 5 minutes to handle possible time overlapping & late updates
        start_time = str(latest_record_time - timedelta(minutes=5))
        tracker_query_text = 'updated: > "' + start_time + '"'
        return tracker_query_text
    elif TRACKER_INITIAL_HISTORY_DEPTH != '':
        tracker_query_text = 'updated: >now()-' + TRACKER_INITIAL_HISTORY_DEPTH
        return tracker_query_text
    else:
        return 'updated: >now() - 1y'


def get_tracker_issue_list(query_url_base=TRACKER_API_URL_BASE_FOR_ISSUE_LIST, headers=TRACKER_HEADERS, query_text='updated: >now()-1y'):
    """
    Load issue list from Yandex Tracker using scroll method, see doc:
    https://cloud.yandex.ru/docs/tracker/concepts/issues/search-issues#scroll
    
    Arguments:
        query_url_base (str): Yandex Tracker API URL base
        headers (str): Yandex Tracker API Haders
        query_text (str): Yandex Tracker query for search issues
    Returns:
        json object with records
    """
    query_url = query_url_base+TRACKER_API_URL_PARAMS_FOR_ISSUE_LIST
    #Query to filter Tracker issues
    query_body={'query': query_text}
    #Make Tracker API call
    response = requests.post(query_url, headers=headers, json=query_body) #response = requests.post(query_url, headers=headers, json=query_body)
    issues_data = response.json()
    
    #loop wile number of collected data less than tootal records in query result
    while len(issues_data) < int(response.headers['X-Total-Count']):
        scrollId=response.headers['X-Scroll-Id']
        scrollToken=response.headers['X-Scroll-Token']
        query_url=query_url_base+'?scrollId='+scrollId+'&scrollToken='+scrollToken
        response = requests.post(query_url, headers=headers, json=query_body)
        issues_data.extend(response.json())
    
    print('Tracker data loaded, total records: ', len(issues_data))
    return issues_data

def get_tracker_issue_changelog_for_key(issue_key='', headers=TRACKER_HEADERS):
    """
    Load issue changelog from Yandex Tracker using scroll method, see doc:
    https://cloud.yandex.ru/docs/tracker/concepts/issues/search-issues#scroll
    
    Arguments:
        query_url_base (str): Yandex Tracker API URL base
        headers (str): Yandex Tracker API Haders
        query_text (str): Yandex Tracker query for search issues
    Returns:
        json object with records
    """
    query_url = TRACKER_API_URL_BASE_FOR_ISSUE_CHANGELOG+issue_key+TRACKER_API_URL_PARAMS_FOR_ISSUE_CHANGELOG
    #Query to filter Tracker issues
    #query_body={'query': query_text}
    #Make Tracker API call
    response = requests.get(query_url, headers=headers)
    changelog_data = response.json()
    try: 
        query_url=response.links['next']['url']
    except KeyError:
        query_url=''

    #loop wile number of collected data less than tootal records in query result
    while query_url != '':
        response = requests.get(query_url, headers=headers)
        try: 
            query_url=response.links['next']['url']
        except KeyError:
            query_url=''
        changelog_data.extend(response.json())
    
    print('Issue '+ issue_key + ' history data loaded, total records: ', len(changelog_data))
    return changelog_data

def get_tracker_issues_changelog(issues_json_data):
    """
    Collect changelog for isssues represented in json
    
    Arguments:
        issues_json_data (json): input JSON data
    Returns:
        Pandas json object with records
    """
    changelog_json = []
    for i in issues_json_data:
        changelog_json.extend(get_tracker_issue_changelog_for_key(issue_key=i['key']))

    return changelog_json

def shape_issues_data(json_data):
    """
    Convert json data to Pandas dataframe and add 'org_id' column
    
    Arguments:
        json_data (json): input JSON data
    Returns:
        Pandas dataframe object with records
    """
    raw_df = pd.json_normalize(json_data, sep='_', max_level=2)
    raw_df.insert(0, 'organization_id', os.environ['TRACKER_ORG_ID'])
    
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
    #function to transform sprint column from list of dictionaries 
    #to simple comma separated string
    def format_sprint_column(item):
        if (type(item)==list):
            s=''
            for i in range(len(item)):
                s += item[i]['display']
                if i < len(item)-1:
                    s += ', '
            return s

    try:
        raw_df['boards_names'] = raw_df['boards'].apply(format_boards_column)
    except KeyError:
        pass
    try:
        raw_df['components_display'] = raw_df['components'].apply(format_components_column)
    except KeyError:
        pass
    try:
        raw_df['sprint_display'] = raw_df['sprint'].apply(format_sprint_column)
    except KeyError:
        pass

    #filter out unnecessary colums
    shaped_df = pd.DataFrame(columns=issues_columns)
    for col in issues_columns:
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
        #apply Lambda fuction to each datetime column to unify datetime sting representation - trim tast 3 symbols of microseconds
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

def shape_issue_changelog_data(json_data):
    """
    Convert issues changelog json data to Pandas dataframe
    
    Arguments:
        json_data (json): input JSON data
    Returns:
        Pandas dataframe object with records
    """
    raw_df = pd.json_normalize(json_data, sep='_', max_level=2)
    raw_df.insert(0, 'organization_id', os.environ['TRACKER_ORG_ID'])
    #expand list in the 'fields' field to duplicate rows
    raw_df = raw_df.explode('fields')
    t=1

    #get (fields -> field -> display) data
    def get_field_display(item):
        s = item['field']['display']
        return s

    #get (fields -> from -> display) data
    def get_from_display(item):
        try: 
            if item['field']['id'] in ('status', 'resolution', 'assignee') :
                s = item['from']['display']
            else:
                s = item['from'] 
        except: 
            s = ''
        return s

    #get (fields -> to -> display) data
    def get_to_display(item):
        try: 
            if item['field']['id'] in ('status', 'resolution', 'assignee'):
                s = item['to']['display']
            else:
                s = item['to']
        except: 
            s = ''
        return s
 
    raw_df['field_display'] = raw_df['fields'].apply(get_field_display)
    raw_df['from_display'] = raw_df['fields'].apply(get_from_display)
    raw_df['to_display'] = raw_df['fields'].apply(get_to_display)
    
    #filter our unnecessary columns
    shaped_df = pd.DataFrame(columns=issue_changelog_columns)
    for col in issue_changelog_columns:
        try:
            shaped_df[col] = raw_df[col]
        except KeyError:
            shaped_df[col] = ''
    
    #reformat dateTime columns
    #List of columns with dateTime data format
    date_time_columns: List[str] = [
        'updatedAt'
    ]   
    #Round dateTime columns up to seconds
    for col in date_time_columns:
        shaped_df[col] = pd.to_datetime(shaped_df[col]).dt.tz_localize(None).fillna('1970-01-01 00:00:00.000') #.round('S')
        #apply Lambda fuction to each datetime column to unify datetime sting representation - trim tast 3 symbols of microseconds
        shaped_df[col] = shaped_df[col].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])

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
        drop_issues_table_query = '''drop table if exists ''' + CH_ISSUES_TABLE + ''';'''
        run_clickhouse_query(drop_issues_table_query)

    create_issues_table_query = '''
        CREATE TABLE IF NOT EXISTS ''' + CH_ISSUES_TABLE + '''
        (
            organization_id                     String,
            self                                String,
            id                                  String,
            key                                 String,
            version                             String,
            storyPoints                         Decimal(15,2),
            summary                             String,
            statusStartTime                     DateTime64(3, 'Europe/Moscow'),
            boards_names                        String,
            createdAt                           DateTime64(3, 'Europe/Moscow'),
            commentWithoutExternalMessageCount  Decimal(15,2),
            votes                               Decimal(15,2),
            commentWithExternalMessageCount     Decimal(15,2),
            deadline                            String,
            updatedAt                           DateTime64(3, 'Europe/Moscow'),
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
            sprint_display                      String,
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
            lastCommentUpdatedAt                DateTime64(3, 'Europe/Moscow'),
            followers                           String,
            pendingReplyFrom                    String,
            end                                 DateTime64(3, 'Europe/Moscow'),
            start                               DateTime64(3, 'Europe/Moscow'),
            project_display                     String,
            votedBy_display                     String,
            aliases                             String,
            previousQueue_display               String,
            access                              String,
            resolvedAt                          DateTime64(3, 'Europe/Moscow'),
            resolvedBy_display                  String,
            resolution_display                  String,
            lastQueue_display                   String
        )
        ENGINE = ReplacingMergeTree()  
        ORDER BY (id) 
        '''
    run_clickhouse_query(create_issues_table_query)

    #issues changelog data
    if (drop_table):
        drop_changelog_table_query = '''drop table if exists ''' + CH_CHANGELOG_TABLE + ''';'''
        run_clickhouse_query(drop_changelog_table_query)

    create_changelog_table_query = '''
        CREATE TABLE IF NOT EXISTS ''' + CH_CHANGELOG_TABLE + '''
        (
            organization_id                     String,
            id                                  String,
            issue_key                           String,
            updatedAt                           DateTime64(3, 'Europe/Moscow'),
            updatedBy_display                   String,
            type                                String,
            field_display                       String,
            from_display                        String,
            to_display                          String,
            worklog                             String
        )
        ENGINE = ReplacingMergeTree()  
        ORDER BY (id, field_display) 
        '''
    run_clickhouse_query(create_changelog_table_query)
    
    create_issues_view = '''
        CREATE OR REPLACE VIEW v_tracker_issues AS
        SELECT organization_id, `self`, id, `key`, version, storyPoints, 
        summary, statusStartTime, boards_names, createdAt, 
        commentWithoutExternalMessageCount, votes, 
        commentWithExternalMessageCount, deadline, updatedAt, favorite, 
        updatedBy_display, type_display, priority_display, 
        createdBy_display, assignee_display, queue_key, queue_display, 
        status_display, previousStatus_display, parent_key, parent_display, 
        components_display, sprint_display, epic_display, 
        previousStatusLastAssignee_display, originalEstimation, spent, 
        tags, estimation, checklistDone, checklistTotal, emailCreatedBy,
        sla, emailTo, emailFrom, lastCommentUpdatedAt, followers, 
        pendingReplyFrom, `end`, `start`, project_display, 
        votedBy_display, aliases, previousQueue_display, access, 
        resolvedAt, resolvedBy_display, resolution_display, 
        lastQueue_display
        FROM (
            SELECT organization_id, `self`, id, `key`, version, storyPoints, 
            summary, statusStartTime, boards_names, createdAt, 
            commentWithoutExternalMessageCount, votes, 
            commentWithExternalMessageCount, deadline, updatedAt, favorite, 
            updatedBy_display, type_display, priority_display, 
            createdBy_display, assignee_display, queue_key, queue_display, 
            status_display, previousStatus_display, parent_key, parent_display, 
            components_display, sprint_display, epic_display, 
            previousStatusLastAssignee_display, originalEstimation, spent, 
            tags, estimation, checklistDone, checklistTotal, emailCreatedBy,
            sla, emailTo, emailFrom, lastCommentUpdatedAt, followers, 
            pendingReplyFrom, `end`, `start`, project_display, 
            votedBy_display, aliases, previousQueue_display, access, 
            resolvedAt, resolvedBy_display, resolution_display, 
            lastQueue_display,
            row_number() over (partition by id order by updatedAt desc) as lvl
            FROM db1.''' + CH_ISSUES_TABLE + '''
        ) T WHERE T.lvl = 1;
    '''
    run_clickhouse_query(create_issues_view)

    create_changelog_view = '''
        CREATE OR REPLACE VIEW v_tracker_changelog AS
        SELECT id, issue_key, updatedAt, updatedBy_display, `type`,
        field_display, from_display, to_display, worklog
        FROM (
            SELECT organization_id, id, issue_key, updatedAt, updatedBy_display, `type`,
            field_display, from_display, to_display, worklog,
            row_number() over (partition by organization_id, id, field_display order by updatedAt desc) as lvl
            FROM db1.''' + CH_CHANGELOG_TABLE + '''
        ) T WHERE T.lvl = 1;
    '''
    run_clickhouse_query(create_changelog_view)

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

def upload_clickhouse_data(data, table_name):
    """
    Exec clickhouse query
    
    Arguments:
        data (string): Data to be uploaded in CSV format
    Returns:
        response from database
    """
    """
    url = 'https://{host}:8443/?database={db}'.format(
        host=os.environ['CH_HOST'],
        db=os.environ['CH_DB'])
    """
    query_dict = {
        'query': 'INSERT INTO ' + table_name + ' FORMAT TabSeparatedWithNames'
    }
    response = requests.post(CH_URL, data=data, params=query_dict, headers=AUTH, verify=CERT)
    result = response.text
    if response.status_code == 200:
        return result
    else:
        print(response.text)
        raise ValueError(response.text)

def upload_data_to_db(issues_df, changelog_df):
    """
    Upload two datafarems to database
    
    Arguments:
        issues_df (Dataframe): dataframe with tracker data
        changelog_df (Dataframe): dataframe with issues changelog data
    Returns:
        Nothing
    """
    #init_database(drop_table=False)
    #Prepare issues data to upload: escaping \n to allow fields with new lines be represented correctly in CSV format 
    issues_content = issues_df.replace("\n", "\\\n", regex=True).to_csv(index=False, sep='\t')
    issues_content = issues_content.encode('utf-8')
    #Prepare changelog data to upload: escaping \n to allow fields with new lines be represented correctly in CSV format 
    changelog_content = changelog_df.replace("\n", "\\\n", regex=True).to_csv(index=False, sep='\t')
    changelog_content = changelog_content.encode('utf-8')   
    upload_clickhouse_data(issues_content, CH_ISSUES_TABLE)
    upload_clickhouse_data(changelog_content, CH_CHANGELOG_TABLE)

def handler(event, context):
    init_database(drop_table=False)
    tracker_query_text = get_issues_query_text()
    tracker_isses_json_data = get_tracker_issue_list(TRACKER_API_URL_BASE_FOR_ISSUE_LIST, query_text=tracker_query_text)
    tracker_isses_changelog_json_data = get_tracker_issues_changelog(tracker_isses_json_data)
    tracker_issues_df_data = shape_issues_data(tracker_isses_json_data)
    tracker_issues_changelog_df_data = shape_issue_changelog_data(tracker_isses_changelog_json_data)
    upload_data_to_db(tracker_issues_df_data, tracker_issues_changelog_df_data)

if __name__ == "__main__":
    handler(None, None);