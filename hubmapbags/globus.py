import pandas as pd
import datetime
import urllib
import math
import requests
from . import uuids
from . import apis

def __prepare_dataframe( hubmap_id, data, instance='prod', token=None ):
    df = pd.DataFrame( data )
    metadata = apis.get_dataset_info( hubmap_id, token=token, instance='prod' )

    df['contains_human_genetic_sequences'] = metadata['contains_human_genetic_sequences']
    df['data_types'] = metadata['data_types'][0]
    df['dataset_id'] = metadata['hubmap_id']
    df['dataset_uuid'] = metadata['uuid']
    df['status'] = metadata['status']
    df['doi'] = metadata['doi_url']

    if 'base_dir' in df.keys():
        df.drop(['base_dir'], axis=1, inplace = True)

    if 'globus_ready' not in df.keys():
        df['globus_ready'] = None

    if 'globus_status_code' not in df.keys():
        df['globus_status_code'] = None

    if 'globus_timestamp' not in df.keys():
        df['globus_timestamp'] = None

    if 'globus_login' not in df.keys():
        df['globus_timestamp'] = None

    return df

def __get_globus_link( dataset_uuid, path ):
    url = 'https://g-d00e7b.09193a.5898.dn.glob.us/7277b2460f9c548004496508684a90ef/' + dataset_uuid + '/' +  path
    return url

def __check_globus_link( url ):
    response = requests.request('HEAD', url )
    return response

def __globus_populate( df, index ):
    datum = df.loc[index]

    if datum['globus_ready'] is None or math.isnan(datum['globus_ready']):
        datum['globus'] = __get_globus_link( datum['dataset_uuid'], datum['path'] )
        response = __check_globus_link( datum['globus'] )
        if response.status_code == 200:
            status = True
        else:
            status = False

        if response.url.find('auth.globus.org') and response.url.find('prompt=login'):
            df.at[index,'globus_login'] = True
        else:
            df.at[index,'globus_login'] = False

        df.at[index,'globus'] = datum['globus']
        df.at[index,'globus_ready'] = status
        df.at[index,'globus_status_code'] = int(response.status_code)
        df.at[index,'globus_timestamp'] = datetime.datetime.now()

    return df

def get_dataset_info( hubmap_id, instance='prod', token=None ):
    data = uuids.get_uuids( hubmap_id, instance=instance, token=token )
    df = __prepare_dataframe( hubmap_id, data, instance=instance, token=token )

    for index in range(len(df)):
        df = __globus_populate( df, index )

    return df