import hubmapbags
import pandas as pd
from pathlib import Path

token = '<this-is-my-token>'

#get all ids for a given assay_type
assay_type = 'SNARE-RNAseq2'
hubmap_ids = hubmapbags.apis.get_ids( assay_type, token=token )

#convert to dataframe
df = pd.DataFrame(hubmap_ids)

#i only care about those in 'New'
df = df[df['status'] == 'New']

#helper function to get datasets that belong to UCSD
def __get_group_name( hubmap_id ):
    metadata = hubmapbags.apis.get_dataset_info(hubmap_id, instance='prod', token=token)
    return metadata['group_name']

df['group_name'] = df['hubmap_id'].apply(__get_group_name)
df = df[df['group_name'] == 'University of California San Diego TMC']

#helper function that will get the directory for each dataset
def __get_directory( hubmap_id ):
    return hubmapbags.apis.get_directory( hubmap_id, instance='prod', token=token )
df['directory'] = df['hubmap_id'].apply(__get_directory)

#helper function that will check the number of files per dataset
def __is_empty(directory):
    files = list(Path(directory).glob('**/*'))
    if len(files) == 0:
        return True
    else:
        return False
    
df['globus_directory_empty'] = df['directory'].apply(__is_empty)

#pretty print
df['assay_type'] = assay_type
df=df[['assay_type','status','uuid','hubmap_id','globus_directory_empty']]
df=df.sort_values('globus_directory_empty')
print(df.to_markdown())