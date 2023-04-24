import hubmapbags
from datetime import datetime
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from warnings import warn as warning
from datetime import datetime

token = 'AgXd5N6dOqE3jqMw37g9xK4rzo6Wka2Nqde6x36yJY1aQNwerVhwCK6MYlykXyaGg4naeeYB6GzJQGt0Ew7xqHWnrv'
instance = 'prod' #default instance is test

# get assay types
assay_names = hubmapbags.get_assay_types()

report = pd.DataFrame()
for assay_name in assay_names:
    print(assay_name)
    datasets = pd.DataFrame(hubmapbags.get_hubmap_ids( assay_name=assay_name, token=token ))

    if datasets.empty:
        continue
    
    #clean up
    datasets = datasets[(datasets['data_type'] != 'image_pyramid')]
    datasets = datasets[(datasets['status'] == 'Published')]

    datasets['has_uuids'] = None
    datasets['number_of_uuids'] = None
    for index, datum in tqdm(datasets.iterrows()):
        datasets.loc[index, 'number_of_uuids'] = hubmapbags.uuids.get_number_of_uuids( datum['hubmap_id'], instance=instance, token=token )

        if datasets.loc[index, 'number_of_uuids'] == 0:
            datasets.loc[index, 'has_uuids'] = False
        else:
            datasets.loc[index, 'has_uuids'] = True
    
    if report.empty:
        report = datasets
    else:
        report = pd.concat( [report, datasets] )

now = datetime.now() 
directory = 'uuid-data-report'

if not Path(directory).exists():
    Path(directory).mkdir()
report.to_csv( directory + '/' + str(now.strftime('%Y%m%d')) + '.tsv', sep='\t', index=False )


