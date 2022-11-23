import hubmapbags
from datetime import datetime
from pathlib import Path
import pandas as pd
from tqdm import tqdm
from warnings import warn as warning

token = ''
instance = 'prod' #default instance is test

id = 'HBM974.JDVS.328'

print(hubmapbags.uuids.get_number_of_uuids(id, instance=instance, token=token))
