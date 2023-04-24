import hubmapbags
import pandas as pd
from tqdm import tqdm

token = ""
instance = "prod"  # default instance is test

id = "HBM974.JDVS.328"

print(hubmapbags.uuids.get_number_of_uuids(id, instance=instance, token=token))
