from datetime import datetime

import hubmapbags
import pandas as pd

# if running locally laptop set ncores to 2-4
from pandarallel import pandarallel
from tqdm import tqdm

ncores = 16
pandarallel.initialize(progress_bar=True, nb_workers=ncores)

token = "<this-is-my-token"

# get all dataset HuBMAP IDs
assay_types = hubmapbags.apis.get_assay_types(token=token)

datasets = []
for assay_type in tqdm(assay_types):
    hubmap_ids = hubmapbags.apis.get_ids(assay_type, token=token)
    datasets.extend(hubmap_ids)

df = pd.DataFrame(datasets)

# only care about those in state 'New'
df = df[df["status"] == "New"]


# get group name
def __get_group_name(hubmap_id):
    metadata = hubmapbags.apis.get_dataset_info(hubmap_id, instance="prod", token=token)
    return metadata["group_name"]


df["group_name"] = df["hubmap_id"].parallel_apply(__get_group_name)


# get local directory
def __get_directory(hubmap_id):
    return hubmapbags.apis.get_directory(hubmap_id, instance="prod", token=token)


df["directory"] = df["hubmap_id"].parallel_apply(__get_directory)
df


# get dataset type
def __get_dataset_type(hubmap_id):
    return hubmapbags.apis.get_dataset_type(hubmap_id, instance="prod", token=token)


df["dataset_type"] = df["hubmap_id"].parallel_apply(__get_dataset_type)
df


# get timestamp
def __get_timestamp(hubmap_id):
    metadata = hubmapbags.apis.get_dataset_info(hubmap_id, instance="prod", token=token)
    return datetime.fromtimestamp(metadata["created_timestamp"] / 1000.0)


df["created_datetime"] = df["hubmap_id"].parallel_apply(__get_timestamp)

# pretty print and save
df = df.sort_values("created_datetime")
df.to_csv("report.tsv", sep="\t", index=False)
print(df.to_markdown())
