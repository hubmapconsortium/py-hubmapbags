from datetime import datetime
from pathlib import Path

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from . import apis, utilities


def __is_protected(hubmap_id, token=None):
    metadata = apis.get_dataset_info(hubmap_id, instance="prod", token=token)
    try:
        return metadata["contains_human_genetic_sequences"]
    except:
        return None


def __get_published_timestamp(hubmap_id, token=None):
    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return datetime.fromtimestamp(metadata["published_timestamp"] / 1000.0)
    except:
        return None


def __get_created_timestamp(hubmap_id, token=None):
    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return datetime.fromtimestamp(metadata["created_timestamp"] / 1000.0)
    except:
        return None


def __get_group_name(hubmap_id, token=None):
    metadata = apis.get_dataset_info(hubmap_id, instance="prod", token=token)
    return metadata["group_name"]


def __get_data_type(hubmap_id, token=None):
    metadata = apis.get_dataset_info(hubmap_id, instance="prod", token=token)
    return metadata["data_types"]


def __get_dataset_type(hubmap_id, token=None):
    return apis.get_dataset_type(hubmap_id, instance="prod", token=token)


def daily(token: str) -> pd.DataFrame:
    """
    Creates daily report and returns a helpful dataframe
    """

    ncores = 16
    pandarallel.initialize(progress_bar=True, nb_workers=ncores)

    utilities.pprint("Getting list of assay types")
    assay_types = apis.get_assay_types(token=token)

    print("Getting list of dataset IDs")
    datasets = []
    for assay_type in tqdm(assay_types):
        hubmap_ids = apis.get_ids(assay_type, token=token)
        datasets.extend(hubmap_ids)

    df = pd.DataFrame(datasets)

    utilities.pprint("Getting group name")
    df["group_name"] = df["hubmap_id"].parallel_apply(__get_group_name, token=token)

    utilities.pprint("Getting data type")
    df["data_type"] = df["hubmap_id"].parallel_apply(__get_data_type, token=token)

    utilities.pprint("Get dataset type")
    df["dataset_type"] = df["hubmap_id"].parallel_apply(__get_dataset_type, token=token)

    utilities.pprint("Getting creation timestamp")
    df["created_datetime"] = df["hubmap_id"].parallel_apply(
        __get_created_timestamp, token=token
    )

    utilities.pprint("Getting published timestamp")
    df["published_datetime"] = df["hubmap_id"].parallel_apply(
        __get_published_timestamp, token=token
    )

    utilities.pprint("Getting protected status")
    df["is_protected"] = df["hubmap_id"].parallel_apply(__is_protected, token=token)

    print("Sorting dataframe")
    df = df.sort_values("published_datetime", ascending=False)

    report_output_directory = "daily-report"
    if not Path(report_output_directory).exists():
        Path(report_output_directory).mkdir()

    now = datetime.now()
    report_output_filename = (
        report_output_directory + "/" + str(now.strftime("%Y%m%d")) + ".tsv"
    )

    try:
        df.to_csv(report_output_filename, sep="\t", index=False)
    except:
        print(f"Unable to save dataframe to {report_output_filename}.")
    return df
