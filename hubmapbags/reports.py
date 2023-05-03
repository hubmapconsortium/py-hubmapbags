from datetime import datetime
from pathlib import Path

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from . import apis, utilities


def __is_protected(hubmap_id, token=None):
    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
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
    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["group_name"]
    except:
        return None


def __get_data_type(hubmap_id, token=None):
    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["data_types"]
    except:
        return None


def __get_dataset_type(hubmap_id, token=None):
    try:
        return apis.get_dataset_type(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
    except:
        return None


def daily(token: str, ncores=16) -> pd.DataFrame:
    """
    Creates daily report and returns a helpful dataframe
    """

    now = datetime.now()
    report_output_directory = "daily-report"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )

    if Path(report_output_filename).exists():
        df = pd.read_csv(report_output_filename, sep="\t")
        return df
    else:
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
        df["dataset_type"] = df["hubmap_id"].parallel_apply(
            __get_dataset_type, token=token
        )

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

        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        try:
            df.to_csv(report_output_filename, sep="\t", index=False)
        except:
            print(f"Unable to save dataframe to {report_output_filename}.")
        return df


def daily2(token: str, ncores=16) -> pd.DataFrame:
    """
    Creates daily report and returns a helpful dataframe
    """

    now = datetime.now()
    report_output_directory = "daily-report"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )

    if Path(report_output_filename).exists():
        df = pd.read_csv(report_output_filename, sep="\t")
        return df
    else:
        pandarallel.initialize(progress_bar=True, nb_workers=ncores)

        utilities.pprint("Getting list of assay types")
        assay_types = apis.get_assay_types(token=token)
        datasets = []
        for assay_type in tqdm(assay_types):
            hubmap_ids = apis.get_ids(assay_type, token=token)
            datasets.extend(hubmap_ids)

        df = pd.DataFrame(datasets)

        utilities.pprint("Getting list of dataset IDs")
        for index, dataset in tqdm(df.iterrows()):
            hubmap_id = dataset["hubmap_id"]
            metadata = apis.get_dataset_info(hubmap_id, instance="prod", token=token)

            if "contains_human_genetic_sequences" in metadata.keys():
                df.loc["is_protected", index] = metadata[
                    "contains_human_genetic_sequences"
                ]
            else:
                df.loc["is_protected", index] = None

            if "group_name" in metadata.keys():
                df.loc["group_name", index] = metadata["group_name"]
            else:
                df.loc["group_name", index] = None

            if "published_timestamp" in metadata.keys():
                df.loc["published_timestamp", index] = datetime.fromtimestamp(
                    metadata["published_timestamp"] / 1000.0
                )
            else:
                df.loc["published_timestamp", index] = None

            if "created_timestamp" in metadata.keys():
                df.loc["created_timestamp", index] = datetime.fromtimestamp(
                    metadata["created_timestamp"] / 1000.0
                )
            else:
                df.loc["created_timestamp", index] = None

            if "data_types" in metadata.keys():
                df.loc["data_types", index] = str(metadata["data_types"])
            else:
                df.loc["data_types", index] = None

            if metadata["direct_ancestors"][0]["entity_type"] == "Sample":
                df.loc["data_types", index] = "Primary"
            elif metadata["direct_ancestors"][0]["entity_type"] == "Dataset":
                df.loc["data_types", index] = "Derived"
            else:
                return "Unknown"

        print("Sorting dataframe")
        df = df.sort_values("published_datetime", ascending=False)

        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        try:
            df.to_csv(report_output_filename, sep="\t", index=False)
        except:
            print(f"Unable to save dataframe to {report_output_filename}.")
        return df


def daily3(token: str, ncores=5) -> pd.DataFrame:
    """
    Creates daily report and returns a helpful dataframe
    """

    now = datetime.now()
    report_output_directory = "daily-report"
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )

    if Path(report_output_filename).exists():
        df = pd.read_csv(report_output_filename, sep="\t")
        return df
    else:
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
        df["dataset_type"] = df["hubmap_id"].parallel_apply(
            __get_dataset_type, token=token
        )

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

        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        try:
            df.to_csv(report_output_filename, sep="\t", index=False)
        except:
            print(f"Unable to save dataframe to {report_output_filename}.")
        return df
