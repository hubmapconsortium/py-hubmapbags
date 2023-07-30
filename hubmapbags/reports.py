from datetime import datetime
from pathlib import Path
import os

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
    try:
        metadata = apis.get_dataset_info(hubmap_id, instance="prod", token=token)
        return metadata["group_name"]
    except:
        return None


def __get_data_type(hubmap_id, token=None):
    try:
        metadata = apis.get_dataset_info(hubmap_id, instance="prod", token=token)
        return metadata["data_types"]
    except:
        return None


def __get_dataset_type(hubmap_id, token=None):
    try:
        return apis.get_dataset_type(hubmap_id, instance="prod", token=token)
    except:
        return None


def daily(token: str, ncores=16, backup=True) -> pd.DataFrame:
    """
    Creates daily report and returns a helpful dataframe
    """

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

    utilities.pprint("\nGetting data type")
    df["data_type"] = df["hubmap_id"].parallel_apply(__get_data_type, token=token)

    utilities.pprint("\nGet dataset type")
    df["dataset_type"] = df["hubmap_id"].parallel_apply(__get_dataset_type, token=token)

    utilities.pprint("\nGetting creation timestamp")
    df["created_datetime"] = df["hubmap_id"].parallel_apply(
        __get_created_timestamp, token=token
    )

    utilities.pprint("\nGetting published timestamp")
    df["published_datetime"] = df["hubmap_id"].parallel_apply(
        __get_published_timestamp, token=token
    )

    utilities.pprint("\nGetting protected status")
    df["is_protected"] = df["hubmap_id"].parallel_apply(__is_protected, token=token)

    print("\nSorting dataframe")
    df = df.sort_values("published_datetime", ascending=False)

    # Remove duplicates
    original_length = len(df)
    df = df.drop_duplicates(subset=["hubmap_id"])
    if original_length - len(df) > 0:
        print(f"Duplicates have been removed from dataframe.")

    now = datetime.now()

    report_output_directory = "daily-report"
    if not Path(report_output_directory).exists():
        Path(report_output_directory).mkdir()
    report_output_filename = (
        f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
    )

    try:
        print(f"Saving report as {report_output_filename}")
        df.to_csv(report_output_filename, sep="\t", index=False)
    except:
        print(f"Unable to save dataframe to {report_output_filename}.")

    report_output_directory = "/hive/hubmap/bdbags/reports"
    if Path(report_output_directory).exists():
        report_output_filename = (
            f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
        )

        try:
            print(f"Saving report as {report_output_filename}")
            df.to_csv(report_output_filename, sep="\t", index=False)
        except:
            print(f"Unable to save dataframe to {report_output_filename}.")
    else:
        print(f"Output directory {report_output_directory} does not exist.")

    return df

    if backup:
        report_output_directory = "/hive/hubmap/bdbags/reports/"
        report_output_filename = (
            f'{report_output_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
        )

        if Path(report_output_directory).exists():
            print(f"Saving backup copy to {report_output_filename}")
            df.to_csv(report_output_filename, sep="\t", index=False)

            report_output_symlink = f"{report_output_directory}today.tsv"
            if Path(report_output_symlink).is_symlink():
                Path(report_output_symlink).unlink()

            try:
                os.symlink(report_output_filename, report_output_symlink)
                print(
                    f"Symlink created: {report_output_symlink} -> {report_output_filename}"
                )
            except OSError as e:
                print(f"Failed to create symlink: {e}")
