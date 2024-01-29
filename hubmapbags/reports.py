from datetime import datetime
from pathlib import Path

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from . import apis, plots, utilities


def __is_protected(hubmap_id, token=None):
    """
    Determine whether a given HuBMAP ID corresponds to a dataset containing human genetic sequences.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts the information indicating if the dataset contains human genetic sequences. If an error
    occurs during the retrieval process or the information is not found, the function returns None.

    :param hubmap_id: The HuBMAP ID for which the protection status is to be checked.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: True if the dataset associated with the HuBMAP ID contains human genetic sequences, False otherwise.
             Returns None if the information cannot be retrieved.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.
       - The "contains_human_genetic_sequences" key is assumed to be present in the metadata returned by the HuBMAP API.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["contains_human_genetic_sequences"]
    except:
        return None


def __get_published_timestamp(hubmap_id, token=None):
    """
    Retrieve the publication timestamp associated with a given HuBMAP ID.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts and converts the publication timestamp. If an error occurs during the retrieval process or
    the timestamp is not found, the function returns None.

    :param hubmap_id: The HuBMAP ID for which the publication timestamp is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The datetime representation of the publication timestamp associated with the HuBMAP ID if found, otherwise None.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.
       - The timestamp returned by the HuBMAP API is assumed to be in milliseconds since the Unix epoch.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return datetime.fromtimestamp(metadata["published_timestamp"] / 1000.0)
    except:
        return None


def __get_created_timestamp(hubmap_id, token=None):
    """
    Retrieve the creation timestamp associated with a given HuBMAP ID.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts and converts the creation timestamp. If an error occurs during the retrieval process or
    the timestamp is not found, the function returns None.

    :param hubmap_id: The HuBMAP ID for which the creation timestamp is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The datetime representation of the creation timestamp associated with the HuBMAP ID if found, otherwise None.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.
       - The timestamp returned by the HuBMAP API is assumed to be in milliseconds since the Unix epoch.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return datetime.fromtimestamp(metadata["created_timestamp"] / 1000.0)
    except:
        return None


def __get_group_name(hubmap_id, token=None):
    """
    Retrieve the group name associated with a given HuBMAP ID.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts the group name. If an error occurs during the retrieval process or the group name is not found,
    the function returns None.

    :param hubmap_id: The HuBMAP ID for which the group name is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The group name associated with the HuBMAP ID if found, otherwise None.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["group_name"]
    except:
        return None


def __get_data_type(hubmap_id, token=None):
    """
    Retrieve the data type associated with a given HuBMAP ID.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts the data type. If an error occurs during the retrieval process or the data type is not found,
    the function returns None.

    :param hubmap_id: The HuBMAP ID for which the data type is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The data type associated with the HuBMAP ID if found, otherwise None.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["data_types"]
    except:
        return None


def __get_dataset_type(hubmap_id, token=None):
    """
    Retrieve the dataset type for a given HuBMAP ID.

    This function calls the `get_dataset_type` function from the `apis` module to fetch the dataset type.
    If an error occurs during the retrieval process, the function returns None.

    :param hubmap_id: The HuBMAP ID for which the dataset type is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The dataset type if found, otherwise None.

    .. note::
       - The `apis.get_dataset_type` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.

    """

    try:
        return apis.get_dataset_type(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
    except:
        return None


def daily(token: str, ncores=16) -> pd.DataFrame:
    """
    Generate a daily report of datasets with details like group name, data type, creation timestamp,
    and more, for given assay types.

    This function fetches datasets associated with each assay type and compiles a dataframe with various
    details. The function then sorts the dataframe based on the published date and saves the output in
    a TSV (tab-separated values) format. Additionally, the function generates a plot based on group distributions.

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param ncores: Number of cores to be used for parallel processing. Default is 16.
    :type ncores: int, optional

    :return: A pandas DataFrame containing details for each dataset, sorted by published date.

    .. note::
       - The report is saved in the `daily-report` directory with the filename format YYYYMMDD.tsv
         (e.g., 20230804.tsv for August 4, 2023).
       - If the report for the current date exists, the function loads the report from the file
         instead of re-fetching all the data.

    .. warning::
       - Ensure that a valid token is provided to access the HuBMAP API.
       - The private helper functions (`__get_group_name`, `__get_data_type`, etc.)
         are assumed to exist and work properly.

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

        print("\nSorting dataframe")
        df = df.sort_values("published_datetime", ascending=False)

        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        try:
            df.to_csv(report_output_filename, sep="\t", index=False)
        except:
            print(f"Unable to save dataframe to {report_output_filename}.")

        hive_directory = "/hive/hubmap/bdbags/reports/"
        report_output_backup_file = (
            f'{hive_directory}/{str(now.strftime("%Y%m%d"))}.tsv'
        )

        symlink = "/hive/hubmap/bdbags/reports/today.tsv"
        if Path(symlink).exists():
            Path(symlink).unlink()
            Path(symlink).symlink_to(report_output_backup_file)
