from datetime import datetime
from pathlib import Path
import requests
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


def __has_sra_url(hubmap_id, token=None):
    """
    Determine whether a given HuBMAP ID as an existing value for the dbgap_sra_experiment_url metadata field.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts the information indicating if the dataset contains human genetic sequences. If an error
    occurs during the retrieval process or the information is not found, the function returns None.

    :param hubmap_id: The HuBMAP ID for which the protection status is to be checked.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The SRA experiment url for the dbgap study associated with the HuBMAP ID if found, otherwise None.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.
       - The dbgap_sra_experiment_url key is not present in the metadata returned by the HuBMAP API if the datasets is not associated with a dbGaP study.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["dbgap_sra_experiment_url"]
    except:
        return None


def __has_dbgap_url(hubmap_id, token=None):
    """
    Determine whether a given HuBMAP ID as an existing value for the dbgap_study_url metadata field.

    This function calls the `get_dataset_info` function from the `apis` module to fetch the dataset metadata and
    subsequently extracts the information indicating if the dataset contains human genetic sequences. If an error
    occurs during the retrieval process or the information is not found, the function returns None.

    :param hubmap_id: The HuBMAP ID for which the protection status is to be checked.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API. Default is None.
    :type token: str, optional

    :return: The url for the dbGaP study associated with the HuBMAP ID if found, otherwise None.

    .. note::
       - The `apis.get_dataset_info` function should be properly implemented and imported.
       - This function is designed as a private helper function for the primary `daily` report generation function.

    .. warning::
       - Ensure that a valid token is provided if required by the HuBMAP API.
       - The dbgap_study_url key is not present in the metadata returned by the HuBMAP API if the dataset is not associated with a dbGaP study.

    """

    try:
        metadata = apis.get_dataset_info(
            hubmap_id, instance="prod", token=token, overwrite=False
        )
        return metadata["dbgap_study_url"]
    except:
        return None


def daily() -> pd.DataFrame:
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
        url = "https://ingest.api.hubmapconsortium.org/datasets/data-status"  # The URL to get the data from
        try:
            response = requests.get(url)  # Send a request to the URL to get the data
            response.raise_for_status()  # Check if the request was successful (no errors)
            json_data = response.json()  # Convert the response to JSON format

            # Ensure 'data' key exists in the JSON
            if "data" in json_data:  # Check if the JSON contains the key 'data'
                df = pd.DataFrame(
                    json_data["data"]
                )  # Create a DataFrame using the data under 'data' key
            else:
                raise KeyError(
                    "'data' key not found in the JSON response"
                )  # Raise an error if 'data' key is missing
        except (
            ValueError,
            KeyError,
        ) as e:  # Catch errors related to value or missing keys
            print(f"Error loading data: {e}")  # Print the error message
            return pd.DataFrame()  # Return an empty DataFrame if there is an error
        except (
            requests.RequestException
        ) as e:  # Catch errors related to the request itself
            print(f"Request failed: {e}")  # Print the error message
            return pd.DataFrame()  # Return an empty DataFrame if the request fails

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

        return df
