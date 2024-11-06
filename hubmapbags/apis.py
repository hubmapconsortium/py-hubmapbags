import glob
import json
import os
from logging import warning
from pathlib import Path
from warnings import warn as warning
import traceback
from pprint import pprint
import pandas as pd
import requests
from tabulate import tabulate

from . import utilities
from . import reports


def is_primary(hubmap_id: str, token: str, instance: str = "prod") -> bool:
    """
    Determine if a given HuBMAP ID corresponds to a primary sample.

    This function fetches the metadata of the specified HuBMAP ID and determines
    if its entity type is "Sample". If an error occurs during the metadata retrieval,
    a warning is raised with the error message.

    :param hubmap_id: The HuBMAP ID to be checked.
    :type hubmap_id: str

    :param token: Authentication token to access the HuBMAP data.
    :type token: str

    :param instance: Instance of the HuBMAP service, default is "prod".
    :type instance: str, optional

    :return: Returns True if the HuBMAP ID corresponds to a primary sample, else returns False.
    :rtype: bool

    .. warning::
       - An error in metadata retrieval will raise a warning with the respective error message.
    """

    metadata = get_ancestors_info(hubmap_id, instance=instance, token=token)
    if "entity_type" in metadata[0].keys() and metadata[0]["entity_type"] == "Sample":
        return True
    else:
        if "error" in metadata[0]:
            warning(metadata[0]["error"])
        return False


def __compute_number_of_files(directory: str) -> int:
    """
    Compute the total number of files in a specified local directory.

    This helper function calculates the total number of files recursively
    within the specified directory.

    :param directory: The local directory path for which the number of files is to be counted.
    :type directory: str

    :return: The total number of files found in the specified directory.
    :rtype: int
    """

    pathname = directory + "/**/*"
    files = glob.glob(pathname, recursive=True)

    return len(files)


def __check_if_folder_is_empty(directory: str) -> bool:
    """
    Check if the specified local directory is empty.

    This helper function determines if the given directory is empty by checking its contents.

    :param directory: The local directory path which needs to be checked for emptiness.
    :type directory: str

    :return: Returns True if the directory is empty, otherwise False.
    :rtype: bool
    """

    if not os.listdir(directory):
        return True
    else:
        return False


def __get_instance(instance: str) -> str:
    """
    Retrieve the appropriate instance value based on the provided input.

    This helper function determines the correct instance suffix or name based on a provided keyword (e.g., "dev", "prod", or "test").

    :param instance: The desired instance keyword. Acceptable values are "dev", "prod", and "test".
    :type instance: str

    :return: Returns the corresponding instance value. If an unknown option is provided, a warning is raised and the default value ".test" is returned.
    :rtype: str

    .. warning::
       - If an unknown option is provided, a warning is raised.
    """

    if instance.lower() == "dev":
        return ".dev"
    elif instance.lower() == "prod":
        return "prod"
    elif instance.lower() == "test":
        return ".test"
    else:
        warning("Unknown option " + str(instance) + ". Setting default value to test.")
        return ".test"


def __query_ancestors_info(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    """
    Query and retrieve ancestor information for a given HuBMAP ID.

    This helper function fetches the ancestor information of a specified HuBMAP ID
    by making a request to the HuBMAP entity API.

    :param hubmap_id: The HuBMAP ID for which ancestor information is needed.
    :type hubmap_id: str

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param instance: Instance of the HuBMAP service, default is "prod". Acceptable values are "dev", "prod", and "test".
    :type instance: str, optional

    :param debug: If True, will output additional debug information. Default is False.
    :type debug: bool, optional

    :return: Returns a dictionary containing the response from the HuBMAP entity API.
    :rtype: dict

    .. warning::
       - If the token is not set or is invalid, a warning is raised.
    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    if __get_instance(instance) == "prod":
        URL = f"https://entity.api.hubmapconsortium.org/ancestors/{hubmap_id}"
    else:
        URL = (
            "https://entity-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/ancestors/"
            + hubmap_id
        )

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    r = requests.get(URL, headers=headers)
    return r


def get_ancestors_info(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = True,
    debug: bool = False,
) -> dict:
    """
    Retrieve ancestor information for a given HuBMAP ID.

    This function fetches the ancestor information of a specified HuBMAP ID.
    It checks if a cached version exists locally and, if not (or if overwrite is set to True),
    it queries the HuBMAP entity API to fetch the data.

    :param hubmap_id: The HuBMAP ID for which ancestor information is needed.
    :type hubmap_id: str

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param instance: Instance of the HuBMAP service, default is "prod". Acceptable values are "dev", "prod", and "test".
    :type instance: str, optional

    :param overwrite: If set to True, will fetch and overwrite the cached version even if it exists. Default is True.
    :type overwrite: bool, optional

    :param debug: If True, will output additional debug information. Default is False.
    :type debug: bool, optional

    :return: Returns a dictionary containing the ancestor information for the given HuBMAP ID.
    :rtype: dict

    .. warning::
       - If the returned JSON object is empty, a warning is raised.
       - If the API request does not return expected data, a warning is raised with the response message.
    """

    directory = ".ancestors"
    file = os.path.join(directory, hubmap_id + ".json")
    if os.path.exists(file) and not overwrite:
        if debug:
            print("Loading existing JSON file.")
        j = json.load(open(file, "r"))
    else:
        if debug:
            print("Get information from ancestors via the entity-api.")
        r = __query_ancestors_info(
            hubmap_id, instance=instance, token=token, debug=debug
        )
        j = json.loads(r.text)

    if j is None:
        warning("JSON object is empty.")
        return j
    elif "message" in j:
        warning("Request response is empty. Not populating dataframe.")
        print(j["message"])
        return None
    else:
        if not os.path.exists(directory):
            os.mkdir(directory)
        with open(file, "w") as outfile:
            json.dump(j, outfile, indent=4)
        return j


def __query_provenance_info(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    """
    Query and retrieve provenance information for a given HuBMAP ID.

    This helper function fetches the provenance information of a specified HuBMAP ID
    by making a request to the HuBMAP entity API.

    :param hubmap_id: The HuBMAP ID for which provenance information is needed.
    :type hubmap_id: str

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param instance: Instance of the HuBMAP service, default is "prod". Acceptable values are "dev", "prod", and "test".
    :type instance: str, optional

    :param debug: If True, will output additional debug information. Default is False.
    :type debug: bool, optional

    :return: Returns a dictionary containing the response from the HuBMAP entity API regarding the provenance information.
    :rtype: dict

    .. warning::
       - If the token is not set or is invalid, a warning is raised.
    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    if __get_instance(instance) == "prod":
        URL = f"https://entity.api.hubmapconsortium.org/datasets/{hubmap_id}/prov-info?format=json"
    else:
        URL = (
            "https://entity-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/datasets/"
            + hubmap_id
            + "/prov-info?format=json"
        )

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}
    r = requests.get(URL, headers=headers)
    return r


def __query_dataset_info(hubmap_id: str, token: str, debug: bool = False) -> dict:
    """
    Query and retrieve dataset information for a given HuBMAP ID.

    This helper function fetches the dataset information of a specified HuBMAP ID
    by making a request to the HuBMAP entity API.

    :param hubmap_id: The HuBMAP ID for which dataset information is required.
    :type hubmap_id: str

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param instance: Instance of the HuBMAP service, default is "prod". Acceptable values are "dev", "prod", and "test".
    :type instance: str, optional

    :param debug: If True, will output additional debug information. Default is False.
    :type debug: bool, optional

    :return: Returns a dictionary containing the response from the HuBMAP entity API regarding the dataset information.
    :rtype: dict

    .. warning::
       - If the token is not set or is invalid, a warning is raised.
    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    URL = f"https://entity.api.hubmapconsortium.org/entities/{hubmap_id}"

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    r = requests.get(URL, headers=headers)
    return r


def get_dataset_info(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = True,
    debug: bool = True,
) -> dict:

    directory = ".datasets"
    file = os.path.join(directory, hubmap_id + ".json")
    if os.path.exists(file) and not overwrite:
        j = json.load(open(file, "r"))
    else:
        r = __query_dataset_info(hubmap_id, token=token, debug=debug)
        if r is None:
            warning("JSON object is empty.")
            return r
        j = json.loads(r.text)

    if "message" in j:
        warning("Request response is empty. Not populating dataframe.")
        print(j["message"])
        return None
    else:
        if not os.path.exists(directory):
            os.mkdir(directory)
        with open(file, "w") as outfile:
            json.dump(j, outfile, indent=4)
        return j


def get_provenance_info(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = False,
    debug: bool = False,
) -> dict:
    """
    Retrieve provenance information for a given HuBMAP ID.

    Fetches the provenance information of a specified HuBMAP ID either
    from a local JSON file or by querying the HuBMAP entity API.

    :param hubmap_id: The HuBMAP ID for which provenance information is needed.
    :type hubmap_id: str

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param instance: Instance of the HuBMAP service. Default is "prod". Valid options are "dev", "prod", and "test".
    :type instance: str, optional

    :param overwrite: If True, it will overwrite any existing local JSON file with new data from the API. Default is False.
    :type overwrite: bool, optional

    :param debug: If True, will print additional debug information. Default is False.
    :type debug: bool, optional

    :return: A dictionary containing provenance information. If an error occurs, appropriate warnings are provided.
    :rtype: dict

    .. warning::
       - If the request response from the API is empty or an error, a warning is raised and the function might return None.
    """

    directory = ".provenance"
    file = os.path.join(directory, hubmap_id + ".json")
    if os.path.exists(file) and not overwrite:
        if debug:
            print("Loading existing JSON file")
        j = json.load(open(file, "r"))
    else:
        if debug:
            print("Get information provenance info via the entity-api.")
        r = __query_provenance_info(
            hubmap_id, instance=instance, token=token, debug=debug
        )
        j = json.loads(r.text)

    if j is None:
        warning("JSON object is empty.")
        return j
    elif "message" in j:
        warning("Request response is empty. Not populating dataframe.")
        print(j["message"])
        return None
    else:
        if not os.path.exists(directory):
            os.mkdir(directory)
        with open(file, "w") as outfile:
            json.dump(j, outfile, indent=4)
        return j


def get_all_ids(token: str, debug: bool = False) -> pd.DataFrame:
    """
    Retrieve all HuBMAP IDs for given assay types.

    Fetches all HuBMAP IDs for specified assay types using the HuBMAP entity API
    and compiles them into a pandas DataFrame.

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param debug: If True, will print additional debug information. Default is False.
    :type debug: bool, optional

    :return: A DataFrame containing all HuBMAP IDs for the given assay types.
    :rtype: pd.DataFrame

    .. warning::
       - If the token is not set or invalid, a warning is raised and the function might return None.
    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    datasets = []
    assay_types = get_assay_types(token=token)
    for assay_type in assay_types:
        hubmap_ids = get_ids(assay_type, instance="prod", token=token)
        datasets.extend(hubmap_ids)

    return pd.DataFrame(datasets)


def get_ids(assay_name: str, token: str, debug: bool = False) -> dict:
    """
    Retrieve HuBMAP IDs for a specific assay type.

    Uses the HuBMAP entity API to fetch HuBMAP IDs associated with a given assay type.

    :param assay_name: The name of the assay for which IDs are to be retrieved.
    :type assay_name: str

    :param token: Authentication token to access the HuBMAP entity API.
    :type token: str

    :param debug: If True, will print additional debug information. Default is False.
    :type debug: bool, optional

    :return: A list of dictionaries containing 'uuid', 'hubmap_id', and 'status' for each HuBMAP ID of the given assay type.
    :rtype: list[dict]

    .. warning::
       - If the token is not set or invalid, a warning is raised and the function might return None.
       - If an error is present in the API response, a warning is raised with the error message.

    .. note::
       - This function internally relies on another utility function, '__query_hubmap_ids', to perform the API query.
    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    answer = __query_hubmap_ids(assay_name, token=token, debug=debug)

    if "error" in answer.keys():
        warning(answer["error"])
        return None

    data = answer["hits"]["hits"]

    results = []
    for datum in data:
        row = pd.DataFrame(
            {
                "uuid": datum["_source"]["uuid"],
                "hubmap_id": datum["_source"]["hubmap_id"],
                "status": datum["_source"]["status"],
            }
        )
        results = pd.concat([results, row], ignore_index=True)

    return results



def get_hubmap_ids(
    assay_name: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:

    df = reports.daily()
    df = df[df['dataset_type']==assay_name]

    is_protected = lambda s: s == 'protected'

    df['is_protected'] = df['data_access_level'].apply(is_protected)
    df = df[['uuid','hubmap_id','status','is_primary','is_protected','dataset_type','group_name']] 
    df = df.rename(columns={"dataset_type": "data_type"})

    return df


def __query_hubmap_ids(assayname: str, token: str, debug: bool = False) -> dict:
    """
    Send a query to the HuBMAP API to fetch HuBMAP IDs for a given assay type.

    This function communicates directly with the HuBMAP search API to fetch HuBMAP IDs for a specific assay type.
    The response contains essential information such as 'hubmap_id', 'uuid', 'group_name', 'status', and 'data_types'.

    :param assayname: The name of the assay for which IDs and metadata are to be retrieved.
    :type assayname: str

    :param token: Authentication token to access the HuBMAP search API. If token is None, the request will not contain
                  an "Authorization" header.
    :type token: str

    :param debug: If True, will print additional debug information. Default is False. Currently unused in the function
                  but can be utilized for future enhancements.
    :type debug: bool, optional

    :return: A dictionary containing the API response, which includes the search hits and associated metadata.
    :rtype: dict

    .. note::
       - The function targets the '/v3/search' endpoint of the HuBMAP search API.
       - The size parameter in the body of the request is set to 500, which means it will retrieve up to 500 results.
         Adapt this number if a different number of results is desired.

    .. warning::
       - If the token is not valid or if there's any issue with the request, the API might return an error message
         within the response dictionary.
    """

    url = "https://search.api.hubmapconsortium.org/v3/search"

    if token is None:
        headers = {"Accept": "application/json"}
    else:
        headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    body = {
        "size": 500,
        "_source": {
            "include": ["hubmap_id", "uuid", "group_name", "status", "data_types"]
        },
        "query": {
            "bool": {
                "must": [{"match_phrase": {"data_types": assayname}}],
                "filter": [{"match": {"entity_type": "Dataset"}}],
            }
        },
    }

    data = requests.post(url=url, headers=headers, json=body).json()
    return data


def __is_valid(file: str) -> str:
    """
    Check if a file contains the string "No error" to determine its validity.

    The function reads the content of a file and searches for the substring "No error". If found, it is considered
    as a valid file, otherwise it is deemed invalid.

    :param file: The path to the file that needs to be checked.
    :type file: str

    :return: Returns "VALID" if the file contains the string "No error", otherwise returns "INVALID".
    :rtype: str

    .. note::
       - Ensure that the provided file path exists and is readable to avoid any file read errors.
       - The function currently checks for the exact string "No error". Any variation in the case or spacing will
         not be matched.

    .. warning::
       - Do not forget to close the file after reading to release resources.
    """

    string1 = "No error"
    file1 = open(file, "r")
    readfile = file1.read()

    answer = "INVALID"
    if string1 in readfile:
        answer = "VALID"

    file1.close()
    return answer


def is_protected(hubmap_id: str, token: str, instance: str = "prod") -> bool:
    """
    Check if a given dataset, identified by its hubmap_id, has a protected data access level.

    This function fetches metadata information about a dataset and checks if its data access level is marked as
    'protected'. If the dataset is protected, the function returns True, otherwise, it returns False.

    :param hubmap_id: The unique identifier for the HubMap dataset to be checked.
    :type hubmap_id: str

    :param token: The authentication token to access the HubMap API.
    :type token: str

    :param instance: The instance of the HubMap service to target, defaulting to "prod".
    :type instance: str, optional

    :return: Returns True if the dataset has a data access level of 'protected', otherwise returns False.
    :rtype: bool

    .. note::
       - A valid authentication token is required to access the HubMap API.
       - The function utilizes the utilities module for token validation.
       - Ensure the provided hubmap_id corresponds to an existing dataset on HubMap.

    .. warning::
       - This function assumes the HubMap API returns 'data_access_level' in its response. If the response format changes
         in the future, this function might not work as expected.

    .. example::
       >>> is_protected("abcd1234", "sample_token")
       False
    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    metadata = get_dataset_info(hubmap_id, instance=instance, token=token)
    if (
        "data_access_level" in metadata.keys()
        and metadata["data_access_level"] == "protected"
    ):
        return True
    else:
        return False


def pretty_print_info_about_new_datasets(assay_name: str, debug: bool = False) -> None:
    """
    Pretty print a tabulated summary of new datasets of a given assay type.

    This function fetches datasets of a specified assay type using the HubMap API, and then generates a table
    showcasing specific information about datasets with a status of "New". Information includes UUID, HuBMAP ID,
    directory location, status, number of files, associated metadata, contributors, antibodies, and validation status.

    :param assay_name: The assay type for which the datasets should be fetched.
    :type assay_name: str

    :param debug: If True, debug statements will be printed. Default is False.
    :type debug: bool, optional

    :return: None. The function will print the generated table to the console.

    .. note::
       - Assumes access to the specified directories on the system.
       - The function uses the `tabulate` library to generate a table for display.
       - Assumes the HubMap API returns datasets with the specified structure. If the response format changes
         in the future, this function might not work as expected.

    .. warning::
       - Directory paths are hard-coded and might need adjustment based on the environment or system changes.

    .. example::
       >>> pretty_print_info_about_new_datasets("RNA-seq")
       +--------+-------------+-----------+---------+------------------+------------------+-----------------+---------------+---------------------+
       | UUID   | HuBMAP ID   | Directory | Status  | Number of files  | Metadata TSV     | Contributors TSV| Antibodies TSV| Validation status   |
       +--------+-------------+-----------+---------+------------------+------------------+-----------------+---------------+---------------------+
       |...     |...          |...        |...      |...               |...               |...              |...            |...                  |
       +--------+-------------+-----------+---------+------------------+------------------+-----------------+---------------+---------------------+

    """

    if debug:
        print("Using search API to retrieve datasets with data type " + assay_name)
    answer = get_hubmap_ids(assay_name, debug=False)

    data = [
        [
            "UUID",
            "HuBMAP ID",
            "Directory",
            "Status",
            "Number of files",
            "Metadata TSV",
            "Contributors TSV",
            "Antibodies TSV",
            "Validation status",
        ]
    ]
    for datum in answer:
        if debug:
            print("Processing dataset " + datum["uuid"])

        if datum["status"] == "New":
            if Path(
                "/hive/hubmap/data/consortium/"
                + datum["group_name"]
                + "/"
                + datum["uuid"]
            ).exists():
                directory = (
                    "/hive/hubmap/data/consortium/"
                    + datum["group_name"]
                    + "/"
                    + datum["uuid"]
                    + "/"
                )
                number_of_files = __compute_number_of_files(directory)
                metadata = glob.glob(directory + "*metadata*.tsv")
                if not metadata:
                    metadata = ""
                else:
                    metadata = metadata[0].replace(directory, "")

                contributors = glob.glob(directory + "*contributors*.tsv")
                if not contributors:
                    contributors = ""
                else:
                    contributors = contributors[0].replace(directory, "")

                antibodies = glob.glob(directory + "/*antibodies*.tsv")
                if not antibodies:
                    antibodies = ""
                else:
                    antibodies = antibodies[0].replace(directory, "")

                report = glob.glob(directory + "validation_report.txt")
                if not report:
                    report = "-"
                else:
                    report = __is_valid(directory + "validation_report.txt")
            else:
                directory = "NOT AVAILABLE"
                number_of_files = "-"
                metadata = ""
                contributors = ""
                antibodies = ""

            row = pd.DataFrame(
                [
                    datum["uuid"],
                    datum["hubmap_id"],
                    directory,
                    datum["status"],
                    number_of_files,
                    metadata,
                    contributors,
                    antibodies,
                    report,
                ]
            )

            data = pd.concat([data, row], ignore_index=True)

    table = tabulate(data, headers="firstrow", tablefmt="grid")
    utilities.pprint(assay_name)
    print(table)


def pretty_print_info_about_all_new_datasets(
    filename: str, debug: bool = False
) -> None:
    """
    Pretty print a tabulated summary of new datasets for all assay types.

    This function fetches datasets of all available assay types using the HubMap API, and then generates a table
    showcasing specific information about datasets with a status of "New". Information includes UUID, HuBMAP ID,
    assay type, directory location, status, folder emptiness, associated metadata, contributors, antibodies,
    and validation status.

    :param filename: Path to the file where the tabulated information will be saved. If not provided,
                     the table will just be printed to the console.
    :type filename: str

    :param debug: If True, debug statements will be printed. Default is False.
    :type debug: bool, optional

    :return: None. The function will print the generated table to the console and/or save it to the provided file.

    .. note::
       - Assumes access to the specified directories on the system.
       - The function uses the `tabulate` library to generate a table for display and `pandas` to save the table to file.
       - Assumes the HubMap API returns datasets with the specified structure. If the response format changes
         in the future, this function might not work as expected.

    .. warning::
       - Directory paths are hard-coded and might need adjustment based on the environment or system changes.

    .. example::
       >>> pretty_print_info_about_all_new_datasets("output.tsv", debug=True)
       +--------+-------------+-----------+---------+------------------+------------------+------------------+-----------------+---------------+---------------------+
       | UUID   | HuBMAP ID   | Assay Type| Status  | Directory        | Is empty?        | Metadata TSV     | Contributors TSV| Antibodies TSV| Validation status   |
       +--------+-------------+-----------+---------+------------------+------------------+-----------------+---------------+---------------------+
       |...     |...          |...        |...      |...               |...               |...               |...              |...            |...                  |
       +--------+-------------+-----------+---------+------------------+------------------+-----------------+---------------+---------------------+

    """

    if debug:
        utilities.pprint("Retrieving list of assay types")
    assay_types = get_assay_types()

    answer = []
    for assay_type in assay_types:
        if debug:
            print("Using search API to retrieve datasets with data type " + assay_type)
        answer.extend(get_hubmap_ids(assay_type, debug=debug))

    data = [
        [
            "UUID",
            "HuBMAP ID",
            "Assay Type",
            "Status",
            "Directory",
            "Is empty?",
            "Metadata TSV",
            "Contributors TSV",
            "Antibodies TSV",
            "Validation status",
        ]
    ]
    if debug:
        utilities.pprint("Processings datasets")

    for datum in answer:
        if debug:
            print("Processing dataset " + datum["uuid"])
        if datum["status"] == "New":
            if Path(
                "/hive/hubmap/data/consortium/"
                + datum["group_name"]
                + "/"
                + datum["uuid"]
            ).exists():
                directory = (
                    "/hive/hubmap/data/consortium/"
                    + datum["group_name"]
                    + "/"
                    + datum["uuid"]
                    + "/"
                )
                metadata = glob.glob(directory + "*metadata*.tsv")
                if not metadata:
                    metadata = ""
                else:
                    metadata = metadata[0].replace(directory, "")

                contributors = glob.glob(directory + "*contributors*.tsv")
                if not contributors:
                    contributors = ""
                else:
                    contributors = contributors[0].replace(directory, "")

                antibodies = glob.glob(directory + "/*antibodies*.tsv")
                if not antibodies:
                    antibodies = ""
                else:
                    antibodies = antibodies[0].replace(directory, "")

                report = glob.glob(directory + "validation_report.txt")
                if not report:
                    report = "-"
                else:
                    report = __is_valid(directory + "validation_report.txt")

                is_empty = __check_if_folder_is_empty(directory)
            else:
                directory = "NOT AVAILABLE"
                is_empty = ""
                number_of_files = "-"
                metadata = ""
                contributors = ""
                antibodies = ""

            data.append(
                [
                    datum["uuid"],
                    datum["hubmap_id"],
                    datum["data_type"],
                    datum["status"],
                    directory,
                    is_empty,
                    metadata,
                    contributors,
                    antibodies,
                    report,
                ]
            )

    if filename:
        df = pd.DataFrame(
            data[1:],
            columns=[
                "UUID",
                "HuBMAP ID",
                "Assay Type",
                "Status",
                "Directory",
                "Is empty?",
                "Metadata TSV",
                "Contributors TSV",
                "Antibodies TSV",
                "Validation status",
            ],
        )
        df.to_csv(filename, sep="\t", index=False)

    table = tabulate(data, headers="firstrow", tablefmt="grid")
    print(table)


def pretty_print_hubmap_ids(assay_name: str, debug: bool = False) -> None:
    """
    Pretty print a tabulated summary of HubMap IDs for a given assay type.

    This function fetches datasets for a specific assay type using the HubMap API and then generates a table
    showcasing specific information about those datasets, including their UUID, HuBMAP ID, associated group name,
    and current status.

    :param assay_name: The name of the assay type for which the datasets are to be fetched.
    :type assay_name: str

    :param debug: If True, debug statements will be printed. Default is False.
    :type debug: bool, optional

    :return: None. The function will print the generated table to the console.

    .. note::
       - The function uses the `tabulate` library to generate a table for display.
       - Assumes the HubMap API returns datasets with the specified structure. If the response format changes
         in the future, this function might not work as expected.

    .. example::
       >>> pretty_print_hubmap_ids("RNA-seq", debug=True)
       +--------+-------------+------------+---------+
       | UUID   | HuBMAP ID   | Group Name | Status  |
       +--------+-------------+------------+---------+
       |...     |...          |...         |...      |
       +--------+-------------+------------+---------+

    """

    answer = get_hubmap_ids(assay_name, debug=False)

    data = [["UUID", "HuBMAP ID", "Group Name", "Status"]]
    for datum in answer:
        data.append(
            [datum["uuid"], datum["hubmap_id"], datum["group_name"], datum["status"]]
        )

    table = tabulate(data, headers="firstrow", tablefmt="grid")
    print(table)


def get_directory(hubmap_id: str, token: str, instance: str = "prod") -> str:
    """
    Retrieve the directory path for a given HuBMAP dataset ID.

    Depending on whether the dataset contains human genetic sequences or not, the function will return either
    a directory from the 'protected' or 'public' paths. The directory structure is determined based on the
    HuBMAP ID provided and the associated metadata fetched from the HuBMAP API.

    :param hubmap_id: The HuBMAP ID of the dataset for which the directory path is to be determined.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :return: Directory path string. If there is an error in fetching metadata, it returns None.

    .. note::
       - The function assumes a certain directory structure based on whether the dataset contains human genetic sequences.
       - If the metadata format returned by the HuBMAP API changes in the future, this function might not work as expected.

    .. example::
       >>> get_directory("HGXXX", "your_token_here")
       '/hive/hubmap/data/public/HGXXX-UUID'

    """

    metadata = get_dataset_info(hubmap_id, instance=instance, token=token)
    if "error" in metadata:
        warning(metadata["error"])
        return None

    if (
        "contains_human_genetic_sequences" in metadata
        and metadata["contains_human_genetic_sequences"]
    ):
        directory = (
            "/hive/hubmap/data/protected/"
            + metadata["group_name"]
            + "/"
            + metadata["uuid"]
        )
    else:
        directory = "/hive/hubmap/data/public/" + metadata["uuid"]

    return directory


def get_files(hubmap_id: str, token: str, instance: str = "prod") -> list:
    """
    Retrieve a list of all files associated with a given HuBMAP dataset ID.

    This function determines the directory path for a given HuBMAP ID and returns a list of
    all files contained within that directory, including subdirectories.

    :param hubmap_id: The HuBMAP ID of the dataset for which the files are to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :return: List of file paths. If the directory does not exist, or there is an error in accessing
             the directory, it returns None.

    .. note::
       - If the directory associated with the HuBMAP ID does not exist or is inaccessible due to
         permission issues, the function will return None and might print a warning.
       - The function uses the `pathlib` module to handle directories and files.

    .. example::
       >>> get_files("HGXXX", "your_token_here")
       [PosixPath('/hive/hubmap/data/public/HGXXX-UUID/file1.txt'),
        PosixPath('/hive/hubmap/data/public/HGXXX-UUID/subdir/file2.txt')]
    """

    directory = get_directory(hubmap_id, instance=instance, token=token)

    try:
        if Path(directory).exists():
            files = list(Path(directory).glob("**/*"))
            files = [x for x in files if x.is_file()]
            return files
        else:
            return None
    except:
        warning(
            "Unable to access files in directory. More than likely a permission file."
        )
        return None


def get_number_of_files(hubmap_id: str, token: str, instance: str = "prod") -> int:
    """
    Retrieve the number of files associated with a given HuBMAP dataset ID.

    This function calls the `get_files` function to determine the list of files
    for a given HuBMAP ID and then returns the count of those files.

    :param hubmap_id: The HuBMAP ID of the dataset for which the count of files is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :return: An integer representing the count of files for the provided HuBMAP ID.
             If there's an error or the directory does not exist, it returns None.

    .. note::
       - If there's an error or the directory associated with the HuBMAP ID does not exist,
         the function will return None.
       - This function is dependent on the `get_files` function to retrieve the list of files.

    .. example::
       >>> get_number_of_files("HGXXX", "your_token_here")
       15

    """

    answer = get_files(hubmap_id, instance=instance, token=token)

    if answer is None:
        return None
    else:
        return len(answer)


def __query_donor_info(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    """
    Retrieve donor information for a given HuBMAP ID using the HuBMAP API.

    This function queries the HuBMAP API to get information about a donor associated
    with a given HuBMAP ID. The token is used for authentication to access the API.

    :param hubmap_id: The HuBMAP ID for which the donor information is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :param debug: If set to True, debug information will be printed. Default is False.
    :type debug: bool, optional

    :return: A dictionary containing information about the donor associated with
             the given HuBMAP ID. Returns None if there's an error or if the token
             is not set.

    .. note::
       - This is an internal/private function and should not be directly used
         by the end users.
       - The function is dependent on `utilities.__get_token` to verify and retrieve
         the token for authentication.

    .. example::
       >>> __query_donor_info("HGXXX", "your_token_here")
       {'uuid': 'xxxx', 'donor': {...}, ...}

    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    if __get_instance(instance) == "prod":
        URL = f"https://entity.api.hubmapconsortium.org/entities/{hubmap_id}"
    else:
        URL = (
            "https://entity-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/entities/"
            + hubmap_id
        )

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    r = requests.get(URL, headers=headers)
    return r


def get_donor_info(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = True,
    debug: bool = False,
) -> dict:
    """
    Retrieve the donor information associated with a given HuBMAP ID and optionally save it as a JSON file.

    This function obtains the donor information from the HuBMAP API based on a given HuBMAP ID.
    The donor information is then saved as a JSON file in a `.donor` directory. If the file already
    exists, the function will overwrite it by default, unless specified otherwise.

    :param hubmap_id: The HuBMAP ID for which donor information is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :param overwrite: If set to True, the existing JSON file with donor information will be overwritten.
                      Default is True.
    :type overwrite: bool, optional

    :param debug: If set to True, debug information will be printed. Default is False.
    :type debug: bool, optional

    :return: A dictionary containing the donor information for the given HuBMAP ID.
             Returns None if there's an error or if the request response is empty.

    .. note::
       - The function saves the donor information as a JSON file in a `.donor` directory
         with the filename as `<donor_hubmap_id>.json`.
       - The function utilizes `get_provenance_info` and `__query_donor_info` to gather
         and process the necessary donor information.

    .. example::
       >>> get_donor_info("HGXXX", "your_token_here")
       {'uuid': 'xxxx', 'donor': {...}, ...}

    """

    # get donor ID from dataset ID
    metadata = get_provenance_info(
        hubmap_id, instance=instance, token=token, debug=debug
    )
    hubmap_donor_id = metadata["donor_hubmap_id"][0]

    directory = ".donor"
    file = os.path.join(directory, hubmap_donor_id + ".json")
    if os.path.exists(file) and not overwrite:
        j = json.load(open(file, "r"))
    else:
        r = __query_donor_info(
            hubmap_donor_id, instance=instance, token=token, debug=debug
        )
        if r is None:
            warning("JSON object is empty.")
            return r
        j = json.loads(r.text)

    if "message" in j:
        warning("Request response is empty. Not populating dataframe.")
        print(j["message"])
        return None
    else:
        if not os.path.exists(directory):
            os.mkdir(directory)
        with open(file, "w") as outfile:
            json.dump(j, outfile, indent=4)
        return j


def __query_entity_info(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    """
    Internal function to query the HuBMAP API for entity information based on a given HuBMAP ID.

    This function sends a GET request to the HuBMAP API to obtain entity information associated with the
    provided HuBMAP ID. The response from the API is a dictionary containing the entity details.

    :param hubmap_id: The HuBMAP ID for which entity information is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :param debug: If set to True, debug information will be printed. Default is False.
    :type debug: bool, optional

    :return: A dictionary containing the entity information for the given HuBMAP ID.
             Returns None if there's an error or if the request response is empty.

    .. note::
       - The function utilizes utility functions like `__get_token` and `__get_instance` to obtain
         necessary authorization tokens and environment instances.
       - The URL for the GET request is constructed based on the provided instance parameter.

    .. warning::
       - Ensure that a valid token is provided to access the HuBMAP API.

    .. example::
       >>> __query_entity_info("HGXXX", "your_token_here")
       {'uuid': 'xxxx', 'entity': {...}, ...}

    """

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    if __get_instance(instance) == "prod":
        URL = f"https://entity.api.hubmapconsortium.org/entities/{hubmap_id}"
    else:
        URL = (
            "https://entity-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/entities/"
            + hubmap_id
        )

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    r = requests.get(URL, headers=headers)
    return r


def get_entity_info(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = True,
    debug: bool = True,
) -> dict:
    """
    Retrieve entity information from HuBMAP API based on a given HuBMAP ID and cache it locally.

    This function checks if the information related to the given HuBMAP ID is already stored locally.
    If not, or if overwrite is set to True, the HuBMAP API is queried for the entity information.
    The response from the API is then saved as a JSON file in a directory named `.entity`.

    :param hubmap_id: The HuBMAP ID for which entity information is to be retrieved.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance environment (e.g., "prod"). Default is "prod".
    :type instance: str, optional

    :param overwrite: If set to True, overwrite the local cache if it exists. Default is True.
    :type overwrite: bool, optional

    :param debug: If set to True, debug information will be printed. Default is True.
    :type debug: bool, optional

    :return: A dictionary containing the entity information for the given HuBMAP ID.
             Returns None if there's an error or if the request response is empty.

    .. note::
       - The function uses the internal function `__query_entity_info` to query the HuBMAP API.
       - Local caching of the response helps reduce unnecessary API calls, making retrieval faster for frequently
         accessed HuBMAP IDs.

    .. warning::
       - Ensure that a valid token is provided to access the HuBMAP API.
       - If the response contains a "message" key, it indicates an error or an empty response.

    .. example::
       >>> get_entity_info("HGXXX", "your_token_here")
       {'uuid': 'xxxx', 'entity': {...}, ...}

    """

    directory = ".entity"
    file = os.path.join(directory, hubmap_id + ".json")
    if os.path.exists(file) and not overwrite:
        j = json.load(open(file, "r"))
    else:
        r = __query_entity_info(hubmap_id, instance=instance, token=token, debug=debug)
        if r is None:
            warning("JSON object is empty.")
            return r
        j = json.loads(r.text)

    if "message" in j:
        warning("Request response is empty. Not populating dataframe.")
        print(j["message"])
        return None
    else:
        if not os.path.exists(directory):
            os.mkdir(directory)
        with open(file, "w") as outfile:
            json.dump(j, outfile, indent=4)
        return j


def get_assay_types(token: str, debug: bool = False) -> list:
    df = reports.daily()
    assays = df["dataset_type"].unique()

    return assays


def get_dataset_type(
    hubmap_id: str, token: str, instance: str = "prod", overwrite: bool = False
) -> str:
    """
    Retrieve the type of a given HuBMAP dataset based on its ancestors.

    Determines whether a HuBMAP dataset is primary or derived based on its direct ancestors.
    If the direct ancestor is a sample, the dataset is primary. If the direct ancestor is
    another dataset, the dataset is derived.

    :param hubmap_id: The HuBMAP identifier for the dataset.
    :type hubmap_id: str

    :param token: Authorization token to access the HuBMAP API.
    :type token: str

    :param instance: Specifies the instance of the HuBMAP API to query (e.g., "prod" for production).
                     Default is "prod".
    :type instance: str, optional

    :param overwrite: Determines whether to overwrite existing data or use cached data. Default is False.
    :type overwrite: bool, optional

    :return: A string indicating the dataset type. Can be "Primary", "Derived", or "Unknown".

    .. note::
       - The function relies on the direct ancestors provided by the HuBMAP API to determine dataset type.
       - A dataset is deemed "Unknown" if it doesn't fit into the "Primary" or "Derived" categories based
         on its direct ancestors.

    .. warning::
       Ensure that a valid token is provided to access the HuBMAP API.

    """

    metadata = get_dataset_info(
        hubmap_id, instance="prod", token=token, overwrite=overwrite
    )

    if "publication_ancillary" in metadata["data_types"]:
        return "Publication"
    elif metadata["direct_ancestors"][0]["entity_type"] == "Sample":
        return "Primary"
    elif metadata["direct_ancestors"][0]["entity_type"] == "Dataset":
        return "Derived"
    else:
        return "Unknown"
