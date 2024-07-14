import json
from pathlib import Path
from warnings import warn as warning
import uuid

import pandas as pd
import requests

from . import apis, magic, utilities


def load_local_file_with_remote_uuids(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = False,
    debug: bool = False,
) -> pd.DataFrame:
    print("Loading local file for HuBMAP ID " + hubmap_id + " with remote UUIDs")
    dataset = magic.__extract_datasets_from_input(
        hubmap_id, instance=instance, token=token
    )

    if dataset is None:
        warning("No datasets found. Exiting.")
        return False

    data_directory = dataset["full_path"][0]

    if not Path(".data").is_dir():
        Path(".data").mkdir()
    temp_file = ".data/" + data_directory.replace("/", "_").replace(" ", "_") + ".pkl"
    if Path(temp_file).is_file():
        df = pd.read_pickle(temp_file)
        return df
    else:
        warning("Unable to find or load file " + temp_file)
        return pd.DataFrame()


def populate_local_file_with_remote_uuids(
    hubmap_id: str,
    token: str,
    instance: str = "prod",
    overwrite: bool = False,
    debug: bool = False,
) -> bool:
    """
    Helper function that populates (but does not generate) a local pickle file with remote UUIDs.
    """

    print("Populating dataset with HuBMAP ID " + hubmap_id + " with remote UUIDs")
    dataset = magic.__extract_datasets_from_input(
        hubmap_id, instance=instance, token=token
    )

    if dataset is None:
        warning("No datasets found. Exiting.")
        return False

    data_directory = dataset["full_path"][0]
    computing = data_directory.replace("/", "_").replace(" ", "_") + ".computing"
    done = "." + data_directory.replace("/", "_").replace(" ", "_") + ".done"
    broken = "." + data_directory.replace("/", "_").replace(" ", "_") + ".broken"

    if not Path(".data").is_dir():
        Path(".data").mkdir()
    temp_file = ".data/" + data_directory.replace("/", "_").replace(" ", "_") + ".pkl"

    if Path(computing).is_file():
        warning(
            "Computing file "
            + computing
            + " exists. Another process is computing checksums. Not populating local file."
        )
        return False
    elif not Path(computing).is_file() and not Path(done).is_file():
        print("File " + done + " not found on disk. Not populating local file.")
        return False
    elif Path(done).is_file():
        if Path(temp_file).is_file():
            if not should_i_generate_uuids(
                hubmap_id, instance=instance, token=token, debug=debug
            ):
                print("Attempting to populate local file")
                df = pd.read_pickle(temp_file)
                uuids = get_uuids(
                    hubmap_id, instance=instance, token=token, debug=debug
                )

                for i in range(len(df)):
                    for uuid in uuids:
                        if df.loc[i, "relative_local_id"] == uuid["path"]:
                            df.loc[i, "hubmap_uuid"] = uuid["file_uuid"]

                print("Updating local file " + temp_file + " with UUIDs.")
                df.to_pickle(temp_file)
                df.to_csv(temp_file.replace("pkl", "tsv"), sep="\t", index=False)
                return True
            else:
                return False


def __get_instance(instance: str) -> str:
    """
    Helper method that determines what instance to use.
    """

    if instance.lower() == "dev":
        return ".dev"
    elif instance.lower() == "prod":
        return "prod"
    elif instance.lower() == "test":
        return ".test"
    else:
        if instance is None:
            warning('Instance not set. Setting default value to "test".')
        else:
            warning(
                "Unknown option " + str(instance) + ". Setting default value to test."
            )
        return ".test"


def __query_existence(
    uuid: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    if __get_instance(instance) == "prod":
        URL = "https://uuid.api.hubmapconsortium.org/" + uuid + "/exists"
    else:
        URL = (
            "https://uuid-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/"
            + uuid
            + "/exists"
        )

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    r = requests.get(URL, headers=headers)
    return r


def __query_uuids(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    if __get_instance(instance) == "prod":
        URL = "https://uuid.api.hubmapconsortium.org/" + hubmap_id + "/files"
    else:
        URL = (
            "https://uuid-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/"
            + hubmap_id
            + "/files"
        )

    headers = {"Authorization": "Bearer " + token, "accept": "application/json"}

    r = requests.get(URL, headers=headers)
    return r


def get_uuids(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> dict:
    """
    Get UUIDs, if any, given a HuBMAP id.
    """

    r = __query_uuids(hubmap_id, instance=instance, token=token, debug=debug)

    if r.status_code == 303:
        link = r.content  # Amazon S3 bucket link
        file = f"/tmp/{str(uuid.uuid4())}.json"

        with open(file, "wb") as f:
            f.write(requests.get(link).content)
            j = json.load(open(file, "rb"))
    else:
        j = json.loads(r.text)

    return j


def get_number_of_uuids(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> int:
    """
    Get number of UUIDs associated with this HuBMAP id using the UUID API.
    """

    try:
        uuids = get_uuids(hubmap_id, instance=instance, token=token, debug=debug)
        uuids = pd.DataFrame.from_dict(uuids)
        return len(uuids[uuids["base_dir"] == "DATA_UPLOAD"])
    except:
        return 0


def has_uuids(hubmap_id: str, token: str, instance: str = "prod") -> bool:
    if get_number_of_uuids(hubmap_id, instance="prod", token=token) == 0:
        return False
    else:
        return True


def generate(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = True
) -> bool:
    """
    Main function that generates UUIDs using the UUID-API.
    """

    print("Generating UUIDs for dataset with HuBMAP ID " + hubmap_id)
    dataset = magic.__extract_dataset_info_from_db(
        hubmap_id, token=token, instance=instance
    )

    if dataset is None:
        warning("No datasets found with given dataset ID. Exiting process.")
        return False

    dataset = dataset.squeeze()
    data_directory = dataset["full_path"]
    computing = data_directory.replace("/", "_").replace(" ", "_") + ".computing"
    done = "." + data_directory.replace("/", "_").replace(" ", "_") + ".done"
    broken = "." + data_directory.replace("/", "_").replace(" ", "_") + ".broken"

    if not Path(".data").is_dir():
        Path(".data").mkdir()
    temp_file = ".data/" + data_directory.replace("/", "_").replace(" ", "_") + ".pkl"

    token = utilities.__get_token(token)
    if token is None:
        warning("Token not set.")
        return None

    answer = populate_local_file_with_remote_uuids(
        hubmap_id, instance=instance, token=token, debug=False
    )

    try:
        if debug:
            print("Loading temp file " + temp_file + ".")
        df = pd.read_pickle(temp_file)
    except:
        if debug:
            print("Unable to load pickle file " + temp_file + ". Exiting process.")
        return False

    return df

    if __get_instance(instance) == "prod":
        URL = "https://uuid.api.hubmapconsortium.org/hmuuid/"
    else:
        URL = (
            "https://uuid-api"
            + __get_instance(instance)
            + ".hubmapconsortium.org/hmuuid/"
        )

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0",
        "Authorization": "Bearer " + token,
        "Content-Type": "application/json",
    }

    if len(df) <= 1000:
        if df["hubmap_uuid"].isnull().all():
            file_info = []
            for datum in df.iterrows():
                datum = datum[1]
                duuid = datum["dataset_uuid"]
                filename = datum["local_id"][
                    datum["local_id"].find(duuid) + len(duuid) + 1 :
                ]
                file_info.append(
                    {
                        "path": filename,
                        "size": datum["size_in_bytes"],
                        "checksum": datum["sha256"],
                        "base_dir": "DATA_UPLOAD",
                    }
                )

            payload = {}
            payload["parent_ids"] = [duuid]
            payload["entity_type"] = "FILE"
            payload["file_info"] = file_info
            params = {"entity_count": len(file_info)}

            if debug:
                print("Generating UUIDs")
            r = requests.post(
                URL,
                params=params,
                headers=headers,
                data=json.dumps(payload),
                allow_redirects=True,
                timeout=120,
            )

            if r.status_code == 400:
                warning(r.text)
            else:
                j = json.loads(r.text)

            if "message" in j:
                warning("Request response is empty. Not populating dataframe.")
                print(j["message"])
                return False
            else:
                for datum in j:
                    df.loc[
                        df["local_id"].str.contains(datum["file_path"]), "hubmap_uuid"
                    ] = datum["uuid"]

                if debug:
                    print(
                        "Updating pickle file "
                        + temp_file
                        + " with the request response."
                    )

                df.to_pickle(temp_file)
                with open(temp_file.replace("pkl", "json"), "w") as outfile:
                    json.dump(j, outfile, indent=4)
        else:
            if debug:
                print("HuBMAP UUID column is populated. Skipping generation.")
    else:
        if debug:
            print(
                "Data frame has "
                + str(len(df))
                + " items. Partitioning into smaller chunks."
            )

        n = 1000  # chunk row size
        dfs = [df[i : i + n] for i in range(0, df.shape[0], n)]

        counter = 0
        for frame in dfs:
            counter = counter + 1
            if debug:
                print(
                    "Computing UUIDs on partition "
                    + str(counter)
                    + " of "
                    + str(len(dfs))
                    + "."
                )

            file_info = []
            for datum in frame.iterrows():
                datum = datum[1]
                filename = datum["local_id"][
                    datum["local_id"].find(duuid) + len(duuid) + 1 :
                ]
                file_info.append(
                    {
                        "path": filename,
                        "size": datum["size_in_bytes"],
                        "checksum": datum["sha256"],
                        "base_dir": "DATA_UPLOAD",
                    }
                )

            payload = {}
            payload["parent_ids"] = [duuid]
            payload["entity_type"] = "FILE"
            payload["file_info"] = file_info
            params = {"entity_count": len(file_info)}

            if frame["hubmap_uuid"].isnull().all():
                if debug:
                    print("Generating UUIDs")

                r = requests.post(
                    URL,
                    params=params,
                    headers=headers,
                    data=json.dumps(payload),
                    allow_redirects=True,
                    timeout=120,
                )
                j = json.loads(r.text)

                if "message" in j:
                    if debug:
                        print("Request response. Not populating data frame.")
                    print(j["message"])
                    return False
                else:
                    for datum in j:
                        df.loc[
                            df["local_id"].str.contains(datum["file_path"]),
                            "hubmap_uuid",
                        ] = datum["uuid"]

                    if debug:
                        print(
                            "Updating pickle file "
                            + file
                            + " with the results of this chunk."
                        )
                    df.to_pickle(file)
            else:
                print("HuBMAP UUID chunk is populated. Skipping recomputation.")


def should_i_generate_uuids(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> bool:
    """
    Helper function that compares the number of files on disk versus the number of
    entries in the UUID-API database.
    """

    number_of_files = apis.get_number_of_files(
        hubmap_id, instance=instance, token=token
    )
    number_of_entries_in_db = get_number_of_uuids(
        hubmap_id, instance=instance, token=token
    )

    if number_of_entries_in_db == 0:
        return True

    if number_of_entries_in_db == number_of_files:
        return False

    if number_of_files != 0 and number_of_files > number_of_entries_in_db:
        warning(
            "There are more entries in local file than in database. Either a job is running computing checksums or a job failed."
        )
        return None

    if number_of_files != 0 and number_of_files < number_of_entries_in_db:
        warning(
            "There are more entries in database than files in local db. More than likely UUIDs were generate more than once. Contact a system administrator."
        )
        return None


def is_complete(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> bool:
    """
    A dataset is considered to be complete if the number of remote UUIDs matches the number of local number of files. False, otherwise.
    """

    number_of_files = apis.get_number_of_files(
        hubmap_id, instance=instance, token=token
    )
    number_of_entries_in_db = get_number_of_uuids(
        hubmap_id, instance=instance, token=token
    )

    return number_of_files == number_of_entries_in_db
