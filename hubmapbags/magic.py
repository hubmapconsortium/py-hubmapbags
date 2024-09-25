from random import sample
import logging
from uuid import uuid4
import traceback
import os
from shutil import rmtree, move, copytree
import pandas as pd
from pprint import pprint
from pathlib import Path
from datetime import datetime
from . import (
    analysis_type,
    anatomy,
    apis,
    assay_type,
    biosample,
    biosample_disease,
    biosample_from_subject,
    biosample_gene,
    biosample_in_collection,
    biosample_substance,
    collection,
    collection_anatomy,
    collection_compound,
    collection_defined_by_project,
    collection_disease,
    collection_gene,
    collection_in_collection,
    collection_phenotype,
    collection_protein,
    collection_substance,
    collection_taxonomy,
    compound,
    dcc,
    disease,
)
from . import file as files
from . import data_type as data_type_file
from . import (
    file_describes_biosample,
    file_describes_collection,
    file_describes_subject,
    file_format,
    file_in_collection,
    gene,
    id_namespace,
    ncbi_taxonomy,
    phenotype,
    phenotype_disease,
    phenotype_gene,
    project_in_project,
    project,
    protein,
    protein_gene,
    reports,
    subject,
    subject_disease,
    subject_in_collection,
    subject_phenotype,
    subject_race,
    subject_role_taxonomy,
    subject_substance,
    substance,
    utilities,
)


def __extract_dataset_info_from_db(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> pd.DataFrame:
    """
    Extract dataset information from the HubMap database.

    :param hubmap_id: The HubMap ID of the dataset.
    :type hubmap_id: str
    :param token: Authentication token for accessing the database.
    :type token: str
    :param instance: The database instance to connect to (default is "prod").
    :type instance: str
    :param debug: Set to True for debugging mode (default is False).
    :type debug: bool

    :return: A Pandas DataFrame containing the extracted dataset information.
    :rtype: pd.DataFrame

    :raises UserWarning: If the information cannot be retrieved from the database.

    .. note::
        - The returned DataFrame has the following columns:
            - ds.group_name
            - ds.uuid
            - ds.hubmap_id
            - dataset_uuid
            - ds.status
            - ds.data_types
            - first_sample_id
            - first_sample_uuid
            - organ_type
            - organ_id
            - donor_id
            - donor_uuid
            - is_protected
            - full_path

        - If the dataset contains human genetic sequences, it is considered protected,
          and the data is retrieved from the protected data path. Otherwise, it is
          retrieved from the public data path.

    :example:

    >>> df = __extract_dataset_info_from_db("dataset123", token="mytoken")

    """

    j = apis.get_dataset_info(hubmap_id, token=token, instance=instance)
    if j is None:
        warnings.warn("Unable to extract data from database.")
        return None

    hmid = j.get("hubmap_id")
    hmuuid = j.get("uuid")
    status = j.get("status")
    try:
        data_types = j.get("data_types")[0]
    except:
        data_types = j.get("dataset_type")

    group_name = j.get("group_name")
    group_uuid = j.get("group_uuid")
    first_sample_id = j.get("direct_ancestors")[0].get("hubmap_id")
    first_sample_uuid = j.get("direct_ancestors")[0].get("uuid")

    if j.get("contains_human_genetic_sequences") == False:
        is_protected = False
    else:
        is_protected = True

    j = apis.get_provenance_info(hubmap_id, instance=instance, token=token)
    organ_type = j.get("organ_type")[0]
    organ_hmid = j.get("organ_hubmap_id")[0]
    organ_uuid = j.get("organ_uuid")[0]
    donor_hmid = j.get("donor_hubmap_id")[0]
    donor_uuid = j.get("donor_uuid")[0]

    if is_protected:
        full_path = os.path.join("/hive/hubmap/data/protected", group_name, hmuuid)
    else:
        full_path = os.path.join("/hive/hubmap/data/public", hmuuid)

    headers = [
        "ds.group_name",
        "ds.uuid",
        "ds.hubmap_id",
        "dataset_uuid",
        "ds.status",
        "ds.data_types",
        "first_sample_id",
        "first_sample_uuid",
        "organ_type",
        "organ_id",
        "donor_id",
        "donor_uuid",
        "is_protected",
        "full_path",
    ]

    df = pd.DataFrame(columns=headers)
    df = pd.concat(
        [
            df,
            pd.DataFrame(
                [
                    {
                        "ds.group_name": group_name,
                        "ds.uuid": group_uuid,
                        "ds.hubmap_id": hmid,
                        "dataset_uuid": hmuuid,
                        "ds.data_types": data_types,
                        "ds.status": status,
                        "ds.data_types": data_types,
                        "first_sample_id": first_sample_id,
                        "first_sample_uuid": first_sample_uuid,
                        "organ_type": organ_type,
                        "organ_id": organ_hmid,
                        "donor_id": donor_hmid,
                        "donor_uuid": donor_uuid,
                        "is_protected": is_protected,
                        "full_path": full_path,
                    }
                ]
            ),
        ],
        ignore_index=True,
        sort=True,
    )

    return df


def __extract_datasets_from_input(
    input: str, token: str, instance: str = "prod"
) -> dict:
    """
    Extract datasets from input sources.

    This function extracts datasets either from one or multiple metadata files
    (specified by the 'inputs' parameter) or directly from the HubMap database
    (using the provided HuBMAP IDs). It returns a dictionary containing information
    about the datasets.

    :param inputs: A single string or a list of strings. If a single string is provided,
                   it is treated as an input source. If it is a file path, datasets
                   are extracted from the specified metadata file. If it is a list
                   of strings, each element is treated as an input source. If an
                   element is a file path, datasets are extracted from the corresponding
                   metadata file. If an element is a HuBMAP ID, information about the
                   dataset is retrieved from the database.
    :type inputs: Union[str, List[str]]

    :param tokens: A single authentication token or a list of authentication tokens
                   corresponding to each input source in 'inputs' (required if any
                   input is a HuBMAP ID).
    :type tokens: Union[str, List[str]]

    :param instance: The database instance to connect to (e.g., "prod" for production).
                    (default is "prod")
    :type instance: str, optional

    :return: A dictionary containing information about the extracted datasets.
             The structure of the dictionary depends on the source of the datasets:
             - If 'inputs' is a single metadata file, the dictionary contains metadata
               for each dataset in that file.
             - If 'inputs' is a list of metadata files, the dictionary contains combined
               metadata for all datasets across the files.
             - If 'inputs' is a single HuBMAP ID or a list of HuBMAP IDs, the dictionary
               contains information for the specified dataset(s).
    :rtype: dict

    :raises UserWarning: If no datasets are found in any of the metadata files or in
                        the database.

    :Example:

    Extract datasets from a single metadata file:
    >>> input_file = "metadata.tsv"
    >>> token = "mytoken"
    >>> datasets = __extract_datasets_from_input(input_file, token)

    Extract datasets from multiple metadata files:
    >>> input_files = ["metadata1.tsv", "metadata2.tsv"]
    >>> tokens = ["mytoken1", "mytoken2"]
    >>> datasets = __extract_datasets_from_input(input_files, tokens)

    Extract a dataset by HuBMAP ID from the database:
    >>> hubmap_id = "dataset123"
    >>> token = "mytoken"
    >>> datasets = __extract_datasets_from_input(hubmap_id, token)

    Extract datasets by multiple HuBMAP IDs from the database:
    >>> hubmap_ids = ["dataset123", "dataset456"]
    >>> tokens = ["mytoken1", "mytoken2"]
    >>> datasets = __extract_datasets_from_input(hubmap_ids, tokens)

    """

    if os.path.isfile(input):
        utilities.pprint("Extracting datasets from " + input)
        metadata_file = input
        datasets = pd.read_csv(metadata_file, sep="\t")

        if datasets is None:
            warnings.warn("No datasets found. Exiting process.")
        else:
            print("Number of datasets found is " + str(datasets.shape[0]))
    else:
        utilities.pprint("Processing dataset with HuBMAP ID " + input)
        datasets = __extract_dataset_info_from_db(input, token=token, instance=instance)

        if datasets is None:
            warnings.warn("No datasets found. Exiting process.")

    return datasets


def __get_donor_url(donor_id: str, token: str, instance: str = "prod") -> str:
    """
    Get the URL of the donor's information page.

    This function retrieves the URL of the information page for a donor specified
    by the provided donor ID. The URL can point to either a registered DOI page
    or the HubMap Consortium portal, depending on the availability of the registered DOI.

    :param donor_id: The donor ID for which to retrieve the information page URL.
    :type donor_id: str

    :param token: Authentication token for accessing donor information.
    :type token: str

    :param instance: The database instance to connect to (e.g., "prod" for production).
                    (default is "prod")
    :type instance: str, optional

    :return: The URL of the donor's information page, either the registered DOI page
             or the HubMap Consortium portal page.
    :rtype: str

    :Example:

    Get the URL for a donor with a registered DOI:
    >>> donor_id = "donor123"
    >>> token = "mytoken"
    >>> url = __get_donor_url(donor_id, token)

    Get the URL for a donor without a registered DOI:
    >>> donor_id = "donor456"
    >>> token = "mytoken"
    >>> url = __get_donor_url(donor_id, token)
    """

    metadata = apis.get_entity_info(donor_id, instance=instance, token=token)
    url = f'https://portal.hubmapconsortium.org/browse/donor/{metadata["uuid"]}'

    return url


def __get_sample_url(sample_id: str, token: str, instance: str = "prod") -> str:
    """
    Get the URL of the sample's information page.

    This function retrieves the URL of the information page for a sample specified
    by the provided sample ID. The URL can point to either a registered DOI page
    or the HubMap Consortium portal, depending on the availability of the registered DOI.

    :param sample_id: The sample ID for which to retrieve the information page URL.
    :type sample_id: str

    :param token: Authentication token for accessing sample information.
    :type token: str

    :param instance: The database instance to connect to (e.g., "prod" for production).
                    (default is "prod")
    :type instance: str, optional

    :return: The URL of the sample's information page, either the registered DOI page
             or the HubMap Consortium portal page.
    :rtype: str

    :Example:

    Get the URL for a sample with a registered DOI:
    >>> sample_id = "sample123"
    >>> token = "mytoken"
    >>> url = __get_sample_url(sample_id, token)

    Get the URL for a sample without a registered DOI:
    >>> sample_id = "sample456"
    >>> token = "mytoken"
    >>> url = __get_sample_url(sample_id, token)

    """

    metadata = apis.get_entity_info(sample_id, instance=instance, token=token)

    if "registered_doi" in metadata.keys():
        return f'https://doi.org/{metadata["registered_doi"]}'
    else:
        return f'https://portal.hubmapconsortium.org/browse/sample/{metadata["uuid"]}'


def __get_dataset_url(dataset_id: str, token: str, instance: str = "prod") -> str:
    """
    Get the URL of the dataset's information page.

    This function retrieves the URL of the information page for a dataset specified
    by the provided dataset ID. The URL can point to either a registered DOI page
    or the HubMap Consortium portal, depending on the availability of the registered DOI.

    :param dataset_id: The dataset ID for which to retrieve the information page URL.
    :type dataset_id: str

    :param token: Authentication token for accessing dataset information.
    :type token: str

    :param instance: The database instance to connect to (e.g., "prod" for production).
                    (default is "prod")
    :type instance: str, optional

    :return: The URL of the dataset's information page, either the registered DOI page
             or the HubMap Consortium portal page.
    :rtype: str

    :Example:

    Get the URL for a dataset with a registered DOI:
    >>> dataset_id = "dataset123"
    >>> token = "mytoken"
    >>> url = __get_dataset_url(dataset_id, token)

    Get the URL for a dataset without a registered DOI:
    >>> dataset_id = "dataset456"
    >>> token = "mytoken"
    >>> url = __get_dataset_url(dataset_id, token)

    """

    metadata = apis.get_entity_info(dataset_id, instance=instance, token=token)

    if "registered_doi" in metadata.keys():
        return f'https://doi.org/{metadata["registered_doi"]}'
    else:
        return f'https://portal.hubmapconsortium.org/browse/dataset/{metadata["uuid"]}'


def __get_donor_metadata(hubmap_id: str, token: str, instance: str = "prod") -> dict:

    metadata = apis.get_donor_info(hubmap_id, instance=instance, token=token)
    donor_metadata = {}
    donor_metadata["local_id"] = metadata["hubmap_id"]
    donor_metadata["local_uuid"] = metadata["uuid"]
    donor_metadata["persistent_id"] = __get_donor_url(
        hubmap_id, instance=instance, token=token
    )
    donor_metadata["granularity"] = "cfde_subject_granularity:0"
    donor_metadata["creation_time"] = None

    if "metadata" in metadata.keys():
        if "living_donor_data" in metadata["metadata"].keys():
            for datum in metadata["metadata"]["living_donor_data"]:
                if datum["preferred_term"] == "Age":
                    donor_metadata["age_at_enrollment"] = datum["data_value"]

            for datum in metadata["metadata"]["living_donor_data"]:
                if datum["data_value"] == "Sex":
                    donor_metadata["sex"] = datum["preferred_term"]
        else:
            for datum in metadata["metadata"]["organ_donor_data"]:
                if datum["preferred_term"] == "Age":
                    donor_metadata["age_at_enrollment"] = datum["data_value"]

            for datum in metadata["metadata"]["organ_donor_data"]:
                if datum["data_value"] == "Sex":
                    donor_metadata["sex"] = datum["preferred_term"]
    else:
        donor_metadata["sex"] = None
        donor_metadata["age_at_enrollment"] = None

    if "sex" in donor_metadata.keys():
        if donor_metadata["sex"] == "Female":
            donor_metadata["sex"] = "cfde_subject_sex:1"

        if donor_metadata["sex"] == "Male":
            donor_metadata["sex"] = "cfde_subject_sex:2"
    else:
        donor_metadata["sex"] = None

    race = {
        "American Indian or Alaska native": "cfde_subject_race:0",
        "Asian or Pacific Islander": "cfde_subject_race:1",
        "Black or African American": "cfde_subject_race:2",
        "White": "cfde_subject_race:3",
        "Unknown": "cfde_subject_race:4",
        "Hispanic": "cfde_subject_race:4",
        "Asian": "cfde_subject_race:5",
        "Native Hawaiian or Other Pacific Islander": "cfde_subject_race:5",
    }

    if "metadata" in metadata.keys():
        if "living_donor_data" in metadata["metadata"].keys():
            for datum in metadata["metadata"]["living_donor_data"]:
                if datum["data_value"] == "Race":
                    donor_metadata["race"] = race[datum["preferred_term"]]

            for datum in metadata["metadata"]["living_donor_data"]:
                if (
                    datum["data_value"] == "Race"
                    and datum["preferred_term"] == "Hispanic"
                ):
                    donor_metadata["ethnicity"] = "cfde_subject_ethnicity:0"
                else:
                    donor_metadata["ethnicity"] = "cfde_subject_ethnicity:1"
        else:
            for datum in metadata["metadata"]["organ_donor_data"]:
                if datum["data_value"] == "Race":
                    donor_metadata["race"] = race[datum["preferred_term"]]

            for datum in metadata["metadata"]["organ_donor_data"]:
                if (
                    datum["data_value"] == "Race"
                    and datum["preferred_term"] == "Hispanic"
                ):
                    donor_metadata["ethnicity"] = "cfde_subject_ethnicity:0"
                else:
                    donor_metadata["ethnicity"] = "cfde_subject_ethnicity:1"
    else:
        donor_metadata["ethnicity"] = None
        donor_metadata["race"] = None

    return donor_metadata


def __get_dataset_metadata(hubmap_id: str, token: str, instance: str = "prod") -> dict:
    """
    Get metadata for a dataset.

    This function retrieves metadata for a dataset specified by the provided HubMap ID.
    The returned metadata includes information such as local ID, local UUID,
    persistent ID (URL), creation time, name, and description.

    :param hubmap_id: The HubMap ID of the dataset for which to retrieve metadata.
    :type hubmap_id: str

    :param token: Authentication token for accessing dataset metadata.
    :type token: str

    :param instance: The database instance to connect to (e.g., "prod" for production).
                    (default is "prod")
    :type instance: str, optional

    :return: A dictionary containing metadata for the dataset.
    :rtype: dict

    :Example:

    Get metadata for a dataset:
    >>> hubmap_id = "dataset123"
    >>> token = "mytoken"
    >>> metadata = __get_dataset_metadata(hubmap_id, token)
    """

    metadata = apis.get_dataset_info(hubmap_id, instance=instance, token=token)
    dataset_metadata = {}
    dataset_metadata["local_id"] = hubmap_id
    dataset_metadata["local_uuid"] = metadata["uuid"]
    dataset_metadata["persistent_id"] = __get_dataset_url(
        hubmap_id, instance=instance, token=token
    )
    dataset_metadata["creation_time"] = metadata["published_timestamp"]
    dataset_metadata["name"] = hubmap_id

    if "description" in metadata.keys():
        dataset_metadata["description"] = metadata["title"]
    else:
        dataset_metadata["description"] = None

    return dataset_metadata

    if (
        "direct_ancestors" in metadata.keys()
        and "dbgap_study_url" in metadata["direct_ancestors"]
    ):
        dataset_metadata["dbgap_study_id"] = (
            metadata["direct_ancestors"][0]["dbgap_study_url"].split("=")[1].strip()
        )
    else:
        dataset_metadata["dbgap_study_id"] = None


def __get_biosample_metadata(
    hubmap_id: str, token: str, instance: str = "prod"
) -> dict:
    """
    Get metadata for a biosample.

    This function retrieves metadata for a biosample specified by the provided HubMap ID.
    The returned metadata includes information such as local ID, project local ID,
    persistent ID (URL), creation time, name, and description.

    :param hubmap_id: The HubMap ID of the biosample for which to retrieve metadata.
    :type hubmap_id: str

    :param token: Authentication token for accessing biosample metadata.
    :type token: str

    :param instance: The database instance to connect to (e.g., "prod" for production).
                    (default is "prod")
    :type instance: str, optional

    :return: A dictionary containing metadata for the biosample.
    :rtype: dict

    :Example:

    Get metadata for a biosample:
    >>> hubmap_id = "biosample123"
    >>> token = "mytoken"
    >>> metadata = __get_biosample_metadata(hubmap_id, token)

    """

    metadata = apis.get_entity_info(hubmap_id, instance=instance, token=token)
    biosample_metadata = {}
    biosample_metadata["local_id"] = hubmap_id
    biosample_metadata["project_local_id"] = metadata["uuid"]
    biosample_metadata["persistent_id"] = __get_sample_url(
        biosample_id, instance=instance, token=token
    )
    biosample_metadata["creation_time"] = metadata["published_timestamp"]
    biosample_metadata["name"] = hubmap_id
    biosample_metadata["description"] = metadata["description"]

    return biosample_metadata


def aggregate(directory: str):
    tsv_files = [
        "analysis_type.tsv",
        "anatomy.tsv",
        "assay_type.tsv",
        "biosample.tsv",
        "biosample_disease.tsv",
        "biosample_from_subject.tsv",
        "biosample_gene.tsv",
        "biosample_in_collection.tsv",
        "biosample_substance.tsv",
        "collection.tsv",
        "collection_anatomy.tsv",
        "collection_compound.tsv",
        "collection_defined_by_project.tsv",
        "collection_disease.tsv",
        "collection_gene.tsv",
        "collection_in_collection.tsv",
        "collection_phenotype.tsv",
        "collection_protein.tsv",
        "collection_substance.tsv",
        "collection_taxonomy.tsv",
        "compound.tsv",
        "data_type.tsv",
        "dcc.tsv",
        "disease.tsv",
        "file_describes_biosample.tsv",
        "file_describes_collection.tsv",
        "file_describes_subject.tsv",
        "file_format.tsv",
        "file_in_collection.tsv",
        "gene.tsv",
        "id_namespace.tsv",
        "ncbi_taxonomy.tsv",
        "phenotype.tsv",
        "phenotype_disease.tsv",
        "phenotype_gene.tsv",
        "project.tsv",
        "project_in_project.tsv",
        "protein.tsv",
        "protein_gene.tsv",
        "subject.tsv",
        "subject_disease.tsv",
        "subject_in_collection.tsv",
        "subject_phenotype.tsv",
        "subject_race.tsv",
        "subject_role_taxonomy.tsv",
        "subject_substance.tsv",
        "substance.tsv",
    ]

    output_directory = "submission"
    if Path(output_directory).exists():
        rmtree(output_directory)
    Path(output_directory).mkdir()

    for tsv_file in tsv_files:
        df = pd.DataFrame()
        p = Path(directory).glob(f"**/{tsv_file}")
        files = list(p)

        for file in files:
            print(f"Appending file {file}")
            temp = pd.read_csv(file, sep="\t")
            df = pd.concat([df, temp], axis=0).reset_index(drop=True)

        output_filename = f"{output_directory}/{tsv_file}"
        df.to_csv(output_filename, sep="\t", index=False)


def do_it(
    input: str,
    token: str,
    inventory_directory: str = "/hive/hubmap/bdbags/inventory",
    output_directory="bdbags",
    build_bags: bool = False,
    overwrite: bool = False,
    debug: bool = True,
) -> bool:

    if not Path("logs").exists():
        Path("logs").mkdir()

    if Path(output_directory).exists():
        Path(output_directory).mkdir()

    now = datetime.now()
    log_filename = "hubmapbags-" + str(now.strftime("%Y%m%d")) + ".log"
    logging.basicConfig(
        filename=f"logs/{log_filename}",
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    datasets = __extract_datasets_from_input(input, token=token)
    if datasets is None:
        logging.critical("Unable to extract dataset information from the given input")
        return False
    else:
        logging.info(f"Extracted dataset information from {input}")

    for index, dataset in datasets.iterrows():
        status = dataset["ds.status"].lower()
        data_type = (
            dataset["ds.data_types"]
            .replace("[", "")
            .replace("]", "")
            .replace("'", "")
            .lower()
        )

        if "ds.hubmap_id" in dataset.keys():
            logging.info(f'HuBMAP ID set to {dataset["ds.hubmap_id"]}')
            hubmap_id = dataset["ds.hubmap_id"]
        else:
            logging.error("Unable to extract HuBMAP ID from dataset metadata")
            return False

        if "dataset_uuid" in dataset.keys():
            logging.info(f'HuBMAP UUID set to {dataset["dataset_uuid"]}')
            hubmap_uuid = dataset["dataset_uuid"]
        else:
            ("Unable to extract HuBMAP UUID from dataset metadata")
            return False

        if "ds.group_name" in dataset.keys():
            logging.info(f'Group name set to {dataset["ds.group_name"]}')
            data_provider = dataset["ds.group_name"]
        else:
            logging.error("Unable to extract group name from dataset metadata")
            return False

        if status != "published":
            logging.critical(
                f"Dataset {hubmap_id} is not published. Stopping computation."
            )
            print(f"Dataset {hubmap_id} is not published. Stopping computation.")
            return False

        try:
            dataset_metadata = __get_dataset_metadata(hubmap_id, token=token)
            logging.info(f"Gathered additional metadata to continue processing dataset")
        except:
            logging.critical(
                f"Unable to extract additional dataset metadata for {hubmap_id}"
            )
            return False

        dbgap_study_id = __get_dbgap_study_id(hubmap_id=hubmap_id, token=token)
        print(f"dbGaP study ID is set to {dbgap_study_id}")

        try:
            hubmap_url = __get_dataset_url(hubmap_id, token=token)
            dataset_metadata["hubmap_url"] = hubmap_url
            logging.info(f"Extracted dataset URL ({hubmap_url})")
        except:
            logging.warning("Unable to extract HuBMAP dataset URL")
            dataset_metadata["hubmap_url"] = None

        if "first_sample_id" in dataset.keys():
            biosample_id = dataset["first_sample_id"]
            logging.info(f"Extracted first sample ID {biosample_id}")
        else:
            logging.critical("Unable to extract first sample ID")
            return False

        try:
            biosample_url = __get_sample_url(biosample_id, token=token)
            logging.info(f"Extracted sample URL ({biosample_url})")
        except:
            logging.critical(f"Unable to extract biosample metadata for {hubmap_id}")
            return False

        if "full_path" in dataset.keys():
            data_directory = dataset["full_path"]
            logging.info(f"EXtracted data directory full ({data_directory})")
        else:
            logging.critical(f"Data directory full path is not available")
            return False

        if Path(data_directory).exists():
            print(f"Building bag for dataset in {data_directory}")
            logging.info(f"Building bag for dataset in {data_directory}")

        if Path(output_directory).exists():
            print(f"Moving bag to {output_directory}")
            logging.info(f"Moving bag to {output_directory}")
            move(data_directory, output_directory)

        computing = data_directory.replace("/", "_").replace(" ", "_") + ".computing"
        done = "." + data_directory.replace("/", "_").replace(" ", "_") + ".done"
        broken = "." + data_directory.replace("/", "_").replace(" ", "_") + ".broken"

        try:
            donor_metadata = __get_donor_metadata(hubmap_id, token=token)
            logging.info(f"Gathered donor metadata to continue processing dataset")
        except Exception as e:
            logging.critical(f"Unable to extract donor metadata for {hubmap_id}")
            traceback.print_exc()
            return False

        if "local_uuid" in donor_metadata.keys():
            donor_metadata["project_local_id"] = data_provider
            logging.info(f"Donor project local ID set to {data_provider}")
        else:
            logging.critical(
                f"Unable to extract project local ID (project_local_id) from donor metadata"
            )
            return False

        if "organ_id" in dataset.keys():
            donor_metadata["organ_id"] = dataset["organ_id"]
            logging.info(f'Donor organ ID set to {dataset["organ_id"]}')
        else:
            logging.critical(f"Unable to extract organ ID from donor metadata")
            return False

        if "organ_type" in dataset.keys():
            donor_metadata["organ_shortcode"] = dataset["organ_type"]
            logging.info(f'Donor organ type set to {dataset["organ_type"]}')
        else:
            logging.critical(f"Unable to extract organ type from donor metadata")
            return False

        if overwrite:
            print("Erasing old checkpoint. Re-computing checksums.")
            logging.info("Erasing old checkpoint. Re-computing checksums.")
            if Path(done).exists():
                try:
                    Path(done).unlink()
                except:
                    logging.warning(f"Unable to remove file {done}.")
                    return False

        if Path(done).exists():
            print(
                f"Checkpoint found. Avoiding computation. To re-compute erase file {done}"
            )
            logging.info(
                f"Checkpoint found. Avoiding computation. To re-compute erase file {done}"
            )
        elif Path(computing).exists():
            logging.info(
                "Computing checkpoint found. Avoiding computation since another process is building this bag."
            )
            print(
                "Computing checkpoint found. Avoiding computation since another process is building this bag."
            )
        else:
            with open(computing, "w") as file:
                pass

            print(f"Creating checkpoint {computing}")
            logging.info(f"Creating checkpoint {computing}")

            if build_bags:
                logging.info(f"Checking if output directory exists")
                print("Checking if output directory exists")
                output_directory = f'{data_type}-{status}-{dataset["dataset_uuid"]}'

                if Path(output_directory).exists() and Path(output_directory).is_dir():
                    print("Output directory found. Removing old copy.")
                    logging.info("Output directory found. Removing old copy.")
                    rmtree(output_directory)
                    print(f"Creating output directory {output_directory}")
                    logging.info(f"Creating output directory {output_directory}")
                    os.mkdir(output_directory)
                else:
                    print(
                        f"Output directory {output_directory} does not exist. Creating directory."
                    )
                    logging.info(
                        f"Output directory {output_directory} does not exist. Creating directory."
                    )
                    os.mkdir(output_directory)

                print("Making file.tsv")
                logging.info("Making file.tsv")

                if not Path(".data").exists():
                    logging.info("Make directory .data/")
                    Path(".data").mkdir()

                temp_file = ".data/" + hubmap_uuid + ".tsv"
                logging.info("")

                if overwrite:
                    print("Removing precomputed checksums")
                    logging.info("Removing precomputed checksums")
                    if Path(temp_file).exists():
                        Path(temp_file).unlink()

                answer = files.create_manifest(
                    project_id=data_provider,
                    assay_type=data_type,
                    directory=data_directory,
                    inventory_directory=inventory_directory,
                    output_directory=output_directory,
                    dbgap_study_id=dbgap_study_id,
                    token=token,
                    dataset_hmid=hubmap_id,
                    dataset_uuid=hubmap_uuid,
                )

                print("Making biosample.tsv")
                logging.info("Making biosample.tsv")
                biosample.create_manifest(
                    biosample_id,
                    biosample_url,
                    data_provider,
                    donor_metadata["organ_shortcode"],
                    output_directory,
                )

                print("Making biosample_in_collection.tsv")
                logging.info("Making biosample_in_collection.tsv")
                biosample_in_collection.create_manifest(
                    biosample_id, hubmap_id, output_directory
                )

                print("Making project.tsv")
                logging.info("Making project.tsv")
                project.create_manifest(data_provider, output_directory)

                print("Making project_in_project.tsv")
                logging.info("Making project_in_project.tsv")
                project_in_project.create_manifest(data_provider, output_directory)

                print("Making biosample_from_subject.tsv")
                logging.info("Making biosample_from_subject.tsv")
                biosample_from_subject.create_manifest(
                    biosample_id, donor_metadata["local_id"], output_directory
                )

                print("Making ncbi_taxonomy.tsv")
                logging.info("Making ncbi_taxonomy.tsv")
                ncbi_taxonomy.create_manifest(output_directory)

                print("Making collection.tsv")
                logging.info("Making collection.tsv")
                collection.create_manifest(dataset_metadata, output_directory)

                print("Making collection_defined_by_project.tsv")
                logging.info("Making collection_defined_by_project.tsv")
                collection_defined_by_project.create_manifest(
                    hubmap_id, data_provider, output_directory
                )

                print("Making file_describes_collection.tsv")
                logging.info("Making file_describes_collection.tsv")
                file_describes_collection.create_manifest(
                    hubmap_id=hubmap_id,
                    token=token,
                    hubmap_uuid=hubmap_uuid,
                    directory=data_directory,
                    inventory_directory=inventory_directory,
                    output_directory=output_directory,
                )

                print("Making dcc.tsv")
                logging.info("Making dcc.tsv")
                dcc.create_manifest(output_directory)

                print("Making id_namespace.tsv")
                logging.info("Making id_namespace.tsv")
                id_namespace.create_manifest(output_directory)

                print("Making subject.tsv")
                logging.info("Making subject.tsv")
                subject.create_manifest(donor_metadata, output_directory)

                print("Making subject_in_collection.tsv")
                logging.info("Making subject_in_collection.tsv")
                subject_in_collection.create_manifest(
                    donor_metadata["local_id"], hubmap_id, output_directory
                )

                print("Making file_in_collection.tsv")
                logging.info("Making file_in_collection.tsv")
                answer = file_in_collection.create_manifest(
                    hubmap_id=hubmap_id,
                    token=token,
                    hubmap_uuid=hubmap_uuid,
                    directory=data_directory,
                    inventory_directory=inventory_directory,
                    output_directory=output_directory,
                )

                print("Creating empty files")
                logging.info("Creating empty files")
                file_describes_subject.create_manifest(output_directory)
                file_describes_biosample.create_manifest(output_directory)
                anatomy.create_manifest(output_directory)
                assay_type.create_manifest(output_directory)
                analysis_type.create_manifest(output_directory)
                biosample_disease.create_manifest(output_directory)
                biosample_gene.create_manifest(output_directory)
                biosample_substance.create_manifest(output_directory)
                collection_anatomy.create_manifest(output_directory)
                collection_compound.create_manifest(output_directory)
                collection_disease.create_manifest(output_directory)
                collection_gene.create_manifest(output_directory)
                collection_in_collection.create_manifest(output_directory)
                collection_phenotype.create_manifest(output_directory)
                collection_protein.create_manifest(output_directory)
                collection_substance.create_manifest(output_directory)
                collection_taxonomy.create_manifest(output_directory)
                analysis_type.create_manifest(output_directory)
                compound.create_manifest(output_directory)
                data_type_file.create_manifest(output_directory)
                disease.create_manifest(output_directory)
                gene.create_manifest(output_directory)
                phenotype_disease.create_manifest(output_directory)
                phenotype_gene.create_manifest(output_directory)
                phenotype.create_manifest(output_directory)
                protein.create_manifest(output_directory)
                protein_gene.create_manifest(output_directory)
                substance.create_manifest(output_directory)
                file_format.create_manifest(output_directory)
                ncbi_taxonomy.create_manifest(output_directory)
                subject_disease.create_manifest(output_directory)
                subject_phenotype.create_manifest(output_directory)
                subject_race.create_manifest(output_directory)
                subject_role_taxonomy.create_manifest(output_directory)
                subject_substance.create_manifest(output_directory)
                file_format.create_manifest(output_directory)
                collection_substance.create_manifest(output_directory)
                subject_substance.create_manifest(output_directory)
            else:
                output_directory = (
                    data_type + "-" + status + "-" + dataset["dataset_uuid"]
                )
                answer = files.create_manifest(
                    project_id=data_provider,
                    assay_type=data_type,
                    directory=data_directory,
                    inventory_directory=inventory_directory,
                    output_directory=output_directory,
                    dbgap_study_id=dbgap_study_id,
                    token=token,
                    dataset_hmid=hubmap_id,
                    dataset_uuid=hubmap_uuid,
                )

            print(f"Removing checkpoint {computing}")
            logging.info(f"Removing checkpoint {computing}")
            Path(computing).unlink()

            print(f"Creating final checkpoint {done}")
            logging.info(f"Creating final checkpoint {done}")

            with open(done, "w") as file:
                pass

            if build_bags:
                if not Path("bags").exists():
                    Path("bags").mkdir()

                if Path(f"bags/{output_directory}").exists():
                    rmtree(f"bags/{output_directory}")
                move(output_directory, "bags")

    return True


def __get_dbgap_study_id(hubmap_id: str, token: str, debug: bool = False):
    metadata = apis.get_dataset_info(hubmap_id=hubmap_id, token=token)
    if "dbgap_study_url" in metadata.keys():
        dbgap_study_id = metadata["dbgap_study_url"].replace(
            "https://www.ncbi.nlm.nih.gov/projects/gap/cgi-bin/study.cgi?study_id=", ""
        )
    else:
        dbgap_study_id = None

    return dbgap_study_id


def create_submission(
    token: str,
    build_bags: bool,
    inventory_directory="/hive/hubmap/bdbags/inventory",
    output_directory="bdbags",
    data_types_to_ignore=[
        "LC-MS",
        "Publication",
    ],
    debug: bool = True,
):

    broken_datasets = []

    df = reports.daily()
    df = df[df["status"] == "Published"]
    df = df[df["is_primary"] == True]
    df = df[df["data_access_level"].isin(["protected", "public"])]
    print("The number of datasets to process is {len(df)}")

    for index, datum in df.iterrows():
        if datum["dataset_type"] in data_types_to_ignore:
            print(
                f"Ignoring dataset {datum['hubmap_id']} with assay type {datum['dataset_type']}"
            )
        else:
            try:
                if (
                    datum["status"] == "Published"
                    and datum["data_access_level"] == "public"
                ):
                    hubmap_id = datum["hubmap_id"]
                    dbgap_study_id = __get_dbgap_study_id(
                        hubmap_id=hubmap_id, token=token
                    )

                    do_it(
                        hubmap_id,
                        token=token,
                        inventory_directory=inventory_directory,
                        output_directory=output_directory,
                        overwrite=False,
                        build_bags=True,
                    )
                elif (
                    datum["status"] == "Published"
                    and datum["data_access_level"] == "protected"
                ):
                    hubmap_id = datum["hubmap_id"]
                    dbgap_study_id = __get_dbgap_study_id(
                        hubmap_id=hubmap_id, token=token
                    )

                    do_it(
                        hubmap_id,
                        token=token,
                        inventory_directory=inventory_directory,
                        output_directory=output_directory,
                        overwrite=False,
                        build_bags=True,
                    )
                else:
                    print(f'Avoiding computation of dataset {datum["hubmap_id"]}.')
            except Exception as e:
                print(f'Failed to process dataset {datum["hubmap_id"]}.')
                traceback.print_exc()
                broken_datasets += [datum["hubmap_id"]]

    if not broken_datasets:
        print(f"The datasets that failed to processs are")
        pprint(broken_datasets)


def generate_random_sample(directory: str, number_of_samples: int = 10):
    output_directory = str(uuid4()).replace("-", "")
    if Path(output_directory).exists():
        rmtree(output_directory)
    Path(output_directory).mkdir()

    temp_directory = f"/tmp/{str(uuid4()).replace('-','')}/"
    Path(temp_directory).mkdir()

    directories = [item for item in Path(directory).iterdir() if item.is_dir()]
    directories = sample(directories, number_of_samples)

    for directory in directories:
        copytree(directory, f"{temp_directory}/{Path(directory).stem}")

    tsv_files = [
        "analysis_type.tsv",
        "anatomy.tsv",
        "assay_type.tsv",
        "biosample.tsv",
        "biosample_disease.tsv",
        "biosample_from_subject.tsv",
        "biosample_gene.tsv",
        "biosample_in_collection.tsv",
        "biosample_substance.tsv",
        "collection.tsv",
        "collection_anatomy.tsv",
        "collection_compound.tsv",
        "collection_defined_by_project.tsv",
        "collection_disease.tsv",
        "collection_gene.tsv",
        "collection_in_collection.tsv",
        "collection_phenotype.tsv",
        "collection_protein.tsv",
        "collection_substance.tsv",
        "collection_taxonomy.tsv",
        "compound.tsv",
        "data_type.tsv",
        "dcc.tsv",
        "disease.tsv",
        "file.tsv",
        "file_describes_biosample.tsv",
        "file_describes_collection.tsv",
        "file_describes_subject.tsv",
        "file_format.tsv",
        "file_in_collection.tsv",
        "gene.tsv",
        "id_namespace.tsv",
        "ncbi_taxonomy.tsv",
        "phenotype.tsv",
        "phenotype_disease.tsv",
        "phenotype_gene.tsv",
        "project.tsv",
        "project_in_project.tsv",
        "protein.tsv",
        "protein_gene.tsv",
        "subject.tsv",
        "subject_disease.tsv",
        "subject_in_collection.tsv",
        "subject_phenotype.tsv",
        "subject_race.tsv",
        "subject_role_taxonomy.tsv",
        "subject_substance.tsv",
        "substance.tsv",
    ]

    for tsv_file in tsv_files:
        p = Path(temp_directory).glob(f"**/{tsv_file}")
        files = list(p)

        df = pd.DataFrame()
        for file in files:
            print(f"Appending file {file}")
            temp = pd.read_csv(file, sep="\t")
            df = pd.concat([df, temp], axis=0).reset_index(drop=True)

        output_filename = f"{output_directory}/{tsv_file}"
        df = df.drop_duplicates()
        df.to_csv(output_filename, sep="\t", index=False)
