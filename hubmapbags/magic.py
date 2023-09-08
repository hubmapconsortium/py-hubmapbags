import logging
import os
from shutil import rmtree
import pandas as pd
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
    data_type,
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
    subject,
    subject_disease,
    subject_in_collection,
    subject_phenotype,
    subject_race,
    subject_role_taxonomy,
    subject_substance,
    substance,
    utilities,
    uuids,
)


def __extract_dataset_info_from_db(
    hubmap_id: str, token: str, instance: str = "prod", debug: bool = False
) -> pd.DataFrame:
    """
    Helper function that uses the HuBMAP APIs to get dataset info.
    """

    j = apis.get_dataset_info(hubmap_id, token=token, instance=instance)
    if j is None:
        warnings.warn("Unable to extract data from database.")
        return None

    hmid = j.get("hubmap_id")
    hmuuid = j.get("uuid")
    status = j.get("status")
    data_types = j.get("data_types")[0]
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
    Helper function that returns a list of valid datasets (if any).
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
    metadata = apis.get_entity_info(donor_id, instance=instance, token=token)

    if "registered_doi" in metadata.keys():
        return f'https://doi.org/{metadata["registered_doi"]}'
    else:
        return f'https://portal.hubmapconsortium.org/browse/donor/{metadata["uuid"]}'


def __get_sample_url(sample_id: str, token: str, instance: str = "prod") -> str:
    metadata = apis.get_entity_info(sample_id, instance=instance, token=token)

    if "registered_doi" in metadata.keys():
        return f'https://doi.org/{metadata["registered_doi"]}'
    else:
        return f'https://portal.hubmapconsortium.org/browse/sample/{metadata["uuid"]}'


def __get_dataset_url(dataset_id: str, token: str, instance: str = "prod") -> str:
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
        dataset_metadata["description"] = metadata["description"]
    else:
        dataset_metadata["description"] = None

    return dataset_metadata


def __get_biosample_metadata(
    hubmap_id: str, token: str, instance: str = "prod"
) -> dict:
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


def do_it(
    input: str,
    token: str,
    dbgap_study_id: None,
    instance: str = "prod",
    build_bags: bool = False,
    overwrite: bool = False,
    debug: bool = True,
) -> bool:
    """
    Magic function that (1) computes checksums, (2) generates UUIDs and, (3) builds a big data bag given a HuBMAP ID.

    :param input: A string representing a HuBMAP ID or a TSV file with one line per dataset, e.g. HBM632.JSNP.578
    :type input: string
    :param dbgap_study_id: A string representing a dbGaP study ID, e.g. phs00265
    :type dbgap_study_id: None or string
    :param overwrite: If set to TRUE, then it will overwrite an existing pickle file associated with the HuBMAP ID
    :type overwrite: boolean
    :param build_bags: If set to TRUE, the it will build the big data bag.
    :type build_bags: boolean
    :param token: A token to access HuBMAP resources
    :type token: string or None
    :param instance: Either 'dev', 'test' or 'prod'
    :type instance: string
    :param debug: debug flag
    :type debug: boolean
    :rtype: boolean
    """

    if not Path("logs").exists():
        Path("logs").mkdir()

    now = datetime.now()
    log_filename = "hubmapbags-" + str(now.strftime("%Y%m%d")) + ".log"
    logging.basicConfig(
        filename=f"logs/{log_filename}",
        filemode="w",
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    datasets = __extract_datasets_from_input(input, token=token, instance=instance)
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
            dataset_metadata = __get_dataset_metadata(
                hubmap_id, instance=instance, token=token
            )
            logging.info(f"Gathered additional metadata to continue processing dataset")
        except:
            logging.critical(
                f"Unable to extract additional dataset metadata for {hubmap_id}"
            )
            return False

        try:
            hubmap_url = __get_dataset_url(hubmap_id, instance=instance, token=token)
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
            biosample_url = __get_sample_url(
                biosample_id, instance=instance, token=token
            )
            logging.info(f"Extracted sample URL ({biosample_url})")
        except:
            logging.critical(f"Unable to extract biosample metadata for {hubmap_id}")
            return False

        if "full_path" in dataset.keys():
            data_directory = dataset["full_path"]
            logging.info(f"EXtracted data direcstatustory full ({data_directory})")
        else:
            logging.critical(f"Data directory full path is not available")
            return False

        print(f"Building bag for dataset in {data_directory}")
        logging.info(f"Building bag for dataset in {data_directory}")
        computing = data_directory.replace("/", "_").replace(" ", "_") + ".computing"
        done = "." + data_directory.replace("/", "_").replace(" ", "_") + ".done"
        broken = "." + data_directory.replace("/", "_").replace(" ", "_") + ".broken"

        # get donor information
        try:
            donor_metadata = __get_donor_metadata(
                hubmap_id, instance=instance, token=token
            )
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
                    output_directory=output_directory,
                )

                print("Creating empty files")
                logging.info("Creating empty files")
                file_describes_subject.create_manifest(output_directory)
                file_describes_biosample.create_manifest(output_directory)
                anatomy.create_manifest(output_directory)
                assay_type.create_manifest(output_directory)
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
            if build_bags:
                if not Path("bags").exists():
                    Path("bags").mkdir()

                if Path(f"bags/{output_directory}").exists():
                    shutil.rmtree(f"bags/{output_directory}")
                shutil.move(output_directory, "bags")

    return True


def get_dataset_info_from_local_file(
    hubmap_id: str, token: str, instance: str = "prod", build_bags: bool = False
):
    dataset = __extract_datasets_from_input(hubmap_id, token=token, instance=instance)
    if dataset is None:
        return False

    data_directory = dataset["full_path"]
    print("Preparing bag for dataset " + data_directory)

    computing = data_directory.replace("/", "_").replace(" ", "_") + ".computing"
    done = "." + data_directory.replace("/", "_").replace(" ", "_") + ".done"
    broken = "." + data_directory.replace("/", "_").replace(" ", "_") + ".broken"

    organ_shortcode = dataset["organ_type"]
    organ_id = dataset["organ_id"]
    donor_id = dataset["donor_id"]

    if overwrite:
        print("Erasing old checkpoint. Re-computing checksums.")
        if Path(done).exists():
            Path(done).unlink()

    if Path(done).exists():
        print(
            "Checkpoint found. Avoiding computation. To re-compute erase file " + done
        )
    elif Path(computing).exists():
        print(
            "Computing checkpoint found. Avoiding computation since another process is building this bag."
        )
    else:
        with open(computing, "w") as file:
            pass

        print(f"Creating checkpoint {computing}")

        if status == "new":
            print("Dataset is not published. Aborting computation.")
            return

        if build_bags:
            print("Checking if output directory exists.")
            output_directory = data_type + "-" + status + "-" + dataset["dataset_uuid"]

            print("Creating output directory " + output_directory + ".")
            if Path(output_directory).exists() and Path(output_directory).is_dir():
                print("Output directory found. Removing old copy.")
                rmtree(output_directory)
                os.mkdir(output_directory)
            else:
                print("Output directory does not exist. Creating directory.")
                os.mkdir(output_directory)

            print("Making file.tsv")
            if not Path(".data").exists():
                Path(".data").mkdir()
            temp_file = (
                ".data/" + data_directory.replace("/", "_").replace(" ", "_") + ".pkl"
            )
