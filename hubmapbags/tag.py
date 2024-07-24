from .apis import get_dataset_info
from pathlib import Path


def _is_upload_directory_empty(metadata):
    """Check if the upload directory is empty."""
    return None


def _is_doi_org_url(metadata):
    """
    Checks if the metadata contains a DOI (Digital Object Identifier) URL that points to the 'doi.org' domain.

    Parameters:
    metadata (dict): A dictionary containing metadata information. It is expected to have a key "doi_url" if a DOI URL is present.

    Returns:
    bool: True if the metadata contains a "doi_url" key and the value of "doi_url" includes "doi.org", otherwise False.

    Example:
    >>> metadata = {"doi_url": "https://doi.org/10.1234/example"}
    >>> _is_doi_org_url(metadata)
    True

    >>> metadata = {"doi_url": "https://example.com/10.1234/example"}
    >>> _is_doi_org_url(metadata)
    False

    >>> metadata = {"title": "A Sample Paper"}
    >>> _is_doi_org_url(metadata)
    False
    """
    return "doi_url" in metadata and "doi.org" in metadata["doi_url"]


def _missing_contributors_metadata_file(metadata):
    """
    Checks if the contributors metadata file exists based on the metadata provided.

    Parameters:
    metadata (dict): A dictionary containing metadata information. It is expected to have the necessary information to locate the contributors file.

    Returns:
    bool or None:
        - `True` if the contributors file is expected but does not exist.
        - `False` if the contributors file exists.
        - `None` if the contributors file is not specified in the metadata.

    Example:
    >>> metadata = {"contributors_file": "/path/to/contributors.txt"}
    >>> _missing_contributors_metadata_file(metadata)
    False  # Assuming the file exists

    >>> metadata = {"contributors_file": "/path/to/missing_contributors.txt"}
    >>> _missing_contributors_metadata_file(metadata)
    True  # Assuming the file does not exist

    >>> metadata = {"title": "A Sample Paper"}
    >>> _missing_contributors_metadata_file(metadata)
    None  # No contributors file specified in metadata
    """
    if __get_contributors_file(metadata) is not None:
        return not Path(__get_contributors_file(metadata)).exists()
    else:
        return None


def __get_directory(metadata):
    """
    Constructs and returns the directory path based on the metadata provided.

    Parameters:
    metadata (dict): A dictionary containing metadata information. It should include the key "local_directory_rel_path" and, depending on its value, "uuid".

    Returns:
    str: The constructed directory path based on the metadata.

    Example:
    >>> metadata = {"local_directory_rel_path": "protected/example/path", "uuid": "1234-5678"}
    >>> __get_directory(metadata)
    '/hive/hubmap/data/protected/example/path'

    >>> metadata = {"local_directory_rel_path": "example/path", "uuid": "1234-5678"}
    >>> __get_directory(metadata)
    '/hive/hubmap/data/public/1234-5678'
    """
    if "protected" in metadata["local_directory_rel_path"]:
        directory = f'/hive/hubmap/data/{metadata["local_directory_rel_path"]}'
    else:
        directory = f'/hive/hubmap/data/public/{metadata["uuid"]}'

    return directory


def __get_contributors_file(metadata):
    """
    Retrieves the path to the contributors file based on the metadata provided.

    Parameters:
    metadata (dict): A dictionary containing metadata information. It should include nested keys to locate the contributors file path.

    Returns:
    str or None: The constructed path to the contributors file if the relevant metadata is present, otherwise None.

    Example:
    >>> metadata = {
    ...     "ingest_metadata": {
    ...         "metadata": {
    ...             "contributors_path": "contributors/contributors.txt"
    ...         }
    ...     },
    ...     "local_directory_rel_path": "protected/example/path",
    ...     "uuid": "1234-5678"
    ... }
    >>> __get_contributors_file(metadata)
    '/hive/hubmap/data/protected/example/path/contributors/contributors.txt'

    >>> metadata = {
    ...     "ingest_metadata": {
    ...         "metadata": {}
    ...     },
    ...     "local_directory_rel_path": "example/path",
    ...     "uuid": "1234-5678"
    ... }
    >>> __get_contributors_file(metadata)
    None
    """
    if (
        "ingest_metadata" in metadata
        and "metadata" in metadata["ingest_metadata"]
        and "contributors_path" in metadata["ingest_metadata"]["metadata"]
    ):
        return f'{__get_directory(metadata)}/{metadata["ingest_metadata"]["metadata"]["contributors_path"]}'
    else:
        return None


def _is_dataset_directory_empty(metadata):
    """
    Checks if the directory for a dataset is empty based on the metadata provided.

    Parameters:
    metadata (dict): A dictionary containing metadata information. It must include the "entity_type" key and relevant information to construct the directory path.

    Returns:
    bool or None:
        - `True` if the directory exists and is empty.
        - `False` if the directory exists and is not empty.
        - `True` if the directory does not exist.
        - `None` if the entity type is not "Dataset".

    Example:
    >>> metadata = {
    ...     "entity_type": "Dataset",
    ...     "local_directory_rel_path": "example/path",
    ...     "uuid": "1234-5678"
    ... }
    >>> _is_dataset_directory_empty(metadata)
    True  # Assuming the directory is empty

    >>> metadata = {
    ...     "entity_type": "Sample",
    ...     "local_directory_rel_path": "example/path",
    ...     "uuid": "1234-5678"
    ... }
    >>> _is_dataset_directory_empty(metadata)
    None
    """
    if metadata["entity_type"] == "Dataset":
        directory = __get_directory(metadata)

        if Path(directory).exists():
            files = list(Path(directory).glob("**/*"))
            files = [f for f in files if f.is_file() and not f.name.startswith(".")]

            return len(files) == 0
        else:
            print(f"Directory {directory} does not exist")
            return True
    else:
        return None


def _is_contributors_metadata_file_empty(metadata):
    file = __get_contributors_file(metadata)
    if file is not None and Path(file).exists():
        if Path(file).stat().st_size == 0:
            return True
        else:
            return False
    else:
        return None


def _has_registration_metadata(metadata):
    """Check if the registration metadata is present."""
    return None


def _has_doi_url(metadata):
    return "doi_url" in metadata


def _missing_instrument_metadata_file(metadata):
    """Check if the instrument metadata file is missing."""
    return None


def _has_orcid_contributor_metadata(metadata):
    """Check if the ORCID contributor metadata is present."""
    return None


def _has_empty_directories(metadata):
    directory = Path(__get_directory(metadata))
    if (
        len(
            [
                subdir
                for subdir in directory.rglob("*")
                if subdir.is_dir() and not any(subdir.iterdir())
            ]
        )
        > 0
    ):
        return True
    else:
        return False


def _instrument_metadata_file_is_empty(metadata):
    """Check if the instrument metadata file is empty."""
    return None


def _missing_assay_specific_files(metadata):
    """Check if assay-specific files are missing."""
    return None


def _assay_specific_files_are_empty(metadata):
    """Check if assay-specific files are empty."""
    return None


def _missing_dataset_files_in_metadata(metadata):
    """Check if dataset files are missing in the metadata."""
    return None


def _extra_datasets_in_upload(metadata):
    """Check if there are extra datasets in the upload."""
    return None


def __is_nonstandard_directory_structure(metadata):
    return None


def __has_nonstandard_filenames_extensions(metadata):
    return None


def __is_awating_review(metadata):
    return None


def __unrecognized_provenance(metadata):
    return None


def __is_awaiting_processing_pipeline(metadata):
    return None


def __failed_to_produce_derived_dataset(metadata):
    return None


def __is_flagged_for_deletion(metadata):
    return None


def __missing_description_field_in_the_portal(metadata):
    return None


def dataset(hubmap_id, token=None, debug=False):
    """Retrieve dataset information and generate tags based on metadata."""
    metadata = get_dataset_info(hubmap_id, token=token)
    if not metadata:
        print("Unable to retrieve metadata.")
        return {}

    checks = {
        "hubmap_id": metadata["hubmap_id"],
        "uuid": metadata["uuid"],
        "status": metadata["status"],
        "dataset_type": metadata["dataset_type"],
        "missing_description_field_in_the_portal": __missing_description_field_in_the_portal(
            metadata
        ),
        "is_flagged_for_deletion": __is_flagged_for_deletion(metadata),
        "failed_to_produce_derived_dataset": __failed_to_produce_derived_dataset(
            metadata
        ),
        "is_awaiting_processing_pipeline": __is_awaiting_processing_pipeline(metadata),
        "is_awating_review": __is_awating_review(metadata),
        "unrecognized_provenance": __unrecognized_provenance(metadata),
        "has_nonstandard_filenames_extensions": __has_nonstandard_filenames_extensions(
            metadata
        ),
        "is_nonstandard_directory_structure": __is_nonstandard_directory_structure(
            metadata
        ),
        "extra_datasets_in_upload": _extra_datasets_in_upload(metadata),
        "missing_dataset_files_in_metadata": _missing_dataset_files_in_metadata(
            metadata
        ),
        "assay_specific_files_are_empty": _assay_specific_files_are_empty(metadata),
        "missing_assay_specific_files": _missing_assay_specific_files(metadata),
        "is_upload_directory_empty": _is_upload_directory_empty(metadata),
        "is_dataset_directory_empty": _is_dataset_directory_empty(metadata),
        "is_doi_org_url": _is_doi_org_url(metadata),
        "is_contributors_metadata_file_empty": _is_contributors_metadata_file_empty(
            metadata
        ),
        "missing_contributors_metadata_file": _missing_contributors_metadata_file(
            metadata
        ),
        "missing_instrument_metadata_file": _missing_instrument_metadata_file(metadata),
        "has_registration_metadata": _has_registration_metadata(metadata),
        "has_orcid_contributor_metadata": _has_orcid_contributor_metadata(metadata),
        "instrument_metadata_file_is_empty": _instrument_metadata_file_is_empty(
            metadata
        ),
        "has_doi_url": _has_doi_url(metadata),
        "has_empty_directories": _has_empty_directories(metadata),
    }

    tags = dict(sorted(checks.items()))
    return tags
