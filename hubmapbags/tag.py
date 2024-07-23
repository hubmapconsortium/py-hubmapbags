from .apis import get_dataset_info
from pathlib import Path

def _is_upload_directory_empty(metadata):
    """Check if the upload directory is empty."""
    return None


def _is_doi_org_url(metadata):
    return 'doi_url' in metadata and 'doi.org' in metadata['doi_url']

def _missing_contributors_metadata_file(metadata):
    """Check if the contributors metadata file is missing."""
    return None


def _is_dataset_directory_empty(metadata):
    if metadata['entity_type'] == 'Dataset':
        directory = f'/hive/hubmap/data/{metadata["local_directory_rel_path"]}'
        if Path(directory).exists():
            # List all files including hidden files (using .glob('**/*'))
            files = list(Path(directory).glob('**/*'))
            # Exclude '.' and '..' (current and parent directory)
            files = [f for f in files if f.is_file() and not f.name.startswith('.')]

            # Return True if no files (including hidden ones) found
            return len(files) == 0
        else:
            print(f'Directory {directory} does not exist')
            return True
    else:
        return None

def _is_contributors_metadata_file_empty(metadata):
    """Check if the contributors metadata file is empty."""
    return None


def _has_registration_metadata(metadata):
    """Check if the registration metadata is present."""
    return None


def _has_doi_url(metadata):
    return 'doi_url' in metadata

def _missing_instrument_metadata_file(metadata):
    """Check if the instrument metadata file is missing."""
    return None


def _has_orcid_contributor_metadata(metadata):
    """Check if the ORCID contributor metadata is present."""
    return None


def _has_empty_directories(metadata):
    """Check if there are empty directories."""
    return None


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
