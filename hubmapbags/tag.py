from .apis import get_dataset_info


def __is_upload_directory_empty(metadata):
    return None


def __is_dataset_directory_empty(metadata):
    return None


def __is_upload_directory_empty(metadata):
    return None


def __has_registration_metadata(metadata):
    return None


def __has_doi_url(metadata):
    return None


def __has_orcid_contributor_metadata(metadata):
    return None


def __has_empty_directories(metadata):
    return None


def dataset(hubmap_id, token=None, debug=False):
    metadata = get_dataset_info(hubmap_id, token=token)
    if not metadata:
        print("Unable to retrieve metadata.")
        return {}
    else:
        tags = {
            "is_upload_directory_empty": __is_upload_directory_empty(metadata),
            "is_dataset_directory_empty": __is_dataset_directory_empty(metadata),
            "is_upload_directory_empty": __is_upload_directory_empty(metadata),
            "has_registration_metadata": __has_registration_metadata(metadata),
            "has_doi_url": __has_doi_url(metadata),
            "has_orcid_contributor_metadata": __has_orcid_contributor_metadata(
                metadata
            ),
            "has_empty_directories": __has_empty_directories(metadata),
        }

        return tags
