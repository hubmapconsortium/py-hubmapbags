import os
import hubmapinventory
import pandas as pd

from .apis import *


def _build_dataframe(
    hubmap_id: str,
    token: str,
    hubmap_uuid: str,
    inventory_directory: str,
    directory: str,
) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "file_id_namespace",
        "file_local_id",
        "collection_id_namespace",
        "collection_local_id",
    ]

    df = hubmapinventory.get(
        hubmap_id=hubmap_id, token=token, inventory_directory=inventory_directory
    )
    df = df[df["filename"].str.contains("metadata.tsv")]
    df["subject_id_namespace"] = id_namespace
    df["file_id_namespace"] = id_namespace
    df = df.rename(columns={"file_uuid": "file_local_id"}, errors="raise")
    df["collection_local_id"] = hubmap_id
    df["collection_id_namespace"] = id_namespace
    df = df[
        [
            "file_id_namespace",
            "file_local_id",
            "collection_id_namespace",
            "collection_local_id",
        ]
    ]

    return df


def create_manifest(
    hubmap_id: str,
    hubmap_uuid: str,
    token: str,
    directory: str,
    inventory_directory: str,
    output_directory: str,
) -> bool:
    filename = os.path.join(output_directory, "file_describes_collection.tsv")
    df = _build_dataframe(
        hubmap_id=hubmap_id,
        token=token,
        hubmap_uuid=hubmap_uuid,
        inventory_directory=inventory_directory,
        directory=directory,
    )
    df.to_csv(filename, sep="\t", index=False)

    return True
