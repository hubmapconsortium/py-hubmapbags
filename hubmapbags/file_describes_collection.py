import os
from pathlib import Path

import pandas as pd

from .apis import *


def _build_dataframe(hubmap_id: str, hubmap_uuid: str, directory: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2023:"
    headers = [
        "file_id_namespace",
        "file_local_id",
        "collection_id_namespace",
        "collection_local_id",
    ]

    df = hubmapinventory.get(hubmap_id=hubmap_id, token=token)
    print(f"Temporary file {temp_file} found. Loading df into memory.")
    df = pd.read_csv(temp_file, sep="\t")

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
    hubmap_id: str, hubmap_uuid: str, directory: str, output_directory: str
) -> bool:
    filename = os.path.join(output_directory, "file_describes_collection.tsv")
    df = _build_dataframe(hubmap_id, hubmap_uuid, directory)
    df.to_csv(filename, sep="\t", index=False)

    return True
