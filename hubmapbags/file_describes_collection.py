import os
import pickle
from pathlib import Path

import pandas as pd
from .apis import *


def _build_dataframe(hubmap_id, hubmap_uuid, directory):
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

    if not Path(".data").exists():
        Path(".data").mkdir()

    temp_file = f".data/{hubmap_uuid}.tsv"
    if Path(temp_file).exists():
        print("Temporary file " + temp_file + " found. Loading df into memory.")
        df = pd.read_csv(temp_file, sep="\t")

        df = df[df["filename"].str.contains("metadata.tsv")]
        df["subject_id_namespace"] = id_namespace
        df["file_id_namespace"] = id_namespace
        print(df.keys())
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
    else:
        df = []

    return df


def create_manifest(hubmap_id, hubmap_uuid, directory, output_directory):
    filename = os.path.join(output_directory, "file_describes_collection.tsv")

    if not Path(".data").exists():
        Path(".data").mkdir()

    temp_file = f".data/{hubmap_uuid}.tsv"
    if not Path(directory).exists() and not Path(temp_file).exists():
        print(
            f"Data directory {directory} does not exist. Temp file was not found either."
        )
        return False
    else:
        if Path(temp_file).exists():
            print("Temp file " + temp_file + " found. Continuing computation.")
        df = _build_dataframe(hubmap_id, hubmap_uuid, directory)
        df.to_csv(filename, sep="\t", index=False)

        return True
