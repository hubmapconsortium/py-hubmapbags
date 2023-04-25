import os
from pathlib import Path

import pandas as pd


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

    if not Path(".data").exists():
        Path(".data").mkdir()

    temp_file = f".data/{hubmap_uuid}.tsv"

    if Path(temp_file).exists():
        print("Temporary file " + temp_file + " found. Loading df into memory.")
        id_namespace = "tag:hubmapconsortium.org,2023:"
        df = pd.read_csv(temp_file, sep="\t")

        df["collection_id_namespace"] = id_namespace
        df["file_id_namespace"] = id_namespace

        df = df.rename(columns={"file_uuid": "file_local_id"}, errors="raise")
        df["collection_local_id"] = hubmap_id

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
    """
    Manifest file builder.
    """
    filename = os.path.join(output_directory, "file_in_collection.tsv")

    if not Path(".data").exists():
        Path(".data").mkdir()
    temp_file = f".data/{hubmap_uuid}.tsv"
    if not Path(directory).exists() and not Path(temp_file).exists():
        print(
            f"Data directory {directory} does not exist. Temp file was not found either."
        )

        return False
    else:
        df = _build_dataframe(hubmap_id, hubmap_uuid, directory)
        df.to_csv(filename, sep="\t", index=False)

        return True
