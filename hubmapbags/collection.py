import os
from datetime import datetime
import pandas as pd


def _convert_to_datetime(neo4j_date_time: str):
    return datetime.fromtimestamp(neo4j_date_time / 1000.0)


def _build_dataframe(dataset_metadata: dict) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2023:"
    headers = [
        "id_namespace",
        "local_id",
        "persistent_id",
        "creation_time",
        "abbreviation",
        "name",
        "description",
        "has_time_series_data",
    ]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "id_namespace": id_namespace,
            "local_id": dataset_metadata["local_id"],
            "persistent_id": dataset_metadata["persistent_id"],
            "creation_time": _convert_to_datetime(dataset_metadata["creation_time"]),
            "name": dataset_metadata["name"],
            "description": dataset_metadata["description"].str.replace("\n", ""),
        },
        ignore_index=True,
    )

    return df


def create_manifest(dataset_metadata: dict, output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "collection.tsv")
        df = _build_dataframe(dataset_metadata)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
