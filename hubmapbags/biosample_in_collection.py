import os

import pandas as pd


def _build_dataframe(biosample_id: str, hubmap_id: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "biosample_id_namespace",
        "biosample_local_id",
        "collection_id_namespace",
        "collection_local_id",
    ]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "biosample_id_namespace": id_namespace,
            "biosample_local_id": biosample_id,
            "collection_id_namespace": id_namespace,
            "collection_local_id": hubmap_id,
        },
        ignore_index=True,
    )

    return df


def create_manifest(biosample_id: str, hubmap_id: str, output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "biosample_in_collection.tsv")
        df = _build_dataframe(biosample_id, hubmap_id)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
