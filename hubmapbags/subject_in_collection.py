import os
import traceback
import pandas as pd


def _build_dataframe(donor_id: str, hubmap_id: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "subject_id_namespace",
        "subject_local_id",
        "collection_id_namespace",
        "collection_local_id",
    ]
    df = pd.DataFrame(columns=headers)
    row = pd.DataFrame(
        {
            "subject_id_namespace": [id_namespace],
            "subject_local_id": [donor_id],
            "collection_id_namespace": [id_namespace],
            "collection_local_id": [hubmap_id],
        },
    )

    df = pd.concat([df, row], ignore_index=True)
    return df


def create_manifest(donor_id: str, hubmap_id: str, output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "subject_in_collection.tsv")
        df = _build_dataframe(donor_id, hubmap_id)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        traceback.print_exc()
        return False
