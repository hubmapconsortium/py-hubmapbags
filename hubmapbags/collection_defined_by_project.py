import os
import traceback
import pandas as pd


def _build_dataframe(collection_id: str, project_id: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "collection_id_namespace",
        "collection_local_id",
        "project_id_namespace",
        "project_local_id",
    ]
    df = pd.DataFrame(columns=headers)
    row = pd.DataFrame(
        {
            "collection_id_namespace": [id_namespace],
            "collection_local_id": [collection_id],
            "project_id_namespace": [id_namespace],
            "project_local_id": [project_id],
        },
    )

    df = pd.concat([df, row], ignore_index=True)
    return df


def create_manifest(collection_id: str, project_id: str, output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "collection_defined_by_project.tsv")
        df = _build_dataframe(collection_id, project_id)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        traceback.print_exc()
        return False
