import os
import traceback
import pandas as pd


def _build_dataframe(data_provider: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "id_namespace",
        "local_id",
        "persistent_id",
        "creation_time",
        "abbreviation",
        "name",
        "description",
    ]
    df = pd.DataFrame(columns=headers)

    parent_local_id = "HuBMAP"
    parent_local_description = "Human BioMolecular Atlas Program"
    row = pd.DataFrame(
        {
            "id_namespace": [id_namespace],
            "local_id": [parent_local_id],
            "abbreviation": [parent_local_id],
            "name": [parent_local_description],
        },
    )
    df = pd.concat([df, row], ignore_index=True)

    row = pd.DataFrame(
        {
            "id_namespace": [id_namespace],
            "local_id": [data_provider],
            "abbreviation": [data_provider.replace(" ", "_")],
            "name": [data_provider],
        },
    )
    df = pd.concat([df, row], ignore_index=True)

    return df


def create_manifest(data_provider: str, output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "project.tsv")
        df = _build_dataframe(data_provider)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        traceback.print_exc()
        return False
