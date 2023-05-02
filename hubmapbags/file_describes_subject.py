import os

import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2023:"
    headers = [
        "file_id_namespace",
        "file_local_id",
        "subject_id_namespace",
        "subject_local_id",
    ]

    df = pd.DataFrame(columns=headers)

    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "file_describes_subject.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
