import os

import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    headers = ["subject_id_namespace", "subject_local_id", "role_id", "taxonomy_id"]

    df = pd.DataFrame(columns=headers)

    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "subject_role_taxonomy.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
