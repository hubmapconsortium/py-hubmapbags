import os

import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    headers = ["biosample_id_namespace", "biosample_local_id", "gene"]

    df = pd.DataFrame(columns=headers)

    return df


def create_manifest(output_directory: str) -> bool:
    """
    Manifest file builder.
    """
    try:
        filename = os.path.join(output_directory, "biosample_gene.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
