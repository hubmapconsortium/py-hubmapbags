import os
import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    headers = ["phenotype", "disease"]

    df = pd.DataFrame(columns=headers)

    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "phenotype_disease.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
