import os

import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = ["id", "clade", "name", "description", "synonyms"]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "id": "NCBI:txid9606",
            "clade": "",
            "name": "Homo sapiens Linnaeus, 1758",
            "description": "Homo sapiens Linnaeus, 1758",
        },
        ignore_index=True,
    )

    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "ncbi_taxonomy.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
