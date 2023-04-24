import os
import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2023:"
    headers = ["id", "abbreviation", "name", "description"]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "id": "tag:hubmapconsortium.org,2023:",
            "abbreviation": "hubmap",
            "name": "hubmap",
            "description": "Human BioMolecular Atlas Program",
        },
        ignore_index=True,
    )

    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "id_namespace.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
