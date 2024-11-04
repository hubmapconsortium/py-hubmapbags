import os
import traceback
import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = ["id", "abbreviation", "name", "description"]

    df = pd.DataFrame(columns=headers)
    row = pd.DataFrame(
        {
            "id": ["tag:hubmapconsortium.org,2024:"],
            "abbreviation": ["hubmap"],
            "name": ["hubmap"],
            "description": ["Human BioMolecular Atlas Program"],
        },
    )

    df = pd.concat([df, row], ignore_index=True)
    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "id_namespace.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        traceback.print_exc()
        return False
