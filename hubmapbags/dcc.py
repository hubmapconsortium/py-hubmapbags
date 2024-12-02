import os
import traceback
import pandas as pd


def _build_dataframe() -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    headers = [
        "id",
        "dcc_name",
        "dcc_abbreviation",
        "dcc_description",
        "contact_email",
        "contact_name",
        "dcc_url",
        "project_id_namespace",
        "project_local_id",
    ]
    df = pd.DataFrame(columns=headers)
    row = pd.DataFrame(
        {
            "id": ["cfde_registry_dcc:hubmap"],
            "project_id_namespace": ["tag:hubmapconsortium.org,2024:"],
            "project_local_id": ["HuBMAP"],
            "contact_email": ["cfde-submissions@hubmapconsortium.org"],
            "contact_name": ["Ivan Cao-Berg"],
            "dcc_abbreviation": ["HuBMAP"],
            "dcc_name": ["HuBMAP"],
            "dcc_description": ["Human BioMolecular Atlas Program"],
            "dcc_url": ["http://portal.hubmapconsortium.org"],
        },
    )

    df = pd.concat([df, row], ignore_index=True)
    df = df[
        [
            "id",
            "dcc_name",
            "dcc_abbreviation",
            "dcc_description",
            "contact_email",
            "contact_name",
            "dcc_url",
            "project_id_namespace",
            "project_local_id",
        ]
    ]

    return df


def create_manifest(output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "dcc.tsv")
        df = _build_dataframe()
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        traceback.print_exc()
        return False
