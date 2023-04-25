import os

import pandas as pd


def _build_dataframe(donor_metadata: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2023:"
    headers = [
        "id_namespace",
        "local_id",
        "project_id_namespace",
        "project_local_id",
        "persistent_id",
        "creation_time",
        "granularity",
        "sex",
        "ethnicity",
        "age_at_enrollment",
    ]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "id_namespace": id_namespace,
            "local_id": donor_metadata["local_id"],
            "project_id_namespace": id_namespace,
            "project_local_id": donor_metadata["project_local_id"],
            "persistent_id": donor_metadata["persistent_id"],
            "granularity": donor_metadata["granularity"],
            "sex": donor_metadata["sex"],
            "ethnicity": donor_metadata["ethnicity"],
        },
        ignore_index=True,
    )

    df = df[
        [
            "id_namespace",
            "local_id",
            "project_id_namespace",
            "project_local_id",
            "persistent_id",
            "creation_time",
            "granularity",
            "sex",
            "ethnicity",
            "age_at_enrollment",
        ]
    ]

    return df


def create_manifest(donor_metadata: dict, output_directory: str) -> bool:
    """
    Manifest file builder.
    """
    try:
        filename = os.path.join(output_directory, "subject.tsv")
        df = _build_dataframe(donor_metadata)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
