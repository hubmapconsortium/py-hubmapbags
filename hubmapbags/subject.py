import os

import pandas as pd


def _build_dataframe(project_id, donor_id, donor_url):
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
            "local_id": donor_id,
            "project_id_namespace": id_namespace,
            "project_local_id": project_id,
            "persistent_id": donor_url,
            "granularity": "cfde_subject_granularity:0",
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


def create_manifest(project_id, donor_id, donor_url, output_directory):
    filename = os.path.join(output_directory, "subject.tsv")
    df = _build_dataframe(project_id, donor_id, donor_url)
    df.to_csv(filename, sep="\t", index=False)

    return True
