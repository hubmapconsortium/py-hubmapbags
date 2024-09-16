import os

import pandas as pd


def _build_dataframe(biosample_id: str, subject_id: str) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "biosample_id_namespace",
        "biosample_local_id",
        "subject_id_namespace",
        "subject_local_id",
        "age_at_sampling",
    ]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "biosample_id_namespace": id_namespace,
            "biosample_local_id": biosample_id,
            "subject_id_namespace": id_namespace,
            "subject_local_id": subject_id,
            "age_at_sampling": "",
        },
        ignore_index=True,
    )

    return df


def create_manifest(biosample_id: str, subject_id: str, output_directory: str) -> bool:
    try:
        filename = os.path.join(output_directory, "biosample_from_subject.tsv")
        df = _build_dataframe(biosample_id, subject_id)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
