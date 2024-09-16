import os
from urllib.request import urlopen
from warnings import warn as warning

import pandas as pd
import yaml


def __get_organ_from_uberon(organ: str) -> str:
    """
    For full list, visit
    https://github.com/hubmapconsortium/search-api/blob/test-release/src/search-schema/data/definitions/enums/organ_types.yaml
    """

    url = "https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/organ_types.yaml"

    warning(
        "Temporarily using the file https://raw.githubusercontent.com/hubmapconsortium/search-api/icaoberg-patch-1/src/search-schema/data/definitions/enums/organ_types.yaml"
    )
    url = "https://raw.githubusercontent.com/hubmapconsortium/search-api/icaoberg-patch-1/src/search-schema/data/definitions/enums/organ_types.yaml"
    with urlopen(url) as f:
        tbl = yaml.safe_load(f)

    organs = {}
    for key in tbl:
        if "iri" in tbl[key]:
            s = tbl[key]["iri"]
            uberon_entry = s[s.rfind("/") + 1 :]
            organs[key] = uberon_entry
            if "description" in tbl[key]:
                desc = tbl[key]["description"]
                organs[desc] = uberon_entry
                organs[desc.lower()] = uberon_entry
            if key == "LK":
                organs["left kidney"] = uberon_entry
            elif key == "RK":
                organs["right kidney"] = uberon_entry
    for i in range(1, 12):
        organs["LY%02d" % i] = organs["LY"]

    return organs[organ].replace("_", ":")


def _build_dataframe(
    biosample_id: str, biosample_url: str, data_provider: str, organ: str
) -> pd.DataFrame:
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2024:"
    headers = [
        "id_namespace",
        "local_id",
        "project_id_namespace",
        "project_local_id",
        "persistent_id",
        "creation_time",
        "sample_prep_method",
        "anatomy",
    ]
    df = pd.DataFrame(columns=headers)
    df = df.append(
        {
            "id_namespace": id_namespace,
            "local_id": biosample_id,
            "project_id_namespace": id_namespace,
            "project_local_id": data_provider,
            "persistent_id": biosample_url,
            "anatomy": __get_organ_from_uberon(organ),
        },
        ignore_index=True,
    )

    return df


def create_manifest(
    biosample_id: str,
    biosample_url: str,
    data_provider: str,
    organ: str,
    output_directory: str,
) -> bool:
    try:
        filename = os.path.join(output_directory, "biosample.tsv")
        df = _build_dataframe(biosample_id, biosample_url, data_provider, organ)
        df.to_csv(filename, sep="\t", index=False)

        return True
    except:
        return False
