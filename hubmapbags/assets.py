import datetime
import math

import pandas as pd
import requests

from . import apis, uuids


def __prepare_dataframe(
    hubmap_id: str, token: str, data: dict, instance: str = "prod"
) -> pd.DataFrame:
    df = pd.DataFrame(data)
    metadata = apis.get_dataset_info(hubmap_id, token=token, instance="prod")

    df["contains_human_genetic_sequences"] = metadata[
        "contains_human_genetic_sequences"
    ]
    df["data_types"] = metadata["data_types"][0]
    df["dataset_id"] = metadata["hubmap_id"]
    df["dataset_uuid"] = metadata["uuid"]
    df["status"] = metadata["status"]
    df["doi"] = metadata["doi_url"]

    if "base_dir" in df.keys():
        df.drop(["base_dir"], axis=1, inplace=True)

    if "assets_ready" not in df.keys():
        df["assets_ready"] = None

    if "assets_status_code" not in df.keys():
        df["assets_status_code"] = None

    if "assets_timestamp" not in df.keys():
        df["assets_timestamp"] = None

    if "assets_login" not in df.keys():
        df["assets_timestamp"] = None

    return df


def __get_assets_link(dataset_uuid: str, path: str) -> str:
    url = (
        "https://g-d00e7b.09193a.5898.dn.glob.us/7277b2460f9c548004496508684a90ef/"
        + dataset_uuid
        + "/"
        + path
    )
    url = "https://g-d00e7b.09193a.5898.dn.glob.us/" + dataset_uuid + "/" + path
    return url


def __check_assets_link(url: str) -> dict:
    response = requests.request("HEAD", url)
    return response


def __assets_populate(df: pd.DataFrame, index: int):
    datum = df.loc[index]

    if datum["assets_ready"] is None or math.isnan(datum["assets_ready"]):
        datum["assets"] = __get_assets_link(datum["dataset_uuid"], datum["path"])
        response = __check_assets_link(datum["assets"])
        if response.status_code == 200:
            status = True
        else:
            status = False

        if response.url.find("auth.assets.org") and response.url.find("prompt=login"):
            df.at[index, "assets_login"] = True
        else:
            df.at[index, "assets_login"] = False

        df.at[index, "assets_ready"] = status
        df.at[index, "assets_status_code"] = int(response.status_code)
        df.at[index, "assets_timestamp"] = datetime.datetime.now()

    return df


def get_dataset_info(
    hubmap_id: str, token: str, instance: str = "prod"
) -> pd.DataFrame:
    data = uuids.get_uuids(hubmap_id, instance=instance, token=token)
    df = __prepare_dataframe(hubmap_id, data, instance=instance, token=token)

    for index in range(len(df)):
        df = __assets_populate(df, index)

    return df
