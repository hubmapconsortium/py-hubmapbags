import datetime
import hashlib
import mimetypes
import os
from itertools import chain
from pathlib import Path
import hubmapinventory
import pandas as pd


def __get_persistent_id(file_uuid: str) -> str:
    url = f"http://hubmap-drs.hubmapconsortium.org/v1/objects/{file_uuid}"
    return url


def __get_filename(file: str) -> str:
    """
    Helper method that returns a CFDE compatible version of a filename
    """

    return file.name.replace(" ", "%20")


def __get_file_extension(file: str) -> str:
    """
    Helper method that returns the file extension.
    """
    return file.suffix


def __get_file_size(file: str) -> int:
    """
    Helper method that computes and returns the file size in bytes.
    """

    return file.stat().st_size


def __get_md5(file: str) -> str:
    """
    Helper method that computes and return a file md5 checksum.
    """

    blocksize = 2**20
    m = hashlib.md5()

    with open(file, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)

    return m.hexdigest()


def __get_relative_local_id(file: str, hubmap_uuid: str) -> str:
    """
    Helper function the return the relative local id.
    """

    file = str(file)
    relative_local_id = file[file.find(hubmap_uuid) + len(hubmap_uuid) + 1 :]
    return relative_local_id


def __get_sha256(file: str) -> str:
    """
    Helper method that computes and return a file sha256 checksum.
    """

    blocksize = 2**20
    m = hashlib.md5()

    with open(file, "rb") as f:
        while True:
            buf = f.read(blocksize)
            if not buf:
                break
            m.update(buf)

    return m.hexdigest()


def __get_file_creation_date(file: str) -> str:
    """
    Helper method that return a file creation date.
    """

    t = os.path.getmtime(str(file))
    return str(datetime.datetime.fromtimestamp(t).strftime("%Y-%m-%d"))


def __get_data_type(file: str) -> str:
    """
    Helper method that maps a file extension to an EDAM data format term.
    """

    extension = __get_file_extension(file)

    try:
        formats = {}
        formats[".tsv"] = "data:2526"  # tsv
        formats[".tif"] = "data:2968"  # tiff
        formats[".tiff"] = "data:2968"  # tiff
        formats[".png"] = "data:2968"  # png
        formats[".jpg"] = "data:2968"  # jpg
        formats[".ome.tiff"] = "data:2968"  # ome.tiff
        formats[".fastq"] = "data:2044"  # txt
        formats[".txt"] = "data:2526"  # txt
        formats[".xml"] = "data:2526"  # xml
        formats[".czi"] = "data:2968"  # czi
        formats[".gz"] = "data:2044"  # gz
        formats[".json"] = "data:2526"  # json
        formats[".xlsx"] = "data:2526"  # xlsx
        formats["._truncated_"] = ""  # ?
        formats[".tgz"] = ""  # tgz
        formats[".tar.gz"] = ""  # tar.gz
        formats[".csv"] = "data:2526"  # csv
        formats[".html"] = "data:2526"  # html
        formats[".htm"] = "data:2526"  # htm
        formats[".h5"] = ""  # h5
        formats[""] = ""  # other

        return formats[extension]
    except:
        print("Unable to find key for data type " + extension)
        return None


def __get_mime_type(file: str) -> str:
    """
    Helper function that return a file MIME type.
    """

    return mimetypes.MimeTypes().guess_type(str(file))[0]


def __get_file_format(file: str) -> str:
    """
    Helper method that maps a file extension to an EDAM file format term.
    """

    extension = __get_file_extension(file)

    try:
        formats = {}
        formats[".tsv"] = "format:2330"  # tsv
        formats[".tif"] = "format:3547"  # tiff
        formats[".tiff"] = "format:3547"  # tiff
        formats[".png"] = "format:3547"  # png
        formats[".jpg"] = "format:3547"  # jpg
        formats[".ome.tiff"] = "format:3547"  # ome.tiff
        formats[".fastq"] = "format:2330"  # txt
        formats[".txt"] = "format:2330"  # txt
        formats[".xml"] = "format:2332"  # xml
        formats[".czi"] = "format:3547"  # czi
        formats[".gz"] = "format:3989"  # gz
        formats[".json"] = "format:2330"  # json
        formats[".xlsx"] = "format:3468"  # xlsx
        formats["._truncated_"] = "format:2330"  # ?
        formats[".tgz"] = "format:3989"  # tgz
        formats[".csv"] = "format:3752"  # csv
        formats[".html"] = "format:2331"  # html
        formats[".htm"] = "format:2331"  # htm
        formats[".tar.gz"] = "format:3989"  # tgz
        formats[".h5"] = "format:3590"  # h5
        formats[""] = ""

        return formats[extension]
    except:
        print("Unable to find key for file format " + extension)
        return None


def __get_dbgap_study_id(file: str, dbgap_study_id: str) -> str:
    if dbgap_study_id == "" or dbgap_study_id is None:
        return ""
    else:
        if str(file).find("tar.gz") > 0:
            return dbgap_study_id
        else:
            return ""


def __get_assay_type_from_obi(assay_type: str) -> str:
    assay = {}
    assay["af"] = "OBI:0003087"  # AF
    assay["atacseq-bulk"] = "OBI:0003089"  # Bulk ATAC-seq
    assay["bulk-rna"] = "OBI:0001271"  # Bulk RNA-seq
    assay["scrna-seq-10x"] = "OBI:0002631"  # scRNA-seq
    assay["snatacseq"] = "OBI:0002762"  # snATAC-seq
    assay["wgs"] = "OBI:0002117"  # WGS
    assay["codex"] = "OBI:0003093"  # CODEX
    assay["lightsheet"] = "OBI:0003098"  # Lightsheet
    assay["imc"] = "OBI:0003096"  # IMC
    assay["imc2d"] = "OBI:0003096"  # IMC
    assay["imc3d"] = "OBI:0003096"  # IMC
    assay["maldi-ims-neg"] = "OBI:0003099"
    assay["maldi-ims-pos"] = "OBI:0003099"
    assay["pas"] = "OBI:0003103"
    assay["slide-seq"] = "OBI:0003107"
    assay["seqfish"] = "OBI:0003094"
    assay["lc-ms-untargeted"] = "OBI:0003097"
    assay["lc-ms_bottom_up"] = "OBI:0003097"
    assay["lc-ms_top_down"] = "OBI:0003097"  # ask alex
    assay["tmt-lc-ms"] = "OBI:0003097"
    assay["lc-ms"] = "OBI:0003097"
    assay["targeted-shotgun-lc-ms"] = "OBI:0003097"
    assay["snrnaseq"] = "OBI:0003109"
    assay["snare-atacseq2"] = "OBI:0003108"
    assay["snare-rnaseq2"] = "OBI:0003108"
    assay["snareseq"] = "OBI:0003108"
    assay["scirnaseq"] = "OBI:0003105"
    assay["sciatacseq"] = "OBI:0003104"
    assay["scrnaseq-10xgenomics-v3"] = "OBI:0002631"
    assay["scrnaseq-10xgenomics-v2"] = "OBI:0002631"
    assay["snrnaseq-10xgenomics-v3"] = "OBI:0003109"
    assay["mibi"] = "OBI:0003100"
    assay["cell-dive"] = "OBI:0003092"
    assay["maldi-ims"] = "OBI:0003099"
    assay["nanodesi"] = "OBI:0003101"
    assay["ms"] = "OBI:0000470"

    return assay[assay_type]


def _get_list_of_files(directory: str):
    return Path(directory).glob("**/*")


def _build_dataframe(
    project_id: str,
    token: None,
    assay_type: str,
    directory: str,
    dbgap_study_id: str,
    dataset_hmid: str,
    dataset_uuid: str,
):
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
        "size_in_bytes",
        "uncompressed_size_in_bytes",
        "sha256",
        "md5",
        "filename",
        "file_format",
        "compression_format",
        "data_type",
        "assay_type",
        "analysis_type",
        "mime_type",
        "bundle_collection_id_namespace",
        "bundle_collection_local_id",
        "dbgap_study_id",
    ]

    df = hubmapinventory.get(hubmap_id=dataset_hmid, token=token)

    df["id_namespace"] = id_namespace
    df["project_id_namespace"] = id_namespace
    df["project_local_id"] = project_id
    df["compression_format"] = None
    df["bundle_collection_id_namespace"] = None
    df["bundle_collection_local_id"] = None
    df["uncompressed_size_in_bytes"] = None
    df["data_type"] = None
    df["analysis_type"] = None

    if "file_uuid" in df.keys():
        df["local_id"] = df["file_uuid"]
        df = df.drop(["file_uuid"], axis=1)
    else:
        df["local_id"] = None

    df["persistent_id"] = df["local_id"].apply(__get_persistent_id)

    df["dbgap_study_id"] = df["local_id"].apply(
        __get_dbgap_study_id, dbgap_study_id=dbgap_study_id
    )

    if "modification_time" in df.keys():
        df = df.rename(columns={"modification_time": "creation_time"})

    if "size" in df.keys():
        df["size_in_bytes"] = df["size"]

    def __fix_edam_string(edam):
        try:
            return edam.replace("http://edamontology.org/format_", "format:")
        except:
            return None

    if "file_format" in df.keys():
        df["file_format"] = df["file_format"].apply(__fix_edam_string)

    if "relative_path" in df.keys():
        df["relative_path"] = df["relative_path"].apply(
            lambda str: str.replace(" ", "%20")
        )
        df = df.rename(columns={"relative_path": "relative_local_id"})

    df = df.rename(columns={"dataset_id": "dataset_hmid"})
    df["assay_type"] = __get_assay_type_from_obi(assay_type)

    if "full_path" in df.keys():
        df = df.drop(["full_path"], axis=1)

    if "download_url" in df.keys():
        df = df.drop(["download_url"], axis=1)

    if "extension" in df.keys():
        df = df.drop(["extension"], axis=1)

    if "file_type" in df.keys():
        df = df.drop(["file_type"], axis=1)

    if "size" in df.keys():
        df = df.drop(["size"], axis=1)

    if "relative_local_id" in df.keys():
        df = df.drop(["relative_local_id"], axis=1)

    df = df[
        [
            "id_namespace",
            "local_id",
            "project_id_namespace",
            "project_local_id",
            "persistent_id",
            "creation_time",
            "size_in_bytes",
            "uncompressed_size_in_bytes",
            "sha256",
            "md5",
            "filename",
            "file_format",
            "compression_format",
            "data_type",
            "assay_type",
            "analysis_type",
            "mime_type",
            "bundle_collection_id_namespace",
            "bundle_collection_local_id",
            "dbgap_study_id",
        ]
    ]

    return df


def create_manifest(
    project_id: str,
    assay_type: str,
    directory: str,
    output_directory: str,
    dbgap_study_id: str,
    token: str,
    dataset_hmid: str,
    dataset_uuid: str,
):
    filename = os.path.join(output_directory, "file.tsv")

    df = _build_dataframe(
        project_id,
        token,
        assay_type,
        directory,
        dbgap_study_id,
        dataset_hmid,
        dataset_uuid,
    )

    if Path(output_directory).exists():
        df.to_csv(filename, sep="\t", index=False)

    return True
