import datetime
import mimetypes
import os
from pathlib import Path

import pandas as pd


def __get_filename(file):
    return file.name


def __get_file_extension(file):
    return file.suffix


def __get_file_size(file):
    return file.stat().st_size


def __get_file_creation_date(file):
    t = os.path.getmtime(str(file))
    return str(datetime.datetime.fromtimestamp(t))


def __get_file_format(file):
    extension = __get_file_extension(file)
    return extension


def __get_data_type(file):
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
        return ""


def __get_mime_type(file):
    return mimetypes.MimeTypes().guess_type(str(file))[0]


def __get_file_format(file):
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
        return ""


def __get_assay_type_from_obi(assay_type):
    assay = {}
    assay["af"] = "CHMO:0000087"  # AF
    assay["atacseq-bulk"] = "OBI:0002039"  # Bulk ATAC-seq
    assay["bulk-rna"] = "OBI:0001271"  # Bulk RNA-seq
    assay["scrna-seq-10x"] = "OBI:0002631"  # scRNA-seq
    assay[""] = "OBI:0002764"  # scATACseq
    assay["snatacseq"] = "OBI:0002762"  # snATAC-seq
    assay["wgs"] = "OBI:0002117"  # WGS

    return assay[assay_type]


def _get_list_of_files(directory):
    return Path(directory).glob("**/*")


def _build_dataframe(hubmap_id, hubmap_uuid, directory):
    """
    Build a dataframe with minimal information for this entity.
    """

    id_namespace = "tag:hubmapconsortium.org,2023:"
    headers = [
        "file_id_namespace",
        "file_local_id",
        "collection_id_namespace",
        "collection_local_id",
    ]

    if not Path(".data").exists():
        Path(".data").mkdir()

    temp_file = f".data/{hubmap_uuid}.tsv"

    if Path(temp_file).exists():
        print("Temporary file " + temp_file + " found. Loading df into memory.")
        id_namespace = "tag:hubmapconsortium.org,2023:"
        df = pd.read_csv(temp_file, sep="\t")

        df["collection_id_namespace"] = id_namespace
        df["file_id_namespace"] = id_namespace

        df = df.rename(columns={"fil_uuid": "file_local_id"}, errors="raise")
        df["collection_local_id"] = hubmap_id

        df[
            [
                "file_id_namespace",
                "file_local_id",
                "collection_id_namespace",
                "collection_local_id",
            ]
        ]

    return df


def create_manifest(hubmap_id, hubmap_uuid, directory, output_directory):
    filename = os.path.join(output_directory, "file_in_collection.tsv")

    if not Path(".data").exists():
        Path(".data").mkdir()

    temp_file = f".data/{hubmap_uuid}.tsv"
    if not Path(directory).exists() and not Path(temp_file).exists():
        print(
            "Data directory "
            + directory
            + " does not exist. Temp file was not found either."
        )
        return False
    else:
        if Path(temp_file).exists():
            print("Temp file " + temp_file + " found. Continuing computation.")
        df = _build_dataframe(hubmap_id, hubmap_uuid, directory)
        df.to_csv(filename, sep="\t", index=False)

        return True
