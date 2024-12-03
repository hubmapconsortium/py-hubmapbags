import sys
import hubmapbags
import os
from pathlib import Path
import traceback

token = os.getenv("TOKEN")
if token is None:
    print("Error: TOKEN environment variable is not set")
    sys.exit(1)

from datetime import datetime


def log_dataset_error_with_timestamp(
    hubmap_id, dataset_type, file_path="error_log.txt"
):
    """
    Logs a dataset error message with the given hubmap_id and the current date and time to a file.

    Parameters:
    - hubmap_id (str): The unique identifier for the dataset.
    - file_path (str): Path to the file where the log should be saved. Default is 'error_log.txt'.

    Returns:
    None
    """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    error_message = f"Dataset: {dataset_type} {hubmap_id} {current_time}\n"
    with open(file_path, "a") as file:
        file.write(error_message)


def format_dataset_error_with_timestamp(hubmap_id, dataset_type):
    """
    Formats a dataset error message with the given hubmap_id and the current date and time.

    Parameters:
    - hubmap_id (str): The unique identifier for the dataset.

    Returns:
    str: Formatted error message.
    """
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f"Dataset: {dataset_type} {hubmap_id} {current_time}"


assays = hubmapbags.apis.get_assay_types(token)
assays = [assay for assay in assays if "[" not in assay]
assays.remove("Publication")
assays.remove("UNKNOWN")
assays.remove("Segmentation Mask")
assays.remove("DARTfish")
assays.remove("Histology")

# 2024.04
# assays.remove("RNAseq")

assays.remove("MIBI")
assays.remove("ATACseq")
assays.remove("3D Imaging Mass Cytometry")
assays.remove("Cell DIVE")
assays.remove("Light Sheet")
assays.remove("CODEX")
assays.remove("WGS")
assays.remove("LC-MS")
assays.remove("MALDI")
assays.remove("Auto-fluorescence")
assays.remove("seqFish")
assays.remove("2D Imaging Mass Cytometry")
assays.remove("Slide-seq")
assays.remove("10X Multiome")
assays.remove("DESI")
assays.remove("SNARE-seq2")

assays.remove("PhenoCycler")
assays.remove("Visium (no probes)")
assays.remove("MUSIC")

for dataset_type in assays:
    df = hubmapbags.reports.daily()
    df = df[df["dataset_type"] == dataset_type]
    df = df[df["status"] == "Published"]

    for index, datum in df.iterrows():
        hubmap_id = datum["hubmap_id"]
        try:
            hubmapbags.magic.do_it(
                hubmap_id,
                dbgap_study_id=None,
                token=token,
                backup_directory=f"{dataset_type.replace(' ','_')}-bdbags",
                build_bags=True,
            )
        except:
            print(f"Failed to process: {hubmap_id}")
            log_dataset_error_with_timestamp(
                hubmap_id, dataset_type, file_path="error_log.txt"
            )
            traceback.print_exc()
            sys.exit(1)

    data_directory = f"{dataset_type.replace(' ','_')}-bdbags"
    output_directory = f"{dataset_type.replace(' ','_')}-aggregate"
    hubmapbags.magic.aggregate(
        directory=data_directory, output_directory=output_directory
    )
