import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import seaborn as sns


def by_data_type(df: pd.DataFrame) -> None:
    """
    Generate and save a visualization of data based on its type.

    This function generates a stacked histogram showcasing the count of each data type
    in a given DataFrame. The plot is generated using the seaborn library,
    and is saved to a 'daily-report' directory. If possible, it will also back up the
    plot to a specific path ('/hive/hubmap/bdbags/reports/').

    :param df: DataFrame containing the data to be visualized.
               It must contain the columns:
               - "dataset_type" to filter primary data,
               - "status" which determines the x-axis of the histogram,
               - "data_type" which determines the hue of the histogram.
    :type df: pd.DataFrame

    :return: None

    .. note::
       - The function attempts to save the plot in two directories, if the path does not exist,
         it will create it.
       - If there are any errors during saving or displaying the plot, they will be printed.
    """

    # ./daily-report
    now = datetime.now()
    fig = plt.gcf()
    fig.set_size_inches(30, 35)
    plt.rcParams["figure.dpi"] = 250

    g = sns.displot(
        df[df["dataset_type"] == "Primary"],
        x="status",
        height=12,
        hue="data_type",
        multiple="stack",
        aspect=1,
        palette="hls",
    )
    plt.xticks(
        df[df["dataset_type"] == "Primary"]["status"],
        df[df["dataset_type"] == "Primary"]["status"],
        rotation=45,
        fontsize=10,
    )

    title = (
        f'HuBMAP Data Status as of {now.strftime("%Y%m%d")} {now.strftime("%H:%M:%S")}'
    )
    g.set(
        xlabel="Data Type",
        ylabel="Count",
        title=title,
    )
    sns.move_legend(g, "right", ncol=2, title="Data Type", frameon=False)

    report_output_directory = "daily-report"
    if not Path(report_output_directory).exists():
        Path(report_output_directory).mkdir()

    report_output_filename = (
        f'{report_output_directory}/data-type-{str(now.strftime("%Y%m%d"))}.png'
    )
    try:
        plt.savefig(report_output_filename)
    except Exception as e:
        print(e)
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        # /hive/hubmap/bdbags/reports/
        report_output_directory = "/hive/hubmap/bdbags/reports"
        if Path(report_output_directory).exists():
            now = datetime.now()
            report_output_filename = (
                f'{report_output_directory}/data-type-{str(now.strftime("%Y%m%d"))}.png'
            )
            print(f"Backing up plot to {report_output_filename}")
            plt.savefig(report_output_filename)
    except Exception as e:
        print(e)
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        plt.show()
    except Exception as e:
        print(e)
        print("Unable to display plot.")


def by_group(df: pd.DataFrame) -> None:
    """
    Generate and save a visualization of data based on its group.

    This function produces a stacked histogram that represents the count of data items per group
    in a given DataFrame. The plot is created with the seaborn library,
    and is saved in the 'daily-report' directory. If possible, a backup
    of the plot will also be saved to the '/hive/hubmap/bdbags/reports/' path.

    :param df: DataFrame containing the data to be visualized.
               The DataFrame must include the columns:
               - "dataset_type" to filter primary data,
               - "group_name" which defines the x-axis of the histogram, and
               - "Status" (or "status", which will be renamed to "Status") that determines the hue of the histogram.
    :type df: pd.DataFrame

    :return: None

    .. note::
       - The function attempts to save the plot in two directories, creating them if they don't exist.
       - Errors encountered during saving or displaying the plot will be printed to the console.
    """

    now = datetime.now()
    fig = plt.gcf()
    fig.set_size_inches(30, 35)
    plt.rcParams["figure.dpi"] = 250

    if "status" in df:
        df.rename(columns={"status": "Status"}, inplace=True)

    g = sns.displot(
        df[df["dataset_type"] == "Primary"],
        height=10,
        x="group_name",
        hue="Status",
        multiple="stack",
        aspect=1.5,
        log_scale=(False, False),
    )  # Apply log scale on y-axis

    plt.xticks(rotation=45, fontsize=10, ha="right")

    g.set(
        xlabel="Groups",
        ylabel="Count",
        title=f'HuBMAP Data Status as of {now.strftime("%Y%m%d")}',
    )

    plt.tight_layout()

    report_output_directory = "daily-report"
    if not Path(report_output_directory).exists():
        Path(report_output_directory).mkdir()

    report_output_filename = (
        f'{report_output_directory}/group-{str(now.strftime("%Y%m%d"))}.png'
    )

    try:
        plt.savefig(report_output_filename)
    except Exception as e:
        print(e)
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        # /hive/hubmap/bdbags/reports/
        report_output_directory = "/hive/hubmap/bdbags/reports"
        if Path(report_output_directory).exists():
            now = datetime.now()
            report_output_filename = (
                f'{report_output_directory}/group-{str(now.strftime("%Y%m%d"))}.png'
            )
            print(f"Backing up plot to {report_output_filename}")
            plt.savefig(report_output_filename)
    except Exception as e:
        print(e)
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        plt.show()
    except Exception as e:
        print(e)
        print("Unable to display plot.")


def by_date(df: pd.DataFrame) -> None:
    raise NotImplementedError()
