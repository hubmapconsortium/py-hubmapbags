import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import seaborn as sns


def by_data_type(df: pd.DataFrame) -> None:
    now = datetime.now()
    plt.rcParams["figure.figsize"] = [50.0, 50.0]
    plt.rcParams["figure.dpi"] = 500

    g = sns.displot(
        df[df["dataset_type"] == "Primary"],
        height=10,
        x="data_type",
        hue="status",
        multiple="stack",
        aspect=2,
    )
    plt.xticks(
        df[df["dataset_type"] == "Primary"]["data_type"],
        df[df["dataset_type"] == "Primary"]["data_type"],
        rotation="vertical",
    )

    plt.tight_layout()

    g.set(xlabel="Data Type", ylabel="Count", title=str(now.strftime("%Y%m%d")))

    sns.move_legend(g, "center right", ncol=1, title="Dataset status", frameon=False)

    # get daily report
    try:
        report_output_directory = "daily-report"
        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        now = datetime.now()
        report_output_filename = (
            f'{report_output_directory}/data-type-{str(now.strftime("%Y%m%d"))}.png'
        )
        plt.savefig(report_output_filename)
    except:
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        plt.show()
    except:
        print("Unable to display plot.")


def by_group(df: pd.DataFrame) -> None:
    now = datetime.now()
    plt.rcParams["figure.figsize"] = [50.0, 50.0]
    plt.rcParams["figure.dpi"] = 500

    group = df.groupby(["group_name", "status"]).count()[["data_type"]]

    fig = plt.gcf()
    fig.set_size_inches(15, 15)

    g = sns.displot(
        df[df["dataset_type"] == "Primary"],
        height=15,
        x="group_name",
        hue="status",
        multiple="stack",
        aspect=1.5,
    )
    sns.move_legend(g, "upper right", ncol=1, title="Status", frameon=False)
    plt.xticks(df["group_name"], df["group_name"], rotation="vertical")

    g.set(xlabel="Groups", ylabel="Count", title=str(now.strftime("%Y%m%d")))

    # get daily report
    try:
        report_output_directory = "daily-report"
        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        now = datetime.now()
        report_output_filename = (
            f'{report_output_directory}/groups-{str(now.strftime("%Y%m%d"))}.png'
        )
        plt.savefig(report_output_filename)
    except:
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        plt.show()
    except:
        print("Unable to display plot.")
