import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import seaborn as sns


def by_data_type(df: pd.DataFrame) -> None:
    df = df[df["status"] == "Published"]

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
    try:
        report_output_directory = "daily-report"
        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        now = datetime.now()
        report_output_filename = (
            f'{report_output_directory}/group-{str(now.strftime("%Y%m%d"))}.png'
        )

        fig = plt.gcf()
        fig.set_size_inches(30, 35)

        g = sns.displot(
            df[df["dataset_type"] == "Primary"],
            height=10,
            x="group_name",
            hue="status",
            multiple="stack",
            aspect=1.5,
            log_scale=(False, True),
        )  # Apply log scale on y-axis
        plt.xticks(rotation=45, fontsize=10, ha="right")

        g.set(xlabel="Groups", ylabel="Count", title=str(now.strftime("%Y%m%d")))

        plt.tight_layout()
        plt.savefig(report_output_filename)
    except:
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        plt.show()
    except:
        print("Unable to display plot.")


def by_date(df: pd.DataFrame) -> None:
    raise NotImplementedError()
