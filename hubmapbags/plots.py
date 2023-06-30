import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import seaborn as sns


def by_data_type(df: pd.DataFrame) -> None:
    try:
        # ./daily-report
        report_output_directory = "daily-report"
        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        now = datetime.now()
        report_output_filename = (
            f'{report_output_directory}/data-type-{str(now.strftime("%Y%m%d"))}.png'
        )

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

        g.set(
            xlabel="Data Type",
            ylabel="Count",
            title=f'HuBMAP Data Status as of {now.strftime("%Y%m%d")} {now.strftime("%H:%M:%S")}',
        )
        sns.move_legend(g, "right", ncol=2, title="Data Type", frameon=False)

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
            title=f'HuBMAP Data Status as of {now.strftime("%Y%m%d")} {now.strftime("%H:%M:%S")}',
        )

        plt.tight_layout()
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

    if "Status" in df:
        df.rename(columns={"Status": "status"}, inplace=True)


def by_date(df: pd.DataFrame) -> None:
    raise NotImplementedError()
