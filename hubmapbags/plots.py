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
    df = df[df["status"] == "Published"]

    now = datetime.now()
    plt.rcParams["figure.figsize"] = [50.0, 50.0]
    plt.rcParams["figure.dpi"] = 500

    group = df.groupby(["group_name", "status"]).count()[["data_type"]]

    # First plot
    plt.figure(figsize=(30, 35))
    g = sns.displot(df[df['dataset_type'] == 'Primary'], height=10, x="group_name", hue="status", multiple='stack', aspect=1.5, log_scale=(False, True))
    plt.xticks(rotation=45, fontsize=10, ha='right')
    g.set(xlabel='Groups', ylabel='Count', title=str(now.strftime("%Y%m%d")))
    report_output_directory = "daily-report"
    Path(report_output_directory).mkdir(exist_ok=True)
    report_output_filename = f'{report_output_directory}/groups-{str(now.strftime("%Y%m%d"))}.png'
    plt.savefig(report_output_filename)
    plt.tight_layout()
    plt.savefig('plot.png')
    plt.close()

    # Second plot
    plt.figure(figsize=(50.0, 50.0))
    g = sns.displot(df[df['dataset_type'] == 'Primary'], x="status", height=12, hue="data_type", multiple='stack', aspect=1, palette="hls")
    plt.xticks(df[df['dataset_type'] == 'Primary']['status'], df[df['dataset_type'] == 'Primary']['status'], rotation=45, fontsize=10)
    g.set(xlabel='Data Type', ylabel='Count', title=str(now.strftime("%Y%m%d")))
    sns.move_legend(g, "upper right", ncol=4, title='Data Type', frameon=False)
    plt.tight_layout()
    plt.savefig('plot.png')
    plt.close()

    # Third plot
    plt.figure(figsize=(50.0, 50.0))
    g = sns.displot(df[df['dataset_type'] == 'Primary'], height=10, x="data_type", hue="status", multiple='stack', aspect=2)
    plt.xticks(df[df['dataset_type'] == 'Primary']['data_type'], df[df['dataset_type'] == 'Primary']['data_type'], rotation=45, fontsize=10, ha='right')
    g.set(xlabel='Data Type', ylabel='Count', title=str(now.strftime("%Y%m%d")))
    sns.move_legend(g, "center right", ncol=1, title='Dataset status', frameon=False)
    plt.tight_layout()
    plt.show()


def by_date(df: pd.DataFrame) -> None:
    df = df[df["status"] == "Published"]

    now = datetime.now()
    plt.rcParams["figure.figsize"] = [15.0, 15.0]
    plt.rcParams["figure.dpi"] = 100

    df = df.groupby("published_datetime").count()
    df = df[["uuid"]]
    df = df.rename({"uuid": "counts"}, axis=1)
    df["cumsum"] = df["counts"].cumsum()

    g = sns.pointplot(x=df.index, y="cumsum", data=df)
    plt.xticks(rotation="vertical")
    g.set(xlabel="Dates", ylabel="Cumulative sum", title=str(now.strftime("%Y%m%d")))
    plt.tight_layout()

    # get daily report
    try:
        report_output_directory = "daily-report"
        if not Path(report_output_directory).exists():
            Path(report_output_directory).mkdir()

        now = datetime.now()
        report_output_filename = (
            f'{report_output_directory}/date-{str(now.strftime("%Y%m%d"))}.png'
        )
        plt.savefig(report_output_filename)
    except:
        print(f"Unable to save plot to {report_output_filename}.")

    try:
        plt.show()
    except:
        print("Unable to display plot.")
