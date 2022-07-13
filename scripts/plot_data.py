#!/bin/python3
from encodings import normalize_encoding
import glob
import sys
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def rename(df):
    df.rename(columns={
        "Time (seconds)": "Time (sec)",
        "Throughput (requests/second)": "Requests (sec)",
        "Average Latency (millisecond)": "Avg. Latency (ms)",
        "99th Percentile Latency (millisecond)": "P99 Latency (ms)"
    }, inplace=True)


def find_plotable_data(csv_pattern):
    files = []
    for arg in sys.argv[1:-1]:
        csv_files = [f for f in os.listdir(arg) if f.endswith(csv_pattern + ".csv")]
        for f in csv_files:
            p = Path(os.path.join(arg, f))
            files.append((str(p.parent.parent.name), str(p)))

    return files


def create_plot(column, files, title, ax=None):
    if len(files) == 0:
        return

    if ax == None:
        ax = plt.gca()
    for driver, file in files:
        colname = driver.upper()
        df = pd.read_csv(file)
        rename(df)
        df.rename(columns={
            column: colname
        }, inplace=True)

        ax.set_title(title)
        a = df.plot(kind='line', x='Time (sec)', y=colname, ax=ax)
        a.set_xlabel("Time (sec)")
        a.set_ylabel(column)


def run():
    files = find_plotable_data("results")
    output_path = sys.argv[-1]

    fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 8))

    outfile_name = os.path.join(output_path,
                                "Requests (5 sec samples)" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    title = "Requests (5 sec)"
    create_plot("Requests (sec)", files, title, ax=axes[0, 0])

    # outfile_name = os.path.join(output_path, "Avg. Latency" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    # title = "Avg Latency"
    # create_plot("Avg. Latency (ms)", files, title, ax=axes[1, 0])

    outfile_name = os.path.join(output_path, "P99 Latency" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    title = "P99 Latency"
    create_plot("P99 Latency (ms)", files, title, ax=axes[0, 1])

    files = find_plotable_data("samples")

    # outfile_name = os.path.join(output_path,
    #                             "Requests (1 sec sample)" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    # title = "Requests (1 sec)"
    # create_plot("Requests (sec)", files, title, ax=axes[0, 1])

    fig.tight_layout(pad=2.0)
    outfile_name = os.path.join(output_path, str(Path(files[0][1]).parent.name) + "_" + datetime.now().strftime(
        "%Y%m%d_%Hh%Mm") + '.png')
    plt.savefig(outfile_name)


if __name__ == '__main__':
    run()
