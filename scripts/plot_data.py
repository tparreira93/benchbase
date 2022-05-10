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


def find_plotable_data(csv_file):
    files = []
    for arg in sys.argv[1:-1]:
        for f in glob.glob("*." + csv_file + ".csv", root_dir=arg):
            p = Path(os.path.join(arg, f))
            files.append((str(p.parent.parent.name), str(p)))

    return files


def create_plot(column, files, title, ax=None):
    if len(files) == 0:
        return

    if ax == None:
        ax = plt.gca()
    for driver, file in files:
        colname = driver.upper() + " " + column
        df = pd.read_csv(file)
        rename(df)
        df.rename(columns={
            column: colname
        }, inplace=True)

        a = df.plot(kind='line', x='Time (sec)', y=colname, ax=ax, title=title)
        a.set_xlabel("Time (sec)")
        a.set_ylabel(column)


def run():
    files = find_plotable_data("results")
    output_path = sys.argv[-1]

    fig, axes = plt.subplots(nrows=2, ncols=2)

    outfile_name = os.path.join(output_path,
                                "Requests (5 sec)" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    create_plot("Requests (sec)", files, outfile_name, ax=axes[0, 0])

    outfile_name = os.path.join(output_path, "Avg Latency" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    create_plot("Avg. Latency (ms)", files, outfile_name, ax=axes[1, 0])

    outfile_name = os.path.join(output_path, "P99 Latency" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    create_plot("P99 Latency (ms)", files, outfile_name, ax=axes[1, 1])

    files = find_plotable_data("samples")

    outfile_name = os.path.join(output_path,
                                "Requests (1 sec)" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    create_plot("Requests (sec)", files, outfile_name, ax=axes[0, 1])

    outfile_name = os.path.join(output_path, str(Path(files[0][1]).parent.name) + "_" + datetime.now().strftime(
        "%Y%m%d_%Hh%Mm") + '.png')
    plt.savefig(outfile_name)


if __name__ == '__main__':
    run()
