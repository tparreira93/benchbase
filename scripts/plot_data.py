#!/bin/python3
from email.mime import base
from encodings import normalize_encoding
import glob
from heapq import merge
from importlib.resources import path
from math import floor
import sys
import os
from pathlib import Path
from tkinter import W
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
        df = df.rename(columns={
            column: colname
        }, inplace=False)

        ax.set_title(title)
        a = df.plot(kind='line', x='Time (sec)', y=colname, ax=ax)
        a.set_xlabel("Time (sec)")
        a.set_ylabel(column)


def create_plot_df(df, type, data, title, x_col, y_col, ax=None):
    df = df.rename(columns={
        "time_bucket": x_col,
        data: type.upper()
    }, inplace=False)

    ax.set_title(title)
    a = df.plot(kind='line', x="Time (sec)", y=type.upper(), ax=ax)
    a.set_xlabel(x_col)
    a.set_ylabel(y_col)


def test_machines(test_name):
    csv_files = []
    for machine in os.listdir(test_name):
        machine = test_name + "/" + machine
        if os.path.isdir(machine):
            csv_files.append(machine + "/" + [f for f in os.listdir(machine) if f.endswith("raw.csv")][0])

    return csv_files


def test_files(test_type):
    tests = dict()
    for t in os.listdir(test_type):
        csv = test_machines(os.path.join(test_type, t))
        tests[t] = csv

    return tests


def join_files(files):
    final_df = pd.read_csv(files[0])
    for f in files[1:]:
        df = pd.read_csv(f)
        final_df = pd.concat([final_df, df], ignore_index=True)
    return final_df


def enhance_data(df: pd.DataFrame, window):
    start = datetime.fromtimestamp(df["Start Time (microseconds)"].min())
    end = datetime.fromtimestamp(df["Start Time (microseconds)"].max())
    run_duration = (end - start).total_seconds()
    buckets = list(range(0, int(run_duration) + window, window))
    results = df.copy()
    results["start_date"] = df.apply(lambda row: datetime.fromtimestamp(row['Start Time (microseconds)']),
                                axis=1)
    results["time"] = results.apply(lambda row: (end - row["start_date"]).total_seconds(), axis=1)
    results["result"] = 1
    results["time_bucket"] = results.apply(lambda row: buckets[floor(row["time"] / window)], axis=1)
    results["latency"] = results["Latency (microseconds)"].apply((lambda row: row / 1000))

    return buckets, results


def compute_statistics(df: pd.DataFrame, window=1):
    buckets, extended_stats = enhance_data(df, window)

    stats = extended_stats[["time_bucket", "time", "result", "latency"]]

    p99 = []
    throughput = []

    for i in range(len(buckets)):
        s = max(i - window, 0)

        bucket_data: pd.DataFrame = stats.loc[(s <= stats["time_bucket"]) & (stats["time_bucket"] <= i)]

        requests = bucket_data.groupby(by="time_bucket").sum()
        requests = requests[["result"]].median()[0]
        tpmC = (60 * requests) / window
        throughput.append(tpmC)
        p99.append(bucket_data[["latency"]].quantile(0.99)[0])

    p99 = pd.DataFrame({'time_bucket': buckets, 'latency': p99})
    throughput = pd.DataFrame({'time_bucket': buckets, 'throughput': throughput})
    stats = throughput.join(p99.set_index("time_bucket"), on="time_bucket")

    return stats


def run():
    base_dir = "tpcc/run-23"

    base_tests = test_files(os.path.join(base_dir, "base"))
    lsd_tests = test_files(os.path.join(base_dir, "lsd"))
    window = 1

    for item in base_tests:
        lsd_results = join_files(lsd_tests[item])
        base_results = join_files(base_tests[item])

        lsd_results = compute_statistics(lsd_results, window=window)
        base_resuts = compute_statistics(base_results, window=window)

        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 8))
        create_plot_df(lsd_results, "lsd", "throughput", "Requests (" + str(window) + " second window average)",
                       "Time (sec)", "tpmC", axes[0])
        create_plot_df(lsd_results, "lsd", "latency", "Latency (" + str(window) + " second)", "Time (sec)",
                       "Latency (ms)", axes[1])
        create_plot_df(base_resuts, "base", "throughput", "Requests (" + str(window) + " second window average)",
                       "Time (sec)", "tpmC", axes[0])
        create_plot_df(base_resuts, "base", "latency", "Latency (" + str(window) + " second)", "Time (sec)",
                       "Latency (ms)", axes[1])

        plt.show()
        # plt.savefig(os.path.join(base_dir, item))

    # files = find_plotable_data("results")
    # output_path = sys.argv[-1]

    # fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(12, 8))

    # outfile_name = os.path.join(output_path,
    #                             "Requests (5 sec samples)" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    # title = "Requests (5 sec)"
    # create_plot("Requests (sec)", files, title, ax=axes[0, 0])

    # # outfile_name = os.path.join(output_path, "Avg. Latency" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    # # title = "Avg Latency"
    # # create_plot("Avg. Latency (ms)", files, title, ax=axes[1, 0])

    # outfile_name = os.path.join(output_path, "P99 Latency" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    # title = "P99 Latency"
    # create_plot("P99 Latency (ms)", files, title, ax=axes[0, 1])

    # files = find_plotable_data("samples")

    # # outfile_name = os.path.join(output_path,
    # #                             "Requests (1 sec sample)" + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm") + '.png')
    # # title = "Requests (1 sec)"
    # # create_plot("Requests (sec)", files, title, ax=axes[0, 1])

    # fig.tight_layout(pad=2.0)
    # outfile_name = os.path.join(output_path, str(Path(files[0][1]).parent.name) + "_" + datetime.now().strftime(
    #     "%Y%m%d_%Hh%Mm") + '.png')
    # plt.savefig(outfile_name)


if __name__ == '__main__':
    run()
