#!/bin/python3
from math import floor
from operator import truediv
import os
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import re
import argparse


def create_plot_df(df, type, data, title, x_col, y_col, ax=None):
    df = df.rename(columns={
        "time_bucket": x_col,
        data: type.upper()
    }, inplace=False)

    ax.set_title(title)
    a = df.plot(kind='line', x="Time (sec)", y=type.upper(), ax=ax)
    a.set_xlabel(x_col)
    a.set_ylabel(y_col)

def is_machine_result_folder(machine):
    if os.path.isdir(machine):
        for f in os.listdir(machine):
            if f.endswith("raw.csv"):
                return True

    return False

def test_machines(test_name):
    csv_files = []
    for machine in os.listdir(test_name):
        machine = os.path.join(test_name, machine)
        if is_machine_result_folder(machine):
            csv_files.append(os.path.join(machine, [f for f in os.listdir(machine) if f.endswith("raw.csv")][0]))

    return csv_files


def test_files(base_dir, test_type):
    dir = os.path.join(base_dir, test_type)
    tests = dict()
    for t in os.listdir(dir):
        csv = test_machines(os.path.join(dir, t))
        tests[t] = csv

    return tests


def join_files(files):
    final_df = pd.read_csv(files[0])
    for f in files[1:]:
        df = pd.read_csv(f)
        final_df = pd.concat([final_df, df], ignore_index=True)
    return final_df


def enhance_data(df: pd.DataFrame):
    end = datetime.fromtimestamp(df["Start Time (microseconds)"].max())
    results = df.copy()
    results["start_date"] = df.apply(lambda row: datetime.fromtimestamp(row['Start Time (microseconds)']), axis=1)
    results["time"] = results.apply(lambda row: (end - row["start_date"]).total_seconds(), axis=1)
    results["result"] = 1
    results["time_bucket"] = results.apply(lambda row: floor(row["time"]), axis=1)
    results["latency"] = results["Latency (microseconds)"].apply((lambda row: row / 1000))
    results["window"] = results["window"].apply((lambda row: row / 1000))
    results["retries"] = results["retries"].apply((lambda row: row if row > 0 else row))
    results["failed"] = results["success"].apply((lambda row: 0 if row == 1 else 1))

    return results


def compute_statistics(extended_stats: pd.DataFrame, window=1):
    stats = extended_stats[["time_bucket", "time", "result", "latency", "window", "success", "failed", "error"]]
    max_time = extended_stats["time"].max()

    throughput = []
    latency = []
    transaction_window = []
    error_rate = []
    buckets = range(floor(max_time))

    for i in buckets:
        s = max(i - window, 0)

        data_buckets = stats.loc[(s <= stats["time_bucket"]) & (stats["time_bucket"] <= i)]
        failed_requests: pd.DataFrame = data_buckets.loc[(stats["success"] == 0)]

        requests = data_buckets.groupby(by="time_bucket").sum()
        requests = requests[["result"]].mean()[0]
        tpmC = (60 * requests) / window

        requests = failed_requests.groupby(by="time_bucket").sum()
        requests = requests[["failed"]].mean()[0]
        failedM = (60 * requests) / window
        
        errors = data_buckets[["error"]].mean()[0]
        errorM = (60 * errors) / window
        
        p99 = data_buckets[["latency"]].quantile(0.99)[0]
        commit_duration = data_buckets[["window"]].quantile(0.99)[0]

        throughput.append(tpmC)
        latency.append(p99)
        transaction_window.append(commit_duration)
        error_rate.append(errorM)

    throughput = pd.DataFrame({'time_bucket': buckets, 'throughput': throughput})
    latency = pd.DataFrame({'time_bucket': buckets, 'latency': p99})
    transaction_window = pd.DataFrame({'time_bucket': buckets, 'window': transaction_window})
    error_rate = pd.DataFrame({'time_bucket': buckets, 'error': error_rate})

    stats = throughput.join(latency.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(transaction_window.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(error_rate.set_index("time_bucket"), on="time_bucket")

    return stats


def get_value_of_match(match, input):
    val = re.match(match, input)
    return val.group(1)


def create_title_name(file_name: str):
    scale = get_value_of_match(r".*Sca(\d+)", file_name)
    terminals = get_value_of_match(r".*Ter(\d+)", file_name)
    duration = get_value_of_match(r".*Dur(\d+)", file_name)
    new_order = get_value_of_match(r".*NO(\d+)", file_name)

    scale_str = str.format("{} Warehouse", scale)
    if int(scale) > 1:
        scale_str += "s"

    terminals_str = str.format("{} Terminal", terminals)
    if int(terminals) > 1:
        terminals_str += "s"

    return str.format("New order - {} and {}", terminals_str, scale_str)


def plot_data(base_results, lsd_results, y_col, y_desc, title, base_dir, file_name):
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(10, 10))

        create_plot_df(base_results, "base", y_col, title, "Time (sec)", y_desc, axes)
        create_plot_df(lsd_results, "lsd", y_col, title, "Time (sec)", y_desc, axes)

        plt.savefig(os.path.join(base_dir, file_name + ".svg"))
        plt.savefig(os.path.join(base_dir, file_name + ".png"))
        plt.savefig(os.path.join(base_dir, file_name + ".pdf"))


def run(args):
    # base_dir = args.directory
    base_dir = "tpcc/fake-run"

    base_tests = test_files(base_dir, "base")
    lsd_tests = test_files(base_dir, "lsd")
    window = 5
    plt.rcParams.update({
        "text.usetex": False,
        "font.family": "sans-serif"
    })
    for item in base_tests:
        title = create_title_name(item)
        lsd_results = join_files(lsd_tests[item])
        base_results = join_files(base_tests[item])

        lsd_results = enhance_data(lsd_results)
        base_results = enhance_data(base_results)

        lsd_results = compute_statistics(lsd_results, window=window)
        base_results = compute_statistics(base_results, window=window)

        plot_data(base_results, lsd_results, "throughput", "tpmC", "Requests (" + str(window) + " sec. rolling window)", base_dir, "throughput_" + item)
        plot_data(base_results, lsd_results, "latency", "Latency (ms)", "Latency (" + str(window) + " sec. rolling window)", base_dir, "latency_" + item)
        plot_data(base_results, lsd_results, "window", "Window (ms)", "Window (" + str(window) + " sec. rolling window)", base_dir, "window_" + item)
        plot_data(base_results, lsd_results, "error", "Errors", "Error rate (" + str(window) + " sec. rolling window)", base_dir, "error_" + item)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--directory",
        dest="directory",
        help="Directory with run results"
    )

    args = parser.parse_args()
    run(args)
