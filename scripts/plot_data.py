#!/bin/python3
from cProfile import label
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
    avg = df[[type.upper()]].mean()[0]
    x_max = df[[x_col]].max()[0]
    color_dict = {'BASE': '#FF7F0E', 'LSD': '#1F77B4'}

    ax.set_title(title)
    ax.axhline(y=avg, xmin=0, xmax=x_max, c=color_dict.get(type.upper()), zorder=0, linestyle='--')
    ax.text(x_max + 6.25, avg, type.upper() + " avg.", color="black", va="center", fontsize=22)
    a = df.plot(kind='line', x="Time (sec)", y=type.upper(), ax=ax, color=color_dict.get(type.upper()))
    a.set_xlabel(x_col)
    a.set_ylabel(y_col)
    # a.set_xlim((0, x_max))


def is_machine_result_folder(machine, pattern):
    if os.path.isdir(machine):
        for f in os.listdir(machine):
            if f.endswith(pattern):
                return True

    return False


def test_machines(test_name, pattern):
    csv_files = []
    for machine in os.listdir(test_name):
        machine = os.path.join(test_name, machine)
        if is_machine_result_folder(machine, pattern):
            csv_files.append(os.path.join(machine, [f for f in os.listdir(machine) if f.endswith(pattern)][0]))

    return csv_files


def test_files(base_dir, test_type, pattern):
    dir = os.path.join(base_dir, test_type)
    tests = dict()
    for t in os.listdir(dir):
        csv = test_machines(os.path.join(dir, t), pattern)
        tests[t] = csv

    return tests


def join_files(files, enhance=False):
    final_df = pd.read_csv(files[0])
    if enhance:
        final_df = enhance_data(final_df)
    for f in files[1:]:
        df = pd.read_csv(f)
        if enhance:
            df = enhance_data(df)
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
    latency_avg = []
    transaction_window = []
    transaction_window_avg = []
    error_rate = []
    failed_rate = []
    buckets = range(floor(max_time))

    for i in buckets:
        s = max(i - window, 0)

        data_buckets = stats.loc[(s <= stats["time_bucket"]) & (stats["time_bucket"] <= i)]
        failed_requests: pd.DataFrame = data_buckets.loc[(stats["success"] == 0)]

        requests = data_buckets.groupby(by="time_bucket").sum()
        requests = requests[["result"]].mean()[0]
        tpmC = (60 * requests) / window

        failed = failed_requests.groupby(by="time_bucket").sum()[["failed"]].mean()[0]

        errors = data_buckets[["error"]].mean()[0]
        errorM = errors / window

        p99 = data_buckets[["latency"]].quantile(0.99)[0]
        ms_avg = data_buckets[["latency"]].mean()[0]
        commit_duration = data_buckets[["window"]].quantile(0.99)[0]
        commit_duration_avg = data_buckets[["window"]].mean()[0]

        throughput.append(tpmC)
        latency.append(p99)
        latency_avg.append(ms_avg)
        transaction_window.append(commit_duration)
        transaction_window_avg.append(commit_duration_avg)
        error_rate.append(errorM)
        failed_rate.append(failed)

    throughput = pd.DataFrame({'time_bucket': buckets, 'tpmC': throughput})
    latency = pd.DataFrame({'time_bucket': buckets, 'latency_99': latency})
    latency_avg = pd.DataFrame({'time_bucket': buckets, 'latency_avg': latency_avg})
    transaction_window = pd.DataFrame({'time_bucket': buckets, 'window_99': transaction_window})
    transaction_window_avg = pd.DataFrame({'time_bucket': buckets, 'window_avg': transaction_window_avg})
    error_rate = pd.DataFrame({'time_bucket': buckets, 'error': error_rate})
    failed_rate = pd.DataFrame({'time_bucket': buckets, 'failed': failed_rate})

    stats = throughput.join(latency.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(latency_avg.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(transaction_window.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(transaction_window_avg.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(error_rate.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(failed_rate.set_index("time_bucket"), on="time_bucket")

    return stats


def compute_sample_stats(samples: pd.DataFrame):
    stats = samples
    stats["time_bucket"] = stats["Time (seconds)"]
    stats["tpmC"] = stats.apply(lambda row: row["Throughput (requests/second)"] * 60, axis=1)
    # stats["latency"] = stats["99th Percentile Latency (microseconds)"]
    stats["latency"] = stats.apply(lambda row: row["99th Percentile Latency (microseconds)"] / 1000, axis=1)
    stats["window"] = stats.apply(lambda row: row["window"] / 1000, axis=1)
    stats = stats[["time_bucket", "latency", "window", "success", "failed", "tpmC"]]

    throughput = []
    latency = []
    latency_avg = []
    transaction_window = []
    transaction_window_avg = []
    failed_rate = []
    max_time = stats["time_bucket"].max()
    buckets = range(floor(max_time))

    for w in buckets:
        data = stats.loc[(stats["time_bucket"] == w)]

        tpmC = data.sum()["tpmC"]
        failed = data.sum()["failed"]
        window = data.quantile(0.99)["window"]
        window_avg = data.quantile(0.99)["window"]
        p99 = data.quantile(0.99)["latency"]
        ms_avg = data.mean()["latency"]


        throughput.append(tpmC)
        latency.append(p99)
        latency_avg.append(ms_avg)
        transaction_window.append(window)
        transaction_window_avg.append(window_avg)
        failed_rate.append(failed)

    throughput = pd.DataFrame({'time_bucket': buckets, 'tpmC': throughput})
    latency = pd.DataFrame({'time_bucket': buckets, 'latency_99': latency})
    latency_avg = pd.DataFrame({'time_bucket': buckets, 'latency_avg': latency_avg})
    transaction_window = pd.DataFrame({'time_bucket': buckets, 'window_99': transaction_window})
    transaction_window_avg = pd.DataFrame({'time_bucket': buckets, 'window_avg': transaction_window_avg})
    failed_rate = pd.DataFrame({'time_bucket': buckets, 'failed': failed_rate})

    stats = throughput.join(latency.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(latency_avg.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(transaction_window.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(transaction_window_avg.set_index("time_bucket"), on="time_bucket")
    stats = stats.join(failed_rate.set_index("time_bucket"), on="time_bucket")
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


def create_file_name(file_name: str, window=None):
    scale = int(get_value_of_match(r".*Sca(\d+)", file_name))
    terminals = int(get_value_of_match(r".*Ter(\d+)", file_name))
    base_name = "all_"
    if "NOPA" in file_name:
        base_name = "without_payment"
    elif "NO" in file_name:
        base_name = "new_order_"
    elif "PA" in file_name:
        base_name = "payment_"
    window_str = ""

    if not window is None:
        window_str = "_" + str(window) + "_sec"

    if scale == 1 and terminals == 10:
        return base_name + "high_contention" + window_str

    if scale == 10 and terminals == 10:
        return base_name + "low_contention" + window_str

    # if (scale == 10 and terminals == 10):
    #     return "mid_contention"

    # if (scale == 10 and terminals == 5):
    #     return "low_contention"

    raise Exception("Unknown scenario with " + str(scale) + " of scale and " + str(terminals) + " terminals")


def plot_data(base_results, lsd_results, y_col, y_desc, title, base_dir, file_name):
        fig, axes = plt.subplots(nrows=1, ncols=1, figsize=(16, 12))
        create_plot_df(lsd_results, "lsd", y_col, title, "Time (sec)", y_desc, axes)
        create_plot_df(base_results, "base", y_col, title, "Time (sec)", y_desc, axes)

        # plt.savefig(os.path.join(base_dir, file_name + ".svg"))
        plt.savefig(os.path.join(base_dir, file_name + ".png"))
        plt.savefig(os.path.join(base_dir, file_name + ".pdf"))

        plt.close(fig)


def plot_computation(base_results, lsd_results, window, base_dir, outfile):
    plot_data(base_results, lsd_results, "tpmC", "tpmC", "tpmC", base_dir,
              outfile + "_throughput")
    plot_data(base_results, lsd_results, "latency_99", "Latency (ms)",
              "Latency percentile 99", base_dir, outfile + "_latency_99")
    plot_data(base_results, lsd_results, "latency_avg", "Latency (ms)",
              "Average latency", base_dir, outfile + "_latency_avg")
    plot_data(base_results, lsd_results, "window_99", "Window (ms)",
              "Window percentile 99", base_dir, outfile + "_window_99")
    plot_data(base_results, lsd_results, "window_avg", "Window (ms)",
              "Average transaction window", base_dir, outfile + "_window_avg")
    # plot_data(base_results, lsd_results, "error", "Errors", "Error rate (" + str(window) + " sec. rolling window)", base_dir, outfile + "_error")
    plot_data(base_results, lsd_results, "failed", "Failed", "Failures per second",
              base_dir, outfile + "_failed")


def plot_samples(base_results, lsd_results, base_dir, outfile):
    plot_data(base_results, lsd_results, "tpmC", "tpmC", "Transactions per second", base_dir, outfile + "_throughput")
    plot_data(base_results, lsd_results, "latency_99", "Latency (ms)", "Latency (P99th)", base_dir,
              outfile + "_latency_99")
    plot_data(base_results, lsd_results, "latency_avg", "Latency (ms)", "Latency (Average)", base_dir,
              outfile + "_latency_avg")
    plot_data(base_results, lsd_results, "window_99", "Window (ms)", "Window (P99th)", base_dir, outfile + "_window_99")
    plot_data(base_results, lsd_results, "window_avg", "Window (ms)", "Window (Average)", base_dir,
              outfile + "_window_avg")
    plot_data(base_results, lsd_results, "failed", "Failed", "Failed", base_dir, outfile + "_failed")


def compute(base_dir, window):
    base_tests = test_files(base_dir, "base", "raw.csv")
    lsd_tests = test_files(base_dir, "lsd", "raw.csv")
    for item in base_tests:
        # title = create_title_name(item)
        outfile = "manual_" + create_file_name(item, window)
        lsd_results = join_files(lsd_tests[item], True)
        base_results = join_files(base_tests[item], True)

        # lsd_results = enhance_data(lsd_results)
        # base_results = enhance_data(base_results)

        lsd_results = compute_statistics(lsd_results, window=window)
        base_results = compute_statistics(base_results, window=window)

        plot_computation(base_results, lsd_results, window, base_dir, outfile)


def compute_samples(base_dir):
    base_tests = test_files(base_dir, "base", "samples.csv")
    lsd_tests = test_files(base_dir, "lsd", "samples.csv")
    for item in base_tests:
        # title = create_title_name(item)
        outfile = "sampled_" + create_file_name(item)
        lsd_results = join_files(lsd_tests[item])
        base_results = join_files(base_tests[item])

        # lsd_results = enhance_data(lsd_results)
        # base_results = enhance_data(base_results)

        lsd_results = compute_sample_stats(lsd_results)
        base_results = compute_sample_stats(base_results)

        plot_samples(base_results, lsd_results, base_dir, outfile)

    return


def compute_aggregate(directories, windows):
    sampled_lsd = dict()
    sampled_base = dict()

    for base_dir in directories:
        sampled_base_tests = test_files(base_dir, "base", "samples.csv")
        sampled_lsd_tests = test_files(base_dir, "lsd", "samples.csv")
        base_tests = test_files(base_dir, "base", "raw.csv")
        lsd_tests = test_files(base_dir, "lsd", "raw.csv")

        for item in sampled_base_tests:
            # title = create_title_name(item)
            outfile_sample = "sampled_" + create_file_name(item)
            agg_outfile_sample = "agg_sampled_" + create_file_name(item)

            sampled_lsd_results = compute_sample_stats(join_files(sampled_lsd_tests[item]))
            sampled_base_results = compute_sample_stats(join_files(sampled_base_tests[item]))

            if agg_outfile_sample in sampled_lsd:
                sampled_lsd[agg_outfile_sample].append(sampled_lsd_results)
            else:
                sampled_lsd[agg_outfile_sample] = [sampled_lsd_results]

            if agg_outfile_sample in sampled_base:
                sampled_base[agg_outfile_sample].append(sampled_base_results)
            else:
                sampled_base[agg_outfile_sample] = [sampled_base_results]

            plot_samples(sampled_base_results, sampled_lsd_results, base_dir, outfile_sample)

        for item in base_tests:
            for window in windows:
                outfile_manual = "manual_" + create_file_name(item, window)

                lsd_results = join_files(lsd_tests[item], True)
                base_results = join_files(base_tests[item], True)

                lsd_results = compute_statistics(lsd_results, window=window)
                base_results = compute_statistics(base_results, window=window)

                plot_computation(base_results, lsd_results, window, base_dir, outfile_manual)

    agg_outfile_sample, sampled_base_list = sampled_base.popitem()
    base = sampled_base_list[0]
    for i in range(1, len(sampled_base_list)):
        base = pd.concat([base, sampled_base_list[i]], ignore_index=True)

    sampled_lsd_list = sampled_lsd.pop(agg_outfile_sample)
    lsd = sampled_lsd_list[0]
    for i in range(1, len(sampled_lsd_list)):
        lsd = pd.concat([lsd, sampled_lsd_list[i]], ignore_index=True)

    base_avg = base.groupby('time_bucket').mean().reset_index()
    lsd_avg = lsd.groupby('time_bucket').mean().reset_index()

    plot_samples(base_avg, lsd_avg, "tpcc", agg_outfile_sample)

    return


def run(args):
    # dirs = [
    #     "tpcc/run-59",
    #     "tpcc/run-71",
    #     "tpcc/run-72"
    # ]

    dirs = [ "tpcc/run-111"]
    use_samples = False

    plt.rcParams.update({
        "text.usetex": False,
        "font.family": "sans-serif",
        "font.size": 26
    })
    for base_dir in dirs:
        if not use_samples is None and use_samples is True:
            compute_samples(base_dir)
        else:
            for window in [5]:
                compute(base_dir, window)
    # compute_aggregate(dirs, [5])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--directory",
        dest="directory",
        help="Directory with run results"
    )
    parser.add_argument(
        "-s",
        "--use-samples",
        dest="use_samples",
        help="Use the samples file"
    )
    parser.add_argument(
        "-a",
        "--average-runs",
        dest="average_runs",
        help="Computes average from all runs passed in --directory, or -d, flag"
    )

    args = parser.parse_args()
    run(args)
