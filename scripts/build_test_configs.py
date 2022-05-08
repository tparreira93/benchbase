#!/bin/python3

import os
from pathlib import Path
from os import listdir
import argparse


def build_folder(output, benchmark, driver):
    path = os.path.join(output, benchmark, driver)
    if not os.path.exists(path):
        os.makedirs(path)

    return path


def build_file_name(benchmark, driver, scale, terminals):
    if driver.__contains__("lsd"):
        driver = "lsd"
    else:
        driver = "base"

    return benchmark + "_d" + driver + "_s" + scale + "_t" + terminals + ".xml"


def find_index(data, value):
    for i in range(len(data)):
        if value.lower() == data[i].lower():
            return i

    raise Exception("Missing parameter " + value)


def create_test_configs(config_file_path: str, template_file_path: str, output: str):
    test_configs = set()
    with open(config_file_path, "r") as config_file, open(template_file_path, "r") as template_file:
        config_lines = config_file.readlines()
        template_config = template_file.read()

        template_keys = config_lines[0].strip().split(",")
        config_lines = [l.strip().split(",") for l in config_lines[1:] if not l.startswith("#")]

        for values in config_lines:
            benchmark = values[find_index(template_keys, "benchmark")]
            driver = values[find_index(template_keys, "driver")]
            scale = values[find_index(template_keys, "scale")]
            terminals = values[find_index(template_keys, "terminals")]
            template = template_config
            keys = template_keys[1:]
            for (k, v) in zip(keys, values[1:]):
                template = template.replace("$" + k, v)

            file_name = build_file_name(benchmark, driver, scale, terminals)
            output_folder = build_folder(output, benchmark, driver)

            path = os.path.join(output_folder, file_name)

            with open(path, "w+") as f:
                f.write(template)

            test_configs.add(output_folder)

    return test_configs


def generate_commands(config_dirs):
    commands = []
    for dir_name in config_dirs:
        p = Path(dir_name)
        base_dir = p.parent.name
        driver = p.name

        cmd_args = " --create=true --load=true --execute=true "
        if driver != "base":
            cmd_args += "-b " + base_dir + "_" + driver
        else:
            cmd_args += "-b " + base_dir

        files = [os.path.join(dir_name, f) for f in listdir(dir_name) if os.path.isfile(os.path.join(dir_name, f))]

        for f in files:
            c = Path(f)

            command = "-c " + str(c.absolute()) + " -d " + os.path.join(c.parent.absolute(), str(c.stem)) + cmd_args
            commands.append(command)

    return commands


def aggregate_results(config_dirs):
    agg = dict()
    for dir_name in config_dirs:
        for d in listdir(dir_name):
            p = Path(d)
            parts = d.split("_")
            k = parts[0] + "_" + "_".join(parts[2:])

            if k in agg:
                agg[k].append(p.stem)
            else:
                agg[k] = [p.stem]

    return agg.values()


def run(args):
    config = args.config
    template = args.template
    output = os.path.abspath(args.output)
    java = os.path.abspath(args.jar)

    config_dirs = create_test_configs(config, template, output)

    commands = generate_commands(config_dirs)

    with open("run.sh", "w+") as f:
        f.writelines(["#!/usr/bin/env bash", os.linesep])

        f.writelines(["pushd " + java, os.linesep])

        for command in commands:
            f.writelines(["java -jar benchbase.jar " + command, os.linesep])

        f.writelines(["popd", os.linesep])

        plots = aggregate_results(config_dirs)

        for p in plots:
            f.writelines(["python3 " + os.path.abspath("plot_data.py") + " " + " ".join(p), os.linesep])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--config",
        dest="config",
        help="Configuration CSV with benchmark tests to generate"
    )
    parser.add_argument(
        "-t",
        "--template",
        dest="template",
        help="Benchmark template"
    )
    parser.add_argument(
        "-o",
        "--output",
        help="Output directory of test results"
    )
    parser.add_argument(
        "-j",
        "--jar-path",
        dest="jar",
        help="Path to benchmark executable"
    )

    args = parser.parse_args()
    run(args)
