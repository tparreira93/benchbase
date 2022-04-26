#!/bin/python3

import os
import sys
from pathlib import Path
from os import listdir


def fix_name(string):
    if string == "lsd":
        return "jdbc:lsd.v2:postgresql"
    elif string == "base":
        return "jdbc:postgresql"
    else:
        return string


def build_folder(output, benchmark, driver):
    path = os.path.join(output, benchmark, driver)
    if not os.path.exists(path):
        os.makedirs(path)

    return path


def build_file_name(benchmark, driver, scale, terminals):
    return benchmark + "_d" + driver + "_s" + scale + "_t" + terminals + ".xml"


def create_test_configs(config_file_path: str, template_file_path: str, output: str):
    test_configs = set()
    with open(config_file_path, "r") as config_file, open(template_file_path, "r") as template_file:
        config_lines = config_file.readlines()
        template_config = template_file.read()

        template_keys = config_lines[0].strip().split(",")
        config_lines = [l.strip().split(",") for l in config_lines[1:] if not l.startswith("#")]

        for values in config_lines:
            benchmark = values[0]
            driver = values[1]
            scale = values[2]
            terminals = values[3]
            template = template_config
            keys = template_keys[1:]
            for (k, v) in zip(keys, values[1:]):
                template = template.replace("$" + k, fix_name(v))

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

        args = " --create=true --load=true --execute=true "
        if driver != "base":
            args += "-b " + base_dir + "_" + driver
        else:
            args += "-b " + base_dir

        for f in listdir(dir_name):
            c = Path(f)

            command = "-c " + str(c.absolute()) + " -d " + str(c.stem) + args
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


def run():
    config = sys.argv[1]
    template = sys.argv[2]
    output = sys.argv[3]

    config_dirs = create_test_configs(config, template, output)

    commands = generate_commands(config_dirs)

    with open("run.sh", "w+") as f:
        f.writelines(["#!/usr/bin/env bash", os.linesep])

        for command in commands:

            f.writelines(["java -jar benchbase.jar " + command, os.linesep])

        plots = aggregate_results(config_dirs)

        for p in plots:
            f.writelines(["python plot_data.py " + " ".join(p), os.linesep])


if __name__ == '__main__':
    run()

