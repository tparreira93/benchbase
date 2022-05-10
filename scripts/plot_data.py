#!/bin/python3
import glob
import sys
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime


def run():
    files = []
    for arg in sys.argv[1:]:
        for f in glob.glob("*.results.csv", root_dir=arg):
            p = Path(os.path.join(arg, f))
            files.append((str(p.parent.parent.name), str(p)))

    if len(files) == 0:
        return

    ax = plt.gca()
    output_folder = sys.argv[2]
    name = str(Path(files[0][1]).parent) + "_" + datetime.now().strftime("%Y%m%d_%Hh%Mm%S")
    for driver, file in files:
        colname = driver.upper() + " Requests (per second)"
        df = pd.read_csv(file)
        df.rename(columns={
            "Throughput (requests/second)": colname
        }, inplace=True)

        a = df.plot(kind='line', x='Time (seconds)', y=colname, ax=ax)
        a.set_xlabel("Time (seconds)")
        a.set_ylabel("Requests (per second)")

    # dfs = pd.concat([pd.read_csv(file).assign(driver=driver) for driver, file in files])


    # dfs.plot(kind='line', x='Time (seconds)', y='Throughput (requests/second)', ax=ax)
    # dfs.plot(kind='line', x='Time (seconds)', y='Throughput (requests/second)', color='red', ax=ax)
    # plt.show()
    plt.savefig(os.path.join(str(output_folder), name + '.png'))


if __name__ == '__main__':
    run()
