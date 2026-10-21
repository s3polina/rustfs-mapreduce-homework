import gc
import os
import threading
import time

import pandas as pd
import psutil

from common import put_text
from mapreduce import run_mapreduce
from duckdb_calc import run_duckdb

def measure(func):
    process = psutil.Process(os.getpid())
    peak = process.memory_info().rss
    stop = False

    def watch():
        nonlocal peak
        while not stop:
            peak = max(peak, process.memory_info().rss)
            time.sleep(0.02)

    t = threading.Thread(target=watch, daemon=True)
    t.start()

    gc.collect()
    start = time.perf_counter()
    func()
    elapsed = time.perf_counter() - start

    stop = True
    t.join(timeout=1)
    return elapsed, peak / 1024 / 1024

def main():
    rows = []

    for method, func in [
        ("MapReduce", lambda: run_mapreduce(save=False)),
        ("DuckDB", lambda: run_duckdb(save=False)),
    ]:
        for attempt in range(1, 4):
            seconds, peak_mb = measure(func)
            row = {
                "method": method,
                "attempt": attempt,
                "seconds": round(seconds, 3),
                "peak_memory_mb": round(peak_mb, 2),
            }
            rows.append(row)
            print(row)

    df = pd.DataFrame(rows)
    summary = df.groupby("method", as_index=False).agg(
        avg_seconds=("seconds", "mean"),
        min_seconds=("seconds", "min"),
        max_seconds=("seconds", "max"),
        avg_peak_memory_mb=("peak_memory_mb", "mean"),
        max_peak_memory_mb=("peak_memory_mb", "max"),
    )

    put_text("results/profiling_runs.csv", df.to_csv(index=False))
    put_text("results/profiling_summary.csv", summary.to_csv(index=False))

    print("\nСводка:")
    print(summary.to_string(index=False))

if __name__ == "__main__":
    main()
