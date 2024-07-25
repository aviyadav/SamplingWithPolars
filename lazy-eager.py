import datetime

import polars as pl
import time


def logic_001():
    start = time.time()
    df = pl.read_csv("data/iris.csv")
    df_small = df.filter(pl.col("SepalLengthCm") > 5)
    # print(df_small)
    df_agg = df_small.group_by("Species").agg(pl.col("SepalWidthCm").mean())
    end = time.time()
    print(df_agg)
    print(f"time taken - {(end - start)}")  #  0.005139827728271484


def logic_002():
    start = time.time()
    q = (
        pl.scan_csv("data/iris.csv")
        .filter(pl.col("SepalLengthCm") > 5)
        .group_by("Species")
        .agg(pl.col("SepalWidthCm").mean())
    )

    df = q.collect()

    end = time.time()
    print(q.explain())
    # print(df)
    print(f"time taken - {(end - start)}") # 0.006868124008178711


def stream_ex():
    start = time.time()
    q1 = (
        pl.scan_csv("data/iris.csv")
        .filter(pl.col("SepalLengthCm") > 5)
        .group_by("Species")
        .agg(pl.col("SepalWidthCm").mean())
    )
    df = q1.collect(streaming=True)
    end = time.time()
    print(q1.explain(streaming=True))
    # print(df)
    print(f"time taken option stream - {end - start}") # 0.09717154502868652


def stream_ex_2():
    start = time.time()
    q2 = pl.scan_csv("data/iris.csv").with_columns(
        pl.col("SepalLengthCm").mean().over("Species")
    )

    df = q2.collect(streaming=True)

    end = time.time()
    print(q2.explain(streaming=True))
    print(f"time taken option stream - {end - start}")  # 0.09717154502868652


if __name__ == '__main__':

    # logic_001()
    # logic_002()
    # stream_ex()
    stream_ex_2()