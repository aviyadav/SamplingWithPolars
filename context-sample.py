import polars as pl
import numpy as np


def selection_ex(df):
    print(df)

    out = df.select(
        pl.sum("nrs"),
        pl.col("names").sort(),
        pl.col("names").first().alias("first name"),
        (pl.mean("nrs") * 10).alias("10xnrs"),
    )

    print(out)


def selection_withCol(df):
    df2 = df.with_columns(
        pl.sum("nrs").alias("nrs_sum"),
        pl.col("random").count().alias("count"),
    )

    print(df2)


def selection_withFilter(df):
    out = df.filter(pl.col("nrs") > 2)

    print(out)


def groupBy_ex(df):
    out = df.group_by("groups").agg(
        pl.sum("nrs"),  # sum nrs by groups
        pl.col("random").count().alias("count"),  # count group members
        # sum random where name != null
        pl.col("random").filter(pl.col("names").is_not_null()).sum().name.suffix("_sum"),
        pl.col("names").reverse().alias("reversed names"),
    )
    print(out)


if __name__ == '__main__':
    df = pl.DataFrame(
        {
            "nrs": [1, 2, 3, None, 5],
            "names": ["foo", "ham", "spam", "egg", None],
            "random": np.random.rand(5),
            "groups": ["A", "A", "B", "C", "B"],
        }
    )

    # selection_ex(df)
    # selection_withCol(df)
    # selection_withFilter(df)
    groupBy_ex(df)