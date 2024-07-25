import polars as pl
from datetime import datetime

def ex_series():
    s = pl.Series("a", [1, 2, 3, 4, 5])

    print(s)


def ex_dataframe():
    df = pl.DataFrame(
        {
            "integer": [1, 2, 3, 4, 5],
            "date": [
                datetime(2022, 1, 1),
                datetime(2022, 1, 2),
                datetime(2022, 1, 3),
                datetime(2022, 1, 4),
                datetime(2022, 1, 5),
            ],
            "float": [4.0, 5.0, 6.0, 7.0, 8.0],
        }
    )

    print(df)
    print(df.head(3))
    print(df.tail(3))
    print(df.sample(2))
    print(df.describe())

if __name__ == '__main__':
    # ex_series()
    ex_dataframe()