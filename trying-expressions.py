import polars as pl
import polars.selectors as cs
from polars.selectors import is_selector
from polars.selectors import expand_selector
import numpy as np
from datetime import date, datetime


def basic_op():
    df = pl.DataFrame(
        {
            "nrs": [1, 2, 3, None, 5],
            "names": ["foo", "ham", "spam", "egg", None],
            "random": np.random.rand(5),
            "groups": ["A", "A", "B", "C", "B"],
        }
    )

    print(df)

    # Numerical op
    df_numerical = df.select(
        (pl.col("nrs") + 5).alias("nrs + 5"),
        (pl.col("nrs") - 5).alias("nrs - 5"),
        (pl.col("nrs") * pl.col("random")).alias("nrs * random"),
        (pl.col("nrs") / pl.col("random")).alias("nrs / random"),
    )

    print(df_numerical)

    # Logical OP
    df_logical = df.select(
        (pl.col("nrs") > 1).alias("nrs > 1"),
        (pl.col("random") <= 0.5).alias("random <= .5"),
        (pl.col("nrs") != 1).alias("nrs != 1"),
        (pl.col("nrs") == 1).alias("nrs == 1"),
        ((pl.col("random") <= 0.5) & (pl.col("nrs") > 1)).alias("and_expr"),  # and
        ((pl.col("random") <= 0.5) | (pl.col("nrs") > 1)).alias("or_expr"),  # or
    )
    print(df_logical)


def column_selection():
    df = pl.DataFrame(
        {
            "id": [9, 4, 2],
            "place": ["Mars", "Earth", "Saturn"],
            "date": pl.date_range(date(2022, 1, 1), date(2022, 1, 3), "1d", eager=True),
            "sales": [33.4, 2142134.1, 44.7],
            "has_people": [False, True, False],
            "logged_at": pl.datetime_range(
                datetime(2022, 12, 1), datetime(2022, 12, 1, 0, 0, 2), "1s", eager=True
            ),
        }
    ).with_row_index("index")

    print(df)

    # SELECT ALL
    out = df.select(pl.col("*"))

    # Is equivalent to
    out = df.select(pl.all())
    print(out)

    # SELECT with exclude cloumns
    out = df.select(pl.col("*").exclude("logged_at", "index"))
    print(out)

    # dt.to_string
    out = df.select(pl.col("date", "logged_at").dt.to_string("%Y-%h-%d"))
    print(out)

    # by regex
    out = df.select(pl.col("^.*(as|sa).*$"))
    print(out)

    # by data type
    out = df.select(pl.col(pl.Int64, pl.UInt32, pl.Boolean).n_unique())
    print(out)

    # by using selectors
    out = df.select(cs.integer(), cs.string())
    print(out)

    # set operations
    out = df.select(cs.numeric() - cs.first())
    print(out)

    out = df.select(cs.by_name("index") | ~cs.numeric())
    print(out)

    # By patterns and substrings
    out = df.select(cs.contains("index"), cs.matches(".*_.*"))
    print(out)

    # Converting to expressions
    out = df.select(cs.temporal().as_expr().dt.to_string("%Y-%h-%d"))
    print(out)

    # Debugging selectors

    out = cs.numeric()
    print(is_selector(out))

    out = cs.boolean() | cs.numeric()
    print(is_selector(out))

    out = cs.numeric() + pl.lit(123)
    print(is_selector(out))

    # expand_selector
    out = cs.temporal()
    print(expand_selector(df, out))

    out = ~(cs.temporal() | cs.numeric())
    print(expand_selector(df, out))


def some_func():
    df = pl.DataFrame(
        {
            "nrs": [1, 2, 3, None, 5],
            "names": ["foo", "ham", "spam", "egg", "spam"],
            "random": np.random.rand(5),
            "groups": ["A", "A", "B", "C", "B"],
        }
    )
    print(df)

    # Unique
    df_alias = df.select(
        pl.col("names").n_unique().alias("unique"),
        pl.approx_n_unique("names").alias("unique_approx"),
    )
    print(df_alias)

    # Conditionals
    df_conditional = df.select(
        pl.col("nrs"),
        pl.when(pl.col("nrs") > 2)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("conditional"),
    )
    print(df_conditional)


def string_expr():
    df = pl.DataFrame({"animal": ["Crab", "cat and dog", "rab$bit", None]})

    out = df.select(
        pl.col("animal").str.len_bytes().alias("byte_count"),
        pl.col("animal").str.len_chars().alias("letter_count"),
    )
    print(out)

    # CHECK FOR EXISTENCE OF A PATTERN
    out = df.select(
        pl.col("animal"),
        pl.col("animal").str.contains("cat|bit").alias("regex"),
        pl.col("animal").str.contains("rab$", literal=True).alias("literal"),
        pl.col("animal").str.starts_with("rab").alias("starts_with"),
        pl.col("animal").str.ends_with("dog").alias("ends_with"),
    )
    print(out)

    # EXTRACT A PATTERN
    df = pl.DataFrame(
        {
            "a": [
                "http://vote.com/ballon_dor?candidate=messi&ref=polars",
                "http://vote.com/ballon_dor?candidat=jorginho&ref=polars",
                "http://vote.com/ballon_dor?candidate=ronaldo&ref=polars",
            ]
        }
    )
    out = df.select(
        pl.col("a").str.extract(r"candidate=(\w+)", group_index=1),
    )
    print(out)

    df = pl.DataFrame({"foo": ["123 bla 45 asd", "xyz 678 910t"]})
    out = df.select(
        pl.col("foo").str.extract_all(r"(\d+)").alias("extracted_nrs"),
    )
    print(out)

    # REPLACE A PATTERN
    df = pl.DataFrame({"id": [1, 2], "text": ["123abc", "abc456"]})
    out = df.with_columns(
        pl.col("text").str.replace(r"abc\b", "ABC"),
        pl.col("text").str.replace_all("a", "-", literal=True).alias("text_replace_all"),
    )
    print(out)

def compute_age():
    return date.today().year - pl.col("birthday").dt.year()


def avg_birthday(gender: str) -> pl.Expr:
    return (
        compute_age()
        .filter(pl.col("gender") == gender)
        .mean()
        .alias(f"avg {gender} birthday")
    )


def get_person() -> pl.Expr:
    return pl.col("first_name") + pl.lit(" ") + pl.col("last_name")


def aggregation_ex():
    # url = "https://theunitedstates.io/congress-legislators/legislators-historical.csv"
    url = "data/legislators-historical.csv"

    schema_overrides = {
        "first_name": pl.Categorical,
        "gender": pl.Categorical,
        "type": pl.Categorical,
        "state": pl.Categorical,
        "party": pl.Categorical,
    }

    dataset = pl.read_csv(url, schema_overrides=schema_overrides).with_columns(
        pl.col("birthday").str.to_date(strict=False)
    )

    print(dataset)

    # Basic aggregations

    q = (
        dataset.lazy()
        .group_by("first_name")
        .agg(
            pl.len(),
            pl.col("gender"),
            pl.first("last_name"),
        )
        .sort("len", descending=True)
        .limit(5)
    )

    df = q.collect()
    print(df)

    # Conditional agg
    q = (
        dataset.lazy()
        .group_by("state")
        .agg(
            (pl.col("party") == "Anti-Administration").sum().alias("anti"),
            (pl.col("party") == "Pro-Administration").sum().alias("pro"),
        )
        .sort("pro", descending=True)
        .limit(5)
    )

    df = q.collect()
    print(df)

    # roup by
    q = (
        dataset.lazy()
        .group_by("state", "party")
        .agg(pl.count("party").alias("count"))
        .filter(
            (pl.col("party") == "Anti-Administration")
            | (pl.col("party") == "Pro-Administration")
        )
        .sort("count", descending=True)
        .limit(5)
    )

    df = q.collect()
    print(df)

    # filtering

    q = (
        dataset.lazy()
        .group_by("state")
        .agg(
            avg_birthday("M"),
            avg_birthday("F"),
            (pl.col("gender") == "M").sum().alias("# male"),
            (pl.col("gender") == "F").sum().alias("# female"),
        )
        .limit(5)
    )

    df = q.collect()
    print(df)

    q = (
        dataset.lazy()
        .sort("birthday", descending=True)
        .group_by("state")
        .agg(
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
        )
        .limit(5)
    )

    df = q.collect()
    print(df)

    q = (
        dataset.lazy()
        .sort("birthday", descending=True)
        .group_by("state")
        .agg(
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
            get_person().sort().first().alias("alphabetical_first"),
        )
        .limit(5)
    )

    df = q.collect()
    print(df)

    q = (
        dataset.lazy()
        .sort("birthday", descending=True)
        .group_by("state")
        .agg(
            get_person().first().alias("youngest"),
            get_person().last().alias("oldest"),
            get_person().sort().first().alias("alphabetical_first"),
            pl.col("gender")
            .sort_by(pl.col("first_name").cast(pl.Categorical("lexical")))
            .first(),
        )
        .sort("state")
        .limit(5)
    )

    df = q.collect()
    print(df)

if __name__ == '__main__':
    # basic_op()
    # column_selection()
    # some_func()
    # string_expr()
    aggregation_ex()
