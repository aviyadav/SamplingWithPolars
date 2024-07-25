import polars as pl


def wf_over_ex(df):
    out = df.select(
        "Type 1",
        "Type 2",
        pl.col("Attack").mean().over("Type 1").alias("avg_attack_by_type"),
        pl.col("Defense")
        .mean()
        .over(["Type 1", "Type 2"])
        .alias("avg_defense_by_type_combination"),
        pl.col("Attack").mean().alias("avg_attack"),
    )

    print(out)


def filter_ex(df):
    filtered = df.filter(pl.col("Type 2") == "Psychic").select(
        "Name",
        "Type 1",
        "Speed",
    )
    print(filtered)

    out = filtered.with_columns(
        pl.col("Name", "Speed").sort_by("Speed", descending=True).over("Type 1"),
    )
    print(out)

    out = df.sort("Type 1").select(
        pl.col("Type 1").head(3).over("Type 1", mapping_strategy="explode"),
        pl.col("Name")
        .sort_by(pl.col("Speed"), descending=True)
        .head(3)
        .over("Type 1", mapping_strategy="explode")
        .alias("fastest/group"),
        pl.col("Name")
        .sort_by(pl.col("Attack"), descending=True)
        .head(3)
        .over("Type 1", mapping_strategy="explode")
        .alias("strongest/group"),
        pl.col("Name")
        .sort()
        .head(3)
        .over("Type 1", mapping_strategy="explode")
        .alias("sorted_by_alphabet"),
    )
    print(out)


if __name__ == '__main__':
    df = pl.read_csv(
        # "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv"
        "data/pokemon.csv"
    )
    # print(df.head())

    # wf_over_ex(df)
    filter_ex(df)
