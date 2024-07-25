import polars as pl

def cr_001():
    data = {"name": ["Alice", "Bob", "Charlie", "David"], "age": [25, 30, 35, 40]}
    df = pl.LazyFrame(data)

    ctx = pl.SQLContext(my_table=df, eager=True)

    result = ctx.execute(
        """
        CREATE TABLE older_people
        AS
        SELECT * FROM my_table WHERE age > 30
        """
    )

    print(ctx.execute("SELECT * FROM older_people"))


def cte_001():
    ctx = pl.SQLContext()
    df = pl.LazyFrame(
        {"name": ["Alice", "Bob", "Charlie", "David"], "age": [25, 30, 35, 40]}
    )
    ctx.register("my_table", df)

    result = ctx.execute(
        """
        WITH older_people AS (
            SELECT * FROM my_table WHERE age > 30
        )
        SELECT * FROM older_people WHERE STARTS_WITH(name,'C')
    """,
        eager=True,
    )

    print(result)


if __name__ == '__main__':
    # cr_001()
    
    cte_001()