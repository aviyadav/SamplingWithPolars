import polars as pl

def example1():
    pokemon = pl.read_csv(
        "https://gist.githubusercontent.com/ritchie46/cac6b337ea52281aa23c049250a4ff03/raw/89a957ff3919d90e6ef2d34235e6bf22304f3366/pokemon.csv"
    )

    with pl.SQLContext(register_globals=True, eager=True) as ctx:
        df_small = ctx.execute("SELECT * FROM pokemon LIMIT 5")

        print(df_small)


def example2():
    df = pl.DataFrame(
        {
            "city": [
                "New York",
                "Los Angeles",
                "Chicago",
                "Houston",
                "Phoenix",
                "Amsterdam",
            ],
            "country": ["USA", "USA", "USA", "USA", "USA", "Netherlands"],
            "population": [8399000, 3997000, 2705000, 2320000, 1680000, 900000],
        }
    )

    ctx = pl.SQLContext(population=df, eager=True)

    print(ctx.execute("SELECT * FROM population"))

    result = ctx.execute(
        """
            SELECT country, AVG(population) as avg_population
            FROM population
            GROUP BY country
        """
    )
    print(result)

    result = ctx.execute(
        """
            SELECT city, population
            FROM population
            ORDER BY population
        """
    )
    print(result)

    income = pl.DataFrame(
        {
            "city": [
                "New York",
                "Los Angeles",
                "Chicago",
                "Houston",
                "Amsterdam",
                "Rotterdam",
                "Utrecht",
            ],
            "country": [
                "USA",
                "USA",
                "USA",
                "USA",
                "Netherlands",
                "Netherlands",
                "Netherlands",
            ],
            "income": [55000, 62000, 48000, 52000, 42000, 38000, 41000],
        }
    )

    ctx.register_many(income=income)
    result = ctx.execute(
        """
            SELECT country, city, income, population
            FROM population
            LEFT JOIN income on population.city = income.city
        """
    )
    print(result)

    result = ctx.execute(
        """
            SELECT city, population
            FROM population
            WHERE STARTS_WITH(country,'U')
        """
    )
    print(result)

    result = ctx.execute(
        """
            SELECT *
            FROM read_csv('data/iris.csv')
        """
    )
    print(result)



if __name__ == '__main__':
    # example1()
    example2()
