from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def setup():
    ratings_path = "Dataset/title.ratings.tsv"
    crew_path = "Dataset/title.crew.tsv"
    names_path = "Dataset/name.basics.tsv"
    episodes_path = "Dataset/title.episode.tsv"
    title_path = "Dataset/title.basics.tsv"
    title_alt_path = "Dataset/title.akas.tsv"
    principals_path = "Dataset/title.principals.tsv"

    spark = (
        SparkSession.builder.appName("IMDb Analysis")
        .config("spark.sql.repl.eagerEval.enabled", True)
        .getOrCreate()
    )

    crew_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(crew_path)
    )
    ratings_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(ratings_path)
    )
    names_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(names_path)
    )
    episodes_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(episodes_path)
    )
    title_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(title_path)
    )
    title_alt_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(title_alt_path)
    )
    principals_df = (
        spark.read.option("header", True)
        .option("sep", "\t")
        .option("inferSchema", True)
        .csv(principals_path)
    )

    return (
        spark,
        crew_df,
        ratings_df,
        names_df,
        episodes_df,
        title_df,
        title_alt_df,
        principals_df,
    )


if __name__ == "__main__":
    (
        spark,
        crew_df,
        ratings_df,
        names_df,
        episodes_df,
        title_df,
        title_alt_df,
        principals_df,
    ) = setup()

    # NOTE:
    # Для простоти кладіть ваші функції сюди, по типу
    # yarko(spark, ......)
    # pavlo(spark, ......)
    # andrew(spark, .......)
    # eugene(spark, ........)
