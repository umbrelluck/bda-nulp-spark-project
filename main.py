from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, row_number
from pyspark.sql.window import Window

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
    spark.sparkContext.setLogLevel("ERROR")

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

def top_rated_movies(title_df, ratings_df):
    result = (
        title_df.filter((col("titleType") == "movie") & (col("isAdult") == 0))
        .join(ratings_df, "tconst")
        .filter(col("numVotes") > 1000)
        .orderBy(col("averageRating").desc())
        .select("primaryTitle", "startYear", "averageRating", "numVotes")
    )

    result.show(10, truncate=False)

def most_active_actors(names_df, principals_df):
    actors = principals_df.filter(col("category") == "actor")

    result = (
        actors.groupBy("nconst")
        .count()
        .join(names_df, "nconst")
        .orderBy(col("count").desc())
        .select("primaryName", "count")
    )

    result.show(10, truncate=False)

def top_directors_with_ratings(crew_df, title_df, ratings_df, names_df):
    directors_df = (
        crew_df
        .withColumn("director", col("directors"))
        .select("tconst", "director")
        .filter(col("director").isNotNull())
    )

    joined = (
        directors_df.join(ratings_df, "tconst")
        .join(title_df, "tconst")
        .filter(col("titleType") == "movie")
        .groupBy("director")
        .avg("averageRating")
        .withColumnRenamed("avg(averageRating)", "avgRating")
        .orderBy(col("avgRating").desc())
    )

    result = joined.join(names_df, joined.director == names_df.nconst).select(
        "primaryName", "avgRating"
    )

    result.show(10, truncate=False)

def popular_genres(title_df, ratings_df):
    from pyspark.sql.functions import explode, split

    exploded = (
        title_df.withColumn("genre", explode(split(col("genres"), ",")))
        .join(ratings_df, "tconst")
        .filter(col("genre").isNotNull())
    )

    result = (
        exploded.groupBy("genre")
        .avg("averageRating")
        .withColumnRenamed("avg(averageRating)", "avgRating")
        .orderBy(col("avgRating").desc())
    )

    result.show(10, truncate=False)

def top_shows_by_season(episodes_df, ratings_df, title_df):
    joined = (
        episodes_df.join(ratings_df, "tconst")
        .filter(col("seasonNumber").isNotNull())
        .groupBy("parentTconst", "seasonNumber")
        .avg("averageRating")
        .withColumnRenamed("avg(averageRating)", "avgSeasonRating")
    )

    result = (
        joined.join(title_df, joined.parentTconst == title_df.tconst)
        .select("primaryTitle", "seasonNumber", "avgSeasonRating")
        .orderBy(col("avgSeasonRating").desc())
    )

    result.show(10, truncate=False)

def top_movie_per_genre(title_df, ratings_df):
    rated_titles = title_df.join(ratings_df, on="tconst") \
        .filter(col("genres").isNotNull() & (col("titleType") == "movie"))

    rated_titles = rated_titles.withColumn("genre", explode(split("genres", ",")))
    window_spec = Window.partitionBy("genre").orderBy(col("averageRating").desc())
    top_ranked = rated_titles.withColumn("rank", row_number().over(window_spec))
    top_movies = top_ranked.filter(col("rank") == 1)
    top_movies.select("genre", "primaryTitle", "averageRating").orderBy("genre").show(truncate=False)

def call_yevhen_functions():
    print("\nTop Rated Movies")
    top_rated_movies(title_df, ratings_df)
    print("\nMost Active Actors")
    most_active_actors(names_df, principals_df)
    print("\nTop Directors With Ratings")
    top_directors_with_ratings(crew_df, title_df, ratings_df, names_df)
    print("\nPopular Genres")
    popular_genres(title_df, ratings_df)
    print("\nTop Shows by Season")
    top_shows_by_season(episodes_df, ratings_df, title_df)
    print("\nTop Movie per Genre")
    top_movie_per_genre(title_df, ratings_df)

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
    # Пропоную просто зробити функцію для виклику своїх функцій і мати спокій, мій приклад:
    #call_yevhen_functions()