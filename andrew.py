from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import split, explode, collect_list


def drama_actors(title_basics_df, title_principals_df, name_basics_df):
    long_drama_df = title_basics_df.filter(
        (F.col("runtimeMinutes") > 150)
        & (F.col("genres").contains("Drama"))
        & (F.col("titleType") == "movie")
    )
    actor_roles_df = title_principals_df.filter(F.col("category") == "actor")
    q1 = (
        long_drama_df.join(actor_roles_df, "tconst")
        .groupBy("nconst")
        .agg(F.count("tconst").alias("movie_count"))
        .join(name_basics_df, "nconst")
        .select("primaryName", "movie_count")
        .orderBy(F.desc("movie_count"))
    )
    q1.show(20)
    q1.write.mode("overwrite").option("header", True).csv("output/andrew.drama_actors")


def films_after_2010(title_basics_df, title_ratings_df):
    joined_df = title_basics_df.join(title_ratings_df, "tconst")
    q2 = joined_df.filter(
        (F.col("startYear") > 2010)
        & (F.col("averageRating") < 5.0)
        & (F.col("numVotes") > 5000)
        & (F.col("titleType") == "movie")
    ).select("primaryTitle", "startYear", "averageRating", "numVotes")
    q2.show(20)
    q2.write.mode("overwrite").option("header", True).csv(
        "output/andrew.films_after_2010"
    )


def documentary_films_1990_to_2024(title_basics_df):
    q3 = (
        title_basics_df.filter(
            (F.col("genres").contains("Documentary"))
            & (F.col("startYear").between(1990, 2024))
            & (F.col("titleType") == "movie")
        )
        .groupBy("startYear")
        .agg(F.count("*").alias("documentary_count"))
        .orderBy("startYear")
    )
    q3.show(20)
    q3.write.mode("overwrite").option("header", True).csv(
        "output/andrew.documentary_films_1990_to_2024"
    )


def tv_series_with_most_seasons(
    title_basics_df, title_episode_df, title_crew_df, name_basics_df
):
    tv_series_df = title_basics_df.filter(F.col("titleType") == "tvSeries").select(
        "tconst", "primaryTitle"
    )

    episodes_per_series = title_episode_df.groupBy("parentTconst").agg(
        F.count("*").alias("episode_count")
    )

    crew_df = title_crew_df.select("tconst", "directors").withColumn(
        "director", explode(split("directors", ","))
    )

    directors_df = (
        crew_df.join(
            name_basics_df.select("nconst", "primaryName"),
            crew_df["director"] == name_basics_df["nconst"],
            "left",
        )
        .groupBy("tconst")
        .agg(collect_list("primaryName").alias("directors"))
    )

    q4 = (
        episodes_per_series.join(
            tv_series_df, episodes_per_series["parentTconst"] == tv_series_df["tconst"]
        )
        .join(directors_df, tv_series_df["tconst"] == directors_df["tconst"], "left")
        .select(tv_series_df["primaryTitle"], "episode_count", "directors")
        .orderBy(F.desc("episode_count"))
    )

    q4.show(20, truncate=False)
    q4.write.mode("overwrite").option("header", True).csv(
        "output/andrew.tv_series_with_most_seasons"
    )


def actors_under_25(
    title_principals_df, name_basics_df, title_ratings_df, title_basics_df
):
    actors_df = title_principals_df.filter(F.col("category").isin("actor", "actress"))
    names_df = name_basics_df.select("nconst", "primaryName", "birthYear")

    debut_joined = (
        actors_df.join(title_basics_df, "tconst")
        .join(title_ratings_df, "tconst")
        .join(names_df, "nconst")
        .withColumn("age", F.col("startYear") - F.col("birthYear"))
    )

    window_spec = Window.partitionBy("nconst").orderBy("startYear")

    q5 = (
        debut_joined.withColumn("rn", F.row_number().over(window_spec))
        .filter((F.col("rn") == 1) & (F.col("age") < 25))
        .select("primaryName", "primaryTitle", "age", "averageRating")
        .orderBy(F.desc("averageRating"), F.asc("age"))
    )

    q5.show(20, truncate=False)
    q5.write.mode("overwrite").option("header", True).csv(
        "output/andrew.actors_under_25"
    )


def top_3_in_each_category(title_basics_df, title_ratings_df):
    rated_movies_df = (
        title_basics_df.filter(F.col("titleType") == "movie")
        .join(title_ratings_df, "tconst")
        .withColumn("genre", explode(split(F.col("genres"), ",")))
    )
    genre_window = Window.partitionBy("genre").orderBy(F.desc("numVotes"))
    q6 = (
        rated_movies_df.withColumn("rank", F.row_number().over(genre_window))
        .filter(F.col("rank") <= 3)
        .select("primaryTitle", "genre", "numVotes", "rank")
    )
    q6.show(20)
    q6.write.mode("overwrite").option("header", True).csv(
        "output/andrew.top_3_in_each_category"
    )


def call_andrew_functions(
    title_basics_df,
    title_principals_df,
    title_ratings_df,
    title_crew_df,
    name_basics_df,
    title_episode_df,
):
    print(
        "Actors which most frequently appear in movies that are over 150 minutes long and belong to the Drama genre"
    )
    drama_actors(title_basics_df, title_principals_df, name_basics_df)

    print(
        "Movies released after 2010 have a rating below 5 and received more than 5,000 votes"
    )
    films_after_2010(title_basics_df, title_ratings_df)

    print("Documentary films were released each year between 1990 and 2024")
    documentary_films_1990_to_2024(title_basics_df)

    print("TV shows had the highest number of seasons and who was their main director")
    tv_series_with_most_seasons(
        title_basics_df, title_episode_df, title_crew_df, name_basics_df
    )

    print(
        "Actors that made their debut in movies before the age of 25 and had the highest ratings for their debut roles"
    )
    actors_under_25(
        title_principals_df, name_basics_df, title_ratings_df, title_basics_df
    )

    print("For each genre, which movies are in the top 3 by number of votes")
    top_3_in_each_category(title_basics_df, title_ratings_df)
