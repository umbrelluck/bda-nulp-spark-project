from pyspark.sql.functions import col, explode, split, row_number, avg, countDistinct
from pyspark.sql.window import Window


RESULTS_DIR = "output"


def top_rated_movies_by_year(title_df, ratings_df):
    movies = (
        title_df.filter((col("titleType") == "movie") & (col("isAdult") == 0))
        .join(ratings_df, "tconst")
        .filter((col("numVotes") > 1000) & (col("startYear").isNotNull()))
    )
    window_spec = Window.partitionBy("startYear").orderBy(col("averageRating").desc())
    ranked = movies.withColumn("rank", row_number().over(window_spec))
    top_movies = (
        ranked.filter(col("rank") <= 3)
        .select("startYear", "primaryTitle", "averageRating", "numVotes", "rank")
        .orderBy("startYear", "rank")
    )

    top_movies.show(20, truncate=False)
    top_movies.write.mode("overwrite").option("header", True).csv(
        f"{RESULTS_DIR}/yevhen.top_rated_movies_by_year"
    )


def most_active_actors_in_movies(principals_df, names_df, title_df):
    actors = principals_df.filter(col("category") == "actor")
    actors_with_titles = actors.join(title_df, on="tconst").filter(
        col("titleType").isin("movie", "tvSeries")
    )

    result = (
        actors_with_titles.groupBy("nconst")
        .agg(countDistinct("tconst").alias("uniqueTitleCount"))
        .join(names_df, "nconst")
        .orderBy(col("uniqueTitleCount").desc())
        .select("primaryName", "uniqueTitleCount")
    )

    result.show(20, truncate=False)
    result.write.mode("overwrite").option("header", True).csv(
        f"{RESULTS_DIR}/yevhen.most_active_actors"
    )


def top_directors_with_ratings(crew_df, ratings_df, title_df, names_df):
    rated_titles = title_df.join(ratings_df, on="tconst").filter(
        col("titleType") == "movie"
    )
    rated_titles_with_directors = rated_titles.join(crew_df, on="tconst").filter(
        col("directors").isNotNull()
    )
    rated_titles_with_directors = rated_titles_with_directors.withColumn(
        "director", explode(split("directors", ","))
    )

    rated_titles_with_directors = rated_titles_with_directors.join(
        names_df.select("nconst", "primaryName"),
        rated_titles_with_directors["director"] == names_df["nconst"],
    )

    top_directors = (
        rated_titles_with_directors.groupBy("primaryName")
        .agg(avg("averageRating").alias("averageRating"))
        .orderBy(col("averageRating").desc())
    )

    top_directors.show(20, truncate=False)
    top_directors.write.mode("overwrite").option("header", True).csv(
        f"{RESULTS_DIR}/yevhen.top_directors"
    )


def popular_genres(title_df, ratings_df):
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

    result.show(20, truncate=False)
    result.write.mode("overwrite").option("header", True).csv(
        f"{RESULTS_DIR}/yevhen.popular_genres"
    )


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

    result.show(20, truncate=False)
    result.write.mode("overwrite").option("header", True).csv(
        f"{RESULTS_DIR}/yevhen.top_shows_by_season"
    )


def top_movie_per_genre(title_df, ratings_df):
    rated_titles = title_df.join(ratings_df, on="tconst").filter(
        col("genres").isNotNull() & (col("titleType") == "movie")
    )

    rated_titles = rated_titles.withColumn("genre", explode(split("genres", ",")))
    window_spec = Window.partitionBy("genre").orderBy(col("averageRating").desc())
    top_ranked = rated_titles.withColumn("rank", row_number().over(window_spec))
    top_movies = (
        top_ranked.filter(col("rank") == 1)
        .select("genre", "primaryTitle", "averageRating")
        .orderBy("genre")
    )

    top_movies.show(20, truncate=False)
    top_movies.write.mode("overwrite").option("header", True).csv(
        f"{RESULTS_DIR}/yevhen.top_movie_per_genre"
    )


def call_yevhen_functions(
    title_df, ratings_df, names_df, principals_df, crew_df, episodes_df
):
    print("\nTop Rated Movies by Year")
    top_rated_movies_by_year(title_df, ratings_df)
    print("\nMost Active Actors by Movies")
    most_active_actors_in_movies(principals_df, names_df, title_df)
    print("\nTop Directors With Ratings")
    top_directors_with_ratings(crew_df, title_df, ratings_df, names_df)
    print("\nPopular Genres")
    popular_genres(title_df, ratings_df)
    print("\nTop Shows by Season")
    top_shows_by_season(episodes_df, ratings_df, title_df)
    print("\nTop Movie per Genre")
    top_movie_per_genre(title_df, ratings_df)
