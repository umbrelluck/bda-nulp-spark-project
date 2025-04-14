from pyspark.sql.functions import col, explode, split, row_number, avg, countDistinct, count, expr, sum, rank, round
from pyspark.sql.window import Window


def non_adult_horror_movies(title_df, ratings_df):
    non_adult_movies = title_df.join(ratings_df, on="tconst").filter(
        (col("isAdult") == 0) &
        (col("genres").contains("Horror")) &
        (col("titleType") == "movie")
    )

    non_adult_movies.select("primaryTitle", "genres", "averageRating") \
        .orderBy(col("averageRating").desc()) \
        .show(20, truncate=False)
    non_adult_movies.write.mode("overwrite").option("header", "true").csv("output/pavlo.non_adult_horrors")


def top_rated_movies(ratings_df, title_df):
    top_movies = ratings_df.join(title_df, on="tconst").filter(
        (col("averageRating") > 8.0) &
        (col("numVotes") > 10000)
    )
    top_movies.select("primaryTitle", "averageRating", "numVotes").orderBy(col("averageRating").desc()).show(20, truncate=False)
    top_movies.write.mode("overwrite").option("header", "true").csv("output/pavlo.top_rated_movies")


def movies_per_year(title_df):
    yearly_movies = title_df.filter(
        (col("startYear").between(2000, 2024)) &
        (col("titleType") == "movie")
    ).groupBy("startYear").agg(count("*").alias("numMovies"))

    yearly_movies.orderBy(col("startYear").desc()).show(20, truncate=False)
    yearly_movies.write.mode("overwrite").option("header", "true").csv("output/pavlo.movies_per_year")


def actor_average_rating(principals_df, ratings_df, names_df, title_df):

    actors = principals_df.filter(col("category").isin("actor", "actress"))
    movies = title_df.filter(col("titleType") == "movie")

    actor_movies = actors.join(movies, on="tconst").join(ratings_df, on="tconst")

    actor_rating_sum = actor_movies.groupBy("nconst").agg(
        sum(expr("averageRating * numVotes")).alias("weighted_sum"),
        sum("numVotes").alias("total_votes"),
        countDistinct("tconst").alias("num_movies")
    )

    actor_filtered = actor_rating_sum.filter(col("num_movies") >= 20)

    actor_rating_avg = actor_filtered.withColumn(
        "average_rating",
        round(col("weighted_sum") / col("total_votes"), 2)
    )

    actor_with_names = actor_rating_avg.join(
        names_df.select("nconst", "primaryName"),
        on="nconst"
    )

    window_spec = Window.orderBy(col("average_rating").desc())
    ranked = actor_with_names.withColumn("rank", rank().over(window_spec))

    ranked.select("rank", "primaryName", "num_movies", "average_rating", "total_votes") \
        .orderBy("rank") \
        .show(20, truncate=False)
    ranked.write.mode("overwrite").option("header", "true").csv("output/pavlo.actor_average_rating")


def longest_movies_writers(title_df, crew_df, names_df, top_n=20):
    movies = title_df.filter(
        (col("titleType") == "movie") &
        (col("runtimeMinutes").isNotNull())
    ).withColumn("runtimeMinutes", col("runtimeMinutes"))

    movies_with_writers = movies.join(crew_df, on="tconst") \
                                .filter(col("writers").isNotNull() & (col("writers") != "\\N"))

    window_spec = Window.orderBy(col("runtimeMinutes").desc())
    top_movies = movies_with_writers.withColumn("rank", row_number().over(window_spec)) \
                                    .filter(col("rank") <= top_n)

    longest_writers = top_movies.withColumn("writer_id", explode(split(col("writers"), ",")))

    writers_with_names = longest_writers.join(
        names_df.select("nconst", "primaryName"),
        longest_writers["writer_id"] == names_df["nconst"],
        how="left"
    )

    writers_with_names.select(
        "primaryTitle", "runtimeMinutes", "primaryName"
    ).orderBy(col("runtimeMinutes").desc(), col("primaryTitle")).show(20, truncate=False)
    writers_with_names.write.mode("overwrite").option("header", "true").csv("output/pavlo.writers_of_longest_movies")


def underrated_directors(title_df, crew_df, ratings_df, names_df, min_votes=1000, max_movies=5, min_movies=2, top_n=20):
    rated_movies = title_df.filter((col("titleType") == "movie")) \
                           .join(ratings_df, on="tconst") \
                           .filter(col("numVotes") >= min_votes)

    with_directors = rated_movies.join(crew_df, on="tconst") \
                                 .filter(col("directors").isNotNull() & (col("directors") != "\\N")) \
                                 .withColumn("director_id", explode(split(col("directors"), ",")))

    director_stats = with_directors.groupBy("director_id").agg(
        round(avg("averageRating"), 2).alias("avg_rating"),
        count("tconst").alias("num_movies")
    ).filter((col("num_movies") <= max_movies) & (col("num_movies") >= min_movies))

    with_names = director_stats.join(names_df.select("nconst", "primaryName"),
                                     director_stats["director_id"] == names_df["nconst"],
                                     "left")

    window_spec = Window.orderBy(col("avg_rating").desc())
    with_rank = with_names.withColumn("rank", row_number().over(window_spec)) \
                          .filter(col("rank") <= top_n)

    with_rank.select("rank", "primaryName", "avg_rating", "num_movies") \
             .orderBy("rank") \
             .show(20, truncate=False)
    with_rank.write.mode("overwrite").option("header", "true").csv("output/pavlo.underrated_directors")


def call_pavlo_functions(
    title_df, ratings_df, names_df, principals_df, crew_df
):
    print("\nTop Rated non Adult Movies by rating")
    non_adult_horror_movies(title_df, ratings_df)
    print("\n Top Rated Movies with more than 10000 votes")
    top_rated_movies(ratings_df, title_df)
    print("\n Films per year between 2000 and 2024")
    movies_per_year(title_df)
    print("\n Actors average rating")
    actor_average_rating(principals_df, ratings_df, names_df, title_df)
    print("\n Writers of the longest films")
    longest_movies_writers(title_df, crew_df, names_df, top_n=20)
    print("\n Underrated directors")
    underrated_directors(title_df, crew_df, ratings_df, names_df, min_votes=1000, max_movies=5)

