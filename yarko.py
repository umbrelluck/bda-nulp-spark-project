import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from itertools import combinations


def best_partnership(title_df, names_df, ratings_df, crew_df):
    hits = ratings_df.filter(F.col("averageRating") > 8)
    movies = title_df.filter(F.col("titleType") == "movie")
    hit_movies = movies.join(hits, on="tconst", how="inner")

    hit_crew = (
        hit_movies.join(crew_df, on="tconst", how="inner")
        .withColumn("director", F.explode(F.split(F.col("directors"), ",")))
        .withColumn("writer", F.explode(F.split(F.col("writers"), ",")))
        .filter((F.col("writer").isNotNull()) & (F.col("director").isNotNull()))
    )

    pair_counts = hit_crew.groupBy("writer", "director").agg(
        F.count("*").alias("collaborations")
    )

    windowSpec = Window.partitionBy("writer").orderBy(F.col("collaborations").desc())

    top_collabs = (
        pair_counts.withColumn("rank", F.row_number().over(windowSpec))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    writer_names = names_df.select(
        F.col("nconst").alias("writer_id"), F.col("primaryName").alias("writerName")
    )
    director_names = names_df.select(
        F.col("nconst").alias("director_id"), F.col("primaryName").alias("directorName")
    )

    top_collabs_fixed = top_collabs.withColumnRenamed(
        "writer", "writer_id"
    ).withColumnRenamed("director", "director_id")

    top_collabs_named = top_collabs_fixed.join(
        writer_names, on="writer_id", how="left"
    ).join(director_names, on="director_id", how="left")

    result = (
        top_collabs_named.filter(
            (F.col("writerName").isNotNull())
            & (F.col("directorName").isNotNull())
            & (F.col("writerName") != F.col("directorName"))
        )
        .select("writerName", "directorName", "collaborations")
        .orderBy(F.col("collaborations").desc())
    )

    result.show(20, truncate=False)
    result.write.mode("overwrite").option("header", True).csv(
        "output/yarko.best_partnership"
    )


def most_widespread_films(title_df, title_alt_df):
    akas_with_lang = title_alt_df.filter(
        (F.col("language").isNotNull()) & (F.col("language") != "\\N")
    )

    language_counts = (
        akas_with_lang.groupBy("titleId")
        .agg(F.countDistinct("language").alias("langCount"))
        .orderBy(F.col("langCount").desc())
    )

    top_100 = language_counts.limit(100)

    top_translated = (
        top_100.join(title_df, top_100.titleId == title_df.tconst)
        .select("primaryTitle", "langCount", "startYear", "genres")
        .orderBy(F.col("langCount").desc())
    )

    top_translated.show(20, truncate=False)
    top_translated.write.mode("overwrite").option("header", True).csv(
        "output/yarko.top_translated"
    )


def new_actors_per_year(principals_df, title_df):
    actors_df = principals_df.filter(F.col("category").isin("actor", "actress"))
    title_df = title_df.filter(F.col("titleType") == "movie")

    actors_with_titles = actors_df.join(
        title_df.select("tconst", "startYear"), on="tconst", how="inner"
    )

    actors_with_titles = actors_with_titles.withColumn(
        "startYearInt", F.col("startYear")
    )

    actors_with_titles = actors_with_titles.filter(
        (F.col("startYearInt") >= 1900) & (F.col("startYearInt") <= 2025)
    )

    first_film_df = actors_with_titles.groupBy("nconst").agg(
        F.min("startYearInt").alias("firstYear")
    )

    new_actors_per_year = (
        first_film_df.groupBy("firstYear").count().orderBy("firstYear", ascending=False)
    )

    new_actors_per_year.show(20)
    new_actors_per_year.write.mode("overwrite").option("header", True).csv(
        "output/yarko.new_actors"
    )


def hate_watched(ratings_df, title_df):
    all = title_df.join(ratings_df, on="tconst")

    min_rating = (
        all.filter(F.col("titleType") == "movie")
        .agg(F.min(F.col("averageRating")))
        .collect()[0][0]
    )

    worst_rated = all.filter(
        (F.col("titleType") == "movie")
        & (F.col("averageRating") == min_rating)
        & F.col("numVotes").isNotNull()
    )

    worst_rated = (
        worst_rated.withColumn("numVotes", F.col("numVotes"))
        .orderBy(F.col("numVotes").desc())
        .select("primaryTitle", "startYear", "averageRating", "numVotes")
    )

    worst_rated.show(20, truncate=False)
    worst_rated.write.mode("overwrite").option("header", True).csv(
        "output/yarko.hate_watched"
    )


def combined_genres(title_df):
    films = (
        title_df.filter((F.col("titleType") == "movie") & F.col("genres").isNotNull())
        .withColumn("genre_array", F.array_sort(F.split(F.col("genres"), ",")))
        .filter(F.size(F.col("genre_array")) >= 2)
    )

    def genre_pairs(genres):
        return [sorted(list(pair)) for pair in combinations(genres, 2)]

    genre_pairs_udf = F.udf(genre_pairs, ArrayType(ArrayType(StringType())))

    pairs = films.withColumn(
        "genre_pairs", genre_pairs_udf(F.col("genre_array"))
    ).select(F.explode(F.col("genre_pairs")).alias("pair"))

    result = pairs.select(
        F.col("pair").getItem(0).alias("firstGenre"),
        F.col("pair").getItem(1).alias("secondGenre"),
    )

    pair_counts = (
        result.groupBy("firstGenre", "secondGenre")
        .agg(F.count("*").alias("filmCount"))
        .orderBy(F.col("filmCount").desc())
    )

    pair_counts.show(20, truncate=False)
    pair_counts.write.mode("overwrite").option("header", True).csv(
        "output/yarko.combined_genres"
    )


def best_actors_combined(title_df, principals_df, names_df):
    movies = title_df.filter(F.col("titleType").isin("movie", "tvSeries", "short"))

    actors = principals_df.filter(F.col("category").isin("actor", "actress"))

    actors_in_movies = actors.join(movies.select("tconst"), on="tconst", how="inner")

    actor_pairs = (
        actors_in_movies.alias("a1")
        .join(actors_in_movies.alias("a2"), on="tconst")
        .filter(F.col("a1.nconst") < F.col("a2.nconst"))
        .select(
            F.col("a1.nconst").alias("actor1"),
            F.col("a2.nconst").alias("actor2"),
            F.col("tconst"),
        )
    )

    pair_counts = actor_pairs.groupBy("actor1", "actor2").agg(
        F.count("tconst").alias("shared_projects")
    )

    windowSpec = Window.partitionBy("actor1").orderBy(F.col("shared_projects").desc())

    ranked_pairs = pair_counts.withColumn(
        "rank", F.row_number().over(windowSpec)
    ).filter(F.col("rank") <= 3)

    names1 = names_df.select(
        F.col("nconst").alias("actor1"), F.col("primaryName").alias("actor1Name")
    )
    names2 = names_df.select(
        F.col("nconst").alias("actor2"), F.col("primaryName").alias("actor2Name")
    )

    final = (
        ranked_pairs.join(names1, on="actor1", how="left")
        .join(names2, on="actor2", how="left")
        .filter(F.col("actor1Name").isNotNull() & F.col("actor2Name").isNotNull())
        .filter(F.col("actor1Name") != F.col("actor2Name"))
    )

    final = final.drop("rank")

    final = (
        final.withColumnRenamed("actor1Name", "firstActor")
        .withColumnRenamed("actor2Name", "secondActor")
        .withColumnRenamed("shared_projects", "projects")
        .orderBy(F.col("projects").desc())
    )

    final.select("firstActor", "secondActor", "projects").show(20, truncate=False)
    final.write.mode("overwrite").option("header", True).csv(
        "output/yarko.actors_combined"
    )


def call_yarko_functions(
    title_df, title_alt_df, ratings_df, names_df, principals_df, crew_df
):
    print("Most common partnerships between actor and writers for 8+ films")
    best_partnership(title_df, names_df, ratings_df, crew_df)

    print("Most widespread films")
    most_widespread_films(title_df, title_alt_df)

    print("Ammount on new actors per year")
    new_actors_per_year(principals_df, title_df)

    print("Hate-watched films")
    hate_watched(ratings_df, title_df)

    print("Most common genre combinations")
    combined_genres(title_df)

    print("Common duets to play in film")
    best_actors_combined(title_df, principals_df, names_df)
