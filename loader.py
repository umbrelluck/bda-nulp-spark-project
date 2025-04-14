from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os

title_basics_schema = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", StringType(), True),
        StructField("endYear", StringType(), True),
        StructField("runtimeMinutes", StringType(), True),
        StructField("genres", StringType(), True),
    ]
)

title_ratings_schema = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("averageRating", StringType(), True),
        StructField("numVotes", IntegerType(), True),
    ]
)

title_akas_schema = StructType(
    [
        StructField("titleId", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", IntegerType(), True),
    ]
)

title_crew_schema = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True),
    ]
)

title_principals_schema = StructType(
    [
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True),
    ]
)

name_basics_schema = StructType(
    [
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", StringType(), True),
        StructField("deathYear", StringType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True),
    ]
)

schemas = {
    "title.basics.tsv": title_basics_schema,
    "title.ratings.tsv": title_ratings_schema,
    "title.akas.tsv": title_akas_schema,
    "title.crew.tsv": title_crew_schema,
    "title.principals.tsv": title_principals_schema,
    "name.basics.tsv": name_basics_schema,
}


def load_database(directory: str) -> dict:
    spark = (
        SparkSession.builder.appName("IMDb BDA Project")
        .config("spark.sql.repl.eagerEval.enabled", True)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "100")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    imdb_dfs = {}

    for file_name, schema in schemas.items():
        file_path = os.path.join(directory, file_name)

        if os.path.exists(file_path):
            df = spark.read.csv(
                file_path, sep="\t", header=True, schema=schema, nullValue="\\N"
            )
            table_name = file_name.replace(".tsv", "")
            imdb_dfs[table_name] = df
        else:
            print(f"File not found: {file_name}, moving on...")

    return imdb_dfs


def test_database(dataframes: dict):
    for name, df in dataframes.items():
        print(f"-- {name} --")
        df.show(5)
