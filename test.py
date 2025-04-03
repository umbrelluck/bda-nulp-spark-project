import marimo

__generated_with = "0.12.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[*]").appName("Test").getOrCreate()
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    df.show()
    return SparkSession, df, spark


if __name__ == "__main__":
    app.run()
