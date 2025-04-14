def analyze(imdb):
    print("Number of records")
    print("title.basics:", imdb["title.basics"].count())
    print("title.akas:", imdb["title.akas"].count())
    print("title.ratings:", imdb["title.ratings"].count())
    print("name.basics:", imdb["name.basics"].count())
    print("title.principals:", imdb["title.principals"].count())
    print("title.crew:", imdb["title.crew"].count())

    for name in [
        "title.basics", "title.akas", "title.ratings",
        "name.basics", "title.principals", "title.crew"
    ]:
        df = imdb[name]
        print(f"=== {name} ===")
        print(f"Number of rows: {df.count()}")
        print(f"Number of columns: {len(df.columns)}")
        df.printSchema()
        print()

    print("Numeric column statistics")
    for name in [
        "title.basics", "title.akas", "title.ratings",
        "name.basics", "title.principals", "title.crew"
    ]:
        df = imdb[name]

        numeric_columns = [col for col, dtype in df.dtypes if dtype in ['int', 'double']]

        if numeric_columns:
            print(f"=== Statistics for numeric columns in table {name} ===")
            df.select(numeric_columns).describe().show()
        else:
            print(f"=== Table {name} has no numeric columns ===")

        print()
