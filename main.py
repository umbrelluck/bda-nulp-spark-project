from loader import load_database, test_database

from yevhen import call_yevhen_functions
from yarko import call_yarko_functions


if __name__ == "__main__":
    IMDb_path = "../Dataset"

    print("------- < Loading > -------")
    imdb = load_database(IMDb_path)

    print("------- < Testing > -------")
    test_database(imdb)

    print("------- < YEVHEN > -------")
    call_yevhen_functions(
        imdb["title.basics"],
        imdb["title.ratings"],
        imdb["name.basics"],
        imdb["title.principals"],
        imdb["title.crew"],
        imdb["title.episode"],
    )

    print("------- < YARKO > -------")
    call_yarko_functions(
        imdb["title.basics"],
        imdb["title.akas"],
        imdb["title.ratings"],
        imdb["name.basics"],
        imdb["title.principals"],
        imdb["title.crew"],
    )
