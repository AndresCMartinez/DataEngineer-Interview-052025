class Constants:
    URL = "http://oscars.yipitdata.com/"
    FILM_FINAL_COLS = [
        "film",
        "year",
        "wiki_url",
        "winner",
        "detail_url",
        "budget",
    ]
    CURRENCY_CONVERSION = {"$": 1, "£": 1.32, "₤": 1.2}
    MULTIPLIER_CONVERSION = {"": 1, "million": 1e6, "billion": 1e9}
