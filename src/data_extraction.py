from dataclasses import dataclass
import pandas as pd
import logging
from .rest_calls import RestSyncCalls, RestAsyncCalls

from .constants import Constants

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("DataExtractionLogger")

@dataclass
class DataExtraction:
    def get_films_data(self) -> pd.DataFrame:
        """
        Fetches the films data from the Yipit API and returns it as a pandas DataFrame.

        Returns:
            pd.DataFrame: The Yipit dataset.
        """
        request_sync = RestSyncCalls(Constants.URL)

        # Fetch the data from the Yipit API
        response_data: dict = request_sync.get_data()

        # Denormalize the JSON data
        films_json: pd.DataFrame = (
            pd.DataFrame(response_data["results"])
            .explode("films")
            .reset_index(drop=True)
        )

        films = pd.concat(
            [
                pd.json_normalize(films_json["films"]),
                films_json.drop(columns=["films"]),
            ],
            axis=1,
        )

        request_async = RestAsyncCalls(films["Detail URL"].tolist())

        # Extract films extra details from the detail_url asyncronously
        # - passing sync calls from 3min to 1.12sec asyncronously
        films["film_extra_details_async"] = (
            request_async.run_async_process()
        )  # 1.12sec

        # Denormalize the additional details
        films_details = pd.concat(
            [
                films.drop(columns=["film_extra_details_async"]),
                pd.json_normalize(films["film_extra_details_async"]),
            ],
            axis=1,
        )

        # Clean and filter the column names
        films_details.columns = [
            i.strip().replace(" ", "_").lower() for i in films_details.columns
        ]

        films_details = films_details[Constants.FILM_FINAL_COLS]

        # Save the final DataFrame to a CSV file
        films_details.to_csv("data/raw_films_data.csv", index=False)

        logger.info("Data Extraction completed successfully. Data saved to data/raw_films_data.csv\n")
        return films_details