import re
import pandas as pd
import logging

from dataclasses import dataclass
from .constants import Constants

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logger = logging.getLogger("DataCleaningLogger")

@dataclass
class DataCleaning:
    def clean_budget(self, x):
        """
        Cleans the budget column by converting it to a numeric value.

        Args:
            x (str): The budget string to clean.

        Returns:
            float: The cleaned budget value.
        """

        if pd.isna(x):
            return 0

        budget_str = str(x).strip().lower()
        # Takes 3 groups: currency, amount, multiplier - million/billion
        pattern = r"([$₤£])+[\s]*([0-9]+(?:[.|,][0-9]+)?)[\s]*((?:[a-z]+)?)"
        search_matches = re.search(pattern, budget_str)

        if not search_matches:
            return -1  # In case of new data, this will handle unseen cases

        final_budget = (
            float(search_matches.group(2).replace(",", ""))
            * Constants.CURRENCY_CONVERSION[search_matches.group(1)]
            * Constants.MULTIPLIER_CONVERSION[search_matches.group(3)]
        )

        # Limit the budget to 10 million USD
        final_budget = final_budget if final_budget < 1e7 else 1e7
        return final_budget

    def clean_films_data(
        self,
    ) -> pd.DataFrame:
        """
        Cleans the films data by removing unnecessary columns and renaming them.

        Returns:
            pd.DataFrame: The cleaned films data.
        """
        # Read the CSV file
        films_data = pd.read_csv("data/raw_films_data.csv")

        # Clean budget column - base on the found cases in the data_validation.ipynb notebook
        films_data["budget_usd"] = films_data["budget"].apply(self.clean_budget)

        # Clean the year column
        films_data["year"] = (
            films_data["year"]
            .fillna("0")
            .apply(lambda x: re.search(r"([0-9]{4})", str(x)).group(1))
            .astype(str)
        )

        films_data.to_csv("data/stage_films_data.csv", index=False)

        logger.info(
            f"Data cleaning completed successfully. Cleaned data saved to data/stage_films_data.csv\n"
        )
        return films_data
