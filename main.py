import logging

from src.data_extraction import DataExtraction
from src.data_cleaning import DataCleaning

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger("FilmDataLogger")

if __name__ == "__main__":
    DataExtraction().get_films_data()  # Data Extraction
    DataCleaning().clean_films_data()  # Data Cleaning

    logger.info("Data Extraction and Cleaning completed successfully.")
