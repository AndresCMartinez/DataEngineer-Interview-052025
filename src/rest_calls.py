from dataclasses import dataclass
import aiohttp
import asyncio
import requests
import time
import logging

from typing import List

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)

logger = logging.getLogger("AsyncLogger")


@dataclass
class RestSyncCalls:
    url: str

    def get_data(self) -> dict:
        """
        Fetches the Yipit dataset Syncronously from the given URL.

        Args:
            None

        Returns:
            dict: The data from the URL, as a dictionary.  Returns an empty dict on final failure.
        """
        response = requests.get(self.url)
        max_tries = 5
        wait_time = 0

        while max_tries > 0:
            try:
                if response.status_code == 403:
                    return {}  # 403 Forbidden
                elif response.status_code != 200:
                    raise Exception(
                        f"Failed to fetch data: {response.status_code}, trying again in {wait_time} seconds..."
                    )

                data = response.json()
                return data

            except Exception as e:
                wait_time = 0.5 * (6 - max_tries)
                time.sleep(wait_time)
                max_tries -= 1
                logger.warning(
                    f"Error: {e} - {self.url} - {max_tries} attempts left. Waiting for {wait_time} seconds..."
                )

        logger.info(
            f"Failed to fetch data after {max_tries} attempts.  Returning empty dict."
        )
        return {}


@dataclass
class RestAsyncCalls:
    urls: List[str]

    async def print_logs(
        self,
        pos: int,
        url: str,
        status: str,
        start_time: float,
    ):
        """
        Prints the logs for the process.

        Args:
            pos: int
                position of the process
            url: str
                url of the process
            status: str
                status of the process
            start_time: float
                start time of the process

        Returns:
            None
        """
        time_elapse = time.time() - start_time
        _endpoint = "/".join(url.split("/")[3:])
        total_urls = len(self.urls)
        logger.info(
            f"\n{round(((pos + 1) / total_urls) * 100)}% --- Time elapsed: "
            f"{int(time_elapse // 60)}min {round(time_elapse % 60, 2)}sec"
            f"--- User #{pos + 1} / {total_urls}, "
            f"--- Status: {status}', --- Endpoint: {_endpoint}\n"
        )

    async def async_request(
        self, session, url: str, pos: int, start_time
    ) -> dict:
        """
        Asynchronously fetches data from a remote URL and returns it as a dictionary.

        Args:
            url (str): The URL to fetch data from.
            pos (int): The position of the URL in the list.
            start_time (float): The start time of the process.

        Returns:
            dict: The data from the URL, as a dictionary.  Returns an empty dict on final failure.
        """
        max_tries = 5

        for _retry in range(max_tries):
            try:
                async with session.get(url) as response:
                    _status = int(response.status)
                    if _status == 403:
                        return {}  # 403 Manual Forbidden
                    elif _status == 200:
                        data = await response.json()
                        await self.print_logs(pos, url, _status, start_time)
                        return data
                    raise Exception(f"Failed to fetch data: {_status}")
            except Exception as e:
                logger.warning(f"Error: {e} - {url} - {_retry + 1}/{max_tries}")
                wait_time = 0.5 * (_retry + 1)
                await asyncio.sleep(wait_time)

        print(
            f"Failed to fetch data after {max_tries} attempts.  Returning empty dict."
        )

    async def get_data_async(self, start_time) -> dict:
        """
        Asynchronously fetches data from a list of URLs and returns it as a dictionary.

        Args:
            start_time (float): The start time of the process.

        Returns:
            dict: The data from the URL, as a dictionary.  Returns an empty dict on final failure.
        """
        async with aiohttp.ClientSession() as session:
            tasks = [
                self.async_request(session, self.urls[_pos], _pos, start_time)
                for _pos in range(len(self.urls))
            ]
            responses = await asyncio.gather(*tasks)
            return responses

        return {}

    def run_async_process(self):
        """
        Runs the async process to fetch data from the given endpoints.

        Args:
            None

        Returns:
            list: The list of tasks fetched from the endpoints.
        """
        _start_time = time.time()
        responses = asyncio.run(self.get_data_async(_start_time))
        _total_time = round(time.time() - _start_time, 2)

        logger.info(
            f"*** Async Function Time Last: {int(_total_time // 60)}min {round(_total_time % 60, 2)}sec ***\n"
        )

        return responses
