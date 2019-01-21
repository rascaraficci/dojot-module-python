import requests
import time
from .logger import Log

LOGGER = Log().color_log()

class HttpRequester:
    @staticmethod
    def do_it(url, token, retry_counter, timeout_sleep):
        payload = None
        while retry_counter > 0:
            try:
                LOGGER.debug("Retrieving data from HTTP endpoint...")
                LOGGER.debug(f"Remaining attempts: {retry_counter}")
                ret = requests.get(
                    url, headers={'authorization': f"Bearer {token}"})

                LOGGER.debug(f"Got an answer from HTTP endpoint.")
                if ret.status_code is not 200:
                    LOGGER.warning(f"Auth returned a non-200 response")
                    LOGGER.warning(f"Code is {ret.status_code}.")
                    LOGGER.warning(f"Message is {ret.text}")
                else:
                    payload = ret.json()
                    LOGGER.debug("... data retrieved from endpoint.")

                retry_counter = 0
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as connection_error:
                LOGGER.warning(
                    f"Connection error while retrieving data: {connection_error}")
                LOGGER.warning(f"Sleeping for {timeout_sleep} before trying again.")
                time.sleep(timeout_sleep)
                LOGGER.warning("Retrying...")
                retry_counter -= 1
            except requests.exceptions.TooManyRedirects as redirects_error:
                # Nothing we can do about this.
                LOGGER.error(
                    f"Received too many redirects while retrieving data: {redirects_error}")
                retry_counter = 0
            except ValueError as value_error:
                LOGGER.error(f"Endpoint returned an invalid JSON: {value_error}")
                retry_counter = 0

        if payload is None:
            LOGGER.warning("Data request payload is still None.")
            return None

        return payload
