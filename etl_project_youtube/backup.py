import requests  # API calls
import json      # JSON handling
import time      # For retry backoff
import logging   # Structured logging
from typing import Optional, Dict, Any, List  # Type hints for clarity

# Setup logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log")
    ]
)


# API ingestion class

class APIIngestor:
    def __init__(self, base_url: str, max_retries: int = 5, backoff_factor: float = 0.5):
        """
        Initialize the API ingestor.

        Args:
            base_url (str): Base URL of the API.
            max_retries (int): Number of retries for failed requests.
            backoff_factor (float): Backoff multiplier for retries.
        """
        self.base_url = base_url
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def _make_request(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict:
        """
        Make a GET request with retry logic.

        Args:
            url (str): Full API endpoint URL.
            params (dict, optional): Query parameters.

        Returns:
            dict: Parsed JSON response.
        """
        retries = 0
        while retries < self.max_retries:
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                else:
                    logging.warning(f"Request failed [{response.status_code}]: {response.text}")
            except requests.RequestException as e:
                logging.warning(f"Request exception: {e}")

            retries += 1
            sleep_time = self.backoff_factor * (2 ** (retries - 1))
            logging.info(f"Retrying in {sleep_time:.1f}s...")
            time.sleep(sleep_time)

        raise Exception(f"Failed to fetch data from {url} after {self.max_retries} retries")

    #fetch_paginated → “I want all users, but I can only carry 100 at a time.”

    def fetch_paginated(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        page_param: str = "page",
        per_page: int = 100,
        max_pages: Optional[int] = None
    ) -> List[Dict]:
        """
        Fetch data from APIs that paginate results.

        Args:
            endpoint (str): API endpoint path.
            params (dict, optional): Query parameters.
            page_param (str): Parameter name for page number.
            per_page (int): Number of results per page.
            max_pages (int, optional): Maximum number of pages to fetch.

        Returns:
            List[Dict]: List of all records fetched.
        """
        results = []
        page = 1

        while True:
            paged_params = params.copy() if params else {}
            paged_params.update({page_param: page, "results": per_page})

            logging.info(f"Fetching page {page} from {endpoint}")
            data = self._make_request(f"{self.base_url}{endpoint}", paged_params)

            items = data.get("results") if isinstance(data, dict) and "results" in data else data
            if not items:
                break

            results.extend(items)
            if max_pages and page >= max_pages:
                break
            if len(items) < per_page:
                break

            page += 1

        return results
    

    #fetch_incremental → “I want only the users added or updated since yesterday.”

    def fetch_incremental(
        self,
        endpoint: str,
        since: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> List[Dict]:
        """
        Fetch only new or updated records since a given timestamp or ID.

        Args:
            endpoint (str): API endpoint path.
            since (str, optional): Timestamp or ID to fetch incremental data from.
            params (dict, optional): Additional query parameters.

        Returns:
            List[Dict]: Incremental data.
        """
        query_params = params.copy() if params else {}
        if since:
            query_params["since"] = since

        logging.info(f"Fetching incremental data from {endpoint} since {since}")
        data = self._make_request(f"{self.base_url}{endpoint}", query_params)
        return data.get("results") if isinstance(data, dict) and "results" in data else data

    def batch_process(self, data: List[Dict], batch_size: int = 50):
        """
        Yield data in batches.

        Args:
            data (List[Dict]): List of records.
            batch_size (int): Size of each batch.

        Yields:
            List[Dict]: Batch of records.
        """
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]



##Instantiting the Class

if __name__ == "__main__":                       #This line ensures the code only runs when the script is executed directly, not when imported as a module in another script.
   
    ingestor = APIIngestor(base_url="https://data.police.uk/api/")

    # Example: Fetch multiple pages from Police UK API
    all_crimes = ingestor.fetch_paginated(
        endpoint="/api/crimes-street/all-crime",
        params = {
    "date": "2025-07",
    "lat": 52.629729,
    "lng": -1.131592},
        per_page=200,
        max_pages=20
    )

    logging.info(f"Total users fetched: {len(all_crimes)}")

    # Process in batches
    for batch in ingestor.batch_process(all_crimes, batch_size=100):
        logging.info(f"Processing batch of {len(batch)} crimes")
        logging.info(f"Batch type: {type(batch)} | Sample: {batch[:2] if isinstance(batch, list) else batch}")
        logging.info(batch[1]["location"]["street"]["name"])



#Light Transformation

