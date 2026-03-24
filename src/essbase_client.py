import logging
from typing import Any

import requests
from keboola.component.exceptions import UserException


class EssbaseClient:
    """Client for Essbase 21C REST API (port 9010, Basic Auth)."""

    def __init__(self, host: str, port: int, username: str, password: str, ssl_verify: bool = True):
        scheme = "https" if ssl_verify else "http"
        self.base_url = f"{scheme}://{host}:{port}/essbase/rest/v1"
        self.ssl_verify = ssl_verify
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers.update({"Content-Type": "application/json"})

    def verify_connection(self) -> str:
        """Return Essbase version string — use to verify connectivity."""
        data = self._get("/about")
        return data.get("version", "unknown")

    def get_applications(self) -> list[dict]:
        return self._get("/applications").get("items", [])

    def get_databases(self, application: str) -> list[dict]:
        return self._get(f"/applications/{application}/databases").get("items", [])

    def import_data_slice(self, application: str, database: str, headers: list[str], rows: list[list]) -> dict:
        """
        POST a batch of data rows to Essbase importdataslice endpoint.

        Payload format:
          {"headers": ["Product", "Jan", "Feb"], "data": [["Cola", "1710", "1666"], ...]}
        """
        payload = {"headers": headers, "data": rows}
        path = f"/applications/{application}/databases/{database}/importdataslice"
        logging.debug("Essbase importdataslice: app=%s db=%s rows=%d", application, database, len(rows))
        return self._post(path, payload)

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.get(url, verify=self.ssl_verify, timeout=300)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            raise UserException(f"Essbase API error [{e.response.status_code}] on GET {path}: {e.response.text}") from e
        except requests.ConnectionError as e:
            raise UserException(f"Cannot connect to Essbase at {self.base_url}: {e}") from e

    def _post(self, path: str, payload: dict) -> Any:
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.post(url, json=payload, verify=self.ssl_verify, timeout=300)
            resp.raise_for_status()
            if resp.content:
                return resp.json()
            return {}
        except requests.HTTPError as e:
            raise UserException(
                f"Essbase API error [{e.response.status_code}] on POST {path}: {e.response.text}"
            ) from e
        except requests.ConnectionError as e:
            raise UserException(f"Cannot connect to Essbase at {self.base_url}: {e}") from e
