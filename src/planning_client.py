import logging
from typing import Any

import requests
from keboola.component.exceptions import UserException


class PlanningClient:
    """Client for Hyperion Planning REST API (port 9000, CSS SSO token auth)."""

    API_PATH = "/HyperionPlanning/rest/v3"

    def __init__(self, base_url: str, username: str, password: str, ssl_verify: bool = True):
        self.base_url = base_url.rstrip("/")
        self.ssl_verify = ssl_verify
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        token = self._login(username, password)
        self.session.headers["Authorization"] = f"Bearer {token}"

    def _login(self, username: str, password: str) -> str:
        url = f"{self.base_url}/interop/logon"
        try:
            resp = requests.post(
                url,
                json={"username": username, "password": password},
                verify=self.ssl_verify,
                timeout=60,
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise UserException(f"Planning authentication failed [{e.response.status_code}]: {e.response.text}") from e
        except requests.ConnectionError as e:
            raise UserException(f"Cannot connect to Planning server at {self.base_url}: {e}") from e

        data = resp.json()
        token = data.get("token") or data.get("userToken")
        if not token:
            raise UserException(f"Planning login succeeded but no token found in response: {data}")
        logging.debug("Planning authentication successful.")
        return token

    def get_applications(self) -> list[dict]:
        return self._get("/applications").get("items", [])

    def import_data_slice(self, application: str, plan_type: str, headers: list[str], rows: list[list]) -> dict:
        """
        POST a batch of data rows to Planning importdataslice endpoint.

        Payload format:
          {"headers": ["Account", "Period", "Value"], "data": [["Sales", "Jan", "1000"], ...]}
        """
        payload = {"headers": headers, "data": rows}
        path = f"/applications/{application}/plantypes/{plan_type}/importdataslice"
        logging.debug("Planning importdataslice: app=%s plan_type=%s rows=%d", application, plan_type, len(rows))
        return self._post(path, payload)

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{self.API_PATH}{path}"
        try:
            resp = self.session.get(url, verify=self.ssl_verify, timeout=300)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            raise UserException(
                f"Planning API error [{e.response.status_code}] on GET {path}: {e.response.text}"
            ) from e
        except requests.ConnectionError as e:
            raise UserException(f"Cannot connect to Planning server at {self.base_url}: {e}") from e

    def _post(self, path: str, payload: dict) -> Any:
        url = f"{self.base_url}{self.API_PATH}{path}"
        try:
            resp = self.session.post(url, json=payload, verify=self.ssl_verify, timeout=300)
            resp.raise_for_status()
            if resp.content:
                return resp.json()
            return {}
        except requests.HTTPError as e:
            raise UserException(
                f"Planning API error [{e.response.status_code}] on POST {path}: {e.response.text}"
            ) from e
        except requests.ConnectionError as e:
            raise UserException(f"Cannot connect to Planning server at {self.base_url}: {e}") from e
