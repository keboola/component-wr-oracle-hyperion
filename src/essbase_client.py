import json
import logging
import time
from typing import Any

import requests
from keboola.component.exceptions import UserException

# Essbase error codes that indicate a transient server-side timeout worth retrying
_RETRYABLE_ESSBASE_CODES = {1042017}
_RETRY_ATTEMPTS = 3
_RETRY_BACKOFF_BASE = 5  # seconds; delay = base * 2^attempt


class EssbaseClient:
    """Client for Essbase 21C REST API (port 9010, Basic Auth)."""

    def __init__(
        self, host: str, port: int, username: str, password: str, use_ssl: bool = True, ssl_verify: bool = True
    ):
        scheme = "https" if use_ssl else "http"
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

    def import_data_slice(
        self,
        application: str,
        database: str,
        headers: list[str],
        rows: list[list],
        context_members: list[str] | None = None,
    ) -> dict:
        """
        Load a batch of data rows into an Essbase BSO database via the 3-step dataload stream API.

        BSO free-form data file format (text/plain):
          <context_member>     ← one line per extra dimension (e.g. "East", "Actual", "Sales")
          col1 col2 col3       ← column members from headers[1:]
          row_member v1 v2 v3  ← headers[0] values; members with spaces are double-quoted

        context_members sets default members for dimensions not represented as columns in the CSV.
        """
        path = f"/applications/{application}/databases/{database}/dataload"
        data_text = self._build_bso_data(headers, rows, context_members or [])
        logging.debug("Essbase dataload: app=%s db=%s rows=%d", application, database, len(rows))

        stream_id = self._start_stream(path)
        try:
            self._push_data(f"{path}/{stream_id}", data_text)
        except Exception:
            self._end_stream(f"{path}/{stream_id}", expect_success=False)
            raise
        return self._end_stream(f"{path}/{stream_id}")

    @staticmethod
    def _quote(member: str) -> str:
        return f'"{member}"' if " " in member else member

    def _build_bso_data(self, headers: list[str], rows: list[list], context_members: list[str]) -> str:
        lines = [self._quote(m) for m in context_members]
        lines.append(" ".join(self._quote(h) for h in headers[1:]))
        for row in rows:
            lines.append(self._quote(str(row[0])) + " " + " ".join(self._quote(str(v)) for v in row[1:]))
        return "\n".join(lines) + "\n"

    def _start_stream(self, path: str) -> str:
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.post(url, json={}, verify=self.ssl_verify, timeout=60)
            resp.raise_for_status()
            return resp.json()["streamId"]
        except (requests.HTTPError, requests.ConnectionError, KeyError) as e:
            raise UserException(f"Essbase dataload: failed to start stream for {path}: {e}") from e

    def _push_data(self, path: str, data: str) -> None:
        url = f"{self.base_url}{path}"
        try:
            resp = self.session.post(
                url, data=data.encode(), headers={"Content-Type": "text/plain"}, verify=self.ssl_verify, timeout=300
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise UserException(f"Essbase dataload: push failed [{e.response.status_code}]: {e.response.text}") from e
        except requests.ConnectionError as e:
            raise UserException(f"Essbase dataload: push connection error: {e}") from e

    def _end_stream(self, path: str, expect_success: bool = True) -> dict:
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                resp = self.session.delete(url, verify=self.ssl_verify, timeout=60)
                resp.raise_for_status()
                result = resp.json() if resp.content else {}
                if expect_success and result.get("errorMessage"):
                    raise UserException(f"Essbase dataload failed: {result['errorMessage']}")
                return result
            except requests.HTTPError as e:
                if self._is_retryable(e) and attempt < _RETRY_ATTEMPTS - 1:
                    delay = _RETRY_BACKOFF_BASE * (2**attempt)
                    logging.warning(
                        "Essbase timeout on finalize %s (attempt %d/%d), retrying in %ds…",
                        path,
                        attempt + 1,
                        _RETRY_ATTEMPTS,
                        delay,
                    )
                    time.sleep(delay)
                    last_exc = e
                    continue
                raise UserException(
                    f"Essbase dataload: finalize failed [{e.response.status_code}]: {e.response.text}"
                ) from e
            except requests.ConnectionError as e:
                raise UserException(f"Essbase dataload: finalize connection error: {e}") from e
        raise UserException(f"Essbase finalize {path} failed after {_RETRY_ATTEMPTS} attempts") from last_exc

    def _get(self, path: str) -> Any:
        url = f"{self.base_url}{path}"
        last_exc: Exception | None = None
        for attempt in range(_RETRY_ATTEMPTS):
            try:
                resp = self.session.get(url, verify=self.ssl_verify, timeout=300)
                resp.raise_for_status()
                return resp.json()
            except requests.HTTPError as e:
                if self._is_retryable(e) and attempt < _RETRY_ATTEMPTS - 1:
                    delay = _RETRY_BACKOFF_BASE * (2**attempt)
                    logging.warning(
                        "Essbase timeout on GET %s (attempt %d/%d), retrying in %ds…",
                        path,
                        attempt + 1,
                        _RETRY_ATTEMPTS,
                        delay,
                    )
                    time.sleep(delay)
                    last_exc = e
                    continue
                raise UserException(
                    f"Essbase API error [{e.response.status_code}] on GET {path}: {e.response.text}"
                ) from e
            except requests.ConnectionError as e:
                raise UserException(f"Cannot connect to Essbase at {self.base_url}: {e}") from e
        raise UserException(f"Essbase GET {path} failed after {_RETRY_ATTEMPTS} attempts") from last_exc

    @staticmethod
    def _is_retryable(e: requests.HTTPError) -> bool:
        """Return True if the Essbase error code indicates a transient timeout."""
        try:
            body = json.loads(e.response.text)
            return body.get("errorCode") in _RETRYABLE_ESSBASE_CODES
        except (ValueError, AttributeError):
            return False
