"""
Oracle Hyperion EPM Writer — main component.

Reads CSV tables from Keboola Storage and imports them into
Essbase (21C REST API) via the dataload streaming endpoint.
"""

import csv
import logging
from collections.abc import Iterable, Iterator
from itertools import islice
from pathlib import Path

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import ColumnMapping, Configuration
from essbase_client import EssbaseClient


def _batched[T](iterable: Iterable[T], size: int) -> Iterator[list[T]]:
    """Yield successive lists of at most `size` items from iterable."""
    it = iter(iterable)
    while batch := list(islice(it, size)):
        yield batch


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)
        try:
            self.client = self._build_essbase_client()
        except UserException:
            self.client = None

    def run(self) -> None:
        if (
            not self.params.destination
            or not self.params.destination.application
            or not self.params.destination.database
        ):
            raise UserException(
                "Configuration error: 'destination.application' and 'destination.database' are required."
            )
        if not self.params.source:
            raise UserException("Configuration error: 'source' is required for the run action.")
        if not self.client:
            self.client = self._build_essbase_client()
        logging.info("Processing: app=%s db=%s", self.params.destination.application, self.params.destination.database)
        version = self.client.verify_connection()
        logging.info("Connected to Essbase version: %s", version)
        self._write_essbase(self.client)

    @sync_action("listApplications")
    def list_applications(self) -> list[SelectElement]:
        """Return all Essbase applications for the destination dropdown."""
        if not self.client:
            self.client = self._build_essbase_client()
        apps = self.client.get_applications()
        return [SelectElement(value=app["name"], label=app.get("alias") or app["name"]) for app in apps]

    @sync_action("listDatabases")
    def list_databases(self) -> list[SelectElement]:
        """Return databases for the selected application."""
        application = self.params.destination.application if self.params.destination else ""
        if not application:
            raise UserException("Select or enter an Application name first, then reload databases.")
        if not self.client:
            self.client = self._build_essbase_client()
        databases = self.client.get_databases(application)
        return [SelectElement(value=db["name"], label=db.get("alias") or db["name"]) for db in databases]

    @staticmethod
    def _normalize(name: str) -> str:
        """Normalize a name for fuzzy matching: strip separators and lowercase."""
        return name.replace("_", "").replace("-", "").replace(" ", "").replace(".", "").lower()

    @classmethod
    def _fuzzy_match(cls, value: str, candidates: set[str]) -> str:
        """Return the best matching candidate for value, or '' if none found.

        Priority:
        1. Exact match
        2. Case-insensitive match
        3. Normalized match (remove _, -, spaces, dots then lowercase)
        """
        if value in candidates:
            return value
        value_lower = value.lower()
        for candidate in candidates:
            if candidate.lower() == value_lower:
                return candidate
        value_norm = cls._normalize(value)
        for candidate in candidates:
            if cls._normalize(candidate) == value_norm:
                return candidate
        return ""

    @sync_action("loadColumnMapping")
    def load_column_mapping(self) -> dict:
        """Pre-fill column mapping with fuzzy-matched Essbase members via MDX."""
        application = self.params.destination.application if self.params.destination else ""
        database = self.params.destination.database if self.params.destination else ""
        if not application or not database:
            raise UserException("Select an Application and Database/Cube first.")

        if not self.client:
            self.client = self._build_essbase_client()

        csv_columns = self._get_input_table_columns()
        dimensions = self.client.get_dimensions(application, database)

        # Fetch all members for each dimension via MDX
        all_members: list[dict] = []
        all_member_names: set[str] = set()
        for dim in dimensions:
            try:
                members = self.client.get_dimension_members_mdx(application, database, dim)
            except UserException:
                members = []
            for m in members:
                all_members.append({"value": m, "label": f"{dim}: {m}"})
                all_member_names.add(m)
        # Also include dimension names as valid targets
        for dim in dimensions:
            all_members.append({"value": dim, "label": dim})
            all_member_names.add(dim)

        source = self.params.source
        existing_by_source = {cm.source: cm.destination for cm in (source.column_mapping if source else [])}

        new_mapping = []
        for col in csv_columns:
            if col in existing_by_source:
                destination = existing_by_source[col]
            else:
                destination = self._fuzzy_match(col, all_member_names)
            new_mapping.append({"source": col, "destination": destination})

        source_dict = source.model_dump() if source else {}
        return {
            "type": "data",
            "data": {
                "source": {**source_dict, "column_mapping": new_mapping},
                "_metadata_": {"essbase_members": all_members},
            },
        }

    @sync_action("loadContextMembers")
    def load_context_members(self) -> dict:
        """Populate context members with base Essbase dimensions not covered by input table columns."""
        application = self.params.destination.application if self.params.destination else ""
        database = self.params.destination.database if self.params.destination else ""
        if not application or not database:
            raise UserException("Select an Application and Database/Cube first.")

        if not self.client:
            self.client = self._build_essbase_client()
        dimensions = self.client.get_dimensions(application, database)
        if not dimensions:
            raise UserException(f"No dimensions found in {application}/{database}.")

        try:
            csv_columns = set(self._get_input_table_columns())
        except UserException:
            csv_columns = set()
        unmatched = [dim for dim in dimensions if dim not in csv_columns]

        source = self.params.source
        existing_by_dim = {
            cm.dimension: {"dimension": cm.dimension, "member": cm.member}
            for cm in (source.context_members if source else [])
        }
        new_context = [existing_by_dim.get(dim, {"dimension": dim, "member": ""}) for dim in unmatched]
        source_dict = source.model_dump() if source else {}
        return {"type": "data", "data": {"source": {**source_dict, "context_members": new_context}}}

    def _get_input_table_columns(self) -> list[str]:
        """Return column names for the configured input table.

        Tries three sources in order:
        1. tables_input_mapping with columns already declared (no API call needed)
        2. Storage API using table.source from tables_input_mapping
        3. Storage API using the table source from the raw storage config
           (fallback for sync actions where files are not mounted)
        """
        input_table_name = self.params.source.input_table if self.params.source else ""
        tables = self.configuration.tables_input_mapping

        table_id: str | None = None

        if tables:
            if input_table_name:
                table = next((t for t in tables if Path(t.full_path).stem == input_table_name), None)
                if table is None:
                    raise UserException(f"Input table '{input_table_name}' not found in the storage mapping.")
            else:
                table = tables[0]

            if table.columns:
                return list(table.columns)

            table_id = table.source

        if not table_id:
            # sync action — files not mounted, read table ID from raw storage config
            raw_tables = self.configuration.config_data.get("storage", {}).get("input", {}).get("tables", [])
            if not raw_tables:
                raise UserException("No input table found. Add an input table in the storage settings first.")
            table_id = raw_tables[0].get("source")

        token = self.environment_variables.token
        url = self.environment_variables.url
        if not token or not url:
            raise UserException("Storage API token not available. Ensure forward_token is enabled for this component.")

        try:
            resp = requests.get(
                f"{url}/v2/storage/tables/{table_id}",
                headers={"X-StorageApi-Token": token},
                timeout=30,
            )
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise UserException(f"Storage API error fetching table columns: {e}") from e
        except (requests.Timeout, requests.ConnectionError) as e:
            raise UserException(f"Storage API connection error fetching table columns: {e}") from e
        return resp.json().get("columns", [])

    @sync_action("testConnection")
    def test_connection(self) -> dict:
        """Sync action: verify Essbase connectivity and return version."""
        if not self.client:
            self.client = self._build_essbase_client()
        version = self.client.verify_connection()
        return {"status": "success", "message": f"Connected to Essbase {version}"}

    def _build_essbase_client(self) -> EssbaseClient:
        if not self.params.essbase_host and not self.params.hyperion_url:
            raise UserException("Connection settings require either 'essbase_host' or 'hyperion_url'.")
        if not self.params.username or not self.params.password:
            raise UserException("Connection settings require 'username' and 'password'.")
        host = self.params.get_essbase_host()
        logging.info("Connecting to Essbase at %s:%d", host, self.params.essbase_port)
        return EssbaseClient(
            host=host,
            port=self.params.essbase_port,
            username=self.params.username,
            password=self.params.password,
            use_ssl=self.params.use_ssl,
            ssl_verify=self.params.ssl_verify,
        )

    def _write_essbase(self, client: EssbaseClient) -> None:
        headers, data_rows = self._read_table(self.params.source.input_table)
        if not headers:
            logging.warning("Input table '%s' is empty — skipping.", self.params.source.input_table)
            return

        if self.params.source.column_mapping:
            headers, data_rows = self._apply_column_mapping(headers, data_rows, self.params.source.column_mapping)
            logging.info("Applied column mapping: %d column(s) -> %s", len(headers), headers)

        total = 0
        for batch in _batched(data_rows, self.params.source.batch_size):
            client.import_data_slice(
                self.params.destination.application,
                self.params.destination.database,
                headers,
                batch,
                context_members=[cm.member for cm in self.params.source.context_members if cm.member],
            )
            total += len(batch)
            logging.info(
                "Essbase %s/%s: imported %d rows so far.",
                self.params.destination.application,
                self.params.destination.database,
                total,
            )
        if total == 0:
            logging.warning("Input table '%s' had no data rows — nothing written.", self.params.source.input_table)
        else:
            logging.info(
                "Essbase write complete: %d total rows to %s/%s.",
                total,
                self.params.destination.application,
                self.params.destination.database,
            )

    @staticmethod
    def _apply_column_mapping(
        headers: list[str], rows: Iterator[list], mapping: list[ColumnMapping]
    ) -> tuple[list[str], Iterator[list]]:
        """Filter and rename columns according to mapping; drops unmapped columns."""
        col_map = {cm.source: cm.destination for cm in mapping}
        mapped = [(i, col_map[h]) for i, h in enumerate(headers) if h in col_map]
        if not mapped:
            logging.warning(
                "Column mapping is configured but no source columns matched. "
                "All %d original columns will pass through unchanged.",
                len(headers),
            )
            return headers, rows
        new_headers = [dest for _, dest in mapped]
        indices = [i for i, _ in mapped]
        return new_headers, ([row[i] for i in indices] for row in rows)

    def _read_table(self, input_table: str = "") -> tuple[list[str], Iterator[list]]:
        """
        Read an input table from Keboola Storage and return (headers, row_iterator).
        Rows are streamed lazily to avoid loading the entire table into memory.
        If input_table is set, selects the table whose filename stem matches; otherwise uses the first.
        """
        all_tables = self.get_input_tables_definitions()
        if not all_tables:
            raise UserException("No input table found in the input mapping.")
        if input_table:
            table = next((t for t in all_tables if Path(t.full_path).stem == input_table), None)
            if table is None:
                raise UserException(f"Input table '{input_table}' not found in the input mapping.")
        else:
            table = all_tables[0]

        with open(table.full_path, newline="", encoding="utf-8") as f:
            headers = next(csv.reader(f), [])

        def _row_iter(path: str) -> Iterator[list]:
            with open(path, newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                next(reader, None)  # skip header
                yield from reader

        return headers, _row_iter(table.full_path)


if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
