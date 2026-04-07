"""
Oracle Hyperion EPM Writer — main component.

Reads CSV tables from Keboola Storage and imports them into
Essbase (21C REST API) via the dataload streaming endpoint.
"""

import csv
import logging
from collections.abc import Iterator
from itertools import islice
from pathlib import Path

import requests
from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import ColumnMapping, Configuration
from essbase_client import EssbaseClient


def _batched(iterable, size: int):
    """Yield successive lists of at most `size` items from iterable."""
    it = iter(iterable)
    while batch := list(islice(it, size)):
        yield batch


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.params = Configuration(**self.configuration.parameters)

    def run(self):
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
        logging.info("Processing: app=%s db=%s", self.params.destination.application, self.params.destination.database)
        client = self._build_essbase_client()
        version = client.verify_connection()
        logging.info("Connected to Essbase version: %s", version)
        self._write_essbase(client)

    @sync_action("listApplications")
    def list_applications(self) -> list[SelectElement]:
        """Return all Essbase applications for the destination dropdown."""
        client = self._build_essbase_client()
        apps = client.get_applications()
        return [SelectElement(value=app["name"], label=app.get("alias") or app["name"]) for app in apps]

    @sync_action("listDatabases")
    def list_databases(self) -> list[SelectElement]:
        """Return databases for the selected application."""
        destination = self.configuration.parameters.get("destination", {})
        application = destination.get("application", "")
        if not application:
            raise UserException("Select or enter an Application name first, then reload databases.")
        client = self._build_essbase_client()
        databases = client.get_databases(application)
        return [SelectElement(value=db["name"], label=db.get("alias") or db["name"]) for db in databases]

    @sync_action("listMembers")
    def list_members(self) -> list[SelectElement]:
        """Return top-level members from all dimensions for the context members dropdown."""
        destination = self.configuration.parameters.get("destination", {})
        application = destination.get("application", "")
        database = destination.get("database", "")
        if not application or not database:
            raise UserException("Select an Application and Database/Cube first.")
        client = self._build_essbase_client()
        dimensions = client.get_dimensions(application, database)
        elements = []
        for dim in dimensions:
            members = client.get_top_level_members(application, database, dim)
            for member in members:
                elements.append(SelectElement(value=member, label=f"{member} ({dim})"))
        return elements

    @sync_action("loadColumnMapping")
    def load_column_mapping(self) -> dict:
        """Pre-fill column mapping rows from the linked input table's Storage columns."""
        csv_columns = self._get_input_table_columns()
        current_source = self.configuration.parameters.get("source", {})
        existing_by_source = {m["source"]: m for m in current_source.get("column_mapping", []) if isinstance(m, dict)}
        new_mapping = [existing_by_source.get(col, {"source": col, "destination": col}) for col in csv_columns]
        return {"type": "data", "data": {"source": {**current_source, "column_mapping": new_mapping}}}

    @sync_action("loadContextMembers")
    def load_context_members(self) -> dict:
        """Populate context members with base Essbase dimensions not covered by input table columns."""
        destination = self.configuration.parameters.get("destination", {})
        application = destination.get("application", "")
        database = destination.get("database", "")
        if not application or not database:
            raise UserException("Select an Application and Database/Cube first.")

        client = self._build_essbase_client()
        dimensions = client.get_dimensions(application, database)
        if not dimensions:
            raise UserException(f"No dimensions found in {application}/{database}.")

        csv_columns = set(self._get_input_table_columns())
        unmatched = [dim for dim in dimensions if dim not in csv_columns]

        current_source = self.configuration.parameters.get("source", {})
        existing_by_dim = {
            cm["dimension"]: cm for cm in current_source.get("context_members", []) if isinstance(cm, dict)
        }
        new_context = [existing_by_dim.get(dim, {"dimension": dim, "member": ""}) for dim in unmatched]
        return {"type": "data", "data": {"source": {**current_source, "context_members": new_context}}}

    def _get_input_table_columns(self) -> list[str]:
        """Return column names for the configured input table.

        Uses ``self.configuration.tables_input_mapping`` (available in sync actions
        even with partial config).  If the mapping already declares columns they are
        returned directly; otherwise the Storage API is called with the forwarded token.
        """
        tables = self.configuration.tables_input_mapping
        if not tables:
            raise UserException("No input table found. Add an input table in the storage settings first.")

        input_table_name = self.configuration.parameters.get("source", {}).get("input_table", "")
        if input_table_name:
            table = next((t for t in tables if Path(t.full_path).stem == input_table_name), None)
            if table is None:
                raise UserException(f"Input table '{input_table_name}' not found in the storage mapping.")
        else:
            table = tables[0]

        if table.columns:
            return list(table.columns)

        # columns not declared in the mapping — fetch from Storage API
        token = self.environment_variables.token
        url = self.environment_variables.url
        if not token or not url:
            raise UserException("Storage API token not available. Ensure forward_token is enabled for this component.")

        resp = requests.get(
            f"{url}/v2/storage/tables/{table.source}",
            headers={"X-StorageApi-Token": token},
            timeout=30,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            raise UserException(f"Storage API error fetching table columns: {e}") from e
        return resp.json().get("columns", [])

    @sync_action("testConnection")
    def test_connection(self) -> dict:
        """Sync action: verify Essbase connectivity and return version."""
        client = self._build_essbase_client()
        version = client.verify_connection()
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
