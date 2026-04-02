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

from keboola.component.base import ComponentBase, sync_action
from keboola.component.exceptions import UserException
from keboola.component.sync_actions import SelectElement

from configuration import ColumnMapping, Configuration, RowConfiguration
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
        raw_rows = self.configuration.config_data.get("config_rows", [])

        if raw_rows:
            rows_to_process = [
                RowConfiguration(**row.get("parameters", {})) for row in raw_rows if not row.get("is_disabled")
            ]
            if not rows_to_process:
                raise UserException("No active configuration rows defined. Enable at least one row.")
        else:
            rows_to_process = [RowConfiguration(**self.configuration.parameters)]

        client = None

        for row_params in rows_to_process:
            logging.info(
                "Processing: app=%s db=%s",
                row_params.destination.application,
                row_params.destination.database,
            )
            if client is None:
                client = self._build_essbase_client(self.params)
                version = client.verify_connection()
                logging.info("Connected to Essbase version: %s", version)
            self._write_essbase(client, row_params)

    @sync_action("listApplications")
    def list_applications(self) -> list[SelectElement]:
        """Return all Essbase applications for the destination dropdown."""
        client = self._build_essbase_client(self.params)
        apps = client.get_applications()
        return [SelectElement(value=app["name"], label=app.get("alias") or app["name"]) for app in apps]

    @sync_action("listDatabases")
    def list_databases(self) -> list[SelectElement]:
        """Return databases for the selected application."""
        destination = self.configuration.parameters.get("destination", {})
        application = destination.get("application", "")
        if not application:
            raise UserException("Select or enter an Application name first, then reload databases.")
        client = self._build_essbase_client(self.params)
        databases = client.get_databases(application)
        return [SelectElement(value=db["name"], label=db.get("alias") or db["name"]) for db in databases]

    @sync_action("listColumns")
    def list_columns(self) -> list[SelectElement]:
        """Return columns from the input mapping table via _metadata_."""
        metadata = self.configuration.parameters.get("_metadata_", {})
        columns = metadata.get("table", {}).get("columns", [])
        if not columns:
            raise UserException("No columns found. Link an input table in the input mapping first.")
        return [SelectElement(value=col, label=col) for col in columns]

    @sync_action("testConnection")
    def test_connection(self) -> dict:
        """Sync action: verify Essbase connectivity and return version."""
        client = self._build_essbase_client(self.params)
        version = client.verify_connection()
        return {"status": "success", "message": f"Connected to Essbase {version}"}

    def _build_essbase_client(self, params: Configuration) -> EssbaseClient:
        host = params.get_essbase_host()
        logging.info("Connecting to Essbase at %s:%d", host, params.essbase_port)
        return EssbaseClient(
            host=host,
            port=params.essbase_port,
            username=params.username,
            password=params.password,
            use_ssl=params.use_ssl,
            ssl_verify=params.ssl_verify,
        )

    def _write_essbase(self, client: EssbaseClient, row: RowConfiguration) -> None:
        headers, data_rows = self._read_table(row.source.input_table)
        if not headers:
            logging.warning("Input table '%s' is empty — skipping.", row.source.input_table)
            return

        if row.source.column_mapping:
            headers, data_rows = self._apply_column_mapping(headers, data_rows, row.source.column_mapping)
            logging.info("Applied column mapping: %d column(s) -> %s", len(headers), headers)

        total = 0
        for batch in _batched(data_rows, row.source.batch_size):
            client.import_data_slice(
                row.destination.application,
                row.destination.database,
                headers,
                batch,
                context_members=row.source.context_members,
            )
            total += len(batch)
            logging.info(
                "Essbase %s/%s: imported %d rows so far.", row.destination.application, row.destination.database, total
            )
        if total == 0:
            logging.warning("Input table '%s' had no data rows — nothing written.", row.source.input_table)
        else:
            logging.info(
                "Essbase write complete: %d total rows to %s/%s.",
                total,
                row.destination.application,
                row.destination.database,
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
