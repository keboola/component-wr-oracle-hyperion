"""
Oracle Hyperion EPM Writer — main component.

Reads CSV tables from Keboola Storage and imports them into
Essbase (21C REST API) or Hyperion Planning via importdataslice.
"""

import csv
import logging
from itertools import islice

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration, RowConfiguration
from essbase_client import EssbaseClient
from planning_client import PlanningClient


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

        essbase_client = None
        planning_client = None

        for row_params in rows_to_process:
            logging.info(
                "Processing: module=%s app=%s table=%s",
                row_params.module,
                row_params.application,
                row_params.input_table,
            )

            if row_params.module == "essbase":
                if essbase_client is None:
                    essbase_client = self._build_essbase_client(self.params)
                    version = essbase_client.verify_connection()
                    logging.info("Connected to Essbase version: %s", version)
                self._write_essbase(essbase_client, row_params)

            elif row_params.module == "planning":
                if planning_client is None:
                    planning_client = self._build_planning_client(self.params)
                self._write_planning(planning_client, row_params)

    def _build_essbase_client(self, params: Configuration) -> EssbaseClient:
        host = params.get_essbase_host()
        logging.info("Connecting to Essbase at %s:%d", host, params.essbase_port)
        return EssbaseClient(
            host=host,
            port=params.essbase_port,
            username=params.username,
            password=params.password,
            ssl_verify=params.ssl_verify,
        )

    def _build_planning_client(self, params: Configuration) -> PlanningClient:
        logging.info("Connecting to Hyperion Planning at %s", params.hyperion_url)
        return PlanningClient(
            base_url=params.hyperion_url,
            username=params.username,
            password=params.password,
            ssl_verify=params.ssl_verify,
        )

    def _write_essbase(self, client: EssbaseClient, row: RowConfiguration) -> None:
        headers, data_rows = self._read_table(row.input_table)
        if not headers:
            logging.warning("Input table '%s' is empty — skipping.", row.input_table)
            return

        total = 0
        for batch in _batched(data_rows, row.batch_size):
            client.import_data_slice(row.application, row.database, headers, batch)
            total += len(batch)
            logging.info("Essbase %s/%s: imported %d rows so far.", row.application, row.database, total)
        logging.info("Essbase write complete: %d total rows to %s/%s.", total, row.application, row.database)

    def _write_planning(self, client: PlanningClient, row: RowConfiguration) -> None:
        headers, data_rows = self._read_table(row.input_table)
        if not headers:
            logging.warning("Input table '%s' is empty — skipping.", row.input_table)
            return

        total = 0
        for batch in _batched(data_rows, row.batch_size):
            client.import_data_slice(row.application, row.plan_type, headers, batch)
            total += len(batch)
            logging.info("Planning %s/%s: imported %d rows so far.", row.application, row.plan_type, total)
        logging.info("Planning write complete: %d total rows to %s/%s.", total, row.application, row.plan_type)

    def _read_table(self, table_name: str) -> tuple[list[str], list[list]]:
        """
        Read an input table from Keboola Storage and return (headers, rows_as_lists).

        Rows are plain lists of string values — matching the importdataslice payload format directly.
        """
        target_name = f"{table_name}.csv"
        all_tables = self.get_input_tables_definitions()
        matching = [t for t in all_tables if t.name == target_name]
        if not matching:
            raise UserException(f"Input table '{target_name}' not found. Available: {[t.name for t in all_tables]}")
        table = matching[0]
        headers = []
        rows = []
        with open(table.full_path, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            for i, row in enumerate(reader):
                if i == 0:
                    headers = row
                else:
                    rows.append(row)
        return headers, rows


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
