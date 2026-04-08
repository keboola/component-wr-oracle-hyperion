"""Unit tests for sync actions — mock the Essbase client to avoid real API calls."""

import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from component import Component
from configuration import Configuration
from essbase_client import EssbaseClient

_CONN = {
    "essbase_host": "essbase.example.com",
    "essbase_port": 9010,
    "username": "admin",
    "#password": "secret",
    "use_ssl": False,
    "ssl_verify": False,
}


def _make_component(parameters: dict, storage: dict | None = None):
    """Instantiate Component with patched Keboola config (no real data dir needed)."""
    config_data = {"parameters": parameters}
    if storage:
        config_data["storage"] = storage

    # Build a mock for the Keboola Configuration object returned by the property
    mock_config = MagicMock()
    mock_config.parameters = parameters
    mock_config.config_data = config_data
    mock_config.tables_input_mapping = []

    with patch.object(Component, "__init__", lambda self: None):
        comp = Component()

    # Patch the property on the class so it returns our mock
    type(comp).configuration = PropertyMock(return_value=mock_config)

    comp.params = Configuration(**parameters)

    # Mock environment_variables
    comp.environment_variables = MagicMock()
    comp.environment_variables.token = "test-token"
    comp.environment_variables.url = "https://connection.keboola.com"

    # Build (or skip) the Essbase client
    conn = parameters.get("connection", {})
    if conn.get("essbase_host") and conn.get("username") and conn.get("#password"):
        comp.client = MagicMock(spec=EssbaseClient)
    else:
        comp.client = None

    return comp


class TestTestConnection(unittest.TestCase):
    def test_returns_version(self):
        comp = _make_component({"connection": _CONN})
        comp.client.verify_connection.return_value = "21.3.0.0.0"
        result = comp.test_connection()
        self.assertEqual(result["status"], "success")
        self.assertIn("21.3.0.0.0", result["message"])
        comp.client.verify_connection.assert_called_once()


class TestListApplications(unittest.TestCase):
    def test_returns_select_elements(self):
        comp = _make_component({"connection": _CONN})
        comp.client.get_applications.return_value = [
            {"name": "Sample", "alias": "Sample App"},
            {"name": "Demo"},
        ]
        result = comp.list_applications()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].value, "Sample")
        self.assertEqual(result[0].label, "Sample App")
        self.assertEqual(result[1].label, "Demo")


class TestListDatabases(unittest.TestCase):
    def test_returns_databases(self):
        comp = _make_component(
            {
                "connection": _CONN,
                "destination": {"application": "Sample", "database": ""},
            }
        )
        comp.client.get_databases.return_value = [
            {"name": "Basic", "alias": "Basic DB"},
        ]
        result = comp.list_databases()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].value, "Basic")
        comp.client.get_databases.assert_called_once_with("Sample")

    def test_raises_when_no_application(self):
        comp = _make_component(
            {
                "connection": _CONN,
                "destination": {"application": "", "database": ""},
            }
        )
        with self.assertRaises(SystemExit):
            comp.list_databases()


class TestLoadColumnMapping(unittest.TestCase):
    def test_populates_mapping_from_storage_api(self):
        comp = _make_component(
            {
                "connection": _CONN,
                "source": {"input_table": "", "batch_size": 1000, "column_mapping": []},
            },
            storage={"input": {"tables": [{"source": "in.c-main.products"}]}},
        )
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"columns": ["Product", "Jan", "Feb"]}
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = comp.load_column_mapping()

        mapping = result["data"]["source"]["column_mapping"]
        self.assertEqual(len(mapping), 3)
        self.assertEqual(mapping[0], {"source": "Product", "destination": "Product"})

    def test_preserves_existing_mapping(self):
        comp = _make_component(
            {
                "connection": _CONN,
                "source": {
                    "input_table": "",
                    "batch_size": 1000,
                    "column_mapping": [{"source": "Product", "destination": "ProductDim"}],
                },
            },
            storage={"input": {"tables": [{"source": "in.c-main.products"}]}},
        )
        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"columns": ["Product", "Jan"]}
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = comp.load_column_mapping()

        mapping = result["data"]["source"]["column_mapping"]
        self.assertEqual(mapping[0], {"source": "Product", "destination": "ProductDim"})
        self.assertEqual(mapping[1], {"source": "Jan", "destination": "Jan"})


class TestLoadContextMembers(unittest.TestCase):
    def test_populates_unmatched_dimensions(self):
        comp = _make_component(
            {
                "connection": _CONN,
                "destination": {"application": "Sample", "database": "Basic"},
                "source": {"input_table": "", "batch_size": 1000, "context_members": []},
            },
            storage={"input": {"tables": [{"source": "in.c-main.products"}]}},
        )
        comp.client.get_dimensions.return_value = ["Product", "Market", "Year", "Scenario"]

        with patch("requests.get") as mock_get:
            mock_resp = MagicMock()
            mock_resp.json.return_value = {"columns": ["Product", "Year"]}
            mock_resp.raise_for_status = MagicMock()
            mock_get.return_value = mock_resp

            result = comp.load_context_members()

        members = result["data"]["source"]["context_members"]
        dims = [m["dimension"] for m in members]
        self.assertEqual(dims, ["Market", "Scenario"])
        self.assertTrue(all(m["member"] == "" for m in members))

    def test_raises_when_no_app_or_db(self):
        comp = _make_component(
            {
                "connection": _CONN,
                "destination": {"application": "", "database": ""},
            }
        )
        with self.assertRaises(SystemExit):
            comp.load_context_members()


if __name__ == "__main__":
    unittest.main()
