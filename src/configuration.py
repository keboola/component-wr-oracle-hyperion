import logging
from typing import Literal

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError


class Configuration(BaseModel):
    """Root-level component configuration — authentication and server settings."""

    username: str
    password: str = Field(alias="#password")
    essbase_host: str
    essbase_port: int = 9010
    ssl_verify: bool = True
    debug: bool = False

    model_config = {"populate_by_name": True}

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration error: {', '.join(error_messages)}")

        if self.debug:
            logging.getLogger().setLevel(logging.DEBUG)


class RowConfiguration(BaseModel):
    """Row-level configuration — one row per write operation."""

    module: Literal["essbase"]
    application: str
    database: str
    input_table: str  # Keboola Storage table to read from (without .csv)
    batch_size: int = 1000  # Rows per API call
    context_members: list[str] = []  # BSO: default members for dimensions not in the CSV

    model_config = {"populate_by_name": True}

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Row configuration error: {', '.join(error_messages)}")
