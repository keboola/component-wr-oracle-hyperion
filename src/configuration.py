import logging

from keboola.component.exceptions import UserException
from pydantic import BaseModel, Field, ValidationError


class ColumnMapping(BaseModel):
    """Maps a single source column to an Essbase member/dimension name."""

    source: str
    destination: str
    required: bool | None = Field(default=None)

    model_config = {"extra": "ignore"}


class SourceConfig(BaseModel):
    """Row-level source parameters — maps to the 'source' block in configRowSchema."""

    batch_size: int = 1000
    column_mapping: list[ColumnMapping] = Field(default_factory=list)

    model_config = {"extra": "ignore"}


class DestinationConfig(BaseModel):
    """Row-level destination parameters — maps to the 'destination' block in configRowSchema."""

    application: str
    database: str

    model_config = {"extra": "ignore"}


class Configuration(BaseModel):
    """Root-level component configuration — authentication and server settings."""

    username: str
    password: str = Field(alias="#password")
    essbase_host: str
    essbase_port: int = 9010
    use_ssl: bool = True
    ssl_verify: bool = True
    debug: bool = False

    model_config = {"populate_by_name": True, "extra": "ignore"}

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration error: {', '.join(error_messages)}")

        if self.debug:
            logging.getLogger().setLevel(logging.DEBUG)

    def get_essbase_host(self) -> str:
        """Return Essbase host."""
        return self.essbase_host


class RowConfiguration(BaseModel):
    """Row-level configuration — one row per write operation."""

    source: SourceConfig
    destination: DestinationConfig

    model_config = {"populate_by_name": True, "extra": "ignore"}

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Row configuration error: {', '.join(error_messages)}")
