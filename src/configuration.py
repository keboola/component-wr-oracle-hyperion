from urllib.parse import urlparse

from keboola.component.exceptions import UserException
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class ColumnMapping(BaseModel):
    """Maps a single source column to an Essbase member/dimension name."""

    model_config = ConfigDict(extra="ignore")

    source: str
    destination: str
    required: bool | None = Field(default=None)


class SourceConfig(BaseModel):
    """Row-level source parameters — maps to the 'source' block in configRowSchema."""

    model_config = ConfigDict(extra="ignore")

    input_table: str = ""
    batch_size: int = Field(default=1000, ge=1)
    context_members: list[str] = Field(default_factory=list)
    column_mapping: list[ColumnMapping] = Field(default_factory=list)


class DestinationConfig(BaseModel):
    """Row-level destination parameters — maps to the 'destination' block in configRowSchema."""

    model_config = ConfigDict(extra="ignore")

    application: str = ""
    database: str = ""


class Configuration(BaseModel):
    """Full component configuration — connection settings plus write operation parameters."""

    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    hyperion_url: str = ""
    essbase_host: str = ""
    essbase_port: int = 9010
    username: str = ""
    password: str = Field(default="", alias="#password")
    use_ssl: bool = True
    ssl_verify: bool = True
    source: SourceConfig | None = None
    destination: DestinationConfig | None = None

    @model_validator(mode="before")
    @classmethod
    def _flatten_connection(cls, data: dict) -> dict:
        """Unwrap the nested 'connection' block if present (UI format)."""
        if isinstance(data, dict) and "connection" in data:
            data = {**data, **data.pop("connection")}
        return data

    def __init__(self, **data):
        try:
            super().__init__(**data)
        except ValidationError as e:
            error_messages = [f"{'.'.join(str(loc) for loc in err['loc'])}: {err['msg']}" for err in e.errors()]
            raise UserException(f"Configuration error: {', '.join(error_messages)}")

    def get_essbase_host(self) -> str:
        """Return Essbase host — falls back to the host from hyperion_url if essbase_host is not set."""
        if self.essbase_host:
            return self.essbase_host
        return urlparse(self.hyperion_url).hostname or self.hyperion_url
