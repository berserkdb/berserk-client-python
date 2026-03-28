"""HTTP client for the Berserk ADX v2 REST endpoint."""

from __future__ import annotations

import httpx

from berserk_client.config import Config
from berserk_client.types import (
    Column,
    ColumnType,
    QueryResponse,
    Table,
    Value,
)

_COLUMN_TYPE_MAP: dict[str, ColumnType] = {
    "bool": ColumnType.BOOL,
    "int": ColumnType.INT,
    "long": ColumnType.LONG,
    "real": ColumnType.REAL,
    "double": ColumnType.REAL,
    "string": ColumnType.STRING,
    "datetime": ColumnType.DATETIME,
    "timespan": ColumnType.TIMESPAN,
    "guid": ColumnType.GUID,
    "uuid": ColumnType.GUID,
    "dynamic": ColumnType.DYNAMIC,
}


class HttpClient:
    """HTTP client for querying via the ADX v2 REST endpoint."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self._client = httpx.AsyncClient(timeout=config.timeout if config else 30.0)

    async def query(self, query: str) -> QueryResponse:
        """Execute a query via the ADX v2 REST endpoint."""
        endpoint = self.config.normalized_endpoint()
        url = f"{endpoint}/v2/rest/query"

        headers: dict[str, str] = {}
        if self.config.username:
            headers["x-bzrk-username"] = self.config.username
        if self.config.client_name:
            headers["x-bzrk-client-name"] = self.config.client_name

        resp = await self._client.post(url, json={"csl": query}, headers=headers)
        resp.raise_for_status()

        frames = resp.json()
        tables: list[Table] = []
        has_errors = False

        for frame in frames:
            if frame.get("FrameType") == "DataTable" and frame.get("TableKind") == "PrimaryResult":
                columns = [
                    Column(
                        name=c["ColumnName"],
                        type=_COLUMN_TYPE_MAP.get(c["ColumnType"], ColumnType.DYNAMIC),
                    )
                    for c in frame.get("Columns", [])
                ]
                rows: list[list[Value]] = frame.get("Rows", [])
                tables.append(
                    Table(
                        name=frame.get("TableName", "PrimaryResult"),
                        columns=columns,
                        rows=rows,
                    )
                )
            elif frame.get("FrameType") == "DataSetCompletion":
                has_errors = frame.get("HasErrors", False)

        if has_errors:
            raise RuntimeError("Query completed with errors")

        return QueryResponse(tables=tables)

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()
