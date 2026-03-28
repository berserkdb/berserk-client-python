"""Shared response types."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ColumnType(str, Enum):
    BOOL = "bool"
    INT = "int"
    LONG = "long"
    REAL = "real"
    STRING = "string"
    DATETIME = "datetime"
    TIMESPAN = "timespan"
    GUID = "guid"
    DYNAMIC = "dynamic"


# A dynamic value from query results.
Value = None | bool | int | float | str | list[Any] | dict[str, Any]


@dataclass
class Column:
    name: str
    type: ColumnType


@dataclass
class Table:
    name: str
    columns: list[Column]
    rows: list[list[Value]]


@dataclass
class ExecutionStats:
    rows_processed: int = 0
    chunks_total: int = 0
    chunks_scanned: int = 0
    query_time_nanos: int = 0
    chunk_scan_time_nanos: int = 0


@dataclass
class QueryWarning:
    kind: str = ""
    message: str = ""


@dataclass
class PartialFailure:
    segment_ids: list[str] = field(default_factory=list)
    message: str = ""


@dataclass
class VisualizationMetadata:
    visualization_type: str = ""
    properties: dict[str, str] = field(default_factory=dict)


@dataclass
class QueryResponse:
    tables: list[Table] = field(default_factory=list)
    stats: ExecutionStats | None = None
    warnings: list[QueryWarning] = field(default_factory=list)
    partial_failures: list[PartialFailure] = field(default_factory=list)
    visualization: VisualizationMetadata | None = None
