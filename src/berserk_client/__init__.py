"""Berserk query client for Python."""

from berserk_client.types import (
    QueryResponse,
    Table,
    Column,
    ColumnType,
    Value,
    ExecutionStats,
    QueryWarning,
    PartialFailure,
    VisualizationMetadata,
)
from berserk_client.config import Config

__all__ = [
    "Config",
    "QueryResponse",
    "Table",
    "Column",
    "ColumnType",
    "Value",
    "ExecutionStats",
    "QueryWarning",
    "PartialFailure",
    "VisualizationMetadata",
]

try:
    from berserk_client.grpc_client import GrpcClient
    __all__.append("GrpcClient")
except ImportError:
    pass

try:
    from berserk_client.http_client import HttpClient
    __all__.append("HttpClient")
except ImportError:
    pass
