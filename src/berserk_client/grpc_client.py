"""gRPC client for the Berserk query service."""

from __future__ import annotations

import grpc

from berserk_client.config import Config
from berserk_client.types import (
    Column,
    ColumnType,
    ExecutionStats,
    PartialFailure,
    QueryResponse,
    QueryWarning,
    Table,
    Value,
    VisualizationMetadata,
)

# Proto column type enum values
_COLUMN_TYPE_MAP: dict[int, ColumnType] = {
    1: ColumnType.BOOL,
    2: ColumnType.INT,
    3: ColumnType.LONG,
    4: ColumnType.REAL,
    5: ColumnType.STRING,
    6: ColumnType.DATETIME,
    7: ColumnType.TIMESPAN,
    8: ColumnType.GUID,
    9: ColumnType.DYNAMIC,
}


def _load_stubs():
    """Lazily import generated proto stubs."""
    # Users must generate these from the vendored proto files:
    #   python -m grpc_tools.protoc -Iproto --python_out=src/berserk_client/_pb \
    #     --grpc_python_out=src/berserk_client/_pb proto/*.proto
    # For now, use proto reflection / dynamic stubs
    from berserk_client._pb import query_pb2, query_pb2_grpc, dynamic_value_pb2
    return query_pb2, query_pb2_grpc, dynamic_value_pb2


class GrpcClient:
    """gRPC client for querying the Berserk query service."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self._channel: grpc.aio.Channel | None = None

    async def _get_channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            target = self.config.grpc_target()
            self._channel = grpc.aio.insecure_channel(target)
        return self._channel

    async def query(
        self,
        query: str,
        since: str | None = None,
        until: str | None = None,
        timezone: str = "UTC",
    ) -> QueryResponse:
        """Execute a query and collect all results."""
        query_pb2, query_pb2_grpc, _ = _load_stubs()

        channel = await self._get_channel()
        stub = query_pb2_grpc.QueryServiceStub(channel)

        metadata = []
        if self.config.username:
            metadata.append(("x-bzrk-username", self.config.username))
        if self.config.client_name:
            metadata.append(("x-bzrk-client-name", self.config.client_name))

        request = query_pb2.ExecuteQueryRequest(
            query=query,
            since=since or "",
            until=until or "",
            timezone=timezone,
        )

        tables: list[Table] = []
        current_schema: tuple[str, list[Column]] | None = None
        current_rows: list[list[Value]] = []
        stats: ExecutionStats | None = None
        warnings: list[QueryWarning] = []
        partial_failures: list[PartialFailure] = []
        visualization: VisualizationMetadata | None = None

        stream = stub.ExecuteQuery(
            request,
            metadata=metadata,
            timeout=self.config.timeout,
        )

        async for frame in stream:
            payload = frame.WhichOneof("payload")

            if payload == "schema":
                if current_schema:
                    name, columns = current_schema
                    tables.append(Table(name=name, columns=columns, rows=current_rows))
                    current_rows = []
                columns = [
                    Column(
                        name=c.name,
                        type=_COLUMN_TYPE_MAP.get(c.type, ColumnType.DYNAMIC),
                    )
                    for c in frame.schema.columns
                ]
                current_schema = (frame.schema.name, columns)

            elif payload == "batch":
                for row in frame.batch.rows:
                    current_rows.append([_convert_value(v) for v in row.values])

            elif payload == "progress":
                p = frame.progress
                stats = ExecutionStats(
                    rows_processed=p.rows_processed,
                    chunks_total=p.chunks_total,
                    chunks_scanned=p.chunks_scanned,
                    query_time_nanos=p.query_time_nanos,
                    chunk_scan_time_nanos=p.chunk_scan_time_nanos,
                )

            elif payload == "error":
                e = frame.error
                raise RuntimeError(f"Query error [{e.code}]: {e.message or e.title}")

            elif payload == "metadata":
                m = frame.metadata
                for pf in m.partial_failures:
                    partial_failures.append(
                        PartialFailure(segment_ids=list(pf.segment_ids), message=pf.message)
                    )
                for w in m.warnings:
                    warnings.append(QueryWarning(kind=w.kind, message=w.message))
                if m.HasField("visualization") and m.visualization.visualization_type:
                    visualization = VisualizationMetadata(
                        visualization_type=m.visualization.visualization_type,
                        properties=dict(m.visualization.properties),
                    )

            elif payload == "done":
                break

        if current_schema:
            name, columns = current_schema
            tables.append(Table(name=name, columns=columns, rows=current_rows))

        return QueryResponse(
            tables=tables,
            stats=stats,
            warnings=warnings,
            partial_failures=partial_failures,
            visualization=visualization,
        )

    async def close(self) -> None:
        """Close the gRPC channel."""
        if self._channel:
            await self._channel.close()
            self._channel = None


def _convert_value(dyn) -> Value:
    """Convert a proto TTDynamic to a Python value."""
    which = dyn.WhichOneof("value")
    if which is None or which == "tt_null":
        return None
    if which == "tt_bool":
        return dyn.tt_bool
    if which == "tt_int":
        return dyn.tt_int
    if which == "tt_long":
        return dyn.tt_long
    if which == "tt_double":
        return dyn.tt_double
    if which == "tt_string":
        return dyn.tt_string
    if which == "tt_timestamp":
        return dyn.tt_timestamp
    if which == "tt_timespan":
        return dyn.tt_timespan
    if which == "tt_array":
        return [_convert_value(v) for v in dyn.tt_array.values]
    if which == "tt_propertybag":
        return {k: _convert_value(v) for k, v in dyn.tt_propertybag.properties.items()}
    return None
