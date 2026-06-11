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
    from berserk_client._pb import (
        common_api_pb2,
        dynamic_value_pb2,
        query_pb2,
        query_pb2_grpc,
    )
    return query_pb2, query_pb2_grpc, dynamic_value_pb2, common_api_pb2


class _GatewayInterceptor(grpc.aio.UnaryStreamClientInterceptor):
    """Rewrites method paths onto the gateway's gRPC mount (e.g.
    /api/grpc/query.QueryService/ExecuteQuery) and attaches the bearer
    token. The gateway authenticates the call and injects the trusted
    identity headers before forwarding."""

    def __init__(self, path_prefix: str, token: str | None) -> None:
        self._prefix = path_prefix.encode()
        self._token = token

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        method = client_call_details.method
        if self._prefix:
            m = method if isinstance(method, (bytes, bytearray)) else method.encode()
            method = self._prefix + bytes(m)
        metadata = list(client_call_details.metadata or [])
        if self._token:
            metadata.append(("authorization", f"Bearer {self._token}"))
        details = client_call_details._replace(method=method, metadata=metadata)
        return await continuation(details, request)


class GrpcClient:
    """gRPC client for querying through the Berserk gateway."""

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self._channel: grpc.aio.Channel | None = None

    async def _get_channel(self) -> grpc.aio.Channel:
        if self._channel is None:
            target = self.config.grpc_target()
            interceptors = [
                _GatewayInterceptor(self.config.grpc_path_prefix, self.config.token)
            ]
            if self.config.is_tls():
                self._channel = grpc.aio.secure_channel(
                    target, grpc.ssl_channel_credentials(), interceptors=interceptors
                )
            else:
                self._channel = grpc.aio.insecure_channel(target, interceptors=interceptors)
        return self._channel

    async def query(
        self,
        query: str,
        since: str | None = None,
        until: str | None = None,
        timezone: str = "UTC",
    ) -> QueryResponse:
        """Execute a query and collect all results."""
        query_pb2, query_pb2_grpc, _, common_api_pb2 = _load_stubs()

        channel = await self._get_channel()
        stub = query_pb2_grpc.QueryServiceStub(channel)

        request = query_pb2.ExecuteQueryRequest(
            query=query,
            since=since or "",
            until=until or "",
            timezone=timezone,
            database=common_api_pb2.DatabaseRef(name=self.config.database or "default"),
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
    """Convert a proto BqlValue to a Python value."""
    which = dyn.WhichOneof("value")
    if which is None or which == "null_value":
        return None
    if which == "bool_value":
        return dyn.bool_value
    if which == "int_value":
        return dyn.int_value
    if which == "long_value":
        return dyn.long_value
    if which == "real_value":
        return dyn.real_value
    if which == "string_value":
        return dyn.string_value
    if which == "datetime_value":
        return dyn.datetime_value
    if which == "timespan_value":
        return dyn.timespan_value
    if which == "array_value":
        return [_convert_value(v) for v in dyn.array_value.values]
    if which == "bag_value":
        return {k: _convert_value(v) for k, v in dyn.bag_value.properties.items()}
    return None
