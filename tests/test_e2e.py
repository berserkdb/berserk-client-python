"""End-to-end tests against a live Berserk cluster.

Set BERSERK_ENDPOINT to run (e.g., BERSERK_ENDPOINT=http://localhost:9510).

To run through an authenticated gateway (the public edge) instead of
directly against the query service:
  BERSERK_TOKEN        CLI bearer token (gateway device flow)
  BERSERK_GRPC_PREFIX  path prefix the gateway mounts gRPC under
                       (e.g. /api/grpc)
"""
import asyncio, os, sys

ENDPOINT = os.environ.get("BERSERK_ENDPOINT")
if not ENDPOINT:
    print("BERSERK_ENDPOINT not set, skipping e2e tests")
    sys.exit(0)

TOKEN = os.environ.get("BERSERK_TOKEN")
GRPC_PREFIX = os.environ.get("BERSERK_GRPC_PREFIX", "")

GRPC_TARGET = ENDPOINT.replace("http://", "").replace("https://", "")
HTTP_TARGET = ENDPOINT if ENDPOINT.startswith("http") else f"http://{ENDPOINT}"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "berserk_client", "_pb"))
import grpc, grpc.aio, common_api_pb2, query_pb2, query_pb2_grpc, httpx


class _GatewayInterceptor(grpc.aio.UnaryStreamClientInterceptor):
    """Rewrites method paths onto the gateway's gRPC mount (e.g.
    /api/grpc/query.QueryService/ExecuteQuery) and attaches the bearer."""

    async def intercept_unary_stream(self, continuation, client_call_details, request):
        method = client_call_details.method
        if GRPC_PREFIX:
            m = method if isinstance(method, (bytes, bytearray)) else method.encode()
            method = GRPC_PREFIX.encode() + bytes(m)
        metadata = list(client_call_details.metadata or [])
        if TOKEN:
            metadata.append(("authorization", f"Bearer {TOKEN}"))
        details = client_call_details._replace(method=method, metadata=metadata)
        return await continuation(details, request)


def channel():
    return grpc.aio.insecure_channel(GRPC_TARGET, interceptors=[_GatewayInterceptor()])


HTTP_HEADERS = {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}

passed = failed = 0

async def run(name, fn):
    global passed, failed
    try:
        await fn(); passed += 1; print(f"  PASS  {name}")
    except Exception as e:
        failed += 1; print(f"  FAIL  {name}: {e}")

async def grpc_simple_query():
    async with channel() as ch:
        stub = query_pb2_grpc.QueryServiceStub(ch)
        req = query_pb2.ExecuteQueryRequest(query="print v = 1", timezone="UTC", database=common_api_pb2.DatabaseRef(name="default"))
        schemas, batches = [], []
        async for f in stub.ExecuteQuery(req, timeout=30):
            p = f.WhichOneof("payload")
            if p == "schema": schemas.append(f.schema)
            elif p == "batch": batches.append(f.batch)
        assert len(schemas) == 1 and schemas[0].columns[0].name == "v"
        assert len(batches) >= 1 and batches[0].rows[0].values[0].long_value == 1

async def grpc_invalid_query():
    async with channel() as ch:
        stub = query_pb2_grpc.QueryServiceStub(ch)
        req = query_pb2.ExecuteQueryRequest(query="not valid!!!", timezone="UTC", database=common_api_pb2.DatabaseRef(name="default"))
        got_error = False
        try:
            async for f in stub.ExecuteQuery(req, timeout=30):
                if f.WhichOneof("payload") == "error": got_error = True; break
        except grpc.aio.AioRpcError: got_error = True
        assert got_error

async def grpc_multi_column():
    async with channel() as ch:
        stub = query_pb2_grpc.QueryServiceStub(ch)
        req = query_pb2.ExecuteQueryRequest(query='print a = 1, b = "hello", c = true', timezone="UTC", database=common_api_pb2.DatabaseRef(name="default"))
        schemas = []
        async for f in stub.ExecuteQuery(req, timeout=30):
            if f.WhichOneof("payload") == "schema": schemas.append(f.schema)
        cols = schemas[0].columns
        assert len(cols) == 3 and cols[0].name == "a" and cols[1].name == "b" and cols[2].name == "c"

async def http_simple_query():
    async with httpx.AsyncClient(timeout=30) as c:
        resp = await c.post(f"{HTTP_TARGET}/v2/rest/query", json={"db": "default", "csl": "print v = 1"}, headers=HTTP_HEADERS)
        assert resp.status_code == 200
        primary = [f for f in resp.json() if f.get("TableKind") == "PrimaryResult"]
        assert len(primary) == 1 and primary[0]["Rows"] == [[1]]

async def http_invalid_query():
    async with httpx.AsyncClient(timeout=30) as c:
        resp = await c.post(f"{HTTP_TARGET}/v2/rest/query", json={"db": "default", "csl": "not valid!!!"}, headers=HTTP_HEADERS)
        assert resp.status_code >= 400

async def main():
    print("gRPC tests:")
    await run("simple_query", grpc_simple_query)
    await run("invalid_query", grpc_invalid_query)
    await run("multi_column", grpc_multi_column)
    print("\nHTTP tests:")
    await run("simple_query", http_simple_query)
    await run("invalid_query", http_invalid_query)
    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)

asyncio.run(main())
