"""End-to-end tests against a live Berserk cluster.

Set BERSERK_ENDPOINT to run (e.g., BERSERK_ENDPOINT=http://localhost:9510).
"""
import asyncio, os, sys

ENDPOINT = os.environ.get("BERSERK_ENDPOINT")
if not ENDPOINT:
    print("BERSERK_ENDPOINT not set, skipping e2e tests")
    sys.exit(0)

GRPC_TARGET = ENDPOINT.replace("http://", "").replace("https://", "")
HTTP_TARGET = ENDPOINT if ENDPOINT.startswith("http") else f"http://{ENDPOINT}"

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "berserk_client", "_pb"))
import grpc, grpc.aio, query_pb2, query_pb2_grpc, httpx

passed = failed = 0

async def run(name, fn):
    global passed, failed
    try:
        await fn(); passed += 1; print(f"  PASS  {name}")
    except Exception as e:
        failed += 1; print(f"  FAIL  {name}: {e}")

async def grpc_simple_query():
    async with grpc.aio.insecure_channel(GRPC_TARGET) as ch:
        stub = query_pb2_grpc.QueryServiceStub(ch)
        req = query_pb2.ExecuteQueryRequest(query="print v = 1", timezone="UTC")
        schemas, batches = [], []
        async for f in stub.ExecuteQuery(req, timeout=30):
            p = f.WhichOneof("payload")
            if p == "schema": schemas.append(f.schema)
            elif p == "batch": batches.append(f.batch)
        assert len(schemas) == 1 and schemas[0].columns[0].name == "v"
        assert len(batches) >= 1 and batches[0].rows[0].values[0].tt_long == 1

async def grpc_invalid_query():
    async with grpc.aio.insecure_channel(GRPC_TARGET) as ch:
        stub = query_pb2_grpc.QueryServiceStub(ch)
        req = query_pb2.ExecuteQueryRequest(query="not valid!!!", timezone="UTC")
        got_error = False
        try:
            async for f in stub.ExecuteQuery(req, timeout=30):
                if f.WhichOneof("payload") == "error": got_error = True; break
        except grpc.aio.AioRpcError: got_error = True
        assert got_error

async def grpc_multi_column():
    async with grpc.aio.insecure_channel(GRPC_TARGET) as ch:
        stub = query_pb2_grpc.QueryServiceStub(ch)
        req = query_pb2.ExecuteQueryRequest(query='print a = 1, b = "hello", c = true', timezone="UTC")
        schemas = []
        async for f in stub.ExecuteQuery(req, timeout=30):
            if f.WhichOneof("payload") == "schema": schemas.append(f.schema)
        cols = schemas[0].columns
        assert len(cols) == 3 and cols[0].name == "a" and cols[1].name == "b" and cols[2].name == "c"

async def http_simple_query():
    async with httpx.AsyncClient(timeout=30) as c:
        resp = await c.post(f"{HTTP_TARGET}/v2/rest/query", json={"csl": "print v = 1"})
        assert resp.status_code == 200
        primary = [f for f in resp.json() if f.get("TableKind") == "PrimaryResult"]
        assert len(primary) == 1 and primary[0]["Rows"] == [[1]]

async def http_invalid_query():
    async with httpx.AsyncClient(timeout=30) as c:
        resp = await c.post(f"{HTTP_TARGET}/v2/rest/query", json={"csl": "not valid!!!"})
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
