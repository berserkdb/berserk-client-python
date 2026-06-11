# berserk-client-python

Python client library for the [Berserk](https://berserk.dev) observability platform.

Clients connect to the **gateway** — the authenticated public edge — using a
bearer token (a CLI access token from the device flow, or a service-principal
token). The gateway authenticates the call and injects the trusted identity
before forwarding to the query service. The gRPC surface is mounted under
`/api/grpc`; the client applies that prefix by default (set
`grpc_path_prefix=""` to connect directly to a query service in dev).

## Installation

```bash
# gRPC transport
pip install berserk-client[grpc]

# HTTP transport (ADX v2 REST)
pip install berserk-client[http]

# Both
pip install berserk-client[all]
```

## Quick Start

### gRPC

```python
import asyncio, os
from berserk_client import Config, GrpcClient

async def main():
    client = GrpcClient(Config(
        endpoint="https://berserk.example.com",
        token=os.environ["BERSERK_TOKEN"],
    ))
    response = await client.query("Logs | where severity == 'error' | take 10")

    for table in response.tables:
        print(f"Table: {table.name} ({len(table.rows)} rows)")

    await client.close()

asyncio.run(main())
```

### HTTP (ADX v2)

```python
import asyncio
from berserk_client import Config, HttpClient

async def main():
    client = HttpClient(Config(
        endpoint="https://berserk.example.com",
        token=os.environ["BERSERK_TOKEN"],
    ))
    response = await client.query("print v = 1")
    print(response.tables)
    await client.close()

asyncio.run(main())
```

## Proto Code Generation

To regenerate gRPC stubs from the vendored proto files:

```bash
python -m grpc_tools.protoc \
    -Iproto \
    --python_out=src/berserk_client/_pb \
    --grpc_python_out=src/berserk_client/_pb \
    proto/*.proto
```

## License

Apache-2.0
