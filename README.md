# berserk-client-python

Python client library for the [Berserk](https://berserk.dev) observability platform.

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
import asyncio
from berserk_client import Config, GrpcClient

async def main():
    client = GrpcClient(Config(endpoint="localhost:9510"))
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
    client = HttpClient(Config(endpoint="http://localhost:9510"))
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
