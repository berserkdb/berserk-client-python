"""Validates that every BqlValue oneof arm in proto/dynamic_value.proto
decodes through the real GrpcClient against a live cluster — one `print`
query produces a column per value type, and each decoded cell is asserted.

Set BERSERK_ENDPOINT to the gateway (e.g. localhost:9500) and
BERSERK_TOKEN to a CLI bearer token. To run directly against a query
service instead, set BERSERK_GRPC_PREFIX="".
"""
import asyncio, os, sys

ENDPOINT = os.environ.get("BERSERK_ENDPOINT")
if not ENDPOINT:
    print("BERSERK_ENDPOINT not set, skipping value-type e2e tests")
    sys.exit(0)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
# Generated stubs use flat absolute imports (import dynamic_value_pb2),
# so their directory must be importable as well.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "berserk_client", "_pb"))

from berserk_client import Config, GrpcClient
from berserk_client.types import ColumnType

GUID = "74be27de-1e4e-49d9-b579-fe0b331d3642"
# 2024-01-15T10:30:00Z. The server emits datetimes as nanoseconds since
# the Unix epoch (NOTE: the proto comment claims ticks since 0001-01-01 —
# the wire disagrees). Python ints are unbounded, so unlike JS there is
# no precision cliff to guard against.
DT_UNIX_NANOS = 1_705_314_600 * 10**9
# Timespans ARE emitted as 100ns ticks: 1h = 3600s * 1e7.
TS_1H_TICKS = 3600 * 10**7

# One column per BqlValue oneof arm, plus in-oneof default values
# (false / 0 / "") which proto3 oneof presence must keep distinguishable
# from null.
QUERY = """print b = true,
  f = false,
  i = toint(42),
  l = tolong(1234567890123),
  z = tolong(0),
  r = 3.14,
  s = "hello",
  es = "",
  dt = todatetime("2024-01-15T10:30:00Z"),
  ts = 1h,
  g = toguid("%s"),
  arr = dynamic([1, "two", true]),
  bag = dynamic({"a": 1, "nested": {"c": false}}),
  n = toint("not-a-number")""" % GUID

# column name -> (expected column type, expected decoded value)
EXPECTED = {
    "b": (ColumnType.BOOL, True),
    "f": (ColumnType.BOOL, False),
    "i": (ColumnType.INT, 42),
    "l": (ColumnType.LONG, 1234567890123),
    "z": (ColumnType.LONG, 0),
    "r": (ColumnType.REAL, 3.14),
    "s": (ColumnType.STRING, "hello"),
    "es": (ColumnType.STRING, ""),
    "dt": (ColumnType.DATETIME, DT_UNIX_NANOS),
    "ts": (ColumnType.TIMESPAN, TS_1H_TICKS),
    # The proto enum has COLUMN_TYPE_GUID, but the engine reports
    # guid-typed expressions as string columns (values arrive on the
    # string_value arm). If the server ever starts emitting GUID,
    # this expectation should flip to ColumnType.GUID.
    "g": (ColumnType.STRING, GUID),
    "arr": (ColumnType.DYNAMIC, [1, "two", True]),
    "bag": (ColumnType.DYNAMIC, {"a": 1, "nested": {"c": False}}),
    "n": (ColumnType.INT, None),
}

passed = failed = 0

def check(name, fn):
    global passed, failed
    try:
        fn(); passed += 1; print(f"  PASS  {name}")
    except Exception as e:
        failed += 1; print(f"  FAIL  {name}: {e}")

async def main():
    global failed
    config = Config(endpoint=ENDPOINT)
    if "BERSERK_TOKEN" in os.environ:
        config.token = os.environ["BERSERK_TOKEN"]
    if "BERSERK_GRPC_PREFIX" in os.environ:
        config.grpc_path_prefix = os.environ["BERSERK_GRPC_PREFIX"]
    client = GrpcClient(config)
    try:
        response = await client.query(QUERY)
        table = next((t for t in response.tables if t.name == "PrimaryResult"), response.tables[0])
        assert len(table.rows) == 1, f"expected 1 row, got {len(table.rows)}"
        row = table.rows[0]
        col_index = {c.name: i for i, c in enumerate(table.columns)}

        print("value-type tests:")
        for name, (expected_type, expected_value) in EXPECTED.items():
            def assert_one(name=name, expected_type=expected_type, expected_value=expected_value):
                assert name in col_index, f"column {name} missing from schema"
                idx = col_index[name]
                actual_type = table.columns[idx].type
                assert actual_type == expected_type, (
                    f"column type for {name}: {actual_type} != {expected_type}"
                )
                actual = row[idx]
                assert actual == expected_value and type(actual) is type(expected_value) or (
                    # bool is an int subclass in Python — require exact
                    # type match for scalars so 1 != True slips through.
                    actual == expected_value and not isinstance(expected_value, bool)
                ), f"decoded value for {name}: {actual!r} != {expected_value!r}"
            check(name, assert_one)
    except Exception as e:
        failed += 1
        print(f"  FAIL  query execution: {e}")
    finally:
        await client.close()

    print(f"\n{passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)

asyncio.run(main())
