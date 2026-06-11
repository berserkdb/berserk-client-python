"""Client configuration."""

from dataclasses import dataclass


@dataclass
class Config:
    """Configuration for connecting to a Berserk gateway."""

    endpoint: str = "http://localhost:9500"
    """Gateway endpoint (e.g., "https://berserk.example.com"). An https
    endpoint uses TLS channel credentials."""

    token: str | None = None
    """Bearer token sent as `authorization` on every call — a CLI access
    token or service-principal token minted by the gateway.
    Unauthenticated calls are rejected by the gateway."""

    grpc_path_prefix: str = "/api/grpc"
    """Path prefix the gateway mounts the gRPC surface under. Set to ""
    when connecting directly to a query service (in-cluster / dev)."""

    timeout: float = 30.0
    """Maximum time for a complete request (seconds)."""

    connect_timeout: float = 10.0
    """Connection timeout (seconds)."""

    database: str = "default"
    """Database to resolve unqualified table names against. Sent on every
    ExecuteQueryRequest as `database.name`."""

    def normalized_endpoint(self) -> str:
        """Ensure endpoint has a scheme prefix."""
        if self.endpoint.startswith(("http://", "https://")):
            return self.endpoint
        return f"http://{self.endpoint}"

    def grpc_target(self) -> str:
        """Return endpoint suitable for gRPC (strip scheme)."""
        ep = self.endpoint
        for prefix in ("http://", "https://"):
            if ep.startswith(prefix):
                ep = ep[len(prefix):]
        return ep

    def is_tls(self) -> bool:
        """True when the endpoint requires TLS."""
        return self.endpoint.startswith("https://")
