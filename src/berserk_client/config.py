"""Client configuration."""

from dataclasses import dataclass, field


@dataclass
class Config:
    """Configuration for connecting to a Berserk query service."""

    endpoint: str = "http://localhost:9510"
    """Query service endpoint."""

    username: str | None = None
    """Username sent as x-bzrk-username header."""

    timeout: float = 30.0
    """Maximum time for a complete request (seconds)."""

    connect_timeout: float = 10.0
    """Connection timeout (seconds)."""

    client_name: str = "berserk-client-python"
    """Client name sent as x-bzrk-client-name header."""

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
