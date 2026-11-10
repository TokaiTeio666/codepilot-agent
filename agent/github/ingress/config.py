"""Settings for the durable GitHub webhook ingress queue."""

from dataclasses import dataclass

from agent.config import ENV


@dataclass(frozen=True)
class IngressSettings:
    """Everything the queue and worker need, resolved from the environment."""

    enabled: bool
    worker_enabled: bool
    redis_url: str
    stream: str
    group: str
    concurrency: int
    worker_count: int
    local_concurrency: int
    max_attempts: int
    retry_base_seconds: float
    retry_max_seconds: float
    handler_timeout_seconds: float
    delivery_ttl_seconds: int

    @property
    def delayed_key(self) -> str:
        return f"{self.stream}:delayed"

    @property
    def dead_stream(self) -> str:
        return f"{self.stream}:dead"

    @property
    def running_key(self) -> str:
        return f"{self.stream}:running"

    @property
    def heartbeat_prefix(self) -> str:
        return f"{self.stream}:heartbeat:"

    @property
    def delivery_prefix(self) -> str:
        return f"{self.stream}:delivery:"

    @classmethod
    def from_env(cls) -> IngressSettings:
        return cls(
            enabled=ENV.GITHUB_INGRESS_QUEUE_ENABLED.get_bool(default=False),
            worker_enabled=ENV.GITHUB_INGRESS_WORKER_ENABLED.get_bool(default=False),
            redis_url=ENV.GITHUB_INGRESS_REDIS_URL.get(),
            stream=ENV.GITHUB_INGRESS_STREAM.get(),
            group=ENV.GITHUB_INGRESS_GROUP.get(),
            concurrency=max(1, ENV.GITHUB_INGRESS_CONCURRENCY.get_int(default=20)),
            worker_count=max(1, ENV.GITHUB_INGRESS_WORKER_COUNT.get_int(default=2)),
            local_concurrency=max(1, ENV.GITHUB_INGRESS_LOCAL_CONCURRENCY.get_int(default=10)),
            max_attempts=max(1, ENV.GITHUB_INGRESS_MAX_ATTEMPTS.get_int(default=3)),
            retry_base_seconds=max(
                0.0, float(ENV.GITHUB_INGRESS_RETRY_BASE_SECONDS.get(default="30"))
            ),
            retry_max_seconds=max(
                0.0, float(ENV.GITHUB_INGRESS_RETRY_MAX_SECONDS.get(default="600"))
            ),
            handler_timeout_seconds=max(
                1.0, float(ENV.GITHUB_INGRESS_HANDLER_TIMEOUT_SECONDS.get(default="600"))
            ),
            delivery_ttl_seconds=max(
                1, ENV.GITHUB_INGRESS_DELIVERY_TTL_SECONDS.get_int(default=86400)
            ),
        )
