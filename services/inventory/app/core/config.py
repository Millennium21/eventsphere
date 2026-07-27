from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "development"
    log_level: str = "INFO"

    # Independent env var from the API service's database_url — in this
    # docker-compose setup they happen to point at the same Postgres
    # instance (different schema each), but each service owns its own
    # setting so they can be pointed at genuinely separate databases
    # (e.g. separate RDS instances) without any code change.
    database_url: str = "postgresql+asyncpg://eventsphere:eventsphere_pw@localhost:5432/eventsphere"

    redis_url: str = "redis://localhost:6379/0"
    lock_ttl_ms: int = 5000
    lock_retry_attempts: int = 10
    lock_retry_delay_seconds: float = 0.1

    optimistic_retry_attempts: int = 5

    kafka_bootstrap_servers: str = "localhost:9092"

    grpc_port: int = 50051
    reservation_ttl_seconds: int = 600  # 10 minutes
    reaper_interval_seconds: int = 30


settings = Settings()
