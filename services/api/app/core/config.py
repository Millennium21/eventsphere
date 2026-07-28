from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    environment: str = "development"
    log_level: str = "INFO"
    api_prefix: str = "/api/v1"

    database_url: str = "postgresql+asyncpg://eventsphere:eventsphere_pw@localhost:5432/eventsphere"

    redis_url: str = "redis://localhost:6379/0"

    kafka_bootstrap_servers: str = "localhost:9092"

    inventory_grpc_target: str = "localhost:50051"
    inventory_grpc_timeout_seconds: float = 5.0

    jwt_secret_key: str = "changeme-generate-a-long-random-secret-and-set-this-via-env"
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    refresh_token_expire_days: int = 7

    rate_limit_per_minute: int = 60

    cors_allow_origins: list[str] = ["http://localhost:3000"]


settings = Settings()
