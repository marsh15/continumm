"""Typed application configuration with safe local defaults."""

from __future__ import annotations

import ipaddress
import os
from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from ``CONTINUUM_`` environment variables."""

    model_config = SettingsConfigDict(
        env_prefix="CONTINUUM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    environment: Literal["development", "test", "production"] = "development"
    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=65535)
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    database_url: SecretStr | None = None
    runtime_database_url: SecretStr | None = None
    operator_database_url: SecretStr | None = None
    operator_github_user_ids: str | None = None
    encryption_key: SecretStr | None = None
    encryption_key_version: str = Field(default="v1", pattern=r"^[A-Za-z0-9_-]{1,32}$")
    encryption_previous_keys: SecretStr | None = None
    enable_github_auth: bool = False
    enable_public_admission: bool = False
    enable_wake_worker: bool = False
    wake_worker_database_url: SecretStr | None = None
    github_client_id: str | None = None
    github_client_secret: SecretStr | None = None
    github_public_origin: str | None = None
    github_state_key: SecretStr | None = None
    github_app_id: int | None = Field(default=None, gt=0)
    github_app_private_key: SecretStr | None = None
    supabase_url: str | None = None
    supabase_secret_key: SecretStr | None = None
    supabase_bucket: str = "continuum-checkpoints"

    @model_validator(mode="before")
    @classmethod
    def use_render_port_in_production(cls, values: object) -> object:
        """Prefer Render's platform port while retaining the local port default."""
        if not isinstance(values, dict):
            return values
        environment = values.get("environment", os.environ.get("CONTINUUM_ENVIRONMENT"))
        render_port = os.environ.get("PORT")
        if environment == "production" and render_port is not None:
            values["port"] = render_port
        return values

    @model_validator(mode="after")
    def validate_deployment_safety(self) -> Settings:
        """Keep developer defaults loopback-only and production explicit."""
        if self.environment in ("development", "test"):
            try:
                host_is_loopback = ipaddress.ip_address(self.host).is_loopback
            except ValueError:
                host_is_loopback = self.host.rstrip(".").lower() == "localhost"
            if not host_is_loopback:
                raise ValueError("development host must be localhost or a loopback IP address")

        if self.environment == "production":
            missing: list[str] = []
            if "host" not in self.model_fields_set:
                missing.append("CONTINUUM_HOST")
            if self._is_missing_secret(self.database_url):
                missing.append("CONTINUUM_DATABASE_URL")
            if self._is_missing_secret(self.runtime_database_url):
                missing.append("CONTINUUM_RUNTIME_DATABASE_URL")
            if self._is_missing_secret(self.encryption_key):
                missing.append("CONTINUUM_ENCRYPTION_KEY")
            if missing:
                names = ", ".join(missing)
                raise ValueError(f"required production configuration is missing: {names}")

        return self

    @staticmethod
    def _is_missing_secret(value: SecretStr | None) -> bool:
        return value is None or not value.get_secret_value().strip()


def safe_settings_error(error: ValidationError) -> str:
    """Format config errors without exposing any submitted values."""
    details = error.errors(include_input=False)
    messages: list[str] = []
    for item in details:
        location = ".".join(str(part) for part in item.get("loc", ()))
        message = str(item["msg"])
        messages.append(f"{location}: {message}" if location else message)
    return "Application configuration is invalid: " + "; ".join(messages)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return settings once per process; values are never logged by this module."""
    return Settings()
