"""Fail-closed limits and operator-only Vercel configuration."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from uuid import UUID

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

_DIGEST_IMAGE = re.compile(r"^[A-Za-z0-9._/:+-]+@sha256:[0-9a-f]{64}$")
_SANDBOX_NAME = re.compile(r"^[a-z][a-z0-9-]{2,62}$")

MAX_SESSION_SECONDS = 300
MAX_ALLOCATION_SECONDS = 900
ALLOCATION_VCPUS = 2
ALLOCATION_MEMORY_MB = 4_096
MAX_COMMAND_SECONDS = 20
MAX_OUTPUT_BYTES = 4_096
MAX_TRANSFER_BYTES = 64 * 1_024
TRANSFER_CHUNK_BYTES = 64
SANDBOX_WORKING_DIRECTORY = "/vercel/sandbox"


@dataclass(frozen=True, slots=True)
class SandboxLimits:
    """Fixed resource envelope for this lifecycle spike."""

    image: str
    session_seconds: int = 120
    vcpus: int = 1
    memory_mb: int = 2_048

    def __post_init__(self) -> None:
        if _DIGEST_IMAGE.fullmatch(self.image) is None:
            raise ValueError("sandbox image must include an immutable sha256 digest")
        if not 1 <= self.session_seconds <= MAX_SESSION_SECONDS:
            raise ValueError(f"session_seconds must be between 1 and {MAX_SESSION_SECONDS}")
        if self.vcpus != 1 or self.memory_mb != 2_048:
            raise ValueError("T04 sandbox resources are fixed at 1 vCPU and 2048 MiB")


@dataclass(frozen=True, slots=True)
class SandboxAllocationLimits:
    """T16 runtime envelope; its duration is reserved by PostgreSQL first."""

    image: str
    session_seconds: int
    vcpus: int = ALLOCATION_VCPUS
    memory_mb: int = ALLOCATION_MEMORY_MB

    def __post_init__(self) -> None:
        if _DIGEST_IMAGE.fullmatch(self.image) is None:
            raise ValueError("sandbox image must include an immutable sha256 digest")
        if not 1 <= self.session_seconds <= MAX_ALLOCATION_SECONDS:
            raise ValueError(f"session_seconds must be between 1 and {MAX_ALLOCATION_SECONDS}")
        if self.vcpus != ALLOCATION_VCPUS or self.memory_mb != ALLOCATION_MEMORY_MB:
            raise ValueError("T16 sandbox resources are fixed at 2 vCPUs and 4096 MiB")


def validate_sandbox_name(name: str) -> str:
    """Accept only generated, non-user-derived provider names."""

    if _SANDBOX_NAME.fullmatch(name) is None:
        raise ValueError("sandbox name must be a lowercase provider-safe identifier")
    return name


def validate_fixture_filename(filename: str) -> str:
    """Keep fixture file operations inside one relative path component."""

    if (
        not filename
        or filename in {".", ".."}
        or "/" in filename
        or "\\" in filename
        or "\x00" in filename
        or len(filename) > 128
    ):
        raise ValueError("sandbox fixture filename must be a single safe path component")
    return filename


class SandboxSmokeSettings(BaseSettings):
    """Explicit hosted-smoke settings, separate from application startup."""

    model_config = SettingsConfigDict(
        env_prefix="CONTINUUM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    enable_sandbox_smoke: bool = False
    sandbox_image: str | None = None
    t04_smoke_id: str | None = None
    vercel_token: SecretStr | None = None
    vercel_team_id: str | None = None
    vercel_project_id: str | None = None

    def require_hosted_smoke(
        self, *, require_smoke_id: bool = True
    ) -> tuple[SandboxLimits, VercelCredentials, str | None]:
        """Require explicit one-off opt-in and complete control-plane config."""

        if not self.enable_sandbox_smoke:
            raise ValueError("set CONTINUUM_ENABLE_SANDBOX_SMOKE=1 to allow hosted sandbox calls")
        if self.sandbox_image is None or not self.sandbox_image.strip():
            raise ValueError("CONTINUUM_SANDBOX_IMAGE must be an immutable digest reference")
        if self.vercel_token is None or not self.vercel_token.get_secret_value().strip():
            raise ValueError("CONTINUUM_VERCEL_TOKEN is required for the hosted sandbox smoke")
        if self.vercel_team_id is None or not self.vercel_team_id.strip():
            raise ValueError("CONTINUUM_VERCEL_TEAM_ID is required for the hosted sandbox smoke")
        if self.vercel_project_id is None or not self.vercel_project_id.strip():
            raise ValueError("CONTINUUM_VERCEL_PROJECT_ID is required for the hosted sandbox smoke")
        limits = SandboxLimits(image=self.sandbox_image.strip())
        smoke_name: str | None = None
        if require_smoke_id:
            if self.t04_smoke_id is None or not self.t04_smoke_id.strip():
                raise ValueError("CONTINUUM_T04_SMOKE_ID must be a stable UUID for create fixtures")
            try:
                smoke_id = UUID(self.t04_smoke_id.strip())
            except ValueError:
                raise ValueError("CONTINUUM_T04_SMOKE_ID must be a canonical UUID") from None
            if str(smoke_id) != self.t04_smoke_id.strip().lower():
                raise ValueError("CONTINUUM_T04_SMOKE_ID must be a canonical UUID")
            smoke_name = f"continuum-t04-{smoke_id.hex}"
        credentials = VercelCredentials(
            token=self.vercel_token.get_secret_value(),
            team_id=self.vercel_team_id.strip(),
            project_id=self.vercel_project_id.strip(),
        )
        return limits, credentials, smoke_name


@dataclass(frozen=True, slots=True)
class VercelCredentials:
    """Control-plane credentials; never passed into a sandbox environment."""

    token: str = field(repr=False)
    team_id: str
    project_id: str
