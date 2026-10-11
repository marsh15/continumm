"""Bounded HTTPX adapter for private Supabase Storage objects."""

from __future__ import annotations

import asyncio
import json
import re
import time
from enum import StrEnum
from urllib.parse import quote, urlsplit

import httpx

from continuum_api.connectivity.database import EffectState

_BUCKET_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")


class StorageFailure(RuntimeError):
    """A redacted Storage failure with mutation-effect knowledge."""

    def __init__(self, code: str, *, effect: EffectState = EffectState.KNOWN) -> None:
        super().__init__(code)
        self.code = code
        self.effect = effect


class StoragePhase(StrEnum):
    """Named operations surfaced in the smoke evidence."""

    BUCKET_CHECK = "bucket_check"
    UPLOAD = "upload"
    DOWNLOAD = "download"
    DELETE = "delete"


class SupabaseStorage:
    """Issue bounded REST calls against one configured private bucket."""

    def __init__(
        self,
        project_url: str,
        secret_key: str,
        bucket: str,
        *,
        client: httpx.AsyncClient | None = None,
        fixture_byte_limit: int = 1024,
        request_deadline_seconds: float = 12.0,
    ) -> None:
        parsed_url = urlsplit(project_url)
        try:
            port = parsed_url.port
        except ValueError:
            port_is_standard = False
        else:
            port_is_standard = port is None or port == 443
        if (
            parsed_url.scheme != "https"
            or parsed_url.hostname is None
            or not parsed_url.hostname.endswith(".supabase.co")
            or parsed_url.username is not None
            or parsed_url.password is not None
            or parsed_url.path not in {"", "/"}
            or parsed_url.query
            or parsed_url.fragment
            or not port_is_standard
        ):
            raise StorageFailure("storage_url_invalid")
        if not secret_key.startswith("sb_secret_"):
            raise StorageFailure("storage_secret_key_invalid")
        if not _BUCKET_PATTERN.fullmatch(bucket):
            raise StorageFailure("storage_bucket_invalid")
        if fixture_byte_limit < 1:
            raise ValueError("fixture byte limit must be positive")
        if request_deadline_seconds <= 0:
            raise ValueError("request deadline must be positive")

        self._bucket = bucket
        self._api_key = secret_key
        self._fixture_byte_limit = fixture_byte_limit
        self._request_deadline_seconds = request_deadline_seconds
        self._base_url = project_url.rstrip("/") + "/storage/v1"
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3, read=5, write=5, pool=2),
            follow_redirects=False,
            trust_env=False,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def check_private_bucket(self) -> None:
        url = f"{self._base_url}/bucket/{quote(self._bucket, safe='')}"
        deadline = time.monotonic() + self._request_deadline_seconds
        try:
            async with asyncio.timeout(self._request_deadline_seconds):
                async with self._client.stream(
                    "GET", url, headers=self._auth_headers()
                ) as response:
                    if response.status_code < 200 or response.status_code >= 300:
                        raise StorageFailure(_http_error_code(response.status_code, "bucket_check"))
                    body = await _read_bounded(
                        response,
                        16 * 1024,
                        StoragePhase.BUCKET_CHECK,
                        deadline=deadline,
                    )
        except StorageFailure:
            raise
        except TimeoutError:
            raise StorageFailure("bucket_check_timeout") from None
        except httpx.TimeoutException:
            raise StorageFailure("bucket_check_timeout") from None
        except httpx.RequestError:
            raise StorageFailure("bucket_check_transport") from None

        try:
            metadata = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise StorageFailure("bucket_metadata_invalid") from None
        if not isinstance(metadata, dict) or metadata.get("public") is not False:
            raise StorageFailure("bucket_not_private")

    async def upload(self, object_key: str, content: bytes) -> None:
        if len(content) > self._fixture_byte_limit:
            raise StorageFailure("fixture_too_large")
        if not _valid_object_key(object_key):
            raise StorageFailure("object_key_invalid")
        url = self._object_url(object_key)
        deadline = time.monotonic() + self._request_deadline_seconds
        try:
            async with asyncio.timeout(self._request_deadline_seconds):
                async with self._client.stream(
                    "POST",
                    url,
                    headers={
                        **self._auth_headers(),
                        "content-type": "application/octet-stream",
                        "x-upsert": "false",
                    },
                    content=content,
                ) as response:
                    _require_success(response.status_code, mutation=True, operation="upload")
                    try:
                        await _read_bounded(
                            response,
                            8 * 1024,
                            StoragePhase.UPLOAD,
                            deadline=deadline,
                        )
                    except StorageFailure as error:
                        raise StorageFailure(error.code, effect=EffectState.UNKNOWN) from None
        except StorageFailure:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise StorageFailure("upload_outcome_unknown", effect=EffectState.UNKNOWN) from None
        except httpx.RequestError:
            raise StorageFailure("upload_outcome_unknown", effect=EffectState.UNKNOWN) from None

    async def download(self, object_key: str) -> bytes:
        if not _valid_object_key(object_key):
            raise StorageFailure("object_key_invalid")
        url = self._authenticated_object_url(object_key)
        deadline = time.monotonic() + self._request_deadline_seconds
        try:
            async with asyncio.timeout(self._request_deadline_seconds):
                async with self._client.stream(
                    "GET", url, headers=self._auth_headers()
                ) as response:
                    _require_success(response.status_code, mutation=False, operation="download")
                    return await _read_bounded(
                        response,
                        self._fixture_byte_limit,
                        StoragePhase.DOWNLOAD,
                        deadline=deadline,
                    )
        except StorageFailure:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise StorageFailure("download_timeout") from None
        except httpx.RequestError:
            raise StorageFailure("download_transport") from None

    async def delete(self, object_key: str) -> None:
        if not _valid_object_key(object_key):
            raise StorageFailure("object_key_invalid")
        url = f"{self._base_url}/object/{quote(self._bucket, safe='')}"
        deadline = time.monotonic() + self._request_deadline_seconds
        try:
            async with asyncio.timeout(self._request_deadline_seconds):
                async with self._client.stream(
                    "DELETE",
                    url,
                    headers=self._auth_headers(),
                    json={"prefixes": [object_key]},
                ) as response:
                    _require_success(response.status_code, mutation=True, operation="delete")
                    try:
                        await _read_bounded(
                            response,
                            8 * 1024,
                            StoragePhase.DELETE,
                            deadline=deadline,
                        )
                    except StorageFailure as error:
                        raise StorageFailure(error.code, effect=EffectState.UNKNOWN) from None
        except StorageFailure:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise StorageFailure("delete_outcome_unknown", effect=EffectState.UNKNOWN) from None
        except httpx.RequestError:
            raise StorageFailure("delete_outcome_unknown", effect=EffectState.UNKNOWN) from None

    async def verify_absent(self, object_key: str) -> None:
        """Confirm that one deleted private object now returns authenticated 404."""
        if not _valid_object_key(object_key):
            raise StorageFailure("object_key_invalid")
        url = self._authenticated_object_url(object_key)
        try:
            async with asyncio.timeout(self._request_deadline_seconds):
                async with self._client.stream(
                    "GET", url, headers=self._auth_headers()
                ) as response:
                    if response.status_code == 404:
                        return
                    if 200 <= response.status_code < 300:
                        raise StorageFailure("delete_not_confirmed")
                    raise StorageFailure(
                        "delete_verify_outcome_unknown", effect=EffectState.UNKNOWN
                    )
        except StorageFailure:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise StorageFailure(
                "delete_verify_outcome_unknown", effect=EffectState.UNKNOWN
            ) from None
        except httpx.RequestError:
            raise StorageFailure(
                "delete_verify_outcome_unknown", effect=EffectState.UNKNOWN
            ) from None

    def _object_url(self, object_key: str) -> str:
        if not _valid_object_key(object_key):
            raise StorageFailure("object_key_invalid")
        return f"{self._base_url}/object/{quote(self._bucket, safe='')}/{_quote_key(object_key)}"

    def _authenticated_object_url(self, object_key: str) -> str:
        return (
            f"{self._base_url}/object/authenticated/{quote(self._bucket, safe='')}/"
            f"{_quote_key(object_key)}"
        )

    def _auth_headers(self) -> dict[str, str]:
        """Return a request-scoped key so an injected client cannot leak it."""
        return {"apikey": self._api_key}


async def _read_bounded(
    response: httpx.Response,
    byte_limit: int,
    phase: StoragePhase,
    *,
    deadline: float,
) -> bytes:
    content_length = response.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > byte_limit:
                raise StorageFailure(f"{phase.value}_response_too_large")
        except ValueError:
            pass

    result = bytearray()
    async for chunk in response.aiter_bytes():
        if time.monotonic() > deadline:
            raise StorageFailure(f"{phase.value}_response_timeout")
        if len(result) + len(chunk) > byte_limit:
            raise StorageFailure(f"{phase.value}_response_too_large")
        result.extend(chunk)
    return bytes(result)


def _require_success(status_code: int, *, mutation: bool, operation: str) -> None:
    if 200 <= status_code < 300:
        return
    outcome = (
        EffectState.UNKNOWN if mutation and not 400 <= status_code < 500 else EffectState.KNOWN
    )
    raise StorageFailure(_http_error_code(status_code, operation), effect=outcome)


def _http_error_code(status_code: int, operation: str) -> str:
    if 400 <= status_code < 500:
        return f"{operation}_http_4xx"
    if 500 <= status_code < 600:
        return f"{operation}_http_5xx"
    return f"{operation}_http_unexpected_status"


def _quote_key(object_key: str) -> str:
    return quote(object_key, safe="/-._~")


def _valid_object_key(value: str) -> bool:
    return bool(value) and not value.startswith("/") and ".." not in value.split("/")
