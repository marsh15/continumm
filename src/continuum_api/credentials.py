"""Server-only authenticated encryption for owner-scoped provider credentials."""

from __future__ import annotations

import base64
import binascii
import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import SecretStr

MAX_CREDENTIAL_BYTES = 4096
_VERSION = re.compile(r"^[A-Za-z0-9_-]{1,32}$")


class CredentialError(RuntimeError):
    """Errors deliberately carry no submitted values or key material."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class CredentialProvider(StrEnum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GITHUB = "github"


class CredentialScope(StrEnum):
    SAVED = "saved"
    RUN = "run"


@dataclass(frozen=True, slots=True)
class CredentialIdentity:
    credential_id: UUID
    owner_id: UUID
    provider: CredentialProvider
    scope: CredentialScope
    run_id: UUID | None = None

    def associated_data(self, key_version: str) -> bytes:
        if (
            not isinstance(self.credential_id, UUID)
            or not isinstance(self.owner_id, UUID)
            or not isinstance(self.provider, CredentialProvider)
            or not isinstance(self.scope, CredentialScope)
            or (self.scope is CredentialScope.RUN) != isinstance(self.run_id, UUID)
            or (self.scope is CredentialScope.SAVED and self.run_id is not None)
            or not isinstance(key_version, str)
            or _VERSION.fullmatch(key_version) is None
        ):
            raise CredentialError("credential_identity_invalid")
        return json.dumps(
            {
                "format": "continuum-provider-credential-v1",
                "credential_id": str(self.credential_id),
                "owner_id": str(self.owner_id),
                "provider": self.provider.value,
                "scope": self.scope.value,
                "run_id": None if self.run_id is None else str(self.run_id),
                "key_version": key_version,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")


@dataclass(frozen=True, slots=True)
class CredentialEnvelope:
    key_version: str
    nonce: bytes = field(repr=False)
    ciphertext: bytes = field(repr=False)


class CredentialCipher:
    """AES-256-GCM; keys stay in deployment configuration, never in the DB.

    Source: https://cryptography.io/en/50.0.2/hazmat/primitives/aead/
    Each encryption uses a fresh CSPRNG 96-bit nonce and the full 128-bit tag.
    """

    def __init__(self, active_version: str, keys: Mapping[str, bytes]) -> None:
        if (
            not isinstance(active_version, str)
            or _VERSION.fullmatch(active_version) is None
            or not 1 <= len(keys) <= 8
            or active_version not in keys
            or any(
                not isinstance(version, str)
                or _VERSION.fullmatch(version) is None
                or not isinstance(key, bytes)
                or len(key) != 32
                for version, key in keys.items()
            )
            or len(set(keys.values())) != len(keys)
        ):
            raise CredentialError("credential_keyring_invalid")
        self.active_version = active_version
        self._keys = dict(keys)

    def __repr__(self) -> str:
        return f"CredentialCipher(active_version={self.active_version!r}, keys=[REDACTED])"

    @classmethod
    def from_settings(cls, settings: object) -> CredentialCipher:
        current = getattr(settings, "encryption_key", None)
        active = getattr(settings, "encryption_key_version", "v1")
        previous = getattr(settings, "encryption_previous_keys", None)
        if not isinstance(current, SecretStr):
            raise CredentialError("credential_keyring_unconfigured")
        previous_values: dict[str, str] = {}
        if previous is not None:
            if not isinstance(previous, SecretStr) or len(previous.get_secret_value()) > 8192:
                raise CredentialError("credential_keyring_invalid")
            try:
                parsed = json.loads(previous.get_secret_value(), object_pairs_hook=_unique_keys)
                if not isinstance(parsed, dict) or any(
                    not isinstance(key, str) or not isinstance(value, str)
                    for key, value in parsed.items()
                ):
                    raise ValueError
                previous_values = parsed
            except (TypeError, ValueError, RecursionError):
                raise CredentialError("credential_keyring_invalid") from None
        if active in previous_values:
            raise CredentialError("credential_keyring_invalid")
        encoded = {**previous_values, active: current.get_secret_value()}
        return cls(
            active, {version: _decode_master_key(value) for version, value in encoded.items()}
        )

    def encrypt(self, identity: CredentialIdentity, secret: SecretStr) -> CredentialEnvelope:
        plaintext = _secret_bytes(secret)
        aad = identity.associated_data(self.active_version)
        nonce = os.urandom(12)
        ciphertext = AESGCM(self._keys[self.active_version]).encrypt(nonce, plaintext, aad)
        return CredentialEnvelope(self.active_version, nonce, ciphertext)

    def decrypt(self, identity: CredentialIdentity, envelope: CredentialEnvelope) -> SecretStr:
        if (
            not isinstance(envelope, CredentialEnvelope)
            or not isinstance(envelope.key_version, str)
            or _VERSION.fullmatch(envelope.key_version) is None
            or not isinstance(envelope.nonce, bytes)
            or len(envelope.nonce) != 12
            or not isinstance(envelope.ciphertext, bytes)
            or not 17 <= len(envelope.ciphertext) <= MAX_CREDENTIAL_BYTES + 16
        ):
            raise CredentialError("credential_ciphertext_invalid")
        key = self._keys.get(envelope.key_version)
        if key is None:
            raise CredentialError("credential_key_version_unavailable")
        try:
            plaintext = AESGCM(key).decrypt(
                envelope.nonce, envelope.ciphertext, identity.associated_data(envelope.key_version)
            )
            secret = SecretStr(plaintext.decode("ascii"))
            _secret_bytes(secret)
            return secret
        except (InvalidTag, UnicodeError, ValueError):
            raise CredentialError("credential_authentication_failed") from None

    def rotate(
        self, identity: CredentialIdentity, envelope: CredentialEnvelope
    ) -> CredentialEnvelope:
        return self.encrypt(identity, self.decrypt(identity, envelope))


def _unique_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate_key")
        result[key] = value
    return result


def _decode_master_key(encoded: str) -> bytes:
    try:
        key = base64.b64decode(encoded, altchars=b"-_", validate=True)
        if len(key) != 32 or base64.urlsafe_b64encode(key).decode("ascii") != encoded:
            raise ValueError
        return key
    except (ValueError, UnicodeError, binascii.Error):
        raise CredentialError("credential_keyring_invalid") from None


def _secret_bytes(secret: SecretStr) -> bytes:
    if not isinstance(secret, SecretStr):
        raise CredentialError("credential_secret_invalid")
    value = secret.get_secret_value()
    if not 1 <= len(value) <= MAX_CREDENTIAL_BYTES or any(
        not 0x21 <= ord(character) <= 0x7E for character in value
    ):
        raise CredentialError("credential_secret_invalid")
    return value.encode("ascii")
