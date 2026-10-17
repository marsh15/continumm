"""Public wiring for GitHub authorization providers used across runtime boundaries."""

from __future__ import annotations

from dataclasses import dataclass

import httpx
from pydantic import SecretStr

from continuum_api.auth.github import (
    GitHubAccessVerifier,
    GitHubAuthorizationClient,
    GitHubAuthorizationError,
    GitHubPublicationFactsProvider,
    GitHubUserTokenStore,
)


@dataclass(frozen=True, slots=True)
class GitHubAuthorizationAdapters:
    """Share one configured client across preflight and publication fact checks."""

    client: GitHubAuthorizationClient
    access_verifier: GitHubAccessVerifier
    publication_facts: GitHubPublicationFactsProvider


def create_github_authorization_adapters(
    *,
    app_id: int,
    app_private_key: SecretStr,
    user_tokens: GitHubUserTokenStore,
    transport: httpx.AsyncBaseTransport | None = None,
) -> GitHubAuthorizationAdapters:
    """Construct both plugs over one live authorization client and token source."""
    client = GitHubAuthorizationClient(
        app_id=app_id,
        app_private_key=app_private_key,
        user_tokens=user_tokens,
        transport=transport,
    )
    return GitHubAuthorizationAdapters(
        client=client,
        access_verifier=GitHubAccessVerifier(client),
        publication_facts=GitHubPublicationFactsProvider(client),
    )


__all__ = [
    "GitHubAccessVerifier",
    "GitHubAuthorizationAdapters",
    "GitHubAuthorizationClient",
    "GitHubAuthorizationError",
    "GitHubPublicationFactsProvider",
    "GitHubUserTokenStore",
    "create_github_authorization_adapters",
]
