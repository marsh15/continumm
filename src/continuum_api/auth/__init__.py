"""GitHub OAuth, browser sessions and encrypted GitHub user credentials."""

from continuum_api.auth.config import AuthConfig
from continuum_api.auth.oauth import GitHubOAuthClient, GitHubOAuthError, GitHubOAuthResult
from continuum_api.auth.sessions import (
    AuthPrincipal,
    GitHubUserTokenStore,
    OAuthRateLimiter,
    OAuthState,
    OAuthStateStore,
    SessionCredentials,
    SessionService,
)

__all__ = [
    "AuthConfig",
    "AuthPrincipal",
    "GitHubOAuthClient",
    "GitHubOAuthError",
    "GitHubOAuthResult",
    "GitHubUserTokenStore",
    "OAuthRateLimiter",
    "OAuthState",
    "OAuthStateStore",
    "SessionCredentials",
    "SessionService",
]
