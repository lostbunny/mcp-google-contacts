"""Credential manager protocol and implementations.

Provides a pluggable interface for retrieving and storing OAuth credentials.
Implement the CredentialManager protocol to add support for any credential store.

Keys used across the protocol:
  "client_id"     — OAuth app client ID
  "client_secret" — OAuth app client secret
  "refresh_token" — OAuth refresh token (long-lived, stored after first auth)
"""

import subprocess
from typing import Optional, Protocol, runtime_checkable


@runtime_checkable
class CredentialManager(Protocol):
    """Protocol for pluggable credential storage backends."""

    def get(self, key: str) -> Optional[str]:
        """Retrieve a credential value by key. Returns None if not found."""
        ...

    def set(self, key: str, value: str) -> None:
        """Store a credential value by key."""
        ...


class OnePasswordCredentialManager:
    """Reads and writes credentials using the 1Password CLI (op).

    Args:
        op_path:              Full path to the op executable.
        vault:                1Password vault name (e.g. "DevVault").
        oauth_item:           Item name holding client_id (username) and
                              client_secret (password).
        refresh_token_item:   Item name holding the refresh token (password field).
    """

    def __init__(
        self,
        op_path: str,
        vault: str,
        oauth_item: str,
        refresh_token_item: str,
    ):
        self._op = op_path
        self._vault = vault
        self._oauth_item = oauth_item
        self._refresh_token_item = refresh_token_item

    def get(self, key: str) -> Optional[str]:
        """Read a credential from 1Password."""
        ref = self._ref_for(key)
        if ref is None:
            return None
        try:
            result = subprocess.run(
                [self._op, "read", ref],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip() or None
        except subprocess.CalledProcessError:
            return None

    def set(self, key: str, value: str) -> None:
        """Write a credential back to 1Password."""
        if key == "refresh_token":
            subprocess.run(
                [
                    self._op, "item", "edit", self._refresh_token_item,
                    f"password={value}",
                    f"--vault={self._vault}",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
        # client_id and client_secret are read-only from the server's perspective

    def _ref_for(self, key: str) -> Optional[str]:
        if key == "client_id":
            return f"op://{self._vault}/{self._oauth_item}/username"
        if key == "client_secret":
            return f"op://{self._vault}/{self._oauth_item}/password"
        if key == "refresh_token":
            return f"op://{self._vault}/{self._refresh_token_item}/password"
        return None
