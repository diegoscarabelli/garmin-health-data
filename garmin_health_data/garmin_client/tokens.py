"""
Token persistence for the vendored Garmin Connect client.

Tokens are stored as a JSON object with three keys:

- ``di_token``: short-lived (~18h) Bearer access token used in the
  ``Authorization`` header for API calls.
- ``di_refresh_token``: longer-lived (~30 days) refresh token used to mint new
  access tokens without re-entering credentials. Rotates on each use.
- ``di_client_id``: the DI OAuth2 client ID extracted from the JWT, needed when
  refreshing the access token.

The on-disk format is identical to the upstream ``python-garminconnect`` fork's
``Client.dump`` output, so existing token files migrate to this client without
re-bootstrapping.

Each function takes the ``GarminClient`` instance as its first argument so that the
client class can stay slim and delegate persistence here.
"""

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Union

from .exceptions import GarminAuthenticationError, GarminConnectionError

if TYPE_CHECKING:
    from .client import GarminClient


def dumps(client: "GarminClient") -> str:
    """
    Serialize a client's DI tokens to a JSON string.

    :param client: GarminClient with populated DI fields.
    :return: JSON string with ``di_token``, ``di_refresh_token``, ``di_client_id``.
    """

    data = {
        "di_token": client.di_token,
        "di_refresh_token": client.di_refresh_token,
        "di_client_id": client.di_client_id,
    }
    return json.dumps(data)


def dump(client: "GarminClient", path: Union[str, Path]) -> None:
    """
    Write a client's DI tokens to disk as ``garmin_tokens.json``.

    Accepts either a directory (in which case ``garmin_tokens.json`` is appended)
    or a ``.json`` file path. Creates parent directories as needed. The file mode
    is forced to ``0o600`` on every write so the secret tokens are never readable
    by other users, even if the file is freshly created (umask) or if a caller
    forgets to chmod after the initial bootstrap.

    :param client: GarminClient with populated DI fields.
    :param path: Directory or ``.json`` file path.
    """

    p = Path(path).expanduser()
    if p.is_dir() or not p.name.endswith(".json"):
        p = p / "garmin_tokens.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    content = dumps(client).encode()
    # Use os.open with an explicit mode so new files are created with 0o600
    # from the start, eliminating the write-then-chmod TOCTOU window where a
    # freshly created file briefly has umask-derived permissions (often 0o644).
    # os.fchmod re-asserts 0o600 on the open fd before any bytes are written,
    # which also covers pre-existing files whose permissions may have drifted.
    fd = os.open(str(p), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, content)
    finally:
        os.close(fd)


def loads(client: "GarminClient", tokenstore: str) -> None:
    """
    Load DI tokens into a client from a JSON string.

    :param client: GarminClient to populate.
    :param tokenstore: JSON string with ``di_token``, ``di_refresh_token``,
        ``di_client_id``.
    :raises GarminConnectionError: If the JSON is malformed.
    :raises GarminAuthenticationError: If the JSON parses but contains no token.
    """

    try:
        data = json.loads(tokenstore)
    except Exception as e:
        raise GarminConnectionError(
            f"Token extraction loads() structurally failed: {e}"
        ) from e

    client.di_token = data.get("di_token")
    client.di_refresh_token = data.get("di_refresh_token")
    client.di_client_id = data.get("di_client_id")
    # Validate all three fields up front so a corrupt or truncated tokenstore
    # raises a clear GarminAuthenticationError at load time rather than a
    # confusing KeyError or silent failure during a later token refresh.
    # di_refresh_token is required because _refresh_di_token() cannot run
    # without it. di_client_id is required because the refresh request uses
    # it to build the Authorization header and request body.
    missing = [
        k for k in ("di_token", "di_refresh_token", "di_client_id") if not data.get(k)
    ]
    if missing:
        raise GarminAuthenticationError(
            f"Token store missing required fields: {missing!r}"
        )


def load(client: "GarminClient", path: Union[str, Path]) -> None:
    """
    Load DI tokens into a client from disk.

    Accepts either a directory containing ``garmin_tokens.json`` or a direct
    ``.json`` file path. Records the resolved path on the client so that
    subsequent token refreshes can persist back to the same file.

    :param client: GarminClient to populate.
    :param path: Directory or ``.json`` file path.
    :raises GarminConnectionError: If the file is missing or unreadable, or if
        the JSON is malformed.
    """

    try:
        p = Path(path).expanduser()
        if p.is_dir() or not p.name.endswith(".json"):
            p = p / "garmin_tokens.json"
        # Record the resolved file path (after expansion + directory->json
        # normalization) so refresh persistence writes back to the same file.
        client._tokenstore_path = str(p)
        loads(client, p.read_text())
    except (GarminAuthenticationError, GarminConnectionError):
        raise
    except Exception as e:
        raise GarminConnectionError(f"Token path not loading cleanly: {e}") from e
