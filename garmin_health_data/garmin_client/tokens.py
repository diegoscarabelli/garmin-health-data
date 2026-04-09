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
    :raises GarminAuthenticationError: If any required token field is missing,
        indicating the client was not fully authenticated before dumping.
    """

    missing = [
        k
        for k, v in (
            ("di_token", client.di_token),
            ("di_refresh_token", client.di_refresh_token),
            ("di_client_id", client.di_client_id),
        )
        if not v
    ]
    if missing:
        raise GarminAuthenticationError(
            f"Cannot serialize unauthenticated client; missing fields: {missing!r}"
        )
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

    Writes to a sibling temp file first, then atomically replaces the destination
    via ``os.replace`` so an interrupted write never leaves a truncated token store.

    :param client: GarminClient with populated DI fields.
    :param path: Directory or ``.json`` file path.
    :raises GarminConnectionError: If the token store cannot be written.
    """

    p = Path(path).expanduser()
    if p.is_dir() or not p.name.endswith(".json"):
        p = p / "garmin_tokens.json"

    content = dumps(client).encode()
    temp_path = p.parent / f".{p.name}.tmp.{os.getpid()}"
    needs_post_close_chmod = not hasattr(os, "fchmod")
    fd: int = -1

    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        # Write to a sibling temp file first, then atomically replace the
        # destination so an interrupted write does not leave a truncated token
        # store behind. O_EXCL ensures we own this temp file exclusively.
        fd = os.open(
            str(temp_path),
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            if not needs_post_close_chmod:
                os.fchmod(fd, 0o600)  # type: ignore[attr-defined]
            total_written = 0
            while total_written < len(content):
                written = os.write(fd, content[total_written:])
                if written == 0:
                    raise OSError("Failed to write token store to disk")
                total_written += written
        finally:
            os.close(fd)
            fd = -1

        if needs_post_close_chmod:
            os.chmod(str(temp_path), 0o600)

        os.replace(str(temp_path), str(p))
    except OSError as e:
        if fd >= 0:
            try:
                os.close(fd)
            except OSError:
                pass
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass
        raise GarminConnectionError(f"Token store dump() write failed: {e}") from e


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
