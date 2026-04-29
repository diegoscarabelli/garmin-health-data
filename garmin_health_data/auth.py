"""
Garmin Connect authentication module.

Handles OAuth token management with Garmin Connect, including Multi-Factor
Authentication (MFA) support.
"""

import os
import sys
from pathlib import Path
from typing import List, Tuple

import click
from garmin_health_data.garmin_client import GarminClient


def get_credentials() -> Tuple[str, str]:
    """
    Get Garmin Connect credentials from user input or environment variables.

    :return: Tuple of (email, password).
    """
    # Try environment variables first.
    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")

    if email and password:
        click.echo(
            click.style("📧 Using credentials from environment variables", fg="cyan")
        )
        click.echo(f"   Email: {email}")
        return email, password

    # Prompt for credentials interactively.
    click.echo(click.style("🔐 Garmin Connect Authentication", fg="cyan", bold=True))
    click.echo()

    email = click.prompt("   Email", type=str)
    password = click.prompt("   Password", type=str, hide_input=True)

    if not email or not password:
        raise click.ClickException("Email and password are required")

    return email, password


def get_mfa_code() -> str:
    """
    Prompt user for MFA code.

    :return: MFA code string.
    """
    click.echo()
    click.echo(click.style("🔢 Multi-Factor Authentication Required", fg="yellow"))
    click.echo("   Check your email or phone for the MFA code")
    click.echo()

    mfa_code = click.prompt("   Enter 6-digit MFA code", type=str)

    if not mfa_code.isdigit() or len(mfa_code) != 6:
        click.secho("⚠️  Warning: MFA code should be 6 digits", fg="yellow")

    return mfa_code


def _handle_mfa_authentication(garmin: GarminClient, result2) -> None:
    """
    Handle MFA authentication with one retry attempt.

    :param garmin: Garmin client instance.
    :param result2: MFA continuation token from login result.
    """
    click.secho("✅ Initial authentication successful", fg="green")

    for attempt in range(2):  # Allow 2 attempts.
        try:
            mfa_code = get_mfa_code()
            click.echo("🔢 Completing MFA authentication...")
            garmin.resume_login(result2, mfa_code)
            click.secho("✅ MFA authentication successful", fg="green", bold=True)
            return  # Success.

        except Exception as e:
            if attempt == 0:  # First attempt failed.
                click.secho(f"❌ MFA authentication failed: {str(e)}", fg="red")
                click.echo("🔄 Please try again with a fresh MFA code")
                continue
            # Second attempt failed.
            click.secho(
                f"❌ MFA authentication failed after 2 attempts", fg="red", bold=True
            )
            raise


def _print_troubleshooting() -> None:
    """
    Print common troubleshooting steps.
    """
    click.echo()
    click.secho("🔍 Troubleshooting:", fg="yellow", bold=True)
    click.echo("   - Verify your email and password are correct")
    click.echo("   - Check for typos or case sensitivity")
    click.echo("   - Ensure you have internet connectivity")
    click.echo("   - If MFA is enabled, make sure the MFA code is current")
    click.echo("   - Try running the command again")
    click.echo("   - Check if Garmin Connect services are operational")
    click.echo()


def discover_accounts(
    base_token_dir: str = "~/.garminconnect",
) -> List[Tuple[str, Path]]:
    """
    Discover Garmin Connect accounts by scanning token subdirectories.

    Each numeric subdirectory in the base token directory represents a user_id with
    saved OAuth tokens.

    :param base_token_dir: Base directory containing per-account token subdirectories.
    :return: Sorted list of (user_id, token_dir_path) tuples.
    :raises FileNotFoundError: If base directory does not exist.
    :raises NotADirectoryError: If base path is not a directory.
    :raises RuntimeError: If no accounts are found.
    """
    base_path = Path(base_token_dir).expanduser()

    if not base_path.exists():
        raise FileNotFoundError(f"Token directory does not exist: {base_path}")

    if not base_path.is_dir():
        raise NotADirectoryError(f"Token path is not a directory: {base_path}")

    # Scan for numeric subdirectories that contain token files.
    accounts = [
        (entry.name, entry)
        for entry in sorted(base_path.iterdir())
        if entry.is_dir() and entry.name.isdigit() and any(entry.iterdir())
    ]

    if accounts:
        return accounts

    # Legacy fallback: check for token files at root level.
    token_files = list(base_path.glob("*token*.json"))
    if token_files:
        click.secho(
            "Warning: Found legacy token layout (tokens at root level). "
            "Run 'garmin auth' to migrate to per-account subdirectories.",
            fg="yellow",
        )
        return [("legacy", base_path)]

    raise RuntimeError(
        f"No accounts found in {base_path}. Run 'garmin auth' to authenticate."
    )


def refresh_tokens(
    email: str,
    password: str,
    base_token_dir: str = "~/.garminconnect",
    silent: bool = False,
) -> None:
    """
    Refresh Garmin Connect tokens with MFA support.

    Authenticates the user, auto-detects their Garmin user ID, and stores tokens in a
    per-account subdirectory under base_token_dir.

    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param base_token_dir: Base directory for per-account token storage.
    :param silent: If True, suppress non-essential output.
    """
    base_path = Path(base_token_dir).expanduser()

    if not silent:
        click.echo()
        click.echo(click.style("🔄 Authenticating with Garmin Connect...", fg="cyan"))
        click.echo(f"   Token storage: {base_path}")
        click.echo()

    try:
        # Initialize vendored client and attempt login with MFA support.
        garmin = GarminClient()
        login_result = garmin.login(email, password, return_on_mfa=True)

        # Handle different return value formats.
        if isinstance(login_result, tuple) and len(login_result) == 2:
            result1, result2 = login_result

            # Handle MFA if required.
            if result1 == "needs_mfa":
                _handle_mfa_authentication(garmin, result2)
            else:
                if not silent:
                    click.secho(
                        "✅ Authentication successful (no MFA required)",
                        fg="green",
                        bold=True,
                    )
        else:
            # Handle case where login() returns single value or None (no MFA).
            if not silent:
                click.secho(
                    "✅ Authentication successful (no MFA required)",
                    fg="green",
                    bold=True,
                )

        # Auto-detect user ID from profile.
        user_id = garmin.get_user_profile().get("id")
        if not user_id:
            raise RuntimeError(
                "Could not determine user ID from Garmin profile. "
                "The 'id' field was missing from get_user_profile() response."
            )

        if not silent:
            click.echo(f"👤 Detected user ID: {user_id}")
            click.echo("💾 Saving authentication tokens...")

        # Ensure base and per-account token directories exist with proper permissions.
        base_path.mkdir(parents=True, exist_ok=True)
        if sys.platform != "win32":
            base_path.chmod(0o700)

        token_path = base_path / str(user_id)
        token_path.mkdir(exist_ok=True)
        if sys.platform != "win32":
            token_path.chmod(0o700)

        # dump() writes garmin_tokens.json with 0o600 from creation time.
        garmin.dump(str(token_path))

        if not silent:
            click.echo()
            click.secho("✅ Tokens successfully saved!", fg="green", bold=True)
            click.echo(f"   User ID:  {user_id}")
            click.echo(f"   Location: {token_path}")
            click.echo()
            click.secho(
                "🎉 Success! You're authenticated with Garmin Connect", fg="green"
            )
            click.echo("   You can now run: garmin extract")
            click.echo()
            click.echo("ℹ️  Tokens auto-refresh transparently during extraction.")

    except Exception as e:
        click.echo()
        click.secho(f"❌ Authentication failed: {str(e)}", fg="red", bold=True)
        _print_troubleshooting()
        raise click.ClickException("Authentication failed")


def check_authentication(base_token_dir: str = "~/.garminconnect") -> bool:
    """
    Check if valid authentication tokens exist for at least one account.

    :param base_token_dir: Base directory where per-account tokens are stored.
    :return: True if at least one account has tokens, False otherwise.
    """
    base_path = Path(base_token_dir).expanduser()

    if not base_path.exists():
        return False

    # Check for per-account subdirectories with tokens.
    for entry in base_path.iterdir():
        if entry.is_dir() and entry.name.isdigit() and any(entry.iterdir()):
            return True

    # Legacy fallback: check for token files at root level.
    return any(base_path.glob("*token*.json"))


def ensure_authenticated(base_token_dir: str = "~/.garminconnect") -> None:
    """
    Ensure user is authenticated, prompt for credentials if not.

    :param base_token_dir: Base directory where per-account tokens are stored.
    :raises click.ClickException: If authentication fails.
    """
    if not check_authentication(base_token_dir):
        click.echo()
        click.secho(
            "No authentication tokens found. Please authenticate first.",
            fg="yellow",
            bold=True,
        )
        click.echo()

        if click.confirm("Would you like to authenticate now?", default=True):
            email, password = get_credentials()
            refresh_tokens(email, password, base_token_dir)
        else:
            raise click.ClickException(
                "Authentication required. Run 'garmin auth' to authenticate."
            )
