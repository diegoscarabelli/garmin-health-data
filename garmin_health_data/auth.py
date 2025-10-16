"""
Garmin Connect authentication module.

Handles OAuth token management with Garmin Connect, including Multi-Factor
Authentication (MFA) support.
"""

import os
from pathlib import Path
from typing import Tuple

import click
from garminconnect import Garmin


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


def _handle_mfa_authentication(garmin: Garmin, result2) -> None:
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


def refresh_tokens(
    email: str,
    password: str,
    token_dir: str = "~/.garminconnect",
    silent: bool = False,
) -> None:
    """
    Refresh Garmin Connect tokens with MFA support.

    :param email: Garmin Connect email.
    :param password: Garmin Connect password.
    :param token_dir: Directory to store tokens.
    :param silent: If True, suppress non-essential output.
    """
    token_path = Path(token_dir).expanduser()

    if not silent:
        click.echo()
        click.echo(click.style("🔄 Authenticating with Garmin Connect...", fg="cyan"))
        click.echo(f"   Token storage: {token_path}")
        click.echo()

    try:
        # Initialize Garmin client with MFA support.
        garmin = Garmin(email=email, password=password, is_cn=False, return_on_mfa=True)

        # Attempt login.
        login_result = garmin.login()

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

        if not silent:
            click.echo("💾 Saving authentication tokens...")

        # Ensure token directory exists with proper permissions.
        token_path.mkdir(parents=True, exist_ok=True)
        token_path.chmod(0o755)

        garmin.garth.dump(str(token_path))

        if not silent:
            click.echo()
            click.secho("✅ Tokens successfully saved!", fg="green", bold=True)
            click.echo(f"   Location: {token_path}")
            click.echo()
            click.secho(
                "🎉 Success! You're authenticated with Garmin Connect", fg="green"
            )
            click.echo("   You can now run: garmin extract")
            click.echo()
            click.echo("ℹ️  Tokens are valid for approximately 1 year")

    except Exception as e:
        click.echo()
        click.secho(f"❌ Authentication failed: {str(e)}", fg="red", bold=True)
        _print_troubleshooting()
        raise click.ClickException("Authentication failed")


def check_authentication(token_dir: str = "~/.garminconnect") -> bool:
    """
    Check if valid authentication tokens exist.

    :param token_dir: Directory where tokens are stored.
    :return: True if tokens exist, False otherwise.
    """
    token_path = Path(token_dir).expanduser()

    # Check if token directory exists and has files.
    if not token_path.exists():
        return False

    # The Garth library stores tokens in this directory.
    # If the directory exists and is not empty, assume we have tokens.
    return any(token_path.iterdir())


def ensure_authenticated(token_dir: str = "~/.garminconnect") -> None:
    """
    Ensure user is authenticated, prompt for credentials if not.

    :param token_dir: Directory where tokens are stored.
    :raises click.ClickException: If authentication fails.
    """
    if not check_authentication(token_dir):
        click.echo()
        click.secho(
            "⚠️  No authentication tokens found. Please authenticate first.",
            fg="yellow",
            bold=True,
        )
        click.echo()

        if click.confirm("Would you like to authenticate now?", default=True):
            email, password = get_credentials()
            refresh_tokens(email, password, token_dir)
        else:
            raise click.ClickException(
                "Authentication required. Run 'garmin auth' to authenticate."
            )
