"""
Extended tests for authentication module.

This test suite provides comprehensive coverage of:
    - Credential management (environment variables and user input).
    - Garmin Connect authentication flows (with and without MFA).
    - Token storage and retrieval.
    - Error handling for authentication failures.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from garmin_health_data.auth import (
    check_authentication,
    get_credentials,
    get_mfa_code,
    refresh_tokens,
)


class TestGetCredentials:
    """
    Test class for credential retrieval functionality.
    """

    @patch.dict(
        os.environ,
        {"GARMIN_EMAIL": "env@example.com", "GARMIN_PASSWORD": "env_pass"},
    )
    @patch("click.echo")
    def test_get_credentials_from_env(self, mock_echo: MagicMock) -> None:
        """
        Test getting credentials from environment variables.

        :param mock_echo: Mock click.echo function.
        """
        email, password = get_credentials()

        assert email == "env@example.com"
        assert password == "env_pass"
        mock_echo.assert_called()

    @patch.dict(os.environ, {}, clear=True)
    @patch("click.prompt", side_effect=["user@example.com", "user_password"])
    def test_get_credentials_from_input(self, mock_prompt: MagicMock) -> None:
        """
        Test getting credentials from user input.

        :param mock_prompt: Mock click.prompt function.
        """
        email, password = get_credentials()

        assert email == "user@example.com"
        assert password == "user_password"
        assert mock_prompt.call_count == 2

    @patch.dict(os.environ, {}, clear=True)
    @patch("click.prompt", side_effect=["", "password"])
    def test_get_credentials_empty_email(self, mock_prompt: MagicMock) -> None:
        """
        Test error handling when email is empty.

        :param mock_prompt: Mock click.prompt function.
        """
        with pytest.raises(Exception):
            get_credentials()


class TestGetMfaCode:
    """
    Test class for MFA code input functionality.
    """

    @patch("click.prompt", return_value="123456")
    def test_get_mfa_code_valid(self, mock_prompt: MagicMock) -> None:
        """
        Test successful MFA code input with valid 6-digit code.

        :param mock_prompt: Mock click.prompt function.
        """
        mfa_code = get_mfa_code()

        assert mfa_code == "123456"
        mock_prompt.assert_called_once()

    @patch("click.prompt", return_value="abc123")
    def test_get_mfa_code_invalid_format(self, mock_prompt: MagicMock) -> None:
        """
        Test MFA code input with non-numeric format.

        :param mock_prompt: Mock click.prompt function.
        """
        mfa_code = get_mfa_code()

        # Function still returns the code.
        assert mfa_code == "abc123"


class TestCheckAuthentication:
    """
    Test class for authentication status checking.
    """

    def test_check_authentication_no_dir(self, tmp_path: Path) -> None:
        """
        Test checking authentication when token directory doesn't exist.

        :param tmp_path: Pytest temporary directory fixture.
        """
        token_dir = tmp_path / "nonexistent"
        assert not check_authentication(str(token_dir))

    def test_check_authentication_empty_dir(self, tmp_path: Path) -> None:
        """
        Test checking authentication with empty token directory.

        :param tmp_path: Pytest temporary directory fixture.
        """
        token_dir = tmp_path / "tokens"
        token_dir.mkdir()
        assert not check_authentication(str(token_dir))

    def test_check_authentication_with_tokens(self, tmp_path: Path) -> None:
        """
        Test checking authentication when tokens exist.

        :param tmp_path: Pytest temporary directory fixture.
        """
        token_dir = tmp_path / "tokens"
        token_dir.mkdir()
        (token_dir / "oauth1_token.txt").write_text("test_token")

        assert check_authentication(str(token_dir))


class TestRefreshTokens:
    """
    Test class for Garmin Connect token refresh flow.
    """

    @patch("garmin_health_data.auth.Garmin")
    @patch("click.echo")
    def test_refresh_tokens_success_no_mfa(
        self,
        mock_echo: MagicMock,
        mock_garmin_class: MagicMock,
    ) -> None:
        """
        Test successful token refresh without MFA.

        :param mock_echo: Mock click.echo function.
        :param mock_garmin_class: Mock Garmin class.
        """
        mock_client = MagicMock()
        mock_client.login.return_value = None
        mock_client.garth = MagicMock()
        mock_garmin_class.return_value = mock_client

        refresh_tokens("test@example.com", "password123")

        mock_garmin_class.assert_called_once()
        mock_client.login.assert_called_once()
        mock_client.garth.dump.assert_called_once()

    @patch("garmin_health_data.auth.Garmin")
    @patch("garmin_health_data.auth.get_mfa_code")
    @patch("click.echo")
    def test_refresh_tokens_success_with_mfa(
        self,
        mock_echo: MagicMock,
        mock_get_mfa: MagicMock,
        mock_garmin_class: MagicMock,
    ) -> None:
        """
        Test successful token refresh with MFA.

        :param mock_echo: Mock click.echo function.
        :param mock_get_mfa: Mock get_mfa_code function.
        :param mock_garmin_class: Mock Garmin class.
        """
        mock_get_mfa.return_value = "123456"
        mock_client = MagicMock()
        mock_client.login.return_value = ("needs_mfa", "mfa_token")
        mock_client.garth = MagicMock()
        mock_garmin_class.return_value = mock_client

        refresh_tokens("test@example.com", "password123")

        mock_garmin_class.assert_called_once()
        mock_client.login.assert_called_once()
        mock_client.resume_login.assert_called_once_with("mfa_token", "123456")
        mock_client.garth.dump.assert_called_once()

    @patch("garmin_health_data.auth.Garmin")
    @patch("click.echo")
    @patch("click.secho")
    def test_refresh_tokens_failure_invalid_credentials(
        self,
        mock_secho: MagicMock,
        mock_echo: MagicMock,
        mock_garmin_class: MagicMock,
    ) -> None:
        """
        Test token refresh failure with invalid credentials.

        :param mock_secho: Mock click.secho function.
        :param mock_echo: Mock click.echo function.
        :param mock_garmin_class: Mock Garmin class.
        """
        import click

        mock_client = MagicMock()
        mock_client.login.side_effect = Exception("401 Unauthorized")
        mock_garmin_class.return_value = mock_client

        with pytest.raises(click.ClickException, match="Authentication failed"):
            refresh_tokens("invalid@example.com", "wrong_password")

        # Should not call garth.dump if login fails.
        mock_client.garth.dump.assert_not_called()

    @patch("garmin_health_data.auth.Garmin")
    @patch("garmin_health_data.auth.get_mfa_code")
    @patch("click.echo")
    def test_refresh_tokens_mfa_retry_on_failure(
        self,
        mock_echo: MagicMock,
        mock_get_mfa: MagicMock,
        mock_garmin_class: MagicMock,
    ) -> None:
        """
        Test MFA authentication with retry on first failure.

        :param mock_echo: Mock click.echo function.
        :param mock_get_mfa: Mock get_mfa_code function.
        :param mock_garmin_class: Mock Garmin class.
        """
        mock_get_mfa.side_effect = ["000000", "123456"]
        mock_client = MagicMock()
        mock_client.login.return_value = ("needs_mfa", "mfa_token")
        mock_client.resume_login.side_effect = [
            Exception("Invalid MFA"),
            None,
        ]
        mock_client.garth = MagicMock()
        mock_garmin_class.return_value = mock_client

        refresh_tokens("test@example.com", "password123")

        assert mock_client.resume_login.call_count == 2
        mock_client.garth.dump.assert_called_once()
