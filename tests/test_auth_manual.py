"""
Tests for the manual browser-ticket authentication bootstrap.

Covers ticket/URL parsing, the DI-token exchange bootstrap, the interactive manual login
flow, and CLI routing for the ``--manual`` / ``--ticket`` options used when Cloudflare
blocks the automated email/password login.
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import click
import pytest
from click.testing import CliRunner

from garmin_health_data.auth import (
    MANUAL_SERVICE_URL,
    _parse_manual_ticket,
    bootstrap_from_ticket,
    manual_auth,
)
from garmin_health_data.cli import auth


class TestParseManualTicket:
    """
    Test class for parsing pasted manual-login values.
    """

    def test_bare_ticket_uses_default_service(self) -> None:
        """
        A raw ST- ticket returns the default mobile service URL.
        """
        ticket, service = _parse_manual_ticket("ST-123-cas")
        assert ticket == "ST-123-cas"
        assert service == MANUAL_SERVICE_URL

    def test_bare_ticket_strips_whitespace(self) -> None:
        """
        Surrounding whitespace is trimmed from a bare ticket.
        """
        ticket, service = _parse_manual_ticket("  ST-123-cas\n")
        assert ticket == "ST-123-cas"
        assert service == MANUAL_SERVICE_URL

    def test_full_url_parses_ticket_and_service(self) -> None:
        """
        A full redirect URL yields the ticket and the service URL (without query).
        """
        ticket, service = _parse_manual_ticket(
            "https://connect.garmin.com/app?ticket=ST-9-sso"
        )
        assert ticket == "ST-9-sso"
        assert service == "https://connect.garmin.com/app"

    def test_url_without_ticket_raises(self) -> None:
        """
        A URL with no ticket parameter raises a ClickException.
        """
        with pytest.raises(click.ClickException):
            _parse_manual_ticket("https://connect.garmin.com/app")


class TestBootstrapFromTicket:
    """
    Test class for the ticket-to-token bootstrap.
    """

    @patch("garmin_health_data.auth.GarminClient")
    @patch("click.echo")
    def test_success_saves_tokens(
        self,
        mock_echo: MagicMock,
        mock_garmin_class: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        A successful exchange saves tokens under a per-account subdirectory.

        :param mock_echo: Mock click.echo function.
        :param mock_garmin_class: Mock Garmin client class.
        :param tmp_path: Per-test token directory.
        """
        mock_garmin = MagicMock()
        mock_garmin.get_user_profile.return_value = {"id": "12345678"}
        mock_garmin_class.return_value = mock_garmin

        user_id = bootstrap_from_ticket("ST-abc-cas", base_token_dir=str(tmp_path))

        assert user_id == "12345678"
        mock_garmin._exchange_service_ticket.assert_called_once_with(
            "ST-abc-cas", service_url=MANUAL_SERVICE_URL
        )
        mock_garmin.dump.assert_called_once()
        assert (tmp_path / "12345678").is_dir()

    @patch("garmin_health_data.auth.GarminClient")
    @patch("click.echo")
    def test_full_url_uses_parsed_service(
        self,
        mock_echo: MagicMock,
        mock_garmin_class: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        A pasted full URL exchanges against the service parsed from that URL.

        :param mock_echo: Mock click.echo function.
        :param mock_garmin_class: Mock Garmin client class.
        :param tmp_path: Per-test token directory.
        """
        mock_garmin = MagicMock()
        mock_garmin.get_user_profile.return_value = {"id": "42"}
        mock_garmin_class.return_value = mock_garmin

        bootstrap_from_ticket(
            "https://connect.garmin.com/app?ticket=ST-9-sso",
            base_token_dir=str(tmp_path),
        )

        mock_garmin._exchange_service_ticket.assert_called_once_with(
            "ST-9-sso", service_url="https://connect.garmin.com/app"
        )

    @patch("garmin_health_data.auth.GarminClient")
    @patch("click.echo")
    def test_exchange_failure_raises_clickexception(
        self,
        mock_echo: MagicMock,
        mock_garmin_class: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        A failed exchange raises ClickException and does not save tokens.

        :param mock_echo: Mock click.echo function.
        :param mock_garmin_class: Mock Garmin client class.
        :param tmp_path: Per-test token directory.
        """
        mock_garmin = MagicMock()
        mock_garmin._exchange_service_ticket.side_effect = RuntimeError("boom")
        mock_garmin_class.return_value = mock_garmin

        with pytest.raises(click.ClickException):
            bootstrap_from_ticket("ST-abc-cas", base_token_dir=str(tmp_path))

        mock_garmin.dump.assert_not_called()

    @patch("garmin_health_data.auth.GarminClient")
    @patch("click.echo")
    def test_missing_user_id_raises(
        self,
        mock_echo: MagicMock,
        mock_garmin_class: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        A profile response without an id raises RuntimeError.

        :param mock_echo: Mock click.echo function.
        :param mock_garmin_class: Mock Garmin client class.
        :param tmp_path: Per-test token directory.
        """
        mock_garmin = MagicMock()
        mock_garmin.get_user_profile.return_value = {}
        mock_garmin_class.return_value = mock_garmin

        with pytest.raises(RuntimeError):
            bootstrap_from_ticket("ST-abc-cas", base_token_dir=str(tmp_path))


class TestManualAuth:
    """
    Test class for the interactive manual login flow.
    """

    @patch("garmin_health_data.auth.bootstrap_from_ticket", return_value="99")
    @patch("click.prompt", return_value="ST-xyz-cas")
    @patch("click.echo")
    @patch("click.secho")
    def test_prompts_and_delegates(
        self,
        mock_secho: MagicMock,
        mock_echo: MagicMock,
        mock_prompt: MagicMock,
        mock_bootstrap: MagicMock,
        tmp_path: Path,
    ) -> None:
        """
        The flow prompts for a ticket and delegates to bootstrap_from_ticket.

        :param mock_secho: Mock click.secho function.
        :param mock_echo: Mock click.echo function.
        :param mock_prompt: Mock click.prompt function.
        :param mock_bootstrap: Mock bootstrap_from_ticket function.
        :param tmp_path: Per-test token directory.
        """
        result = manual_auth(base_token_dir=str(tmp_path))

        assert result == "99"
        mock_prompt.assert_called_once()
        mock_bootstrap.assert_called_once_with(
            "ST-xyz-cas", base_token_dir=str(tmp_path), silent=False
        )


class TestAuthCommandRouting:
    """
    Test class for CLI routing of the manual authentication options.
    """

    @patch("garmin_health_data.cli.bootstrap_from_ticket")
    def test_ticket_option_routes_to_bootstrap(self, mock_bootstrap: MagicMock) -> None:
        """
        ``--ticket`` routes straight to bootstrap_from_ticket without prompting.

        :param mock_bootstrap: Mock bootstrap_from_ticket function.
        """
        result = CliRunner().invoke(auth, ["--ticket", "ST-abc-cas"])

        assert result.exit_code == 0
        mock_bootstrap.assert_called_once_with("ST-abc-cas")

    @patch("garmin_health_data.cli.manual_auth")
    def test_manual_flag_routes_to_manual_auth(self, mock_manual: MagicMock) -> None:
        """
        ``--manual`` routes to the interactive manual_auth flow.

        :param mock_manual: Mock manual_auth function.
        """
        result = CliRunner().invoke(auth, ["--manual"])

        assert result.exit_code == 0
        mock_manual.assert_called_once()


class TestAuthAutomatedFallback:
    """
    Test class for the manual fallback when automated login fails.
    """

    @patch("garmin_health_data.cli.manual_auth")
    @patch(
        "garmin_health_data.cli.refresh_tokens",
        side_effect=click.ClickException("Authentication failed"),
    )
    @patch("garmin_health_data.cli._is_interactive", return_value=False)
    def test_noninteractive_failure_prints_hint_and_raises(
        self,
        mock_interactive: MagicMock,
        mock_refresh: MagicMock,
        mock_manual: MagicMock,
    ) -> None:
        """
        A non-interactive failure prints the --manual hint and does not prompt.

        :param mock_interactive: Mock _is_interactive helper (non-interactive).
        :param mock_refresh: Mock refresh_tokens raising a ClickException.
        :param mock_manual: Mock manual_auth function.
        """
        result = CliRunner().invoke(auth, ["--email", "a@b.co", "--password", "pw"])

        assert result.exit_code != 0
        assert "garmin auth --manual" in result.output
        mock_manual.assert_not_called()

    @patch("garmin_health_data.cli.manual_auth")
    @patch("click.confirm", return_value=True)
    @patch(
        "garmin_health_data.cli.refresh_tokens",
        side_effect=click.ClickException("Authentication failed"),
    )
    @patch("garmin_health_data.cli._is_interactive", return_value=True)
    def test_interactive_failure_confirm_yes_runs_manual(
        self,
        mock_interactive: MagicMock,
        mock_refresh: MagicMock,
        mock_confirm: MagicMock,
        mock_manual: MagicMock,
    ) -> None:
        """
        An interactive failure accepted at the prompt runs the manual flow.

        :param mock_interactive: Mock _is_interactive helper (interactive).
        :param mock_refresh: Mock refresh_tokens raising a ClickException.
        :param mock_confirm: Mock click.confirm returning True.
        :param mock_manual: Mock manual_auth function.
        """
        result = CliRunner().invoke(auth, ["--email", "a@b.co", "--password", "pw"])

        assert result.exit_code == 0
        mock_manual.assert_called_once()

    @patch("garmin_health_data.cli.manual_auth")
    @patch("click.confirm", return_value=False)
    @patch(
        "garmin_health_data.cli.refresh_tokens",
        side_effect=click.ClickException("Authentication failed"),
    )
    @patch("garmin_health_data.cli._is_interactive", return_value=True)
    def test_interactive_failure_confirm_no_raises(
        self,
        mock_interactive: MagicMock,
        mock_refresh: MagicMock,
        mock_confirm: MagicMock,
        mock_manual: MagicMock,
    ) -> None:
        """
        An interactive failure declined at the prompt re-raises without manual login.

        :param mock_interactive: Mock _is_interactive helper (interactive).
        :param mock_refresh: Mock refresh_tokens raising a ClickException.
        :param mock_confirm: Mock click.confirm returning False.
        :param mock_manual: Mock manual_auth function.
        """
        result = CliRunner().invoke(auth, ["--email", "a@b.co", "--password", "pw"])

        assert result.exit_code != 0
        mock_manual.assert_not_called()
