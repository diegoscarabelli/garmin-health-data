"""
Tests for authentication module.
"""

from garmin_health_data.auth import check_authentication


def test_check_authentication_no_tokens(tmp_path):
    """
    Test checking authentication when no tokens exist.
    """
    token_dir = tmp_path / "tokens"
    assert not check_authentication(str(token_dir))


def test_check_authentication_with_tokens(tmp_path):
    """
    Test checking authentication when tokens exist.
    """
    token_dir = tmp_path / "tokens"
    token_dir.mkdir()
    (token_dir / "token.txt").write_text("test")

    assert check_authentication(str(token_dir))


def test_check_authentication_empty_dir(tmp_path):
    """
    Test checking authentication with empty token directory.
    """
    token_dir = tmp_path / "tokens"
    token_dir.mkdir()

    assert not check_authentication(str(token_dir))
