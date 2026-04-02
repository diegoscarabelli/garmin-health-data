"""
Tests for authentication module.
"""

import pytest

from garmin_health_data.auth import check_authentication, discover_accounts


class TestCheckAuthentication:
    """
    Tests for check_authentication with multi-account layout.
    """

    def test_no_token_dir(self, tmp_path):
        """
        Returns False when token directory does not exist.
        """
        token_dir = tmp_path / "tokens"
        assert not check_authentication(str(token_dir))

    def test_empty_dir(self, tmp_path):
        """
        Returns False when token directory is empty.
        """
        token_dir = tmp_path / "tokens"
        token_dir.mkdir()
        assert not check_authentication(str(token_dir))

    def test_per_account_tokens(self, tmp_path):
        """
        Returns True when a numeric subdirectory has token files.
        """
        token_dir = tmp_path / "tokens"
        account_dir = token_dir / "12345678"
        account_dir.mkdir(parents=True)
        (account_dir / "oauth2_token.json").write_text("{}")
        assert check_authentication(str(token_dir))

    def test_empty_numeric_subdir(self, tmp_path):
        """
        Returns False when numeric subdirectory exists but is empty.
        """
        token_dir = tmp_path / "tokens"
        (token_dir / "12345678").mkdir(parents=True)
        assert not check_authentication(str(token_dir))

    def test_legacy_tokens(self, tmp_path):
        """
        Returns True for legacy layout with token JSON files at root.
        """
        token_dir = tmp_path / "tokens"
        token_dir.mkdir()
        (token_dir / "oauth1_token.json").write_text("{}")
        assert check_authentication(str(token_dir))

    def test_non_matching_files_only(self, tmp_path):
        """
        Returns False when only non-token files exist.
        """
        token_dir = tmp_path / "tokens"
        token_dir.mkdir()
        (token_dir / "readme.txt").write_text("not a token")
        assert not check_authentication(str(token_dir))


class TestDiscoverAccounts:
    """
    Tests for discover_accounts function.
    """

    def test_single_account(self, tmp_path):
        """
        Discovers a single numeric subdirectory as an account.
        """
        account_dir = tmp_path / "12345678"
        account_dir.mkdir()
        (account_dir / "oauth2_token.json").write_text("{}")

        accounts = discover_accounts(str(tmp_path))
        assert accounts == [("12345678", account_dir)]

    def test_multiple_accounts_sorted(self, tmp_path):
        """
        Multiple accounts are returned sorted by user_id.
        """
        for uid in ["87654321", "12345678"]:
            d = tmp_path / uid
            d.mkdir()
            (d / "token.json").write_text("{}")

        accounts = discover_accounts(str(tmp_path))
        assert [uid for uid, _ in accounts] == ["12345678", "87654321"]

    def test_skips_non_numeric_dirs(self, tmp_path):
        """
        Non-numeric directories are ignored.
        """
        (tmp_path / "not_a_number").mkdir()
        account_dir = tmp_path / "12345678"
        account_dir.mkdir()
        (account_dir / "oauth2_token.json").write_text("{}")
        (tmp_path / "some_file.txt").write_text("data")

        accounts = discover_accounts(str(tmp_path))
        assert len(accounts) == 1
        assert accounts[0][0] == "12345678"

    def test_no_base_dir_raises(self, tmp_path):
        """
        Raises FileNotFoundError when base directory does not exist.
        """
        with pytest.raises(FileNotFoundError):
            discover_accounts(str(tmp_path / "nonexistent"))

    def test_not_a_directory_raises(self, tmp_path):
        """
        Raises NotADirectoryError when path is a file, not a directory.
        """
        file_path = tmp_path / "a_file"
        file_path.write_text("data")
        with pytest.raises(NotADirectoryError):
            discover_accounts(str(file_path))

    def test_skips_empty_numeric_dirs(self, tmp_path):
        """
        Empty numeric directories are not discovered as accounts.
        """
        (tmp_path / "12345678").mkdir()
        with pytest.raises(RuntimeError, match="No accounts found"):
            discover_accounts(str(tmp_path))

    def test_no_accounts_raises(self, tmp_path):
        """
        Raises RuntimeError when directory is empty (no accounts found).
        """
        with pytest.raises(RuntimeError, match="No accounts found"):
            discover_accounts(str(tmp_path))

    def test_legacy_layout(self, tmp_path):
        """
        Falls back to legacy layout when token JSON files exist at root.
        """
        (tmp_path / "oauth1_token.json").write_text("{}")
        (tmp_path / "oauth2_token.json").write_text("{}")

        accounts = discover_accounts(str(tmp_path))
        assert accounts == [("legacy", tmp_path)]
