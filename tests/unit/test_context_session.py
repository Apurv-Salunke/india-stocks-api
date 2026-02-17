"""Tests for session persistence in india_stocks_api.internal.context."""

import json

from india_stocks_api.internal import context
from india_stocks_api.internal.context import (
    clear_session,
    get_api_key,
    load_session,
    save_session,
)


class TestSaveAndLoad:
    def test_round_trip(self, tmp_session_file):
        save_session("angel", {"access_token": "jwt123", "api_key": "ak"})
        loaded = load_session("angel")
        assert loaded["access_token"] == "jwt123"
        assert loaded["api_key"] == "ak"

    def test_load_nonexistent_broker_returns_empty(self, tmp_session_file):
        assert load_session("zerodha") == {}

    def test_isolation_between_brokers(self, tmp_session_file):
        save_session("angel", {"access_token": "angel_jwt"})
        save_session("zerodha", {"access_token": "zerodha_jwt"})

        assert load_session("angel")["access_token"] == "angel_jwt"
        assert load_session("zerodha")["access_token"] == "zerodha_jwt"

    def test_overwrite_session(self, tmp_session_file):
        save_session("angel", {"access_token": "old"})
        save_session("angel", {"access_token": "new"})
        assert load_session("angel")["access_token"] == "new"


class TestClearSession:
    def test_clear_removes_broker(self, tmp_session_file):
        save_session("angel", {"access_token": "jwt"})
        clear_session("angel")
        assert load_session("angel") == {}

    def test_clear_nonexistent_is_noop(self, tmp_session_file):
        clear_session("nonexistent")  # should not raise

    def test_clear_one_preserves_other(self, tmp_session_file):
        save_session("angel", {"access_token": "a"})
        save_session("zerodha", {"access_token": "z"})
        clear_session("angel")
        assert load_session("zerodha")["access_token"] == "z"


class TestGetApiKey:
    def test_returns_api_key(self, tmp_session_file):
        save_session("angel", {"api_key": "my_key"})
        assert get_api_key("angel") == "my_key"

    def test_returns_none_when_missing(self, tmp_session_file):
        assert get_api_key("angel") is None


class TestFilePermissions:
    def test_session_file_is_chmod_600(self, tmp_session_file):
        save_session("angel", {"access_token": "jwt"})
        mode = tmp_session_file.stat().st_mode & 0o777
        assert mode == 0o600


class TestCorruptData:
    def test_corrupt_json_returns_empty(self, tmp_session_file):
        tmp_session_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_session_file.write_text("NOT VALID JSON{{{", encoding="utf-8")

        # Force reload by resetting the loaded flag
        context._SESSION_LOADED = False
        context._SESSION_CACHE = {}

        assert load_session("angel") == {}

    def test_non_dict_json_returns_empty(self, tmp_session_file):
        tmp_session_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_session_file.write_text('["a","b"]', encoding="utf-8")

        context._SESSION_LOADED = False
        context._SESSION_CACHE = {}

        assert load_session("angel") == {}


class TestSessionLoadedFlag:
    def test_once_loaded_does_not_reread_disk(self, tmp_session_file):
        save_session("angel", {"access_token": "first"})

        # Overwrite the file directly — should NOT be picked up
        tmp_session_file.write_text(
            json.dumps({"angel": {"access_token": "second"}}),
            encoding="utf-8",
        )

        # _SESSION_LOADED is True, so this should return cached data
        assert load_session("angel")["access_token"] == "first"
