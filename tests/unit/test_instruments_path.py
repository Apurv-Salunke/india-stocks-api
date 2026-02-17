"""Tests for deterministic instruments.db path resolution."""

from india_stocks_api.internal.context import (
    _CACHE_DIR,
    _SESSION_FILE,
    get_instruments_db_path,
)


class TestInstrumentsPath:
    def test_returns_path_under_cache(self):
        path = get_instruments_db_path()
        assert path.parent.name == "_cache"
        assert path.name == "instruments.db"

    def test_path_is_absolute(self):
        assert get_instruments_db_path().is_absolute()

    def test_cache_dir_matches_session_file_parent(self):
        assert _CACHE_DIR == _SESSION_FILE.parent

    def test_cache_dir_is_absolute(self):
        assert _CACHE_DIR.is_absolute()

    def test_cache_dir_resolves_to_project_root(self):
        # _cache/ should be a sibling of the india_stocks_api package
        package_dir = _CACHE_DIR.parent / "india_stocks_api"
        assert package_dir.is_dir()
