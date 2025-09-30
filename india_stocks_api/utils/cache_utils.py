"""
Cache utilities for the india-stocks-api package
"""

from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def get_cache_directory() -> Path:
    """
    Get the cache directory for the india-stocks-api package.

    Uses the existing _cache directory in the project root, or creates it if it doesn't exist.
    This ensures compatibility with the existing cache structure.

    Returns:
        Path: Path to the cache directory
    """
    # Get the project root (where _cache directory should be)
    current_dir = Path(__file__).parent.parent.parent

    # Look for _cache directory in project root
    cache_dir = current_dir / "_cache"

    # If _cache doesn't exist in project root, create it
    if not cache_dir.exists():
        cache_dir.mkdir(exist_ok=True)
        logger.info(f"Created cache directory: {cache_dir}")

    return cache_dir


def get_database_path() -> str:
    """
    Get the path to the instruments database file.

    Returns:
        str: Path to the database file in the cache directory
    """
    cache_dir = get_cache_directory()
    db_path = cache_dir / "instruments.db"
    return str(db_path)


def get_cache_file_path(filename: str) -> str:
    """
    Get the path to a cache file in the cache directory.

    Parameters:
        filename (str): Name of the cache file

    Returns:
        str: Full path to the cache file
    """
    cache_dir = get_cache_directory()
    return str(cache_dir / filename)


def ensure_cache_directory() -> Path:
    """
    Ensure the cache directory exists and return its path.

    Returns:
        Path: Path to the cache directory
    """
    cache_dir = get_cache_directory()
    cache_dir.mkdir(exist_ok=True)
    return cache_dir


def clear_cache() -> bool:
    """
    Clear all cache files (database and JSON files).

    Returns:
        bool: True if cache was cleared successfully, False otherwise
    """
    try:
        cache_dir = get_cache_directory()

        # Remove database file
        db_path = cache_dir / "instruments.db"
        if db_path.exists():
            db_path.unlink()
            logger.info(f"Removed database file: {db_path}")

        # Remove JSON cache files
        for json_file in cache_dir.glob("*.json"):
            json_file.unlink()
            logger.info(f"Removed cache file: {json_file}")

        logger.info("Cache cleared successfully")
        return True

    except Exception as e:
        logger.error(f"Error clearing cache: {e}")
        return False


def get_cache_info() -> dict:
    """
    Get information about the cache directory and files.

    Returns:
        dict: Cache information including directory path, file sizes, etc.
    """
    cache_dir = get_cache_directory()

    info = {
        "cache_directory": str(cache_dir),
        "exists": cache_dir.exists(),
        "files": [],
    }

    if cache_dir.exists():
        for file_path in cache_dir.iterdir():
            if file_path.is_file():
                info["files"].append(
                    {
                        "name": file_path.name,
                        "size_bytes": file_path.stat().st_size,
                        "size_mb": round(file_path.stat().st_size / (1024 * 1024), 2),
                    }
                )

    return info
