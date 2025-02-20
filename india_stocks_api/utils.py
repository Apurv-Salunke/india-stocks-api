from datetime import timedelta
from functools import wraps


def chunk_date_range(max_days: int = 5):
    """
    Decorator to fetch data in chunks to bypass API limitations on date ranges.

    Args:
        max_days (int): The maximum number of days the API allows per request.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(
            cls,
            symbol,
            exchange,
            interval,
            from_date,
            to_date,
            headers,
            *args,
            **kwargs,
        ):
            current_start = from_date
            all_candles = []

            while current_start < to_date:
                current_end = min(current_start + timedelta(days=max_days), to_date)
                print(f"Fetching candles from {current_start} to {current_end}")
                candles = func(
                    cls,
                    symbol,
                    exchange,
                    interval,
                    current_start,
                    current_end,
                    headers,
                    *args,
                    **kwargs,
                )
                all_candles.extend(candles)
                current_start = current_end + timedelta(minutes=1)  # Avoid overlap

            return all_candles

        return wrapper

    return decorator


def get_holiday_list(year):
    """
    Get a list of holidays for a given year.
    """
    pass
