from datetime import datetime

def to_iso8601(date_str):
    """Convert date string to ISO 8601 format."""
    return datetime.strptime(date_str, "%Y-%m-%d").isoformat()

def compute_percent_change(open_price, close_price):
    """Calculate percentage change between open and close prices."""
    try:
        return round(((close_price - open_price) / open_price) * 100, 2)
    except ZeroDivisionError:
        return None

def normalize_temperature(temp_f):
    """Convert Fahrenheit to Celsius."""
    return round((temp_f - 32) * 5.0 / 9.0, 2)

def is_extreme_weather(temp_c, threshold=35):
    """Return True if temperature exceeds threshold in Celsius."""
    return temp_c > threshold

def standardize_field_names(record):
    """Convert field names to snake_case (basic version)."""
    return {k.lower().replace(" ", "_"): v for k, v in record.items()}
