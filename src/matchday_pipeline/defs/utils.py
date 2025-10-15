def parse_timestamp_to_seconds(ts: str) -> float:
    """
    Parses provided timestamp in milliseconds to seconds
    Returns 0.0 if input is None or invalid.
    """

    if not ts or not isinstance(ts, str):
        return 0.0

    try:
        parts = ts.split(":")
        if len(parts) != 3:
            return 0.0

        h = int(parts[0])
        m = int(parts[1])
        s = float(parts[2])

        return h * 3600 + m * 60 + s
    except (ValueError, AttributeError):
        return 0.0
