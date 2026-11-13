def _clean(value: str) -> str:
    return value.strip().upper()


def record_key(station_id: str, sample_id: str, metric: str, observed_at: str) -> str:
    return "|".join(_clean(value) for value in (station_id, sample_id, metric, observed_at))


def subject_key(station_id: str, sample_id: str, metric: str) -> str:
    """The logical water-sample/metric that preliminary and retest reports adjudicate together."""
    return "|".join(_clean(value) for value in (station_id, sample_id, metric))


def station_key(station_id: str) -> str:
    return _clean(station_id)


def sample_key(station_id: str, sample_id: str) -> str:
    return f"{station_key(station_id)}|{_clean(sample_id)}"
