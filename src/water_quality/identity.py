def record_key(station_id: str, sample_id: str, metric: str, observed_at: str) -> str:
    return "|".join(value.strip().upper() for value in (station_id, sample_id, metric, observed_at))

def station_key(station_id: str) -> str:
    return station_id.strip().upper()

def sample_key(station_id: str, sample_id: str) -> str:
    return f"{station_key(station_id)}|{sample_id.strip().upper()}"
