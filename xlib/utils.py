import sys
import requests
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime


def get_utc_from_unix_time(unix_ts: Optional[Any], second: int = 1000) -> Optional[datetime]:
    return datetime.utcfromtimestamp(int(unix_ts) / second) if unix_ts else None


def get_exchange_data(url) -> List[Dict[str, Any]]:
    try:
        response = requests.get(url=url)
    except requests.ConnectionError as e:
        logging.error(f"The was an error with the request, {e}")
        sys.exit(1)
    return response.json().get("data", [])  # Get "data" object else return an empty list
