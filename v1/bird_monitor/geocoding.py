from __future__ import annotations

import json
import os
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


class GeocodingError(RuntimeError):
    pass


@dataclass(frozen=True)
class GeocodingResult:
    query: str
    display_name: str
    latitude: float
    longitude: float

    def to_dict(self) -> dict[str, object]:
        return {
            "query": self.query,
            "display_name": self.display_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


def geocode_address(query: str) -> GeocodingResult:
    cleaned_query = query.strip()
    if not cleaned_query:
        raise GeocodingError("Enter an address or place name first.")

    params = urlencode(
        {
            "q": cleaned_query,
            "format": "jsonv2",
            "limit": 1,
        }
    )
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    request = Request(
        url,
        headers={
            "User-Agent": os.getenv(
                "BIRD_MONITOR_GEOCODER_USER_AGENT",
                "bird-monitor/1.0 (+https://github.com/Divitare/animal-recording)",
            ),
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=12) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise GeocodingError(f"Address lookup failed with HTTP {exc.code}.") from exc
    except URLError as exc:
        raise GeocodingError("Address lookup failed. Check internet access on the server.") from exc
    except TimeoutError as exc:
        raise GeocodingError("Address lookup timed out.") from exc
    except json.JSONDecodeError as exc:
        raise GeocodingError("Address lookup returned invalid data.") from exc

    if not payload:
        raise GeocodingError("No matching address was found.")

    item = payload[0]
    try:
        return GeocodingResult(
            query=cleaned_query,
            display_name=str(item.get("display_name") or cleaned_query),
            latitude=float(item["lat"]),
            longitude=float(item["lon"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GeocodingError("Address lookup response was missing coordinates.") from exc
