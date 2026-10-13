"""Discover GBFS feed URLs from the discovery document.

URLs are looked up by feed name at runtime instead of being hardcoded, because
the operator can move the feeds.
"""

import os

import requests

DEFAULT_DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/gbfs.json"
REQUIRED_FEEDS = ("station_information", "station_status")


class DiscoveryError(Exception):
    """The discovery document is unreachable or missing required feeds."""


def parse_feeds(document: dict, language: str = "en") -> dict[str, str]:
    """Return {feed_name: url} for one language from a discovery document."""
    try:
        feeds = document["data"][language]["feeds"]
    except (KeyError, TypeError) as exc:
        raise DiscoveryError(
            f"discovery document has no feeds for language {language!r}"
        ) from exc

    urls = {f["name"]: f["url"] for f in feeds if "name" in f and "url" in f}
    missing = [name for name in REQUIRED_FEEDS if name not in urls]
    if missing:
        raise DiscoveryError(f"discovery document is missing feeds: {missing}")
    return urls


def discover(
    discovery_url: str | None = None,
    language: str = "en",
    timeout: float = 10.0,
) -> dict[str, str]:
    """Fetch the discovery document and return {feed_name: url}."""
    url = discovery_url or os.getenv("GBFS_DISCOVERY_URL", DEFAULT_DISCOVERY_URL)
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        document = response.json()
    except (requests.RequestException, ValueError) as exc:
        raise DiscoveryError(f"could not fetch discovery document from {url}: {exc}") from exc
    return parse_feeds(document, language)


if __name__ == "__main__":
    for name, feed_url in sorted(discover().items()):
        print(f"{name:24} {feed_url}")
