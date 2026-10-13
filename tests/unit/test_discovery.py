import pytest

from ingestion.gbfs.discovery import DiscoveryError, parse_feeds

# Synthetic document, not a captured Citi Bike response.
DOCUMENT = {
    "data": {
        "en": {
            "feeds": [
                {"name": "station_information", "url": "https://example.test/en/info.json"},
                {"name": "station_status", "url": "https://example.test/en/status.json"},
                {"name": "system_alerts", "url": "https://example.test/en/alerts.json"},
            ]
        }
    }
}


def test_parse_feeds_returns_urls_by_name():
    urls = parse_feeds(DOCUMENT)
    assert urls["station_status"] == "https://example.test/en/status.json"
    assert urls["station_information"] == "https://example.test/en/info.json"


def test_parse_feeds_unknown_language():
    with pytest.raises(DiscoveryError, match="language"):
        parse_feeds(DOCUMENT, language="fr")


def test_parse_feeds_missing_required_feed():
    document = {"data": {"en": {"feeds": [DOCUMENT["data"]["en"]["feeds"][0]]}}}
    with pytest.raises(DiscoveryError, match="station_status"):
        parse_feeds(document)


def test_parse_feeds_malformed_document():
    with pytest.raises(DiscoveryError):
        parse_feeds({"unexpected": True})
