# Source Data Definitions

> UrbanFlow source notes; operational procedures elsewhere in this corpus are fictional.

## GBFS

The current Citi Bike GBFS feed exposes a station_id that is a UUID and a separate short_name.
Historical Citi Bike trip files use short-name-style station identifiers. Join history to the
station dimension through short_name; do not join historical identifiers directly to the UUID.
A single GBFS response is one snapshot and must not be described as continuous history.

## Trip history

January 2024 trip CSV data supplies actual ride start/end times, station short-name identifiers,
and rider categories. Source attribution and sampling limitations accompany the fixtures.

## Weather

Open-Meteo observations use one New York City latitude/longitude. They are city-level weather
context, not station-level measurements. Weather correlation does not establish causation.

