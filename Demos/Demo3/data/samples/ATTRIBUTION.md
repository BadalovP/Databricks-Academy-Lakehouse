# Sample data attribution

These small fixtures support offline tests and notebook walkthroughs. They are source material
for a non-commercial educational analysis and are not presented as a stand-alone mirror of any
upstream dataset.

| Fixture | Source | Terms / attribution |
|---|---|---|
| `station_information.sample.json` | Citi Bike GBFS feed discovered from `https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json` | Citi Bike system data, subject to the [NYCBS Data Use Policy](https://citibikenyc.com/data-sharing-policy). |
| `station_status.sample.json` | Citi Bike GBFS station-status feed | Same Citi Bike policy. The observations are a dated sample, not current operational truth. |
| `historical_trips_202401_sample.csv` | Official `202401-citibike-tripdata.zip` in the Citi Bike public S3 bucket | Citi Bike trip history, same policy. Only 40 rows are retained for educational tests. |
| `open_meteo.sample.json` | [Open-Meteo Forecast API](https://open-meteo.com/en/docs) | Weather data by Open-Meteo; source models are described in its documentation. |
| `open_meteo_archive_202401.sample.json` | [Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api), 48 hourly observations for 2024-01-24/25 at 40.7128, -74.0060 | Weather data by Open-Meteo, [CC BY 4.0](https://open-meteo.com/en/license). Retained because the archive response is shaped as parallel arrays rather than a list of objects, and the parser tests must exercise that real shape; the dates match the committed trip sample. |

Run `python scripts/prepare_demo.py --overwrite` only when intentionally refreshing samples.
That command performs bounded public HTTP reads and never connects to Azure.
