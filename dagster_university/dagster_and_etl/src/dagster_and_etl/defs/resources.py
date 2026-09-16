import dagster as dg
from dagster_duckdb import DuckDBResource
import requests


class NASAResource(dg.ConfigurableResource):
    api_key: str

    def get_near_earth_asteroids(self, start_date: str, end_date: str):
        url = "https://api.nasa.gov/neo/rest/v1/feed"
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "api_key": self.api_key,
        }

        resp = requests.get(url, params=params)
        return resp.json()["near_earth_objects"][start_date]


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "database": DuckDBResource(
                database="data/staging/data.duckdb",
            ),
            "nasa": NASAResource(api_key=dg.EnvVar("NASA_API_KEY")),
        }
    )
