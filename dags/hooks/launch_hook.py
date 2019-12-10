import requests
from airflow.hooks.base_hook import BaseHook


class LaunchHook(BaseHook):
    base_url = 'https://launchlibrary.net/'

    def __init__(self, conn_id, api_version=1.4):
        super().__init__(source=None)
        self._conn_id = conn_id
        self._api_version = api_version
        self._conn = None

    def get_conn(self):
        if self._conn is None:
            self._conn = requests.session()
        return self._conn

    def download_rocket_launches(self, start_date: str, end_date: str):
        # query = LaunchHook.base_url + self._api_version + 'launch?startdate=' + start_date + '&enddate=' + end_date
        # response = requests.get(query)
        # return response

        session = self.get_conn()
        response = session.get("{self.base_url}/{self._api_version}/launch",
                               params={"start_date": start_date, "end_date": end_date}, )
        response.raise_for_status()
        return response.json()["launches"]

