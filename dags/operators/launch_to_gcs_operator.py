from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from dags.hooks.launch_hook import LaunchHook

class LaunchToGcsOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
             bucket,
             start_date=None,
             end_date=None,
             delegate_to=None,
             google_cloud_storage_conn_id='google_cloud_default',
             launch_conn_id=None,
             *args, **kwargs):

        super.__init__(*args, **kwargs)
        self._bucket = bucket
        self._delegate_to = delegate_to
        self._start_date = start_date
        self._end_date = end_date
        self._google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self._launch_conn_id = launch_conn_id

    def execute(self, context):
        file = self._download_rocket_launches()
        self._upload_to_gcs(file)

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google Cloud Storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self._google_cloud_storage_conn_id,
            delegate_to=self._delegate_to)
        for object, tmp_file_handle in files_to_upload.items():
            hook.upload(self._bucket, object, tmp_file_handle.name,
                        'application/json')

    def _download_rocket_launches(self):
        hook = LaunchHook(self._launch_conn_id)
        return hook.download_rocket_launches(start_date=self._start_date, end_date=self._end_date)
