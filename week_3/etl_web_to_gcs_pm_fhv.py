from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
# from urllib3 import request
from urllib.request import urlretrieve


@task()
def download(remote_url: str, local_file: str) -> Path:
    """Download files from web to local"""
    urlretrieve(remote_url, local_file)
    print('done')
    return Path(local_file)


@task()
def write_gcs(path: Path) -> None:
    """Upload local file to GCS"""
    gcs_block = GcsBucket.load("zoom-gcs-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return


@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """The main ETL function"""

    remote_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_{year}-{month:02}.csv.gz'
    local_file = f'data/fhv_tripdata_{year}-{month:02}.csv.gz'

    # remote_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/fhv_tripdata_2019-01.csv.gz'

    path = download(remote_url, local_file)
    write_gcs(path)


@flow()
def etl_parent_flow(year, months):
    for month in months:
        etl_web_to_gcs(year, month)

if __name__ == "__main__":
    months = list(range(1,13))
    year = 2019
    etl_parent_flow(year, months)
