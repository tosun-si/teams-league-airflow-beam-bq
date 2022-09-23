import os
from dataclasses import dataclass
from datetime import timedelta

from airflow.utils.dates import days_ago


@dataclass
class Settings:
    dag_folder = os.getenv("DAGS_FOLDER")
    dag_default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
        "start_date": days_ago(1)
    }
    region = "europe-west1"
    project_id = os.getenv("GCP_PROJECT")
    dataset = 'mazlum_test'
    team_stat_table = 'team_stat'
    team_stat_staging_table = 'team_stat_staging'
    dataflow_job_path = os.path.join(
        dag_folder,
        'team_league',
        'job',
        'application',
        'team_league_app.py'
    )
    queries_path = os.path.join(
        dag_folder,
        'team_league',
        'dag',
        'queries'
    )
    subnetwork_env = os.getenv("DATAFLOW_SUBNETWORK")
    service_account_email = os.getenv("DATAFLOW_SERVICE_ACCOUNT")
    subnetwork = (
        f"https://www.googleapis.com/compute/v1/projects/{project_id}/regions/{region}/subnetworks/{subnetwork_env}"
    )

    dataflow_job_options = {
        'project': project_id,
        'project_id': project_id,
        'input_json_file': 'gs://mazlum_dev/team_league/input/json/input_teams_stats_raw.json',
        'staging_location': 'gs://mazlum_dev/dataflow/staging',
        'region': 'europe-west1',
        'setup_file': f"{dag_folder}/setup.py",
        "service_account_email": service_account_email,
        "experiments": "use_runner_v2",
        "sdk_container_image": "eu.gcr.io/emea-c1-dwh-prd/beam_custom:2.37.5",
        "subnetwork": subnetwork,
        "no_use_public_ips": True,
        'temp_location': 'gs://mazlum_dev/dataflow/temp',
        'team_league_dataset': 'mazlum_test',
        'team_stats_table': 'team_stat_staging',
        'bq_write_method': 'FILE_LOADS'
    }
