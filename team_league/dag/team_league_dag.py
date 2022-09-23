import airflow
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowConfiguration
from jinja2 import Template

from team_league.dag.settings import Settings

settings = Settings()


def get_jinja_template(file_path: str) -> Template:
    with open(f'{settings.queries_path}/{file_path}') as fp:
        return Template(fp.read())


with airflow.DAG(
        "team_league_ingestion",
        default_args=settings.dag_default_args,
        schedule_interval=None) as dag:
    truncate_staging_query = get_jinja_template('truncate_team_stat_staging_table.sql').render(
        project_id=settings.project_id,
        dataset=settings.dataset,
        team_stat_staging_table=settings.team_stat_staging_table
    )

    truncate_staging_table = BigQueryInsertJobOperator(
        task_id='truncate_team_stat_staging_table',
        configuration={
            "query": {
                "query": truncate_staging_query,
                "useLegacySql": False,
            }
        },
        location='EU'
    )

    launch_dataflow_job = BeamRunPythonPipelineOperator(
        runner='DataflowRunner',
        py_file=settings.dataflow_job_path,
        task_id='launch_dataflow_job_ingestion_team_stats_staging',
        pipeline_options=settings.dataflow_job_options,
        py_system_site_packages=False,
        py_interpreter='python3',
        dataflow_config=DataflowConfiguration(
            location=settings.region
        )
    )

    merge_final_table_query = get_jinja_template('merge_team_stat_table.sql').render(
        project_id=settings.project_id,
        dataset=settings.dataset,
        team_stat_table=settings.team_stat_table,
        team_stat_staging_table=settings.team_stat_staging_table
    )

    merge_final_table = BigQueryInsertJobOperator(
        task_id='merge_team_stat_table',
        configuration={
            "query": {
                "query": merge_final_table_query,
                "useLegacySql": False,
            }
        },
        location='EU'
    )

    truncate_staging_table >> launch_dataflow_job >> merge_final_table
