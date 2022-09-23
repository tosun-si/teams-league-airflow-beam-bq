from dependency_injector import containers, providers

from team_league.job.application.team_league_pipeline_composer import TeamLeaguePipelineComposer
from team_league.job.infrastructure.io.bigquery.team_stats_bigquery_io_adapter import TeamStatsBigqueryIOAdapter
from team_league.job.infrastructure.io.bigquery.team_stats_bigquery_write_transform import \
    TeamStatsBigqueryWriteTransform
from team_league.job.infrastructure.io.jsonfile.team_stats_jsonfile_io_adapter import TeamStatsJsonFileIOAdapter
from team_league.job.infrastructure.io.jsonfile.team_stats_jsonfile_read_transform import TeamStatsJsonFileReadTransform


class IOTransforms(containers.DeclarativeContainer):
    config = providers.Configuration()

    read_teams_stats_file_transform = providers.Singleton(TeamStatsJsonFileReadTransform,
                                                          pipeline_conf=config)

    write_team_stats_database_transform = providers.Singleton(TeamStatsBigqueryWriteTransform,
                                                              pipeline_conf=config)


class Adapters(containers.DeclarativeContainer):
    io_transforms = providers.DependenciesContainer()

    team_stats_file_io_connector = providers.Singleton(
        TeamStatsJsonFileIOAdapter,
        read_transform=io_transforms.read_teams_stats_file_transform)

    team_stats_database_io_connector = providers.Singleton(
        TeamStatsBigqueryIOAdapter,
        write_transform=io_transforms.write_team_stats_database_transform)


class Pipeline(containers.DeclarativeContainer):
    config = providers.Configuration()

    adapters = providers.DependenciesContainer()

    compose_pipeline = providers.Factory(
        TeamLeaguePipelineComposer,
        pipeline_conf=config,
        team_stats_database_io_connector=adapters.team_stats_database_io_connector,
        team_stats_file_io_connector=adapters.team_stats_file_io_connector
    )
