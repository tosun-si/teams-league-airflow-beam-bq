from abc import ABCMeta, abstractmethod
from apache_beam import Pipeline

from team_league.job.application.pipeline_conf import PipelineConf
from team_league.job.domain_ptransform.team_stats_database_io_connector import TeamStatsDatabaseIOConnector
from team_league.job.domain_ptransform.team_stats_file_io_connector import TeamStatsFileIOConnector
from team_league.job.domain_ptransform.team_stats_transform import TeamStatsTransform


class PipelineComposer(metaclass=ABCMeta):

    @abstractmethod
    def compose(self) -> Pipeline:
        pass


class TeamLeaguePipelineComposer:

    def __init__(self,
                 pipeline_conf: PipelineConf,
                 team_stats_database_io_connector: TeamStatsDatabaseIOConnector,
                 team_stats_file_io_connector: TeamStatsFileIOConnector) -> None:
        super().__init__()
        self.pipeline_conf = pipeline_conf
        self.team_stats_database_io_connector = team_stats_database_io_connector
        self.team_stats_file_io_connector = team_stats_file_io_connector

    def compose(self, pipeline: Pipeline) -> Pipeline:
        (pipeline
         | 'Read team stats' >> self.team_stats_file_io_connector.read_team_stats()
         | 'Team stats domain transform' >> TeamStatsTransform()
         | 'Write team stats to db' >> self.team_stats_database_io_connector.write_team_stats())

        return pipeline
