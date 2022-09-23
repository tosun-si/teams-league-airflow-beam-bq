from apache_beam import PTransform

from team_league.job.domain_ptransform.team_stats_file_io_connector import TeamStatsFileIOConnector
from team_league.job.infrastructure.io.jsonfile.team_stats_jsonfile_read_transform import TeamStatsJsonFileReadTransform


class TeamStatsJsonFileIOAdapter(TeamStatsFileIOConnector):

    def __init__(self,
                 read_transform: TeamStatsJsonFileReadTransform) -> None:
        super().__init__()
        self.read_transform = read_transform

    def read_team_stats(self) -> PTransform:
        return self.read_transform
