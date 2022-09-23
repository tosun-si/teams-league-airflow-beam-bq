import json
from typing import Dict

import apache_beam as beam
from apache_beam import PTransform
from apache_beam.io import ReadFromText
from apache_beam.pvalue import PBegin
from dacite import from_dict

from team_league.job.application.pipeline_conf import PipelineConf
from team_league.job.domain.team_stats_raw import TeamStatsRaw


class TeamStatsJsonFileReadTransform(PTransform):

    def __init__(self,
                 pipeline_conf: PipelineConf):
        super().__init__()
        self.pipeline_conf = pipeline_conf

    def expand(self, inputs: PBegin):
        return (inputs |
                'Read Json file' >> ReadFromText(self.pipeline_conf.input_json_file) |
                'Map str message to Dict' >> beam.Map(self.to_dict) |
                'Deserialize to domain dataclass' >> beam.Map(self.deserialize))

    def to_dict(self, team_stats_raw_as_str: str) -> Dict:
        return json.loads(team_stats_raw_as_str)

    def deserialize(self, team_stats_raw_as_dict: Dict) -> TeamStatsRaw:
        return from_dict(
            data_class=TeamStatsRaw,
            data=team_stats_raw_as_dict
        )
