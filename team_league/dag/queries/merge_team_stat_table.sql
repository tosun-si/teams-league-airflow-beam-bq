MERGE `{{project_id}}.{{dataset}}.{{team_stat_table}}` T
USING `{{project_id}}.{{dataset}}.{{team_stat_staging_table}}` S
ON T.teamName = S.teamName
WHEN MATCHED THEN

UPDATE
SET
    teamScore = S.teamScore,
    teamTotalGoals = S.teamTotalGoals,
    teamSlogan = S.teamSlogan,
    topScorerStats = S.topScorerStats,
    bestPasserStats = S.bestPasserStats

WHEN NOT MATCHED THEN
INSERT ROW;
