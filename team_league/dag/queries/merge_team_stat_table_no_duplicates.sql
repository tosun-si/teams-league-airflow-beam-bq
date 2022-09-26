MERGE `{{project_id}}.{{dataset}}.{{team_stat_table}}` T
USING (
    SELECT
        * EXCEPT (rn)
    FROM
    (
        SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY teamName ORDER BY ingestionDate DESC) AS rn
        FROM `{{project_id}}.{{dataset}}.{{team_stat_staging_table}}`
    )
    WHERE rn = 1
) S ON T.teamName = S.teamName
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
