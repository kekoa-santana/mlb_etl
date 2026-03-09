WITH with_date AS (
    SELECT
        b.*,
        dg.game_date
    FROM staging.batting_boxscores b
    JOIN production.dim_game dg
        ON dg.game_pk = b.game_pk
), 
game_level AS (
    SELECT
        batter_id,
        batter_name,
        game_pk,
        game_date,
        MAX(position) AS position,
        SUM(ground_outs + air_outs + strikeouts) AS outs,
        SUM(strikeouts) AS strikeouts,
        SUM(runs) AS runs,
        SUM(hits) AS hits,
        SUM(hits - doubles - triples - home_runs) AS singles,
        SUM(doubles) AS doubles,
        SUM(triples) AS triples,
        SUM(home_runs) AS homeruns,
        SUM(walks) AS walks,
        SUM(intentional_walks) AS int_walk,
        SUM(hit_by_pitch) AS hbp,
        SUM(at_bats) AS ab,
        SUM(caught_stealing) AS cs,
        SUM(sb) AS sb,
        SUM(sb_pct) AS sb_pct,
        SUM(total_bases) AS tb,
        SUM(rbi) AS rbi,
        SUM(errors) AS errors
    FROM with_date
    GROUP BY 1,2,3,4
)
SELECT 
    batter_id,
    batter_name,
    game_pk,
    game_date,
    position,
    SUM(outs) OVER (PARTITION BY batter_id ORDER BY game_date ROWS BETWEEN 15 PRECEDING AND 1 PRECEDING) AS outs_15,
    SUM(strikeouts) OVER(PARTITION BY batter_id ORDER BY game_date ROWS BETWEEN )
FROM with_date
ORDER BY game_date;