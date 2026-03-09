INSERT INTO production.fact_lineup (
    game_pk, player_id, team_id, batting_order,
    is_starter, position, home_away, season
)
SELECT DISTINCT ON (lb.game_pk, (value->'person'->>'id')::bigint)
    lb.game_pk,
    (value->'person'->>'id')::bigint AS player_id,
    CASE WHEN side.key = 'home'
        THEN (payload::jsonb->'teams'->'home'->'team'->>'id')::int
        ELSE (payload::jsonb->'teams'->'away'->'team'->>'id')::int
    END AS team_id,
    -- batting_order: first digit is lineup slot (100=1, 200=2, ..., 900=9)
    ((value->>'battingOrder')::int / 100) AS batting_order,
    -- is_starter: starters end in 00 (100, 200, ..., 900)
    ((value->>'battingOrder')::int % 100 = 0) AS is_starter,
    value->'position'->>'abbreviation' AS position,
    side.key AS home_away,
    g.season
FROM raw.landing_boxscores lb
CROSS JOIN LATERAL jsonb_each_text(
    jsonb_build_object('home', 'home', 'away', 'away')
) AS side(key, val)
CROSS JOIN LATERAL jsonb_each(
    payload::jsonb->'teams'->side.key->'players'
) AS player(player_key, value)
JOIN production.dim_game g ON g.game_pk = lb.game_pk
WHERE value->>'battingOrder' IS NOT NULL
  AND (value->>'battingOrder')::int > 0
  AND value->'stats'->'batting' IS NOT NULL
ORDER BY lb.game_pk, (value->'person'->>'id')::bigint,
         -- prefer starter (mod=0) over sub, then lower raw order
         (value->>'battingOrder')::int % 100,
         (value->>'battingOrder')::int
ON CONFLICT (game_pk, player_id) DO UPDATE
SET batting_order = EXCLUDED.batting_order,
    is_starter = EXCLUDED.is_starter,
    position = EXCLUDED.position,
    home_away = EXCLUDED.home_away,
    season = EXCLUDED.season;
