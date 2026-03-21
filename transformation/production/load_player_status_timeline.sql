INSERT INTO production.fact_player_status_timeline (
    player_id, status_start_date, status_type,
    status_end_date, team_id, team_name,
    injury_description, days_on_status, season
)
WITH status_events AS (
    -- IL placements
    SELECT
        player_id,
        transaction_date   AS status_start_date,
        'IL-' || REPLACE(il_type, '-day', '') AS status_type,
        COALESCE(to_team_id, from_team_id)    AS team_id,
        COALESCE(to_team_name, from_team_name) AS team_name,
        injury_description,
        transaction_id
    FROM production.dim_transaction
    WHERE is_il_placement = true
      AND il_type IS NOT NULL

    UNION ALL

    -- IL activations
    SELECT
        player_id,
        transaction_date   AS status_start_date,
        'active'           AS status_type,
        COALESCE(to_team_id, from_team_id)    AS team_id,
        COALESCE(to_team_name, from_team_name) AS team_name,
        NULL               AS injury_description,
        transaction_id
    FROM production.dim_transaction
    WHERE is_il_activation = true

    UNION ALL

    -- Assignments / options to minors
    SELECT
        player_id,
        transaction_date   AS status_start_date,
        'option_minors'    AS status_type,
        COALESCE(to_team_id, from_team_id)    AS team_id,
        COALESCE(to_team_name, from_team_name) AS team_name,
        NULL               AS injury_description,
        transaction_id
    FROM production.dim_transaction
    WHERE type_code = 'ASG'

    UNION ALL

    -- DFA
    SELECT
        player_id,
        transaction_date   AS status_start_date,
        'designate'        AS status_type,
        COALESCE(to_team_id, from_team_id)    AS team_id,
        COALESCE(to_team_name, from_team_name) AS team_name,
        NULL               AS injury_description,
        transaction_id
    FROM production.dim_transaction
    WHERE type_code = 'DFA'

    UNION ALL

    -- Release
    SELECT
        player_id,
        transaction_date   AS status_start_date,
        'release'          AS status_type,
        COALESCE(from_team_id, to_team_id)    AS team_id,
        COALESCE(from_team_name, to_team_name) AS team_name,
        NULL               AS injury_description,
        transaction_id
    FROM production.dim_transaction
    WHERE type_code = 'REL'

    UNION ALL

    -- Trade
    SELECT
        player_id,
        transaction_date   AS status_start_date,
        'trade'            AS status_type,
        to_team_id         AS team_id,
        to_team_name       AS team_name,
        NULL               AS injury_description,
        transaction_id
    FROM production.dim_transaction
    WHERE type_code = 'TR'
),
deduped AS (
    SELECT DISTINCT ON (player_id, status_start_date, status_type)
        player_id,
        status_start_date,
        status_type,
        team_id,
        team_name,
        injury_description,
        transaction_id
    FROM status_events
    ORDER BY player_id, status_start_date, status_type, transaction_id DESC
),
with_end_date AS (
    SELECT
        d.*,
        LEAD(d.status_start_date) OVER (
            PARTITION BY d.player_id
            ORDER BY d.status_start_date, d.transaction_id
        ) AS status_end_date
    FROM deduped d
)
SELECT
    player_id,
    status_start_date,
    status_type,
    status_end_date,
    team_id,
    team_name,
    injury_description,
    status_end_date - status_start_date AS days_on_status,
    EXTRACT(YEAR FROM status_start_date)::int AS season
FROM with_end_date

ON CONFLICT (player_id, status_start_date, status_type) DO UPDATE
SET status_end_date     = EXCLUDED.status_end_date,
    team_id             = EXCLUDED.team_id,
    team_name           = EXCLUDED.team_name,
    injury_description  = EXCLUDED.injury_description,
    days_on_status      = EXCLUDED.days_on_status,
    season              = EXCLUDED.season;
