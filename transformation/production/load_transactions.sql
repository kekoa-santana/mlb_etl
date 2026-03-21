INSERT INTO production.dim_transaction (
    transaction_id, player_id, player_name,
    to_team_id, to_team_name, from_team_id, from_team_name,
    transaction_date, effective_date, resolution_date,
    type_code, type_desc, description,
    is_il_placement, is_il_activation, is_il_transfer,
    il_type, injury_description
)
SELECT
    t.transaction_id,
    t.player_id,
    t.player_name,
    t.to_team_id,
    t.to_team_name,
    t.from_team_id,
    t.from_team_name,
    t.date           AS transaction_date,
    t.effective_date,
    t.resolution_date,
    t.type_code,
    t.type_desc,
    t.description,

    -- IL placement: "placed ... on the N-day injured list"
    (t.type_code = 'SC'
        AND t.description ~* 'placed .* on the .* injured list') AS is_il_placement,

    -- IL activation: "activated ... from the N-day injured list"
    (t.type_code = 'SC'
        AND t.description ~* 'activated .* from the .* injured list') AS is_il_activation,

    -- IL transfer: "transferred ... to the N-day injured list"
    (t.type_code = 'SC'
        AND t.description ~* 'transferred .* to the .* injured list') AS is_il_transfer,

    -- Extract IL type (7-day, 10-day, 15-day, 60-day)
    CASE
        WHEN t.description ~* '60-day injured list' THEN '60-day'
        WHEN t.description ~* '15-day injured list' THEN '15-day'
        WHEN t.description ~* '10-day injured list' THEN '10-day'
        WHEN t.description ~* '7-day injured list'  THEN '7-day'
        ELSE NULL
    END AS il_type,

    -- Extract injury description: text after the last period in IL descriptions
    CASE
        WHEN t.type_code = 'SC'
            AND t.description ~* 'injured list'
            AND t.description ~ '\. [A-Z]'
        THEN trim(substring(t.description FROM '\.[ ]+([A-Z][^.]+)\.?$'))
        ELSE NULL
    END AS injury_description

FROM raw.transactions t
WHERE t.player_id IS NOT NULL
ON CONFLICT (transaction_id) DO UPDATE
SET player_name       = EXCLUDED.player_name,
    to_team_id        = EXCLUDED.to_team_id,
    to_team_name      = EXCLUDED.to_team_name,
    from_team_id      = EXCLUDED.from_team_id,
    from_team_name    = EXCLUDED.from_team_name,
    transaction_date  = EXCLUDED.transaction_date,
    effective_date    = EXCLUDED.effective_date,
    resolution_date   = EXCLUDED.resolution_date,
    type_code         = EXCLUDED.type_code,
    type_desc         = EXCLUDED.type_desc,
    description       = EXCLUDED.description,
    is_il_placement   = EXCLUDED.is_il_placement,
    is_il_activation  = EXCLUDED.is_il_activation,
    is_il_transfer    = EXCLUDED.is_il_transfer,
    il_type           = EXCLUDED.il_type,
    injury_description = EXCLUDED.injury_description;
