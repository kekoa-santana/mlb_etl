INSERT INTO production.dim_umpire (
    game_pk, hp_umpire_name
)
SELECT
    lb.game_pk,
    rtrim((regexp_match(
        (SELECT elem->>'value' FROM jsonb_array_elements(payload::jsonb->'info') elem WHERE elem->>'label' = 'Umpires'),
        'HP: ([^.]+)\.'
    ))[1]) AS hp_umpire_name
FROM raw.landing_boxscores lb
WHERE EXISTS (
    SELECT 1 FROM jsonb_array_elements(payload::jsonb->'info') elem
    WHERE elem->>'label' = 'Umpires'
)
ON CONFLICT (game_pk) DO UPDATE
SET hp_umpire_name = EXCLUDED.hp_umpire_name;
