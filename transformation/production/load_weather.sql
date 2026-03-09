INSERT INTO production.dim_weather (
    game_pk, temperature, condition, is_dome,
    wind_speed, wind_direction, wind_category
)
SELECT DISTINCT ON (lb.game_pk)
    lb.game_pk,
    -- temperature: extract leading digits from "73 degrees, Sunny."
    (regexp_match(w.weather_val, '^(\d+) degrees'))[1]::smallint AS temperature,
    -- condition: text after "NN degrees, " with trailing period stripped
    rtrim((regexp_match(w.weather_val, '^\d+ degrees, (.+)'))[1], '.') AS condition,
    -- is_dome: Dome or Roof Closed
    rtrim((regexp_match(w.weather_val, '^\d+ degrees, (.+)'))[1], '.') IN ('Dome', 'Roof Closed') AS is_dome,
    -- wind_speed: extract leading digits from "8 mph, In From LF."
    (regexp_match(wi.wind_val, '^(\d+) mph'))[1]::smallint AS wind_speed,
    -- wind_direction: text after "NN mph, " with trailing period stripped
    rtrim((regexp_match(wi.wind_val, '^\d+ mph, (.+)'))[1], '.') AS wind_direction,
    -- wind_category: simplified for quick filtering
    CASE
        WHEN wi.wind_val ~ 'In From'  THEN 'in'
        WHEN wi.wind_val ~ 'Out To'   THEN 'out'
        WHEN wi.wind_val ~ 'L To R|R To L' THEN 'cross'
        ELSE 'none'
    END AS wind_category
FROM raw.landing_boxscores lb
CROSS JOIN LATERAL (
    SELECT elem->>'value' AS weather_val
    FROM jsonb_array_elements(payload::jsonb->'info') elem
    WHERE elem->>'label' = 'Weather'
) w
CROSS JOIN LATERAL (
    SELECT elem->>'value' AS wind_val
    FROM jsonb_array_elements(payload::jsonb->'info') elem
    WHERE elem->>'label' = 'Wind'
) wi
ON CONFLICT (game_pk) DO UPDATE
SET temperature     = EXCLUDED.temperature,
    condition       = EXCLUDED.condition,
    is_dome         = EXCLUDED.is_dome,
    wind_speed      = EXCLUDED.wind_speed,
    wind_direction  = EXCLUDED.wind_direction,
    wind_category   = EXCLUDED.wind_category;
