ALTER TABLE production.sat_batted_balls
    ADD COLUMN pa_id BIGINT,
    ADD COLUMN xba REAL,
    ADD COLUMN xslg REAL,
    ADD COLUMN xwoba REAL,
    ADD COLUMN woba_value REAL,
    ADD COLUMN babip_value SMALLINT,
    ADD COLUMN iso_value SMALLINT,
    ADD COLUMN hit_location SMALLINT,

    ADD COLUMN hard_hit BOOLEAN,
    ADD COLUMN sweet_spot BOOLEAN,
    ADD COLUMN ideal_contact BOOLEAN,

    ADD COLUMN la_band TEXT,
    ADD COLUMN ev_band TEXT,

    ADD COLUMN hc_x_centered REAL,
    ADD COLUMN spray_bucket TEXT