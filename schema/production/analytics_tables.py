from schema.spec_engine import TableSpec, ColumnSpec


# ── fact_player_form_rolling ─────────────────────────────────────────

FACT_PLAYER_FORM_ROLLING_COLS: dict[str, ColumnSpec] = {
    # identifiers
    'player_id': ColumnSpec(name='player_id', dtype='BigInteger', nullable=False),
    'game_pk': ColumnSpec(name='game_pk', dtype='BigInteger', nullable=False),
    'player_role': ColumnSpec(name='player_role', dtype='String(7)', nullable=False),
    'game_date': ColumnSpec(name='game_date', dtype='DATE', nullable=False),
    'season': ColumnSpec(name='season', dtype='Integer'),

    # batter 15g rolling
    'bat_pa_15': ColumnSpec(name='bat_pa_15', dtype='Integer'),
    'bat_ab_15': ColumnSpec(name='bat_ab_15', dtype='Integer'),
    'bat_h_15': ColumnSpec(name='bat_h_15', dtype='Integer'),
    'bat_2b_15': ColumnSpec(name='bat_2b_15', dtype='Integer'),
    'bat_3b_15': ColumnSpec(name='bat_3b_15', dtype='Integer'),
    'bat_hr_15': ColumnSpec(name='bat_hr_15', dtype='Integer'),
    'bat_bb_15': ColumnSpec(name='bat_bb_15', dtype='Integer'),
    'bat_k_15': ColumnSpec(name='bat_k_15', dtype='Integer'),
    'bat_sb_15': ColumnSpec(name='bat_sb_15', dtype='Integer'),
    'bat_rbi_15': ColumnSpec(name='bat_rbi_15', dtype='Integer'),
    'bat_tb_15': ColumnSpec(name='bat_tb_15', dtype='Integer'),
    'bat_hbp_15': ColumnSpec(name='bat_hbp_15', dtype='Integer'),
    'bat_avg_15': ColumnSpec(name='bat_avg_15', dtype='REAL'),
    'bat_obp_15': ColumnSpec(name='bat_obp_15', dtype='REAL'),
    'bat_slg_15': ColumnSpec(name='bat_slg_15', dtype='REAL'),
    'bat_ops_15': ColumnSpec(name='bat_ops_15', dtype='REAL'),

    # batter 30g rolling
    'bat_pa_30': ColumnSpec(name='bat_pa_30', dtype='Integer'),
    'bat_ab_30': ColumnSpec(name='bat_ab_30', dtype='Integer'),
    'bat_h_30': ColumnSpec(name='bat_h_30', dtype='Integer'),
    'bat_2b_30': ColumnSpec(name='bat_2b_30', dtype='Integer'),
    'bat_3b_30': ColumnSpec(name='bat_3b_30', dtype='Integer'),
    'bat_hr_30': ColumnSpec(name='bat_hr_30', dtype='Integer'),
    'bat_bb_30': ColumnSpec(name='bat_bb_30', dtype='Integer'),
    'bat_k_30': ColumnSpec(name='bat_k_30', dtype='Integer'),
    'bat_sb_30': ColumnSpec(name='bat_sb_30', dtype='Integer'),
    'bat_rbi_30': ColumnSpec(name='bat_rbi_30', dtype='Integer'),
    'bat_tb_30': ColumnSpec(name='bat_tb_30', dtype='Integer'),
    'bat_hbp_30': ColumnSpec(name='bat_hbp_30', dtype='Integer'),
    'bat_avg_30': ColumnSpec(name='bat_avg_30', dtype='REAL'),
    'bat_obp_30': ColumnSpec(name='bat_obp_30', dtype='REAL'),
    'bat_slg_30': ColumnSpec(name='bat_slg_30', dtype='REAL'),
    'bat_ops_30': ColumnSpec(name='bat_ops_30', dtype='REAL'),

    # pitcher 15g rolling
    'pit_ip_15': ColumnSpec(name='pit_ip_15', dtype='REAL'),
    'pit_er_15': ColumnSpec(name='pit_er_15', dtype='Integer'),
    'pit_h_15': ColumnSpec(name='pit_h_15', dtype='Integer'),
    'pit_bb_15': ColumnSpec(name='pit_bb_15', dtype='Integer'),
    'pit_k_15': ColumnSpec(name='pit_k_15', dtype='Integer'),
    'pit_hr_15': ColumnSpec(name='pit_hr_15', dtype='Integer'),
    'pit_bf_15': ColumnSpec(name='pit_bf_15', dtype='Integer'),
    'pit_era_15': ColumnSpec(name='pit_era_15', dtype='REAL'),
    'pit_whip_15': ColumnSpec(name='pit_whip_15', dtype='REAL'),
    'pit_k9_15': ColumnSpec(name='pit_k9_15', dtype='REAL'),
    'pit_bb9_15': ColumnSpec(name='pit_bb9_15', dtype='REAL'),

    # pitcher 30g rolling
    'pit_ip_30': ColumnSpec(name='pit_ip_30', dtype='REAL'),
    'pit_er_30': ColumnSpec(name='pit_er_30', dtype='Integer'),
    'pit_h_30': ColumnSpec(name='pit_h_30', dtype='Integer'),
    'pit_bb_30': ColumnSpec(name='pit_bb_30', dtype='Integer'),
    'pit_k_30': ColumnSpec(name='pit_k_30', dtype='Integer'),
    'pit_hr_30': ColumnSpec(name='pit_hr_30', dtype='Integer'),
    'pit_bf_30': ColumnSpec(name='pit_bf_30', dtype='Integer'),
    'pit_era_30': ColumnSpec(name='pit_era_30', dtype='REAL'),
    'pit_whip_30': ColumnSpec(name='pit_whip_30', dtype='REAL'),
    'pit_k9_30': ColumnSpec(name='pit_k9_30', dtype='REAL'),
    'pit_bb9_30': ColumnSpec(name='pit_bb9_30', dtype='REAL'),

    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_PLAYER_FORM_ROLLING_SPEC = TableSpec(
    name='fact_player_form_rolling',
    pk=['player_id', 'game_pk', 'player_role'],
    columns=FACT_PLAYER_FORM_ROLLING_COLS,
)


# ── fact_platoon_splits ──────────────────────────────────────────────

FACT_PLATOON_SPLITS_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(name='player_id', dtype='BigInteger', nullable=False),
    'season': ColumnSpec(name='season', dtype='Integer', nullable=False),
    'player_role': ColumnSpec(name='player_role', dtype='String(7)', nullable=False),
    'platoon_side': ColumnSpec(name='platoon_side', dtype='String(3)', nullable=False),

    # counting
    'pa': ColumnSpec(name='pa', dtype='Integer'),
    'ab': ColumnSpec(name='ab', dtype='Integer'),
    'h': ColumnSpec(name='h', dtype='Integer'),
    'doubles': ColumnSpec(name='doubles', dtype='Integer'),
    'triples': ColumnSpec(name='triples', dtype='Integer'),
    'hr': ColumnSpec(name='hr', dtype='Integer'),
    'bb': ColumnSpec(name='bb', dtype='Integer'),
    'k': ColumnSpec(name='k', dtype='Integer'),
    'hbp': ColumnSpec(name='hbp', dtype='Integer'),
    'sf': ColumnSpec(name='sf', dtype='Integer'),

    # rates
    'avg': ColumnSpec(name='avg', dtype='REAL'),
    'obp': ColumnSpec(name='obp', dtype='REAL'),
    'slg': ColumnSpec(name='slg', dtype='REAL'),
    'ops': ColumnSpec(name='ops', dtype='REAL'),
    'woba': ColumnSpec(name='woba', dtype='REAL'),
    'k_pct': ColumnSpec(name='k_pct', dtype='REAL'),
    'bb_pct': ColumnSpec(name='bb_pct', dtype='REAL'),

    # pitch-level
    'total_pitches': ColumnSpec(name='total_pitches', dtype='Integer'),
    'whiff_rate': ColumnSpec(name='whiff_rate', dtype='REAL'),
    'chase_rate': ColumnSpec(name='chase_rate', dtype='REAL'),

    # batted ball
    'hard_hit_pct': ColumnSpec(name='hard_hit_pct', dtype='REAL'),
    'sweet_spot_pct': ColumnSpec(name='sweet_spot_pct', dtype='REAL'),
    'xwoba_avg': ColumnSpec(name='xwoba_avg', dtype='REAL'),

    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_PLATOON_SPLITS_SPEC = TableSpec(
    name='fact_platoon_splits',
    pk=['player_id', 'season', 'player_role', 'platoon_side'],
    columns=FACT_PLATOON_SPLITS_COLS,
)


# ── fact_streak_indicator ────────────────────────────────────────────

FACT_STREAK_INDICATOR_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(name='player_id', dtype='BigInteger', nullable=False),
    'game_pk': ColumnSpec(name='game_pk', dtype='BigInteger', nullable=False),
    'player_role': ColumnSpec(name='player_role', dtype='String(7)', nullable=False),
    'game_date': ColumnSpec(name='game_date', dtype='DATE'),
    'season': ColumnSpec(name='season', dtype='Integer'),
    'games_in_window': ColumnSpec(name='games_in_window', dtype='SmallInteger'),

    # batter z-scores
    'bat_avg_10g': ColumnSpec(name='bat_avg_10g', dtype='REAL'),
    'bat_avg_season': ColumnSpec(name='bat_avg_season', dtype='REAL'),
    'bat_avg_zscore': ColumnSpec(name='bat_avg_zscore', dtype='REAL'),
    'bat_obp_10g': ColumnSpec(name='bat_obp_10g', dtype='REAL'),
    'bat_obp_season': ColumnSpec(name='bat_obp_season', dtype='REAL'),
    'bat_obp_zscore': ColumnSpec(name='bat_obp_zscore', dtype='REAL'),
    'bat_slg_10g': ColumnSpec(name='bat_slg_10g', dtype='REAL'),
    'bat_slg_season': ColumnSpec(name='bat_slg_season', dtype='REAL'),
    'bat_slg_zscore': ColumnSpec(name='bat_slg_zscore', dtype='REAL'),

    # pitcher z-scores
    'pit_era_10g': ColumnSpec(name='pit_era_10g', dtype='REAL'),
    'pit_era_season': ColumnSpec(name='pit_era_season', dtype='REAL'),
    'pit_era_zscore': ColumnSpec(name='pit_era_zscore', dtype='REAL'),
    'pit_whip_10g': ColumnSpec(name='pit_whip_10g', dtype='REAL'),
    'pit_whip_season': ColumnSpec(name='pit_whip_season', dtype='REAL'),
    'pit_whip_zscore': ColumnSpec(name='pit_whip_zscore', dtype='REAL'),

    'streak_flag': ColumnSpec(name='streak_flag', dtype='String(4)'),

    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_STREAK_INDICATOR_SPEC = TableSpec(
    name='fact_streak_indicator',
    pk=['player_id', 'game_pk', 'player_role'],
    columns=FACT_STREAK_INDICATOR_COLS,
)


# ── fact_matchup_history ─────────────────────────────────────────────

FACT_MATCHUP_HISTORY_COLS: dict[str, ColumnSpec] = {
    'batter_id': ColumnSpec(name='batter_id', dtype='BigInteger', nullable=False),
    'pitcher_id': ColumnSpec(name='pitcher_id', dtype='BigInteger', nullable=False),

    # counting
    'pa': ColumnSpec(name='pa', dtype='Integer'),
    'ab': ColumnSpec(name='ab', dtype='Integer'),
    'h': ColumnSpec(name='h', dtype='Integer'),
    'doubles': ColumnSpec(name='doubles', dtype='Integer'),
    'triples': ColumnSpec(name='triples', dtype='Integer'),
    'hr': ColumnSpec(name='hr', dtype='Integer'),
    'bb': ColumnSpec(name='bb', dtype='Integer'),
    'k': ColumnSpec(name='k', dtype='Integer'),
    'hbp': ColumnSpec(name='hbp', dtype='Integer'),
    'sf': ColumnSpec(name='sf', dtype='Integer'),

    # rates
    'avg': ColumnSpec(name='avg', dtype='REAL'),
    'obp': ColumnSpec(name='obp', dtype='REAL'),
    'slg': ColumnSpec(name='slg', dtype='REAL'),
    'ops': ColumnSpec(name='ops', dtype='REAL'),

    # pitch-level
    'total_pitches': ColumnSpec(name='total_pitches', dtype='Integer'),
    'whiff_rate': ColumnSpec(name='whiff_rate', dtype='REAL'),
    'chase_rate': ColumnSpec(name='chase_rate', dtype='REAL'),
    'zone_contact_rate': ColumnSpec(name='zone_contact_rate', dtype='REAL'),

    # batted ball
    'avg_exit_velo': ColumnSpec(name='avg_exit_velo', dtype='REAL'),
    'avg_launch_angle': ColumnSpec(name='avg_launch_angle', dtype='REAL'),
    'hard_hit_pct': ColumnSpec(name='hard_hit_pct', dtype='REAL'),
    'xwoba_avg': ColumnSpec(name='xwoba_avg', dtype='REAL'),

    # dates
    'first_matchup_date': ColumnSpec(name='first_matchup_date', dtype='DATE'),
    'last_matchup_date': ColumnSpec(name='last_matchup_date', dtype='DATE'),

    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()',
    ),
}

FACT_MATCHUP_HISTORY_SPEC = TableSpec(
    name='fact_matchup_history',
    pk=['batter_id', 'pitcher_id'],
    columns=FACT_MATCHUP_HISTORY_COLS,
)
