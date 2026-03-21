from schema.spec_engine import TableSpec, ColumnSpec

# ---------------------------------------------------------------------------
# 1. production.fact_milb_player_game
#    Grain: player_id + game_pk + player_role (batter / pitcher)
# ---------------------------------------------------------------------------
FACT_MILB_PLAYER_GAME_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'player_role': ColumnSpec(
        name='player_role',
        dtype='String(7)',
        nullable=False,
        primary_key=True
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE'
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer'
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Integer'
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    'sport_id': ColumnSpec(
        name='sport_id',
        dtype='SmallInteger'
    ),
    'level': ColumnSpec(
        name='level',
        dtype='Text'
    ),
    'parent_org_id': ColumnSpec(
        name='parent_org_id',
        dtype='Integer'
    ),
    # -- batter columns --
    'bat_pa': ColumnSpec(
        name='bat_pa',
        dtype='SmallInteger'
    ),
    'bat_ab': ColumnSpec(
        name='bat_ab',
        dtype='SmallInteger'
    ),
    'bat_h': ColumnSpec(
        name='bat_h',
        dtype='SmallInteger'
    ),
    'bat_2b': ColumnSpec(
        name='bat_2b',
        dtype='SmallInteger'
    ),
    'bat_3b': ColumnSpec(
        name='bat_3b',
        dtype='SmallInteger'
    ),
    'bat_hr': ColumnSpec(
        name='bat_hr',
        dtype='SmallInteger'
    ),
    'bat_r': ColumnSpec(
        name='bat_r',
        dtype='SmallInteger'
    ),
    'bat_rbi': ColumnSpec(
        name='bat_rbi',
        dtype='SmallInteger'
    ),
    'bat_bb': ColumnSpec(
        name='bat_bb',
        dtype='SmallInteger'
    ),
    'bat_k': ColumnSpec(
        name='bat_k',
        dtype='SmallInteger'
    ),
    'bat_hbp': ColumnSpec(
        name='bat_hbp',
        dtype='SmallInteger'
    ),
    'bat_sb': ColumnSpec(
        name='bat_sb',
        dtype='SmallInteger'
    ),
    'bat_cs': ColumnSpec(
        name='bat_cs',
        dtype='SmallInteger'
    ),
    'bat_tb': ColumnSpec(
        name='bat_tb',
        dtype='SmallInteger'
    ),
    'bat_errors': ColumnSpec(
        name='bat_errors',
        dtype='SmallInteger'
    ),
    # -- pitcher columns --
    'pit_ip': ColumnSpec(
        name='pit_ip',
        dtype='REAL'
    ),
    'pit_er': ColumnSpec(
        name='pit_er',
        dtype='SmallInteger'
    ),
    'pit_r': ColumnSpec(
        name='pit_r',
        dtype='SmallInteger'
    ),
    'pit_h': ColumnSpec(
        name='pit_h',
        dtype='SmallInteger'
    ),
    'pit_bb': ColumnSpec(
        name='pit_bb',
        dtype='SmallInteger'
    ),
    'pit_k': ColumnSpec(
        name='pit_k',
        dtype='SmallInteger'
    ),
    'pit_hr': ColumnSpec(
        name='pit_hr',
        dtype='SmallInteger'
    ),
    'pit_bf': ColumnSpec(
        name='pit_bf',
        dtype='SmallInteger'
    ),
    'pit_w': ColumnSpec(
        name='pit_w',
        dtype='SmallInteger'
    ),
    'pit_l': ColumnSpec(
        name='pit_l',
        dtype='SmallInteger'
    ),
    'pit_sv': ColumnSpec(
        name='pit_sv',
        dtype='SmallInteger'
    ),
    'pit_hld': ColumnSpec(
        name='pit_hld',
        dtype='SmallInteger'
    ),
    'pit_bs': ColumnSpec(
        name='pit_bs',
        dtype='SmallInteger'
    ),
    'pit_pitches': ColumnSpec(
        name='pit_pitches',
        dtype='SmallInteger'
    ),
    'pit_strikes': ColumnSpec(
        name='pit_strikes',
        dtype='SmallInteger'
    ),
    'pit_is_starter': ColumnSpec(
        name='pit_is_starter',
        dtype='Boolean'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_MILB_PLAYER_GAME_SPEC = TableSpec(
    name='fact_milb_player_game',
    pk=['player_id', 'game_pk', 'player_role'],
    columns=FACT_MILB_PLAYER_GAME_COLS
)

# ---------------------------------------------------------------------------
# 2. production.fact_prospect_snapshot
#    Grain: player_id + season
# ---------------------------------------------------------------------------
FACT_PROSPECT_SNAPSHOT_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'season': ColumnSpec(
        name='season',
        dtype='SmallInteger',
        nullable=False,
        primary_key=True
    ),
    'full_name': ColumnSpec(
        name='full_name',
        dtype='Text'
    ),
    'primary_position': ColumnSpec(
        name='primary_position',
        dtype='Text'
    ),
    'bat_side': ColumnSpec(
        name='bat_side',
        dtype='Text'
    ),
    'pitch_hand': ColumnSpec(
        name='pitch_hand',
        dtype='Text'
    ),
    'birth_date': ColumnSpec(
        name='birth_date',
        dtype='DATE'
    ),
    'age_at_season_start': ColumnSpec(
        name='age_at_season_start',
        dtype='SmallInteger'
    ),
    'level': ColumnSpec(
        name='level',
        dtype='Text'
    ),
    'sport_id': ColumnSpec(
        name='sport_id',
        dtype='SmallInteger'
    ),
    'parent_org_id': ColumnSpec(
        name='parent_org_id',
        dtype='Integer'
    ),
    'parent_org_name': ColumnSpec(
        name='parent_org_name',
        dtype='Text'
    ),
    'milb_team_id': ColumnSpec(
        name='milb_team_id',
        dtype='Integer'
    ),
    'milb_team_name': ColumnSpec(
        name='milb_team_name',
        dtype='Text'
    ),
    'status_code': ColumnSpec(
        name='status_code',
        dtype='Text'
    ),
    'mlb_debut_date': ColumnSpec(
        name='mlb_debut_date',
        dtype='DATE'
    ),
    'draft_year': ColumnSpec(
        name='draft_year',
        dtype='SmallInteger'
    ),
    'games_played': ColumnSpec(
        name='games_played',
        dtype='Integer'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_PROSPECT_SNAPSHOT_SPEC = TableSpec(
    name='fact_prospect_snapshot',
    pk=['player_id', 'season'],
    columns=FACT_PROSPECT_SNAPSHOT_COLS
)

# ---------------------------------------------------------------------------
# 3. production.fact_prospect_transition
#    Grain: player_id + event_date + from_level + to_level
# ---------------------------------------------------------------------------
FACT_PROSPECT_TRANSITION_COLS: dict[str, ColumnSpec] = {
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'event_date': ColumnSpec(
        name='event_date',
        dtype='DATE',
        nullable=False,
        primary_key=True
    ),
    'from_level': ColumnSpec(
        name='from_level',
        dtype='Text',
        nullable=False,
        primary_key=True
    ),
    'to_level': ColumnSpec(
        name='to_level',
        dtype='Text',
        nullable=False,
        primary_key=True
    ),
    'from_sport_id': ColumnSpec(
        name='from_sport_id',
        dtype='SmallInteger'
    ),
    'to_sport_id': ColumnSpec(
        name='to_sport_id',
        dtype='SmallInteger'
    ),
    'from_team_name': ColumnSpec(
        name='from_team_name',
        dtype='Text'
    ),
    'to_team_name': ColumnSpec(
        name='to_team_name',
        dtype='Text'
    ),
    'transition_type': ColumnSpec(
        name='transition_type',
        dtype='String(10)'
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

FACT_PROSPECT_TRANSITION_SPEC = TableSpec(
    name='fact_prospect_transition',
    pk=['player_id', 'event_date', 'from_level', 'to_level'],
    columns=FACT_PROSPECT_TRANSITION_COLS
)
