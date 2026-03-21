from schema.spec_engine import TableSpec, ColumnSpec

# ── MiLB Batting Game Logs ──────────────────────────────────────────
# Mirrors staging.batting_boxscores with added level/sport_id context

MILB_BATTING_COLS: dict[str, ColumnSpec] = {
    'batter_id': ColumnSpec(
        name='batter_id',
        dtype='BigInteger',
        primary_key=True,
        nullable=False
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        primary_key=True,
        nullable=False
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='Integer',
        primary_key=True,
        nullable=False
    ),
    'batter_name': ColumnSpec(
        name='batter_name',
        dtype='Text'
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    'position': ColumnSpec(
        name='position',
        dtype='Text'
    ),
    'sport_id': ColumnSpec(
        name='sport_id',
        dtype='SmallInteger',
        nullable=False
    ),
    'level': ColumnSpec(
        name='level',
        dtype='Text',
        nullable=False
    ),
    'parent_org_id': ColumnSpec(
        name='parent_org_id',
        dtype='Integer'
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE',
        nullable=False
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer',
        nullable=False
    ),
    'ground_outs': ColumnSpec(
        name='ground_outs',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'air_outs': ColumnSpec(
        name='air_outs',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'runs': ColumnSpec(
        name='runs',
        dtype='SmallInteger',
        bounds=(0, 8)
    ),
    'doubles': ColumnSpec(
        name='doubles',
        dtype='SmallInteger',
        bounds=(0, 8)
    ),
    'triples': ColumnSpec(
        name='triples',
        dtype='SmallInteger',
        bounds=(0, 8)
    ),
    'home_runs': ColumnSpec(
        name='home_runs',
        dtype='SmallInteger',
        bounds=(0, 6)
    ),
    'strikeouts': ColumnSpec(
        name='strikeouts',
        dtype='SmallInteger',
        bounds=(0, 8)
    ),
    'walks': ColumnSpec(
        name='walks',
        dtype='SmallInteger',
        bounds=(0, 8)
    ),
    'intentional_walks': ColumnSpec(
        name='intentional_walks',
        dtype='SmallInteger',
        bounds=(0, 6)
    ),
    'hits': ColumnSpec(
        name='hits',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'hit_by_pitch': ColumnSpec(
        name='hit_by_pitch',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'at_bats': ColumnSpec(
        name='at_bats',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'caught_stealing': ColumnSpec(
        name='caught_stealing',
        dtype='SmallInteger',
        bounds=(0, 5)
    ),
    'sb': ColumnSpec(
        name='sb',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'sb_pct': ColumnSpec(
        name='sb_pct',
        dtype='REAL',
        bounds=(0.0, 1.0)
    ),
    'plate_appearances': ColumnSpec(
        name='plate_appearances',
        dtype='SmallInteger',
        bounds=(0, 10)
    ),
    'total_bases': ColumnSpec(
        name='total_bases',
        dtype='SmallInteger',
        bounds=(0, 16)
    ),
    'rbi': ColumnSpec(
        name='rbi',
        dtype='SmallInteger',
        bounds=(0, 20)
    ),
    'errors': ColumnSpec(
        name='errors',
        dtype='SmallInteger',
        bounds=(0, 20)
    ),
    'source': ColumnSpec(
        name='source',
        dtype='Text'
    ),
    'load_id': ColumnSpec(
        name='load_id',
        dtype='UUID',
        server_default='gen_random_uuid()'
    ),
    'ingested_at': ColumnSpec(
        name='ingested_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

MILB_BATTING_GAME_LOGS_SPEC = TableSpec(
    name='milb_batting_game_logs',
    pk=['batter_id', 'game_pk', 'team_id'],
    columns=MILB_BATTING_COLS
)


# ── MiLB Pitching Game Logs ────────────────────────────────────────
# Mirrors staging.pitching_boxscores with added level/sport_id context

MILB_PITCHING_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'pitcher_id': ColumnSpec(
        name='pitcher_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    'pitcher_name': ColumnSpec(
        name='pitcher_name',
        dtype='Text'
    ),
    'sport_id': ColumnSpec(
        name='sport_id',
        dtype='SmallInteger',
        nullable=False
    ),
    'level': ColumnSpec(
        name='level',
        dtype='Text',
        nullable=False
    ),
    'parent_org_id': ColumnSpec(
        name='parent_org_id',
        dtype='Integer'
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE',
        nullable=False
    ),
    'season': ColumnSpec(
        name='season',
        dtype='Integer',
        nullable=False
    ),
    'is_starter': ColumnSpec(
        name='is_starter',
        dtype='Boolean'
    ),
    'fly_outs': ColumnSpec(
        name='fly_outs',
        dtype='SmallInteger',
        bounds=(0, 35)
    ),
    'ground_outs': ColumnSpec(
        name='ground_outs',
        dtype='SmallInteger',
        bounds=(0, 35)
    ),
    'air_outs': ColumnSpec(
        name='air_outs',
        dtype='SmallInteger',
        bounds=(0, 35)
    ),
    'runs': ColumnSpec(
        name='runs',
        dtype='SmallInteger',
        bounds=(0, 150)
    ),
    'doubles': ColumnSpec(
        name='doubles',
        dtype='SmallInteger',
        bounds=(0, 100)
    ),
    'triples': ColumnSpec(
        name='triples',
        dtype='SmallInteger',
        bounds=(0, 30)
    ),
    'home_runs': ColumnSpec(
        name='home_runs',
        dtype='SmallInteger',
        bounds=(0, 40)
    ),
    'strike_outs': ColumnSpec(
        name='strike_outs',
        dtype='SmallInteger',
        bounds=(0, 40)
    ),
    'walks': ColumnSpec(
        name='walks',
        dtype='SmallInteger',
        bounds=(0, 30)
    ),
    'intentional_walks': ColumnSpec(
        name='intentional_walks',
        dtype='SmallInteger',
        bounds=(0, 30)
    ),
    'hits': ColumnSpec(
        name='hits',
        dtype='SmallInteger',
        bounds=(0, 45)
    ),
    'hit_by_pitch': ColumnSpec(
        name='hit_by_pitch',
        dtype='SmallInteger',
        bounds=(0, 15)
    ),
    'at_bats': ColumnSpec(
        name='at_bats',
        dtype='SmallInteger',
        bounds=(0, 45)
    ),
    'caught_stealing': ColumnSpec(
        name='caught_stealing',
        dtype='SmallInteger',
        bounds=(0, 20)
    ),
    'stolen_bases': ColumnSpec(
        name='stolen_bases',
        dtype='SmallInteger',
        bounds=(0, 30)
    ),
    'stolen_base_pct': ColumnSpec(
        name='stolen_base_pct',
        dtype='REAL',
        bounds=(0.0, 100.0)
    ),
    'number_of_pitches': ColumnSpec(
        name='number_of_pitches',
        dtype='SmallInteger',
        bounds=(1, 150)
    ),
    'innings_pitched': ColumnSpec(
        name='innings_pitched',
        dtype='REAL',
        bounds=(0, 12)
    ),
    'wins': ColumnSpec(
        name='wins',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'losses': ColumnSpec(
        name='losses',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'saves': ColumnSpec(
        name='saves',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'save_opportunities': ColumnSpec(
        name='save_opportunities',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'holds': ColumnSpec(
        name='holds',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'blown_saves': ColumnSpec(
        name='blown_saves',
        dtype='SmallInteger',
        bounds=(0, 1)
    ),
    'earned_runs': ColumnSpec(
        name='earned_runs',
        dtype='SmallInteger',
        bounds=(0, 20)
    ),
    'batters_faced': ColumnSpec(
        name='batters_faced',
        dtype='SmallInteger',
        bounds=(0, 40)
    ),
    'outs': ColumnSpec(
        name='outs',
        dtype='SmallInteger',
        bounds=(0, 50)
    ),
    'complete_game': ColumnSpec(
        name='complete_game',
        dtype='Boolean'
    ),
    'shutout': ColumnSpec(
        name='shutout',
        dtype='Boolean'
    ),
    'balls': ColumnSpec(
        name='balls',
        dtype='SmallInteger',
        bounds=(0, 50)
    ),
    'strikes': ColumnSpec(
        name='strikes',
        dtype='SmallInteger',
        bounds=(0, 80)
    ),
    'strike_pct': ColumnSpec(
        name='strike_pct',
        dtype='REAL',
        bounds=(0.0, 100.0)
    ),
    'hit_batsmen': ColumnSpec(
        name='hit_batsmen',
        dtype='SmallInteger',
        bounds=(0, 15)
    ),
    'balks': ColumnSpec(
        name='balks',
        dtype='SmallInteger',
        bounds=(0, 50)
    ),
    'wild_pitches': ColumnSpec(
        name='wild_pitches',
        dtype='SmallInteger',
        bounds=(0, 100)
    ),
    'pickoffs': ColumnSpec(
        name='pickoffs',
        dtype='SmallInteger',
        bounds=(0, 75)
    ),
    'rbi': ColumnSpec(
        name='rbi',
        dtype='SmallInteger',
        bounds=(0, 50)
    ),
    'games_finished': ColumnSpec(
        name='games_finished',
        dtype='Boolean'
    ),
    'inherited_runners': ColumnSpec(
        name='inherited_runners',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'inherited_runners_scored': ColumnSpec(
        name='inherited_runners_scored',
        dtype='SmallInteger',
        bounds=(0, 3)
    ),
    'catchers_interference': ColumnSpec(
        name='catchers_interference',
        dtype='SmallInteger',
        bounds=(0, 40)
    ),
    'sac_bunts': ColumnSpec(
        name='sac_bunts',
        dtype='SmallInteger',
        bounds=(0, 40)
    ),
    'sac_flies': ColumnSpec(
        name='sac_flies',
        dtype='SmallInteger',
        bounds=(0, 30)
    ),
    'passed_ball': ColumnSpec(
        name='passed_ball',
        dtype='SmallInteger',
        bounds=(0, 150)
    ),
    'source': ColumnSpec(
        name='source',
        dtype='Text'
    ),
    'ingested_at': ColumnSpec(
        name='ingested_at',
        dtype='DateTime'
    ),
    'load_id': ColumnSpec(
        name='load_id',
        dtype='UUID'
    )
}

MILB_PITCHING_GAME_LOGS_SPEC = TableSpec(
    name='milb_pitching_game_logs',
    pk=['game_pk', 'pitcher_id', 'team_id'],
    columns=MILB_PITCHING_COLS
)
