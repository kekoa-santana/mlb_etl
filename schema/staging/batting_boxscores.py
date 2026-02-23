from schema.spec_engine import TableSpec, ColumnSpec

BATTING_COLS: dict[str, ColumnSpec] = {
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
        dtpye='Text'
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    'position': ColumnSpec(
        name='position',
        dtype='Text'
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
        name='strikeout',
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
    # Redundive with at_bats but going to add anyway for now to check discrepancies
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
        server_default='get_random_uuid()'
    ),
    'ingested_at': ColumnSpec(
        name='UUID',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

BATTING_BOXSCORE_SPEC = TableSpec(
    name='batting_boxscores',
    pk=['batter_id', 'team_id', 'game_pk'],
    columns=BATTING_COLS
)