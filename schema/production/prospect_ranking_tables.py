from schema.spec_engine import TableSpec, ColumnSpec

# ---------------------------------------------------------------------------
# production.dim_prospect_ranking
# Grain: player_id + season + source
# Stores prospect rankings and scouting data from FanGraphs "The Board".
# Source distinguishes preseason ("fg_report") from mid-season ("fg_updated").
# Grade columns are nullable (not all exports include 20-80 grades).
# ---------------------------------------------------------------------------
DIM_PROSPECT_RANKING_COLS: dict[str, ColumnSpec] = {
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
    'source': ColumnSpec(
        name='source',
        dtype='String(20)',
        nullable=False,
        primary_key=True
    ),
    # -- identifiers --
    'fg_id': ColumnSpec(
        name='fg_id',
        dtype='Text'
    ),
    'player_name': ColumnSpec(
        name='player_name',
        dtype='Text'
    ),
    'org': ColumnSpec(
        name='org',
        dtype='String(5)'
    ),
    'position': ColumnSpec(
        name='position',
        dtype='String(15)'
    ),
    'current_level': ColumnSpec(
        name='current_level',
        dtype='String(10)'
    ),
    # -- rankings --
    'overall_rank': ColumnSpec(
        name='overall_rank',
        dtype='SmallInteger',
        bounds=(1, 500)
    ),
    'org_rank': ColumnSpec(
        name='org_rank',
        dtype='SmallInteger',
        bounds=(1, 60)
    ),
    'future_value': ColumnSpec(
        name='future_value',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'risk': ColumnSpec(
        name='risk',
        dtype='String(20)'
    ),
    'eta': ColumnSpec(
        name='eta',
        dtype='SmallInteger'
    ),
    # -- position player grades (20-80 scale) --
    'hit_grade': ColumnSpec(
        name='hit_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'game_power_grade': ColumnSpec(
        name='game_power_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'raw_power_grade': ColumnSpec(
        name='raw_power_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'speed_grade': ColumnSpec(
        name='speed_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'field_grade': ColumnSpec(
        name='field_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'arm_grade': ColumnSpec(
        name='arm_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    # -- pitcher grades (20-80 scale) --
    'fb_grade': ColumnSpec(
        name='fb_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'sl_grade': ColumnSpec(
        name='sl_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'cb_grade': ColumnSpec(
        name='cb_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'ch_grade': ColumnSpec(
        name='ch_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'command_grade': ColumnSpec(
        name='command_grade',
        dtype='SmallInteger',
        bounds=(20, 80)
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

DIM_PROSPECT_RANKING_SPEC = TableSpec(
    name='dim_prospect_ranking',
    pk=['player_id', 'season', 'source'],
    columns=DIM_PROSPECT_RANKING_COLS
)
