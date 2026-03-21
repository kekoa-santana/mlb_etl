from schema.spec_engine import TableSpec, ColumnSpec

# ---------------------------------------------------------------------------
# production.dim_schedule
# Grain: game_pk
# Full season schedule including future unplayed games.
# Used for schedule strength calculations and upcoming opponent analysis.
# Rows with status 'Final' overlap with dim_game; unplayed games have
# status 'Scheduled' or 'Pre-Game'.
# ---------------------------------------------------------------------------
DIM_SCHEDULE_COLS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE',
        nullable=False
    ),
    'season': ColumnSpec(
        name='season',
        dtype='SmallInteger',
        nullable=False
    ),
    'game_type': ColumnSpec(
        name='game_type',
        dtype='String(1)'
    ),
    'status': ColumnSpec(
        name='status',
        dtype='String(20)'
    ),
    'away_team_id': ColumnSpec(
        name='away_team_id',
        dtype='Integer',
        nullable=False
    ),
    'home_team_id': ColumnSpec(
        name='home_team_id',
        dtype='Integer',
        nullable=False
    ),
    'away_team_name': ColumnSpec(
        name='away_team_name',
        dtype='Text'
    ),
    'home_team_name': ColumnSpec(
        name='home_team_name',
        dtype='Text'
    ),
    'venue_id': ColumnSpec(
        name='venue_id',
        dtype='Integer'
    ),
    'venue_name': ColumnSpec(
        name='venue_name',
        dtype='Text'
    ),
    'day_night': ColumnSpec(
        name='day_night',
        dtype='String(5)'
    ),
    'series_description': ColumnSpec(
        name='series_description',
        dtype='Text'
    ),
    'away_probable_pitcher_id': ColumnSpec(
        name='away_probable_pitcher_id',
        dtype='BigInteger'
    ),
    'home_probable_pitcher_id': ColumnSpec(
        name='home_probable_pitcher_id',
        dtype='BigInteger'
    ),
    'created_at': ColumnSpec(
        name='created_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    )
}

DIM_SCHEDULE_SPEC = TableSpec(
    name='dim_schedule',
    pk=['game_pk'],
    columns=DIM_SCHEDULE_COLS
)
