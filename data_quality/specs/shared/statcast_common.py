from data_quality.specs.spec_engine import ColumnSpec

COMMON_PITCH_STAGING_COLUMNS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='Int64',
        nullable = False
    ),
    'game_counter': ColumnSpec(
        name='game_counter',
        dtype='Int64',
        nullable=False,
        bounds=(1, 185),
        original_name='at_bat_number'
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='datetime'
    ),
    'pitcher': ColumnSpec(
        name='pitcher',
        dtype='Int64',
        nullable=False
    ),
    'batter': ColumnSpec(
        name='batter',
        dtype='Int64',
        nullable=False
    ),
}