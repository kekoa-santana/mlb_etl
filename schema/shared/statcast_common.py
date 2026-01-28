from schema.spec_engine import ColumnSpec

COMMON_PITCH_STAGING_COLUMNS: dict[str, ColumnSpec] = {
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable = False,
        primary_key = True
    ),
    'game_counter': ColumnSpec(
        name='game_counter',
        dtype='BigInteger',
        nullable=False,
        bounds=(1, 185),
        original_name='at_bat_number',
        primary_key = True
    ),
    'game_date': ColumnSpec(
        name='game_date',
        dtype='DATE'
    ),
    'pitcher': ColumnSpec(
        name='pitcher',
        dtype='BigInteger',
        nullable=False
    ),
    'batter': ColumnSpec(
        name='batter',
        dtype='BigInteger',
        nullable=False
    ),
}