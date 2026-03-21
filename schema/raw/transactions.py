from schema.spec_engine import ColumnSpec, TableSpec

RAW_TRANSACTIONS_COLS: dict[str, ColumnSpec] = {
    'transaction_id': ColumnSpec(
        name='transaction_id',
        dtype='BigInteger',
        nullable=False,
        primary_key=True
    ),
    'player_id': ColumnSpec(
        name='player_id',
        dtype='BigInteger'
    ),
    'player_name': ColumnSpec(
        name='player_name',
        dtype='Text'
    ),
    'to_team_id': ColumnSpec(
        name='to_team_id',
        dtype='Integer'
    ),
    'to_team_name': ColumnSpec(
        name='to_team_name',
        dtype='Text'
    ),
    'from_team_id': ColumnSpec(
        name='from_team_id',
        dtype='Integer'
    ),
    'from_team_name': ColumnSpec(
        name='from_team_name',
        dtype='Text'
    ),
    'date': ColumnSpec(
        name='date',
        dtype='DATE',
        nullable=False
    ),
    'effective_date': ColumnSpec(
        name='effective_date',
        dtype='DATE'
    ),
    'resolution_date': ColumnSpec(
        name='resolution_date',
        dtype='DATE'
    ),
    'type_code': ColumnSpec(
        name='type_code',
        dtype='Text',
        nullable=False
    ),
    'type_desc': ColumnSpec(
        name='type_desc',
        dtype='Text'
    ),
    'description': ColumnSpec(
        name='description',
        dtype='Text'
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
    ),
}

RAW_TRANSACTIONS_SPEC = TableSpec(
    name='transactions',
    pk=['transaction_id'],
    columns=RAW_TRANSACTIONS_COLS
)
