from schema.spec_engine import ColumnSpec, TableSpec

LANDING_STATCAST_FILES_COLUMNS: dict[str, ColumnSpec] = {
    'run_id': ColumnSpec(
        name='run_id',
        dtype='UUID',
        nullable=False,
        primary_key=True,
        server_default='gen_random_uuid()'
    ),
    'pulled_at': ColumnSpec(
        name='pulled_at',
        dtype='TIMESTAMP(timezone=True)',
        nullable=False,
        server_default='now()'
    ),
    'start_date': ColumnSpec(
        name='start_date',
        dtype='DATE',
        nullable=False
    ),
    'end_date': ColumnSpec(
        name='end_date',
        dtype='DATE',
        nullable=False
    ),
    'row_count': ColumnSpec(
        name='row_count',
        dtype='Integer',
        nullable=False
    ),
    'schema_hash': ColumnSpec(
        name='schema_hash',
        dtype='Text',
        nullable=False
    ),
    'file_path': ColumnSpec(
        name='file_path',
        dtype='Text',
        nullable=False
    ),
    'query_params': ColumnSpec(
        name='query_params',
        dtype='JSONB',
        nullable=False
    ),
}

LANDING_STATCAST_FILES_SPEC = TableSpec(
    name='landing_statcast_files',
    pk=['run_id'],
    columns=LANDING_STATCAST_FILES_COLUMNS
)
