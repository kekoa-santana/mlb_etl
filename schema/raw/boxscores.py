from schema.spec_engine import ColumnSpec, TableSpec

PITCHING_BOXSCORES_COLUMNS: dict[str, ColumnSpec] = {
    'row_num': ColumnSpec(
        name='row_num',
        dtype='Integer'
    ),
    'pitcher_id': ColumnSpec(
        name='pitcher_id',
        dtype='BigInteger',
        nullable=False
    ),
    'pitcher_name': ColumnSpec(
        name='pitcher_name',
        dtype='Text'
    ),
    'team_id': ColumnSpec(
        name='team_id',
        dtype='BigInteger',
        nullable=False
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False
    ),
    'team_name': ColumnSpec(
        name='team_name',
        dtype='Text'
    ),
    'is_starter_text': ColumnSpec(
        name='is_starter_text',
        dtype='Text'
    ),
    'fly_outs_text': ColumnSpec(
        name='fly_outs_text',
        dtype='Text'
    ),
    'ground_outs_text': ColumnSpec(
        name='ground_outs_text',
        dtype='Text'
    ),
    'air_outs_text': ColumnSpec(
        name='air_outs_text',
        dtype='Text'
    ),
    'runs_text': ColumnSpec(
        name='runs_text',
        dtype='Text'
    ),
    'doubles_text': ColumnSpec(
        name='doubles_text',
        dtype='Text'
    ),
    'triples_text': ColumnSpec(
        name='triples_text',
        dtype='Text'
    ),
    'home_runs_text': ColumnSpec(
        name='home_runs_text',
        dtype='Text'
    ),
    'strike_outs_text': ColumnSpec(
        name='strike_outs_text',
        dtype='Text'
    ),
    'walks_text': ColumnSpec(
        name='walks_text',
        dtype='Text'
    ),
    'intentional_walks_text': ColumnSpec(
        name='intentional_walks_text',
        dtype='Text'
    ),
    'hits_text': ColumnSpec(
        name='hits_text',
        dtype='Text'
    ),
    'hit_by_pitch_text': ColumnSpec(
        name='hit_by_pitch_text',
        dtype='Text'
    ),
    'at_bats_text': ColumnSpec(
        name='at_bats_text',
        dtype='Text'
    ),
    'caught_stealing_text': ColumnSpec(
        name='caught_stealing_text',
        dtype='Text'
    ),
    'stolen_bases_text': ColumnSpec(
        name='stolen_bases_text',
        dtype='Text'
    ),
    'stolen_base_percentage_text': ColumnSpec(
        name='stolen_base_percentage_text',
        dtype='Text'
    ),
    'number_of_pitches_text': ColumnSpec(
        name='number_of_pitches_text',
        dtype='Text'
    ),
    'innings_pitched_text': ColumnSpec(
        name='innings_pitched_text',
        dtype='Text'
    ),
    'wins_text': ColumnSpec(
        name='wins_text',
        dtype='Text'
    ),
    'losses_text': ColumnSpec(
        name='losses_text',
        dtype='Text'
    ),
    'saves_text': ColumnSpec(
        name='saves_text',
        dtype='Text'
    ),
    'save_opportunities_text': ColumnSpec(
        name='save_opportunities_text',
        dtype='Text'
    ),
    'holds_text': ColumnSpec(
        name='holds_text',
        dtype='Text'
    ),
    'blown_saves_text': ColumnSpec(
        name='blown_saves_text',
        dtype='Text'
    ),
    'earned_runs_text': ColumnSpec(
        name='earned_runs_text',
        dtype='Text'
    ),
    'batters_faced_text': ColumnSpec(
        name='batters_faced_text',
        dtype='Text'
    ),
    'outs_text': ColumnSpec(
        name='outs_text',
        dtype='Text'
    ),
    'complete_game_text': ColumnSpec(
        name='complete_game_text',
        dtype='Text'
    ),
    'shutout_text': ColumnSpec(
        name='shutout_text',
        dtype='Text'
    ),
    'pitches_thrown_text': ColumnSpec(
        name='pitches_thrown_text',
        dtype='Text'
    ),
    'balls_text': ColumnSpec(
        name='balls_text',
        dtype='Text'
    ),
    'strikes_text': ColumnSpec(
        name='strikes_text',
        dtype='Text'
    ),
    'strike_percentage_text': ColumnSpec(
        name='strike_percentage_text',
        dtype='Text'
    ),
    'hit_batsmen_text': ColumnSpec(
        name='hit_batsmen_text',
        dtype='Text'
    ),
    'balks_text': ColumnSpec(
        name='balks_text',
        dtype='Text'
    ),
    'wild_pitches_text': ColumnSpec(
        name='wild_pitches_text',
        dtype='Text'
    ),
    'pickoffs_text': ColumnSpec(
        name='pickoffs_text',
        dtype='Text'
    ),
    'rbi_text': ColumnSpec(
        name='rbi_text',
        dtype='Text'
    ),
    'games_finished_text': ColumnSpec(
        name='games_finished_text',
        dtype='Text'
    ),
    'runs_scored_per_9_text': ColumnSpec(
        name='runs_scored_per_9_text',
        dtype='Text'
    ),
    'home_runs_per_9_text': ColumnSpec(
        name='home_runs_per_9_text',
        dtype='Text'
    ),
    'inherited_runners_text': ColumnSpec(
        name='inherited_runners_text',
        dtype='Text'
    ),
    'inherited_runners_scored_text': ColumnSpec(
        name='inherited_runners_scored_text',
        dtype='Text'
    ),
    'catchers_interference_text': ColumnSpec(
        name='catchers_interference_text',
        dtype='Text'
    ),
    'sac_bunts_text': ColumnSpec(
        name='sac_bunts_text',
        dtype='Text'
    ),
    'sac_flies_text': ColumnSpec(
        name='sac_flies_text',
        dtype='Text'
    ),
    'passed_ball_text': ColumnSpec(
        name='passed_ball_text',
        dtype='Text'
    ),
    'pop_outs_text': ColumnSpec(
        name='pop_outs_text',
        dtype='Text'
    ),
    'line_outs_text': ColumnSpec(
        name='line_outs_text',
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

PITCHING_BOXSCORES_SPEC = TableSpec(
    name='pitching_boxscores',
    pk=['pitcher_id', 'team_id', 'game_pk'],
    columns=PITCHING_BOXSCORES_COLUMNS
)


LANDING_BOXSCORES_COLUMNS: dict[str, ColumnSpec] = {
    'load_id': ColumnSpec(
        name='load_id',
        dtype='UUID',
        server_default='gen_random_uuid()',
        primary_key=True
    ),
    'ingested_at': ColumnSpec(
        name='ingested_at',
        dtype='TIMESTAMP(timezone=True)',
        server_default='now()'
    ),
    'source': ColumnSpec(
        name='source',
        dtype='Text',
        nullable=False
    ),
    'game_pk': ColumnSpec(
        name='game_pk',
        dtype='BigInteger',
        nullable=False
    ),
    'payload': ColumnSpec(
        name='payload',
        dtype='JSONB',
        nullable=False
    ),
}

LANDING_BOXSCORES_SPEC = TableSpec(
    name='landing_boxscores',
    pk=['load_id'],
    columns=LANDING_BOXSCORES_COLUMNS
)