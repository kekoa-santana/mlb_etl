# MLB PITCHING PIPELINE
### Get Started
1. Create Docker container and/or database with raw, staging, and public schemas
2. 


### DIRECTORY STRUCTURE
- dodgers_pitching
    - data
        - statcast_pitching_ld_{start_date}_{end_date}_{runid}.parquet
    - data_ingestion
        - ingest_boxscores.py
        - ingest_statcast.py
    - data_quality
        - one_time
        - specs
            - production
            - staging
                - builders
                    - build_at_bats.py
                - statcast_at_bats.py
                - statcast_batted_balls.py
                - statcast_pitches.py
            - shared
                - helpers.py
                - statcast_common.py
            - spec_engine.py
    - data_transformation
        - production
        - staging
            - load_table.py
            - staging_pitching_boxscore.py
            - transform_and_load.py
            - transform_pitching_boxscore.sql
    - db_ops
        - one_time
            - init_*
    - docs
        - README.md
    - myenv
    - utils
        - statcast_utils.py
        - utils.py