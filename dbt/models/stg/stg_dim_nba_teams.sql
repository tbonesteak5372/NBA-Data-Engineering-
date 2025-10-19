{{
    config(
        materialized='view'
    )
}}

with src as (
    select *
    from {{ 
        source(
            'raw', 'dim_nba_teams')
         }}
)

select
    team_id,
    team_abbreviation,
    current_timestamp() as loaded_at
from src
