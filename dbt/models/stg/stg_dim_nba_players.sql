{{
    config(
        materialized='view'
    )
}}

with src as (
    select *
    from {{ 
        source(
            'raw', 'dim_nba_players') 
        }}
)

select
    player_id,
    player_name as player_name,
    nickname,
    age,
    current_timestamp() as loaded_at
from src
