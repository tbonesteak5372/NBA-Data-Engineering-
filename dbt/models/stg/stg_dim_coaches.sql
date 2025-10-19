{{
    config(
        materialized='view'
    )
}}

with src as (
    select *
    from {{ 
        source(
            'raw', 'dim_coaches')
         }}
)

select
    team_id,
    coach_id,
    concat(first_name, ' ', last_name) as coach_name,
    current_timestamp() as loaded_at
from src
