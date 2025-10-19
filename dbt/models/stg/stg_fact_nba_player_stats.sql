{{
    config(
        materialized='view'
    )
}}


with src as (
    select *
    from {{ 
        source(
            'raw', 'fact_nba_player_stats')
         }}
)

select
    player_id,
    team_id,
    w_pct as win_percentage,
    min as minutes_played,
    fg_pct as field_goal_pct,
    fg3_pct as three_point_pct,
    ft_pct as free_throw_pct,
    reb as rebounds,
    ast as assists,
    stl as steals,
    blk as blocks,
    tov as turnovers,
    pts as points,
    plus_minus,
    loaded_at
from src