{{
    config(
        materialized = 'table')
}}

WITH src as (
    select *
    from {{
         ref('int_incremental_player_stats')
    }}
)

select
    player_id,
    team_id,
    team_abbreviation,
    minutes_played,
    win_percentage,
    points,
    assists,
    steals,
    blocks,
    turnovers,
    rebounds,
    case when minutes_played > 0 then points / minutes_played end as points_per_minute,
    case when minutes_played > 0 then rebounds / minutes_played end as rebounds_per_minute,
    case when minutes_played > 0 then assists / minutes_played end as assists_per_minute,
    case when minutes_played > 0 then steals / minutes_played end as steals_per_minute,
    case when minutes_played > 0 then blocks / minutes_played end as blocks_per_minute,
    -- “player impact” efficiency
    case when minutes_played > 0 then 
    (points + rebounds + assists + steals + blocks - turnovers) / minutes_played else null 
    end as player_impact_score,
    loaded_at
from src