{{
    config(
        materialized='table'
    )
}}

with player_stats as (
    select *
    from {{ 
        ref('int_incremental_player_stats')
         }}
),
 permin as (
    select *
    from {{
         ref('int_stats_per_min')
    }}
),
team_lookup as (
    select *
    from {{
         ref('team_name')
          }}
)

select
    ps.player_id,
    ps.team_id,
    ps.team_abbreviation,
    td.team_name,
    ps.coach_name,
    ps.minutes_played,
    ps.win_percentage,
    ps.points,
    ps.assists,
    ps.steals,
    ps.blocks,
    ps.turnovers,
    ps.rebounds,
    per.player_impact_score,
    ps.loaded_at
from player_stats ps
left join team_lookup td
    on ps.team_abbreviation = td.team_abbreviation
left join permin per 
    on per.player_id = ps.player_id 
