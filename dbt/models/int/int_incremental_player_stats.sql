{{
    config(
        materialized = 'incremental',
        unique_key = ['player_id', 'team_id', 'coach_id'],
        incremental_strategy = 'merge',
        on_schema_change='sync_all_columns'

    )
}}

with coaches as (
    select *
from {{
    ref('stg_dim_coaches')
}}
),
teams as (
    select *
    from {{
        ref('stg_dim_nba_teams')
    }}
),
players as (
    select *
    from {{
        ref('stg_dim_nba_players')
    }}
),
player_stats as (
    select *
    from {{
        ref('stg_fact_nba_player_stats')
    }}
)

select
    ps.player_id,
    p.player_name,
    ps.team_id,
    t.team_abbreviation,
    c.coach_id,
    c.coach_name,
    ps.win_percentage,
    ps.minutes_played,
    ps.field_goal_pct,
    ps.three_point_pct,
    ps.free_throw_pct,
    ps.rebounds,
    ps.assists,
    ps.steals,
    ps.blocks,
    ps.turnovers,
    ps.points,
    ps.plus_minus,
    ps.loaded_at
from player_stats ps
left join players p 
    on ps.player_id = p.player_id
left join teams t 
    on ps.team_id = t.team_id
left join coaches c 
    on ps.team_id = c.team_id

{% if is_incremental() %}

where ps.loaded_at > (select max(loaded_at) from {{ this }})

{% endif %}