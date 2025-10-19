{{
    config(
        materialized='table'
    )
}}

with player_team as (
    select *
    from {{ ref('int_team') }}
)

select
    team_name,
    coach_name,
    count(distinct player_id) as total_players,
    ROUND(avg(win_percentage) * 100,2) as win_pct,
    ROUND(avg(player_impact_score),2) as avg_player_impact,
    ROUND(sum(points),2) as avg_total_points,
    ROUND(sum(rebounds),2) as avg_total_rebounds,
    ROUND(sum(assists),2) as avg_total_assists,
    ROUND(sum(steals),2) as avg_total_steals,
    ROUND(sum(blocks),2) as avg_total_blocks,
    ROUND(sum(turnovers),2) as avg_total_turnovers,
    current_timestamp() as loaded_at
from player_team
group by team_name, coach_name
order by avg_player_impact desc
