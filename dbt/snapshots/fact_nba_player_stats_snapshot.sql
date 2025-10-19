{% snapshot fact_nba_player_stats_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='player_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

with src as (
    select *
    from {{
        ref('stg_fact_nba_player_stats')
    }}
)

select
    player_id,
    team_id,
    win_percentage,
    minutes_played,
    field_goal_pct,
    three_point_pct,
    free_throw_pct,
    rebounds,
    assists,
    steals,
    blocks,
    turnovers,
    points,
    plus_minus,
    loaded_at
from src

{% endsnapshot %}