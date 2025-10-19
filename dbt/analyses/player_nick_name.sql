select
    player_id,
    player_name,
    nickname,
    case 
        when nickname is null or trim(nickname) = '' 
        then ' No Nickname'
        else ' Has Nickname'
    end as nickname_flag,
    current_timestamp() as queried_at
from {{ 
    ref('stg_dim_nba_players')
     }}