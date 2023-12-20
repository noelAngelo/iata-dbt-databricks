select
    *
from
    event_log({{ ref('brz_airline_trips') }})
where
    event_type = 'flow_progress'
order by
    timestamp
