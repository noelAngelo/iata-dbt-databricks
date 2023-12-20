select
    timestamp,
    message
from event_log(table({{ ref('gld_geospatial_summary') }}))
where
  event_type = 'planning_information'
order by timestamp