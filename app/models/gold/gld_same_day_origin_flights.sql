select origin_airport_name,
window, count(*) as no_same_day_origin_flights
from
    {{ ref("slv_airline_trips") }}
    watermark arrtimestamp delay of interval 10 seconds airline_trips
group by origin_airport_name,
window (arrtimestamp, "1 day")
