{{
    config(
        tblproperties={"delta.enableChangeDataFeed":"true"},
        zorder="airline_name"
    )
}}

    SELECT 
        airline_name
        ,ArrDate
        ,COUNT(*) AS no_flights
        ,SUM(IF(IsArrDelayed = TRUE,1,0)) AS tot_delayed
        ,ROUND(tot_delayed*100/no_flights,2) AS perc_delayed
        FROM {{ ref('slv_airline_trips') }}
        WHERE airline_name IS NOT NULL
        GROUP BY 1,2