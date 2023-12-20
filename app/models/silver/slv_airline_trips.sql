{{ config(unique_key = 'delay_id')}}

with

    origin_airport_codes as (

        select
            iata_code,
            municipality origin_city,
            name as origin_airport_name,
            elevation_ft::int origin_elevation_ft,
            split(coordinates, ',') as origin_coordinates_array
        from {{ ref("brz_airport_codes") }}

    ),

    dest_airport_codes as (

        select
            iata_code,
            municipality dest_city,
            name as dest_airport_name,
            elevation_ft::int dest_elevation_ft,
            split(coordinates, ',') as dest_coordinates_array
        from {{ ref("brz_airport_codes") }}

    ),

    airline_names as (

        select 
            iata, 
            name as airline_name 
        from {{ ref("brz_airline_codes") }}
    ),

    bronze_trips as (

        select
            *,
            to_date(
                string(int(year * 10000 + month * 100 + dayofmonth)), 'yyyyMMdd'
            ) as arrdate,
            to_timestamp(
                string(
                    bigint(
                        year * 100000000
                        + month * 1000000
                        + dayofmonth * 10000
                        + arrtime
                    )
                ),
                'yyyyMMddHHmm'
            ) as arrtimestamp
        from {{ ref("brz_airline_trips") }}
    ),

    final as (

        select
            {{ dbt_utils.generate_surrogate_key(["ArrTimestamp"]) }} as delay_id,
            actualelapsedtime,
            arrdelay::int,
            crsarrtime,
            crsdeptime,
            crselapsedtime,
            cancelled::int,
            arrdate,
            arrtimestamp,
            dayofweek,
            dayofmonth,
            month,
            year,
            depdelay::int,
            deptime,
            dest,
            distance,
            diverted::int,
            flightnum,
            isarrdelayed,
            isdepdelayed,
            origin,
            uniquecarrier,
            airline_name,
            origin_city,
            origin_airport_name,
            origin_elevation_ft,
            origin_coordinates_array,
            dest_city,
            dest_airport_name,
            dest_elevation_ft,
            dest_coordinates_array,
            audit_file_updated_at
        from bronze_trips raw
        inner join origin_airport_codes on raw.origin = origin_airport_codes.iata_code
        inner join dest_airport_codes on raw.dest = dest_airport_codes.iata_code
        inner join airline_names on raw.uniquecarrier = airline_names.iata
    )

select *
from final

{% if is_incremental() %}
where
  audit_file_updated_at > (select max(audit_file_updated_at) from {{ this }})
{% endif %}