-- This query fetches Search and Booking data for all the clients in test and control groups for the selected experiment IDs ( In this case, dreamers-v4 and dreamers-android-v1) 


with consolidated_events AS (
SELECT 
distinct 
    d.app_adjust_id,
    w.experiment_id,  -- A/B test name 
    CASE 
        WHEN w.variation_name = 'ctrl' THEN 'Control'
        WHEN w.variation_name = 'on' THEN 'Test'
        ELSE NULL 
        END AS test_assignment, 
     w.is_active,
    derived_tstamp,
    c.client_id,
    b.booking_id,
    os_family
FROM `snowplow-pipelines.rt_pipeline_prod1.consolidated_events` e 
LEFT JOIN UNNEST (e.com_goeuro_wasabi_context_1) w
LEFT JOIN UNNEST (e.com_goeuro_goeuro_tracking_ids_context_1) c
LEFT JOIN UNNEST (e.com_goeuro_system_versions_context_1) d
left join unnest (e.com_snowplowanalytics_snowplow_mobile_context_1) sp
left join unnest (e.com_goeuro_booking_information_context_1) b
WHERE 
    DATE(derived_tstamp) >= "2022-11-30"   
    AND w.experiment_id in ('dreamers-v4','dreamers-android-v1')
    AND e.platform = 'app' -- app test 
),
dreamers_agg as 
(
select experiment_id,
test_assignment,
app_adjust_id,
client_id,
os_family,
min(derived_tstamp) as first_instance,
count (distinct booking_id) as num_bookings,
from consolidated_events
where client_id is not null and length(client_id)>1
group by 1,2,3,4,5
)
select  dreamers_agg.* ,
-- prev_funnel.session_id,
-- prev_funnel.session_duration_minutes,
funnel.search_id,
funnel.search_ts,
funnel.search_departure_city_name,
funnel.search_departure_country_name,
funnel.search_arrival_city_name,
funnel.search_arrival_country_name,
funnel.order_uuid,
funnel.order_created_ts

from dreamers_agg 
left join `centered-radius-89610.dwh_aggregate.backend_funnel` funnel on dreamers_agg.client_id= funnel.client_id.in_search
-- left join ( select distinct session_id, session_duration_minutes,client_id from  `centered-radius-89610.dwh.fact_funnel` where date(session_first_ts)>="2022-11-30" ) prev_funnel  on dreamers_agg.client_id=prev_funnel.client_id  
where date(funnel.search_ts) >= "2022-11-30"
and funnel.search_id is not null
