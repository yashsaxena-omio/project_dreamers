-- The queries below are used to fetch the data for dreamers-v3


-- This query provides the client IDs for the test and control groups : The result of this was stored in a temp table in sandbox
with consolidated_events AS (
SELECT 
    d.app_adjust_id,
    w.experiment_id,  -- A/B test name 
    c.client_id,
    d.sim_carrier,
    CASE 
        WHEN w.variation_name = 'ctrl' THEN 'Control'
        WHEN w.variation_name = 'on' THEN 'Test'
        ELSE NULL 
        END AS test_assignment, 
    min(derived_tstamp) as first_instance
FROM `snowplow-pipelines.rt_pipeline_prod1.consolidated_events` e 
LEFT JOIN UNNEST (e.com_goeuro_wasabi_context_1) w
LEFT JOIN UNNEST (e.com_goeuro_goeuro_tracking_ids_context_1) c
LEFT JOIN UNNEST (e.com_goeuro_system_versions_context_1) d
WHERE 
    DATE(derived_tstamp) >= "2022-10-25"   
    AND w.experiment_id = 'dreamers-v3'
    AND w.is_active = true -- test must be active 
    AND e.platform = 'app' -- app test 
    -- AND e.dvce_type
GROUP BY 1,2,3,4,5
)
select * from consolidated_events


-- This query references the above stored data from the sandbox and merges with backend funnel to get booking details.
with target_clients as 
(
  SELECT 
distinct client_id, test_assignment
 FROM `omio-dsi.sandbox_yash.dreamers_ab_test` 
 
)
SELECT  
target_clients.client_id,
f.client_id.in_search,
test_assignment,
order_uuid,
date(f.order_created_ts) as order_created_ts
FROM target_clients left join  
`centered-radius-89610.dwh_aggregate.backend_funnel`  f
on  target_clients.client_id = f.client_id.in_search 
where f.client_id.in_search is not null
and f.order_uuid is not null
and length(target_clients.client_id) <> 0 
and length(f.client_id.in_search) <> 0 





