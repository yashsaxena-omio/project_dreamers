-- This query is used to fetch the data for a particular expriment ID (In this case, dreamers-v3)

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
