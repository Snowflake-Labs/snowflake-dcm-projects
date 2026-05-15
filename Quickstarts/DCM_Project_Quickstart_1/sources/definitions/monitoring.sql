define alert DCM_DEMO_1{{env_suffix}}.SERVE.LOW_INVENTORY
warehouse = 'DCM_WH'
schedule = 'USING CRON 0 9 * * * UTC'
if (exists (
    select 1 
    from 
        DCM_DEMO_1{{env_suffix}}.RAW.INVENTORY
    where 
        IN_STOCK < 10 
        and COUNTED_ON >= CURRENT_DATE() - 1
))
then
    call SYSTEM$SEND_EMAIL(
        'dcm_demo_notification',
        'your_registered_user@company.com',
        'DCM Alert: Low Inventory Detected',
        'One or more items have inventory below threshold. Please review the INVENTORY table.'
    );