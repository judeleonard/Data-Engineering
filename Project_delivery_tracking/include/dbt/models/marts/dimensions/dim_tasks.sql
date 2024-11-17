SELECT DISTINCT
    {{ get_current_date() }} AS ingestion_date,
    ROW_NUMBER() OVER (ORDER BY Task) AS task_id, 
    Task AS task_name
FROM {{ ref('stg_joined_entries') }}