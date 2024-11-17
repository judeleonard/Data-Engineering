SELECT DISTINCT
    {{ get_current_date() }} AS ingestion_date,
    ROW_NUMBER() OVER (ORDER BY Client) AS client_id, 
    Client AS client_name
FROM {{ ref('stg_joined_entries') }}