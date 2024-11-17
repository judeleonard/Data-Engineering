SELECT DISTINCT
    {{ get_current_date() }} AS ingestion_date,
    ROW_NUMBER() OVER (ORDER BY Project) AS project_id,
    Project AS project_name,
    c.client_id
FROM {{ ref('stg_joined_entries') }} s
JOIN {{ ref('dim_clients') }} AS c ON s.Client = c.client_name