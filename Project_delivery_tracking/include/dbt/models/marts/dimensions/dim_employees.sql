SELECT DISTINCT
    {{ get_current_date() }} AS ingestion_date,
    ROW_NUMBER() OVER (ORDER BY Name) AS employee_id,
    Name AS employee_name
FROM {{ ref('stg_joined_entries') }}