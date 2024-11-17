SELECT DISTINCT
    {{ get_current_date() }} AS ingestion_date,
    ROW_NUMBER() OVER (ORDER BY Date) AS date_id, 
    Date AS date,
    EXTRACT(DOW FROM Date) AS day_of_week,
    EXTRACT(MONTH FROM Date) AS month,
    EXTRACT(QUARTER FROM Date) AS quarter,
    EXTRACT(YEAR FROM Date) AS year
FROM {{ ref('stg_joined_entries') }}