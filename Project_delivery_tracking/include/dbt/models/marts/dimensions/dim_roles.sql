SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY role) AS role_id,
    role AS role_name
FROM {{ ref('stg_float_allocation') }}