{{ config(tags=['stg_float_allocation']) }}

SELECT * FROM {{ source('time_logger', 'stg_float_allocation') }}  