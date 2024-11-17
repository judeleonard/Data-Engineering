{{ config(tags=['stg_time_logger']) }}

SELECT * FROM {{ source('time_logger', 'stg_time_logger') }}  