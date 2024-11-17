{{ config(tags=['stg_clickups']) }}

SELECT * FROM {{ source('time_logger', 'stg_clickups') }}  