select sale_id from {{ ref('fact_sales') }} where sale_amount < 0 or hg_revenue < 0
