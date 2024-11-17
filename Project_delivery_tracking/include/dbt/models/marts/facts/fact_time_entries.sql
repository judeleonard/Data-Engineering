   {#
    This queries here might not represent actual transformation as we simulated ids from each data using 
    the windows function. This fact is to demonstrate how al the dimensions table would align togetther here
    for operational purposes to answer more business logic questions.
    #}

WITH base_entries AS (
    SELECT
        s.Client,
        s.Project,
        s.Name,
        s.Task,
        s.Date,
        s.Hours,
        s.Note,
        CASE WHEN s.Billable = 'Yes' THEN TRUE ELSE FALSE END AS billable
    FROM {{ ref('stg_joined_entries') }} s
),
joined_data AS (
    SELECT
        c.client_id,
        p.project_id,
        e.employee_id,
        t.task_id,
        d.date_id,
        base.Hours,
        base.Note,
        base.billable
    FROM base_entries base
    JOIN {{ ref('dim_clients') }} c ON base.Client = c.client_name  
    JOIN {{ ref('dim_projects') }} p ON base.Project = p.project_name
    JOIN {{ ref('dim_employees') }} e ON base.Name = e.employee_name
    JOIN {{ ref('dim_tasks') }} t ON base.Task = t.task_name
    JOIN {{ ref('dim_dates') }} d ON base.Date = d.date
)
SELECT
    {{ get_current_date() }} AS ingestion_date,
    client_id,
    project_id,
    employee_id,
    task_id,
    date_id,
    Hours AS hours,
    Note AS note,
    billable
FROM joined_data
