SELECT
    c.Name,
    f.Role,
    SUM(c.hours) AS Total_Tracked_Hours,
    SUM(f.estimated_hours) AS Total_Allocated_Hours,
    MAX(c.date) AS Latest_Date 
FROM
    staging.stg_clickups c
JOIN
    staging.stg_float_allocation f ON c.Name = f.Name
GROUP BY
    c.Name, f.Role
HAVING
    SUM(c.hours) > 100
ORDER BY
    Total_Allocated_Hours DESC;
