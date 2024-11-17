
create_stg_float_allocation_table = ("""
    DROP TABLE IF EXISTS staging.stg_float_allocation;
    CREATE TABLE staging.stg_float_allocation(
        client VARCHAR(255),
        project VARCHAR(255),
        role VARCHAR(255),
        name VARCHAR(255),
        task VARCHAR(255),
        start_date DATE,
        end_date DATE,                     
        estimated_hours INTEGER
    );

""")

create_stg_clickup_table = ("""
    DROP TABLE IF EXISTS staging.stg_clickups;
    CREATE TABLE staging.stg_clickups(
        client VARCHAR(255),
        project VARCHAR(255),
        name VARCHAR(255),
        task VARCHAR(255),
        Date DATE,
        hours DECIMAL(5, 2),                   
        note TEXT,
        billable BOOLEAN
    );

""")

# this table below joins both clickup and float data
# to simulate real analytics using dbt

create_stg_time_logger = ("""
    DROP TABLE IF EXISTS staging.stg_time_logger;
    CREATE TABLE staging.stg_time_logger(
        Client VARCHAR(255),
        Project VARCHAR(255),
        Name VARCHAR(255),
        Task VARCHAR(255),
        Date DATE,
        Hours DECIMAL(5, 2),
        Note TEXT,
        Billable VARCHAR(3)
);

""")

drop_staging_tables = ("""
    DROP TABLE IF EXISTS staging.stg_time_logger;
    DROP TABLE IF EXISTS staging.stg_clickups;
    DROP TABLE IF EXISTS staging.stg_float_allocation;   
""")




