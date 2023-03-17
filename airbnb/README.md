## Project Overview
This is an Analytics Engineering project with the sole aim of utilizing dbt to construct an ELT data pipeline to simulate
Airbnb datasets containing informations about the hosts, listings and reviews. 


### Data Modelling

![](https://github.com/judeleonard/Airbnb_analytics/blob/dev/assets/input_schema.png)


### Tech stack and functions
- `DBT`: For data cleaning and transformation
- `Amazon s3`: Cloud storage
- `Snowflakes`: Cloud data warehouse
- `Preset`: BI tool for developing dashboard

### Data Modeling Workflow
![](https://github.com/judeleonard/Airbnb_analytics/blob/dev/assets/Airbnb_Data_Flow.png)

### Workflow Architecture
![](https://github.com/judeleonard/Airbnb_analytics/blob/dev/assets/Airbnb_architecture.png)

### How to run the project

- Create a project folder and cd into this folder

- Run `make setup` on your terminal to setup dbt in your current directory

- Run `make init` to inialize dbt structure for your project   

- Setup the data warehouse, grant the neccessary permissions and connect it to dbt in the profiles.yml in your directory.
 This can be retrieved using the following command 
        `cat ~/.dbt/profiles.yml`

    setting up data warehouse

    ```sql
    -- Use an admin role
    USE ROLE ACCOUNTADMIN;

    -- Create the `transform` role
    CREATE ROLE IF NOT EXISTS transform;
    GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

    -- Create the default warehouse if necessary
    CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
    GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

    -- Create the `dbt` user and assign to role
    CREATE USER IF NOT EXISTS dbt
    PASSWORD=''
    LOGIN_NAME='dbt'
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE='COMPUTE_WH'
    DEFAULT_ROLE='transform'
    DEFAULT_NAMESPACE='AIRBNB.RAW'
    COMMENT='DBT user used for data transformation';
    GRANT ROLE transform to USER dbt;

    -- Create our database and schemas
    CREATE DATABASE IF NOT EXISTS AIRBNB;
    CREATE SCHEMA IF NOT EXISTS AIRBNB.RAW;

    -- Set up permissions to role `transform`
    GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
    GRANT ALL ON DATABASE AIRBNB to ROLE transform;
    GRANT ALL ON ALL SCHEMAS IN DATABASE AIRBNB to ROLE transform;
    GRANT ALL ON FUTURE SCHEMAS IN DATABASE AIRBNB to ROLE transform;
    GRANT ALL ON ALL TABLES IN SCHEMA AIRBNB.RAW to ROLE transform;
    GRANT ALL ON FUTURE TABLES IN SCHEMA AIRBNB.RAW to ROLE transform;

    ```

- Run `make test` to run all the implemented tests including the singular tests, macros custom tests, generic tests and validation using dbt-expectation

- Run `make docs` to generate the project documentation and `make serve` to deploy this documentation to a webserver accessed at port __8080__



### Visualizing the Snowflake data using Preset

Setting up a BI dashboard in Snowflake would require creating a role for this dashboard and granting the neccessary permission to this role,
in our case, Preset role.

You can use the below command to set up this access

```sql
    -- Use an admin role
    USE ROLE ACCOUNTADMIN;
    CREATE ROLE IF NOT EXISTS REPORTER;
    CREATE USER IF NOT EXISTS PRESET
    PASSWORD=''
    LOGIN_NAME='preset'
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE='COMPUTE_WH'
    DEFAULT_ROLE='REPORTER'
    DEFAULT_NAMESPACE='AIRBNB.DEV'
    COMMENT='Preset user for creating reports';
    GRANT ROLE REPORTER TO USER PRESET;
    GRANT ROLE REPORTER TO ROLE ACCOUNTADMIN;
    GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE REPORTER;
    GRANT USAGE ON DATABASE AIRBNB TO ROLE REPORTER;
    GRANT USAGE ON SCHEMA AIRBNB.DEV TO ROLE REPORTER;
    GRANT SELECT ON ALL TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
    GRANT SELECT ON ALL VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
    GRANT SELECT ON FUTURE TABLES IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;
    GRANT SELECT ON FUTURE VIEWS IN SCHEMA AIRBNB.DEV TO ROLE REPORTER;

```

![](https://videoapi-muybridge.vimeocdn.com/animated-thumbnails/image/f4973f3a-b7e2-4de7-bcfe-bef0a5a84edf.gif?ClientID=vimeo-core-prod&Date=1678973621&Signature=bb7365d4fcbf97c83d651e0c784374c445dc3ec0)



### DBT workflow
![](https://github.com/judeleonard/Airbnb_analytics/blob/dev/assets/dbt_workflow.png)





### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

