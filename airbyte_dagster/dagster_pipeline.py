from dagster import job, repository, resource
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_airbyte import airbyte_resource, airbyte_sync_op
from dagster_mssql import mssql_resource
from dagster_postgres import postgres_db_resource 

# Airbyte resource configuration
ppa_airbyte_resource = airbyte_resource.configured(
    {
        "host": "192.168.10.176",
        "port": "8000",
        "username": "airbyte",
        "password": "password"
    }
)

# PostgreSQL resource configuration
@resource
def postgres_db_resource(context):
    return {
        "host": "192.168.10.177",
        "port": 5432,
        "username": "postgres",
        "password": "secret123",
        "database": "postgres_db",
    }

# SQL Server resource configuration
sqlserver_db = mssql_resource.configured({
    "server": "10.10.1.199",
    "port": 1433,
    "username": "noor.shuhailey",
    "password": "Lzs.user831",
    "database": "PPA",
    "schema": "dbo",
    "trust_cert": "true"
})

# Airbyte sync operation
sync_ppa_asnaf = airbyte_sync_op.configured(
    {"connection_id": "0ea080d7-e172-4a82-8ae5-ecb691b9ec86"},
    name="sync_ppa_asnaf"
)

# dbt resource configuration
dbt = dbt_cli_resource.configured({
    "project_dir": "/home/shuhailey/lzs-ppa/ppa-dbt-dagster/ppa_dbt",
    "profiles_dir": "/home/shuhailey/lzs-ppa/ppa-dbt-dagster/ppa_dbt",
})

@job(resource_defs={"airbyte": ppa_airbyte_resource, "dbt": dbt, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db})
def ppa_data_pipeline():
    sync_ppa_asnaf()  # Uncomment if you want to run the Airbyte sync operation
    dbt_run_op()

@repository
def ppa_repo():
    return [ppa_data_pipeline]
