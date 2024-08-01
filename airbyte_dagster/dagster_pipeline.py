from dagster import job, repository
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_airbyte import airbyte_resource, airbyte_sync_op

# Airbyte resource configuration
ppa_airbyte_resource = airbyte_resource.configured(
    {
        "host": "192.168.10.176",
        "port": "8000",
        "username": "airbyte",
        "password": "password"
    }
)

sync_ppa_asnaf = airbyte_sync_op.configured(
    {"connection_id": "0ea080d7-e172-4a82-8ae5-ecb691b9ec86"},
    name="sync_ppa_asnaf"
)

# dbt resource configuration
dbt = dbt_cli_resource.configured({
    "project_dir": "/home/mnshuhailey/dev/lzs-ppa/ppa-dbt-dagster/ppa_dbt",
    "profiles_dir": "/home/mnshuhailey/dev/lzs-ppa/ppa-dbt-dagster/ppa_dbt",
})

@job(resource_defs={"airbyte": ppa_airbyte_resource, "dbt": dbt})
def ppa_data_pipeline():
    #sync_ppa_asnaf()
    dbt_run_op()

@repository
def ppa_repo():
    return [ppa_data_pipeline]
