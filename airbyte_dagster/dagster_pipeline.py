from dagster import job, op, repository, resource
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_airbyte import airbyte_resource, airbyte_sync_op
import pyodbc
import psycopg2
import json

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
    return psycopg2.connect(
        host="192.168.10.177",
        port=5432,
        user="postgres",
        password="secret123",
        database="postgres_db",
    )

# SQL Server resource configuration
@resource
def sqlserver_db_resource(context):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=10.10.1.199;"
        "DATABASE=PPA;"
        "UID=noor.shuhailey;"
        "PWD=Lzs.user831;"
        "Trust_Connection=yes;"
    )
    try:
        connection = pyodbc.connect(conn_str)
        return connection
    except pyodbc.Error as ex:
        context.log.error(f"SQL Server connection failed: {ex}")
        raise

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

# Custom op to create the table if it doesn't exist
@op(required_resource_keys={"postgres_db", "sqlserver_db"})
def create_table_if_not_exists(context):
    postgres_conn = context.resources.postgres_db
    sqlserver_conn = context.resources.sqlserver_db
    cursor_pg = postgres_conn.cursor()
    cursor_sql = sqlserver_conn.cursor()

    # Fetch sample data to determine JSON keys
    cursor_pg.execute("""
        SELECT _airbyte_data::text
        FROM airbyte_internal.dbo_raw__stream_vwlzs_asnaf
        LIMIT 1
    """)
    sample_data = cursor_pg.fetchone()
    if not sample_data:
        context.log.error("No sample data available to determine JSON keys.")
        return

    json_data = json.loads(sample_data[0])
    keys = json_data.keys()
    
    # Construct the CREATE TABLE statement
    columns = ", ".join([f"{key} VARCHAR(MAX)" for key in keys])
    create_table_query = f"""
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='original_asnaf' AND xtype='U')
    CREATE TABLE dbo.original_asnaf (
        {columns}
    );
    """
    try:
        cursor_sql.execute(create_table_query)
        sqlserver_conn.commit()
        context.log.info("Table original_asnaf created successfully.")
    except Exception as e:
        context.log.error(f"Failed to create table: {e}")
        raise
    finally:
        cursor_pg.close()
        cursor_sql.close()

# Custom op to transfer data from PostgreSQL to SQL Server
@op(required_resource_keys={"postgres_db", "sqlserver_db"})
def transfer_data_to_sqlserver(context):
    postgres_conn = context.resources.postgres_db
    sqlserver_conn = context.resources.sqlserver_db
    cursor_pg = postgres_conn.cursor()
    cursor_sql = sqlserver_conn.cursor()

    try:
        # Fetch data from PostgreSQL with LIMIT 1000
        cursor_pg.execute("""
            SELECT _airbyte_data::text
            FROM airbyte_internal.dbo_raw__stream_vwlzs_asnaf
            LIMIT 1000
        """)
        rows = cursor_pg.fetchall()

        if not rows:
            context.log.warning("No data fetched from PostgreSQL.")
        else:
            context.log.info(f"Fetched {len(rows)} rows from PostgreSQL.")

        # Insert data into SQL Server
        for row in rows:
            json_data = json.loads(row[0])
            columns = ", ".join(json_data.keys())
            placeholders = ", ".join(["?"] * len(json_data))
            values = tuple(json_data.values())
            
            insert_query = f"""
            INSERT INTO dbo.original_asnaf ({columns})
            VALUES ({placeholders})
            """
            cursor_sql.execute(insert_query, values)

        sqlserver_conn.commit()
        context.log.info("Data transfer to SQL Server completed successfully")
    except Exception as e:
        context.log.error(f"Data transfer failed: {e}")
        raise
    finally:
        cursor_pg.close()
        cursor_sql.close()

# Job definition
@job(resource_defs={"airbyte": ppa_airbyte_resource, "dbt": dbt, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db_resource})
def ppa_data_pipeline():
    # sync_ppa_asnaf() # disable for testing transformed data purpose
    create_table_if_not_exists()
    transfer_data_to_sqlserver()
    dbt_run_op()

# Repository definition
@repository
def ppa_repo():
    return [ppa_data_pipeline]
