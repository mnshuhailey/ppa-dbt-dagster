from dagster import job, op, repository, resource
from dagster_dbt import dbt_cli_resource, dbt_run_op
from dagster_airbyte import airbyte_resource, airbyte_sync_op
import pyodbc
import psycopg2

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
@op(required_resource_keys={"sqlserver_db"})
def create_table_if_not_exists(context):
    sqlserver_conn = context.resources.sqlserver_db
    cursor = sqlserver_conn.cursor()

    create_table_query = """
    IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='asnaf_transformed' AND xtype='U')
    CREATE TABLE dbo.asnaf_transformed (
        AsnafID VARCHAR(500),
        AsnafName VARCHAR(500),
        Emel VARCHAR(500),
        Age INT
    );
    """
    cursor.execute(create_table_query)
    sqlserver_conn.commit()
    cursor.close()

# Custom op to transfer data from PostgreSQL to SQL Server
@op(required_resource_keys={"postgres_db", "sqlserver_db"})
def transfer_data_to_sqlserver(context):
    postgres_conn = context.resources.postgres_db
    sqlserver_conn = context.resources.sqlserver_db
    cursor_pg = postgres_conn.cursor()
    cursor_sql = sqlserver_conn.cursor()

    # Log to indicate the start of data reading
    context.log.info("Reading data from PostgreSQL with a LIMIT 10")

    # Read data from PostgreSQL with a LIMIT 10
    cursor_pg.execute("""
        SELECT
            (_airbyte_data->>'vwlzs_asnafId')::text as AsnafID,
            (_airbyte_data->>'vwlzs_AsnafRegistrationIdName')::text as AsnafName,
            (_airbyte_data->>'vwlzs_Email')::text as Emel,
            (_airbyte_data->>'vwlzs_Age')::int as Age
        FROM airbyte_internal.dbo_raw__stream_vwlzs_asnaf
        LIMIT 10
    """)
    rows = cursor_pg.fetchall()

    if not rows:
        context.log.warning("No data fetched from PostgreSQL.")
    else:
        context.log.info(f"Fetched {len(rows)} rows from PostgreSQL.")

    # Log to indicate the start of data insertion
    context.log.info("Inserting data into SQL Server")

    # Insert data into SQL Server
    for row in rows:
        cursor_sql.execute("""
            INSERT INTO dbo.asnaf_transformed (AsnafID, AsnafName, Emel, Age)
            VALUES (?, ?, ?, ?)
        """, row)

    sqlserver_conn.commit()
    cursor_pg.close()
    cursor_sql.close()

    # Log to indicate the end of the operation
    context.log.info("Data transfer to SQL Server completed successfully")

@job(resource_defs={"airbyte": ppa_airbyte_resource, "dbt": dbt, "postgres_db": postgres_db_resource, "sqlserver_db": sqlserver_db_resource})
def ppa_data_pipeline():
    dbt_run_op()
    create_table_if_not_exists()
    transfer_data_to_sqlserver()

@repository
def ppa_repo():
    return [ppa_data_pipeline]
