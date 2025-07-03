from redshift_to_postgres import redshift_to_local_pg_partitioned
import redshift_connector

# Redshift 연결
rs_conn = redshift_connector.connect(
    host="your-redshift-host",
    database="dev",
    user="admin",
    password="secret"
)

query_map = {
    "analytics.sales": "SELECT * FROM prod.sales WHERE sale_date = '2025-07-01'"
}

partition_info = {
    "analytics.sales": ("sale_date", "2025-07-01")
}

index_info = {
    "analytics.sales": ["sale_date"]
}

# 호출
redshift_to_local_pg_partitioned(
    rs_conn=rs_conn,
    query_map=query_map,
    partition_info=partition_info,
    index_info=index_info
)
