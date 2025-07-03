# main.py

from redshift_to_postgres import redshift_to_local_pg_partitioned, PG_CONFIG
from dotenv import load_dotenv
import os
import redshift_connector

# ✅ .env 파일 로드
load_dotenv()

# ✅ Redshift 커넥션 (직접 셋팅된 로컬 Redshift 사용자가 연결)
rs_conn = redshift_connector.connect(
    host=os.getenv("REDSHIFT_HOST"),
    database=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD")
)

# ✅ 쿼리 정의: alias → SELECT 쿼리
query_map = {
    "analytics.sales": """
        SELECT *
        FROM prod.sales
        WHERE sale_date = '2025-07-01'
    """,
    "analytics.users": """
        SELECT *
        FROM raw.users
        WHERE reg_date = '2025-01-01'
    """
}

# ✅ (선택) 삭제 기준: alias → (컬럼명, 값)
partition_info = {
    "analytics.sales": ("sale_date", "2025-07-01"),
    "analytics.users": ("reg_date", "2025-01-01")
}

# ✅ (선택) 인덱스 생성 대상: alias → 컬럼 리스트
index_info = {
    "analytics.sales": ["sale_date", "product_id"],
    "analytics.users": ["reg_date", "user_id"]
}

# ✅ 실행
redshift_to_local_pg_partitioned(
    rs_conn=rs_conn,
    query_map=query_map,
    partition_info=partition_info,
    index_info=index_info,
    pg_config=PG_CONFIG
)
