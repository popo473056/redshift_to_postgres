# redshift_to_postgres.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Any, Optional

# ✅ PostgreSQL 접속 기본 설정 (환경변수 또는 하드코딩 가능)
PG_CONFIG = {
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "mypassword"),
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB", "mydb")
}

def get_pg_engine(pg_config: dict = PG_CONFIG):
    """PostgreSQL SQLAlchemy 엔진 생성"""
    pg_url = f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    return create_engine(pg_url)

def redshift_to_local_pg_partitioned(
    rs_conn,
    query_map: dict[str, str],
    table_map: Optional[dict[str, tuple[str, str]]] = None,
    partition_info: Optional[dict[str, tuple[str, Any]]] = None,
    index_info: Optional[dict[str, list[str]]] = None,
    pg_config: dict = PG_CONFIG
):
    """
    Redshift → PostgreSQL 적재 모듈
    - 테이블 자동 매핑
    - 파티션 또는 전체 삭제
    - ANALYZE + VACUUM 필수
    - 인덱스는 선택

    Parameters
    ----------
    rs_conn : redshift_connector.Connection
        Redshift 커넥션

    query_map : dict
        {alias: Redshift 쿼리문}

    table_map : dict
        {alias: (pg_schema, pg_table)} (생략 가능)

    partition_info : dict
        {alias: (column, value)} 기준 삭제

    index_info : dict
        {alias: [column1, column2]} 인덱스 생성용
    """
    pg_engine = get_pg_engine(pg_config)

    with pg_engine.connect() as conn:
        for alias, query in query_map.items():
            print(f"\n📥 Redshift → DataFrame 로딩: {alias}")
            try:
                df = pd.read_sql(query, rs_conn)
            except Exception as e:
                print(f"❌ Redshift 쿼리 실패 ({alias}): {e}")
                continue

            # 테이블명 추출
            if table_map and alias in table_map:
                pg_schema, pg_table = table_map[alias]
            elif "." in alias:
                pg_schema, pg_table = alias.split(".", 1)
            else:
                print(f"❌ 테이블명 추론 실패: {alias}")
                continue
            full_table = f"{pg_schema}.{pg_table}"

            # 삭제
            if partition_info and alias in partition_info:
                col, val = partition_info[alias]
                try:
                    conn.execute(text(f"DELETE FROM {full_table} WHERE {col} = :val"), {"val": val})
                    print(f"🧹 삭제: {full_table} WHERE {col} = '{val}'")
                except Exception as e:
                    print(f"⚠️ 파티션 삭제 실패: {e}")
            else:
                try:
                    conn.execute(text(f"DELETE FROM {full_table}"))
                    print(f"🧹 전체 삭제: {full_table}")
                except Exception as e:
                    print(f"⚠️ 전체 삭제 실패: {e}")

            # 저장
            try:
                df.to_sql(
                    name=pg_table,
                    con=pg_engine,
                    index=False,
                    if_exists="append",
                    method="multi",
                    schema=pg_schema
                )
                print(f"✅ 저장 완료: {full_table} ({len(df)} rows)")
            except Exception as e:
                print(f"❌ 저장 실패: {full_table}: {e}")
                continue

            # 인덱스
            if index_info and alias in index_info:
                for col in index_info[alias]:
                    idx_name = f"idx_{pg_table}_{col}"
                    try:
                        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {full_table} ({col})"))
                        print(f"⚡ 인덱스 생성: {idx_name}")
                    except Exception as e:
                        print(f"⚠️ 인덱스 실패: {e}")

            # ANALYZE + VACUUM
            try:
                conn.execute(text(f"ANALYZE {full_table}"))
                conn.execute(text(f"VACUUM ANALYZE {full_table}"))
                print(f"📊 통계/정리 완료: {full_table}")
            except Exception as e:
                print(f"⚠️ ANALYZE/VACUUM 실패: {e}")
