# redshift_to_postgres.py

import os
import pandas as pd
from sqlalchemy import create_engine, text
from typing import Any, Optional
from dotenv import load_dotenv

# 🔐 .env 로드
load_dotenv()

# ✅ 기본 PostgreSQL 설정 (env 또는 기본값)
PG_CONFIG = {
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "mypassword"),
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB", "mydb")
}


def get_pg_engine(pg_config: dict = PG_CONFIG):
    """SQLAlchemy PostgreSQL 엔진 생성"""
    pg_url = f"postgresql+psycopg2://{pg_config['user']}:{pg_config['password']}@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
    return create_engine(pg_url)


def ensure_pg_schema(conn, schema: str):
    """스키마가 없으면 자동 생성"""
    try:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
        print(f"📁 스키마 확인/생성 완료: {schema}")
    except Exception as e:
        print(f"⚠️ 스키마 생성 실패: {e}")


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
    - 스키마 자동 생성
    - 파티션/전체 삭제 → 저장 → 인덱스 → ANALYZE/VACUUM
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

            # ✅ 스키마 없으면 자동 생성
            ensure_pg_schema(conn, pg_schema)

            # 🧹 기존 데이터 삭제
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

            # 💾 데이터 저장
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

            # ⚡ 인덱스 생성 (선택)
            if index_info and alias in index_info:
                for col in index_info[alias]:
                    idx_name = f"idx_{pg_table}_{col}"
                    try:
                        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {full_table} ({col})"))
                        print(f"⚡ 인덱스 생성: {idx_name}")
                    except Exception as e:
                        print(f"⚠️ 인덱스 실패: {e}")

            # 📊 통계 갱신
            try:
                conn.execute(text(f"ANALYZE {full_table}"))
                conn.execute(text(f"VACUUM ANALYZE {full_table}"))
                print(f"📊 통계/정리 완료: {full_table}")
            except Exception as e:
                print(f"⚠️ ANALYZE/VACUUM 실패: {e}")
