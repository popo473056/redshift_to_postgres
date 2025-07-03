# redshift_to_postgres

Redshift에서 데이터를 추출하여 **로컬 PostgreSQL에 저장**하고,  
**삭제 조건, 인덱스 생성, 통계 분석(ANALYZE + VACUUM)**까지 자동 수행하는 유틸리티입니다.  
`.env` 파일을 통해 접속 정보를 안전하게 관리할 수 있습니다.

---

## 📦 주요 기능

- ✅ Redshift 쿼리 실행 후 로컬 PostgreSQL에 저장
- ✅ 저장 전 삭제 처리 (전체 또는 특정 파티션 기준)
- ✅ 저장 후 VACUUM / ANALYZE 자동 수행
- ✅ 인덱스는 선택적으로 자동 생성
- ✅ 접속 정보는 `.env`로 관리

---

## 🏗️ 설치 및 로컬 DB 설정

### 1️⃣ PostgreSQL 설치 (macOS 기준)

```bash
brew install postgresql
brew services start postgresql  # 항상 켜짐
```

> macOS가 아니면 [공식 설치 가이드](https://www.postgresql.org/download/) 참고

### 2️⃣ DB 및 스키마 생성

```bash
createdb mydb
psql -U postgres -d mydb -c "CREATE SCHEMA IF NOT EXISTS analytics;"
```

---

## 📁 폴더 구조

```
redshift_to_postgres/
│
├── redshift_to_postgres.py      # 핵심 모듈
├── main.py                      # 사용 예시
├── init_postgres.ipynb          # PostgreSQL 초기 설정용 노트북
├── .env                         # 접속 정보 저장
└── README.md                    # 이 파일
```

---

## 🔐 .env 예시

`.env` 파일을 프로젝트 루트에 생성하고 다음 내용을 입력하세요:

```env
# PostgreSQL 접속 정보
PG_USER=postgres
PG_PASSWORD=mypassword
PG_HOST=localhost
PG_PORT=5432
PG_DB=mydb

# Redshift 접속 정보
REDSHIFT_HOST=your-redshift-cluster.redshift.amazonaws.com
REDSHIFT_DB=dev
REDSHIFT_USER=admin
REDSHIFT_PASSWORD=yourpassword
```

> ✅ `.gitignore`에 `.env`를 반드시 추가하세요

```
.env
```

---

## 💻 코드에서 .env 불러오기

```python
from dotenv import load_dotenv
import os

load_dotenv()  # .env 파일 로딩

# PostgreSQL
PG_CONFIG = {
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB")
}

# Redshift
import redshift_connector
RS_CONN = redshift_connector.connect(
    host=os.getenv("REDSHIFT_HOST"),
    database=os.getenv("REDSHIFT_DB"),
    user=os.getenv("REDSHIFT_USER"),
    password=os.getenv("REDSHIFT_PASSWORD")
)
```

---

## 🚀 사용법 예시 (`main.py`)

```python
from redshift_to_postgres import redshift_to_local_pg_partitioned
from dotenv import load_dotenv
import os
import redshift_connector

# .env 로드
load_dotenv()

# Redshift 커넥션
# rs_conn = redshift_connector.connect(
#     host=os.getenv("REDSHIFT_HOST"),
#     database=os.getenv("REDSHIFT_DB"),
#     user=os.getenv("REDSHIFT_USER"),
#     password=os.getenv("REDSHIFT_PASSWORD")
# )

📌 Redshift 커넥션은 이미 셋팅된 `rs_conn` 객체를 직접 주입합니다.


# PostgreSQL 설정
pg_config = {
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "host": os.getenv("PG_HOST"),
    "port": int(os.getenv("PG_PORT", 5432)),
    "database": os.getenv("PG_DB")
}

# 쿼리 정의
query_map = {
    "analytics.sales": "SELECT * FROM prod.sales WHERE sale_date = '2025-07-01'"
}

# 삭제 조건 (선택)
partition_info = {
    "analytics.sales": ("sale_date", "2025-07-01")
}

# 인덱스 (선택)
index_info = {
    "analytics.sales": ["sale_date"]
}

# 실행
redshift_to_local_pg_partitioned(
    rs_conn=rs_conn,
    query_map=query_map,
    partition_info=partition_info,
    index_info=index_info,
    pg_config=pg_config
)
```

---

## 🧪 PostgreSQL 초기 설정 (Jupyter)

`init_postgres.ipynb` 파일에 포함되어 있습니다. 예:

```python
import psycopg2

conn = psycopg2.connect(host="localhost", user="postgres", password="mypassword", dbname="postgres")
conn.autocommit = True
cur = conn.cursor()

cur.execute("CREATE DATABASE mydb")
cur.close()
conn.close()
```

---

## 🧰 설치 의존성

```bash
pip install pandas sqlalchemy psycopg2-binary redshift-connector python-dotenv
```

---

## ✨ 향후 개선 아이디어

- WHERE 절 자동 파싱 후 파티션 추론
- 병렬 적재 (multi-threading)
- Airflow / cron 연동

---

## 📮 문의

깃허브 Issue 또는 PR 환영합니다.
