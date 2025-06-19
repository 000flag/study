import pymysql
import json

def get_connection():
    host = input("DB 호스트: ")
    user = input("DB 사용자명: ")
    password = input("DB 비밀번호: ")
    db = input("DB 이름: ")
    port = int(input("포트 (기본 3306): ") or "3306")

    return pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8'
    )

def get_datetime_columns():
    conn = get_connection()
    cursor = conn.cursor()
    db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db

    cursor.execute(f"SHOW TABLES FROM `{db_name}`")
    tables = [row[0] for row in cursor.fetchall()]

    result = {}

    for table in tables:
        cursor.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
        """, (db_name, table))

        datetime_cols = [
            row[0] for row in cursor.fetchall()
            if row[1].lower() in ("datetime", "timestamp")
        ]

        result[table] = datetime_cols

    cursor.close()
    conn.close()

    print(json.dumps(result, indent=2, ensure_ascii=False))

if __name__ == "__main__":
    get_datetime_columns()
