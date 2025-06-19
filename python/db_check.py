import pymysql
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dateutil import parser
import json
import os

def is_valid_datetime(value):
    return value not in ("0000-00-00", "0000-00-00 00:00:00", None, "")

def get_connection():
    host = input("DB 호스트: ")
    user = input("DB 사용자명: ")
    password = input("DB 비밀번호: ")
    db = input("DB 이름: ")
    port = int(input("포트 (기본 3306): ") or "3306")

    conn = pymysql.connect(
        host=host,
        user=user,
        password=password,
        db=db,
        port=port,
        charset='utf8'
    )
    print(f"\n>>> DB 연결 성공: {db}\n")
    return conn

def get_table_list(cursor, db_name):
    cursor.execute(f"SHOW TABLES FROM `{db_name}`")
    return [row[0] for row in cursor.fetchall()]

def get_datetime_column(cursor, db_name, table):
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
    """, (db_name, table))

    datetime_columns = [row[0] for row in cursor.fetchall()
                        if row[1].lower() in ("datetime", "timestamp")]

    if not datetime_columns:
        return None

    priority = ['reg_dt', 'created_at', 'insert_dt', 'log_time']
    for col in priority:
        if col in datetime_columns:
            return col

    return datetime_columns[0]

def get_date_range(cursor, db_name, table, datetime_col):
    try:
        cursor.execute(f"""
            SELECT MIN(`{datetime_col}`), MAX(`{datetime_col}`)
            FROM `{db_name}`.`{table}`
            WHERE `{datetime_col}` IS NOT NULL
        """)
        result = cursor.fetchone()

        if not result or not is_valid_datetime(result[0]) or not is_valid_datetime(result[1]):
            return None, None

        start = parser.parse(result[0]) if isinstance(result[0], str) else result[0]
        end = parser.parse(result[1]) if isinstance(result[1], str) else result[1]
        return start, end

    except Exception as e:
        print(f"쿼리 실패: {table} - {e}")
        return None, None

def get_avg_row_length(cursor, db_name, table):
    cursor.execute("""
        SELECT AVG_ROW_LENGTH
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
    """, (db_name, table))
    result = cursor.fetchone()
    return result[0] if result and result[0] else 0

def date_range_by_unit(start, end, unit):
    ranges = []
    current = start

    while current < end:
        if unit == 'week':
            next_point = current + timedelta(days=7)
            label = f"{current:%Y-W%U}"
        elif unit == 'month':
            next_point = current + relativedelta(months=1)
            label = f"{current:%Y-%m}"
        elif unit == 'year':
            next_point = current + relativedelta(years=1)
            label = f"{current:%Y}"
        else:
            raise ValueError("단위 오류")

        ranges.append((label, current, next_point))
        current = next_point

    return ranges

def estimate_storage(cursor, db_name, table, datetime_col, start, end, avg_len):
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM `{db_name}`.`{table}`
        WHERE `{datetime_col}` >= %s AND `{datetime_col}` < %s
    """, (start, end))
    row_count = cursor.fetchone()[0]
    estimated_mb = row_count * avg_len / 1024 / 1024
    return round(estimated_mb, 2)

def safe_date_format(date_obj):
    return date_obj.strftime('%Y-%m-%d') if hasattr(date_obj, 'strftime') else str(date_obj)

def main():
    conn = get_connection()
    cursor = conn.cursor()
    db_name = conn.db.decode() if isinstance(conn.db, bytes) else conn.db

    tables = get_table_list(cursor, db_name)
    final_result = {}
    skipped = {}

    for table in tables:
        datetime_col = get_datetime_column(cursor, db_name, table)
        if not datetime_col:
            reason = "datetime 컬럼 없음"
            print(f"건너뜀: {table} ({reason})")
            skipped[table] = reason
            continue

        print(f"테이블 분석 중: {table} (기준 컬럼: {datetime_col})")

        start_date, end_date = get_date_range(cursor, db_name, table, datetime_col)
        if not start_date or not end_date:
            reason = "날짜 정보 없음 또는 이상값 존재"
            print(f"건너뜀: {table} ({reason})")
            skipped[table] = reason
            continue

        avg_len = get_avg_row_length(cursor, db_name, table)
        if avg_len == 0:
            reason = "AVG_ROW_LENGTH = 0"
            print(f"건너뜀: {table} ({reason})")
            skipped[table] = reason
            continue

        table_result = {
            "startDate": safe_date_format(start_date),
            "endDate": safe_date_format(end_date),
            "week": {},
            "month": {},
            "year": {}
        }

        for unit in ['week', 'month', 'year']:
            for label, s, e in date_range_by_unit(start_date, end_date, unit):
                mb = estimate_storage(cursor, db_name, table, datetime_col, s, e, avg_len)
                if mb > 0:
                    table_result[unit][label] = f"{mb:,.3f}"

        final_result[table] = table_result

    if skipped:
        final_result["skipped"] = skipped

    cursor.close()
    conn.close()

    output = json.dumps(final_result, indent=2, ensure_ascii=False)

    print("\n결과값:")
    print(output)

    filename = f"db_size_estimate_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(output)

    print(f"\n분석 완료! 결과 파일: {filename}")
    print(f"파일 경로: {os.path.abspath(filename)}")

if __name__ == "__main__":
    main()
